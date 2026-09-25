package slogseq

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// messages returns the @m field of each event.
func messages(events []map[string]any) []any {
	var out []any
	for _, e := range events {
		out = append(out, e["@m"])
	}
	return out
}

func TestRunBackgroundFlusher_FlushOnBatchSize(t *testing.T) {
	srv := newSeqServer(t, nil)
	// A long interval, so only reaching the batch size triggers a flush.
	_, handler := newServerLogger(t, srv, WithBatchSize(2), WithFlushInterval(time.Hour))

	handler.HandleCLEFEvent(CLEFEvent{Message: "event1", Timestamp: time.Now()})
	handler.HandleCLEFEvent(CLEFEvent{Message: "event2", Timestamp: time.Now()})

	require.Eventually(t, func() bool { return srv.Requests() == 1 }, time.Second, 5*time.Millisecond,
		"expected a flush once the batch was full")
	require.NoError(t, handler.Close())

	assert.Equal(t, 1, srv.Requests(), "Close should have nothing left to send")
	assert.Equal(t, []any{"event1", "event2"}, messages(srv.Events()))
	assert.Empty(t, handler.workers[0].retryBuffer)
}

func TestRunBackgroundFlusher_FlushOnInterval(t *testing.T) {
	srv := newSeqServer(t, nil)
	// A large batch size, so only the interval triggers a flush.
	_, handler := newServerLogger(t, srv, WithBatchSize(10), WithFlushInterval(20*time.Millisecond))

	handler.HandleCLEFEvent(CLEFEvent{Message: "event1", Timestamp: time.Now()})

	require.Eventually(t, func() bool { return srv.Requests() == 1 }, time.Second, 5*time.Millisecond,
		"expected a flush after the interval")
	require.NoError(t, handler.Close())

	assert.Equal(t, []any{"event1"}, messages(srv.Events()))
	assert.Empty(t, handler.workers[0].retryBuffer)
}

func TestRunBackgroundFlusher_RetryOnFailure(t *testing.T) {
	// The first request fails, the rest succeed.
	srv := newSeqServer(t, func(n int) int {
		if n == 1 {
			return http.StatusInternalServerError
		}
		return http.StatusCreated
	})
	var errs []error
	_, handler := newServerLogger(t, srv,
		WithBatchSize(2),
		WithFlushInterval(time.Hour),
		WithErrorHandlerFunc(func(err error) { errs = append(errs, err) }),
	)

	// The first batch fails and goes to the retry buffer.
	handler.HandleCLEFEvent(CLEFEvent{Message: "first1", Timestamp: time.Now()})
	handler.HandleCLEFEvent(CLEFEvent{Message: "first2", Timestamp: time.Now()})
	require.Eventually(t, func() bool { return srv.Requests() == 1 }, time.Second, 5*time.Millisecond)

	// The next flush resends the retry buffer before sending the new batch.
	handler.HandleCLEFEvent(CLEFEvent{Message: "second1", Timestamp: time.Now()})
	handler.HandleCLEFEvent(CLEFEvent{Message: "second2", Timestamp: time.Now()})
	require.Eventually(t, func() bool { return srv.Requests() == 3 }, time.Second, 5*time.Millisecond)
	require.NoError(t, handler.Close())

	assert.Equal(t, 3, srv.Requests())
	assert.Equal(t, []any{"first1", "first2", "second1", "second2"}, messages(srv.Events()))
	assert.Empty(t, handler.workers[0].retryBuffer)
	require.Len(t, errs, 1)
	assert.ErrorContains(t, errs[0], "500")
}

func TestPurgeOldEvents(t *testing.T) {
	// Directly test purgeOldEvents, ensuring that events older than a certain cutoff are removed.
	now := time.Now()

	// Some events older than 5 minutes, some newer
	oldEvent := CLEFEvent{Message: "old", Timestamp: now.Add(-10 * time.Minute)}
	newEvent := CLEFEvent{Message: "new", Timestamp: now.Add(-1 * time.Minute)}

	handler := &SeqHandler{
		workers: []worker{{retryBuffer: []CLEFEvent{oldEvent, newEvent}}},
		errorHandlerFunc: func(err error) {
			return
		},
	}
	w := &handler.workers[0]

	cutoff := now.Add(-5 * time.Minute)
	handler.purgeOldEvents(w, cutoff)

	// We expect only the new event to remain (the old one is older than cutoff).
	require.Len(t, w.retryBuffer, 1, "expected only one event left in retryBuffer")
	assert.Equal(t, "new", w.retryBuffer[0].Message)
}

func TestEncodeEvent_ClefFields(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	out := encodeEvent(CLEFEvent{
		Timestamp: ts,
		Message:   "real",
		Level:     CLEFLevelInformation.String(),
		Properties: map[string]any{
			"@m":    "x",
			"@l":    "Fatal",
			"@@t":   "already escaped",
			"plain": 1,
		},
	})
	assert.Equal(t, ts.Format(time.RFC3339Nano), out["@t"])
	assert.Equal(t, "real", out["@m"])
	assert.Equal(t, "Information", out["@l"])
	assert.Equal(t, "x", out["@@m"])
	assert.Equal(t, "Fatal", out["@@l"])
	assert.Equal(t, "already escaped", out["@@@t"])
	assert.Equal(t, 1, out["plain"])

	// Zero time and empty level are left out.
	out = encodeEvent(CLEFEvent{Message: "span event"})
	assert.NotContains(t, out, "@t")
	assert.NotContains(t, out, "@l")
	assert.Equal(t, "span event", out["@m"])
}
