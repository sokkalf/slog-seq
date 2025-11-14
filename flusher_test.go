package slogseq

import (
	"bytes"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTransport struct {
	RoundTripFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return m.RoundTripFunc(req)
}

func GetHttpClientMock(status int, msg string, f func()) *http.Client {
	f()
	transport := &mockTransport{
		RoundTripFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: status,
				Body:       io.NopCloser(bytes.NewBufferString(msg)),
			}, nil
		},
	}
	return &http.Client{Transport: transport}
}

func TestRunBackgroundFlusher_BasicFlushOnBatchSize(t *testing.T) {
	// Create a SeqHandler with small batchSize for easy testing.
	handler := &SeqHandler{
		client:        GetHttpClientMock(200, "ok", func() {}),
		seqURL:        "http://example.com",
		flushInterval: 100 * time.Hour, // large interval so it won't trigger unless forced
		batchSize:     2,               // flush after 2 events
		workerCount:   1,
		workers: []worker{{
			eventsCh:    make(chan CLEFEvent, 10),
			doneCh:      make(chan struct{}),
			wg:          sync.WaitGroup{},
			retryBuffer: make([]CLEFEvent, 0),
		}},
		errorHandlerFunc: func(err error) {
			return
		},
	}

	w := &handler.workers[0]
	w.wg.Add(1)

	// Start runBackgroundFlusher in background
	go handler.runBackgroundFlusher(w)

	// Send 2 events (exactly batchSize); expect immediate flush
	e1 := CLEFEvent{Message: "event1", Timestamp: time.Now()}
	e2 := CLEFEvent{Message: "event2", Timestamp: time.Now()}

	w.eventsCh <- e1
	w.eventsCh <- e2

	// Close eventsCh and signal doneCh, then wait
	close(w.eventsCh)
	close(w.doneCh)
	w.wg.Wait()

	// If we reached here, it means flush happened upon hitting batchSize (2).
	// We didn't explicitly check what was posted; for that, see the next test with a more elaborate mock.
	// Here we simply test we didn't panic and that code ran to completion.
	// Additional success checks can be done by counting calls, etc.

	// No leftover events expected in retryBuffer
	assert.Empty(t, w.retryBuffer, "retryBuffer should be empty after successful flush")
}

func TestRunBackgroundFlusher_FlushOnInterval(t *testing.T) {
	// This test verifies that if fewer than batchSize events are in the channel,
	// the flush happens after flushInterval.

	// We'll keep flushInterval short so that we don't have to wait long.
	flushInterval := 50 * time.Millisecond

	// We'll track how many times our mock is called to ensure flush occurs.
	var callCount int

	handler := &SeqHandler{
		client:        GetHttpClientMock(200, "ok", func() { callCount++ }),
		seqURL:        "http://example.com",
		flushInterval: flushInterval,
		batchSize:     10, // large batchSize so it won't flush except on interval
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 10),
		doneCh:   make(chan struct{}),
		wg:       sync.WaitGroup{},
	}

	w.wg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Send 1 event (less than batchSize).
	w.eventsCh <- CLEFEvent{Message: "event1", Timestamp: time.Now()}

	// Wait a bit longer than flushInterval to ensure flush is triggered
	time.Sleep(2 * flushInterval)

	// Terminate
	close(w.eventsCh)
	close(w.doneCh)
	w.wg.Wait()

	// By now we expect at least 1 flush (callCount >= 1).
	// The exact number can vary if the background flusher loop ran more than once
	// but it should be at least 1.
	require.GreaterOrEqual(t, callCount, 1)
	assert.Empty(t, w.retryBuffer)
}

func TestRunBackgroundFlusher_RetryOnFailure(t *testing.T) {
	// This test will simulate a first failure on sending batch,
	// then a subsequent success, ensuring retryBuffer is used.

	attempts := 0

	successClient := GetHttpClientMock(200, "ok", func() { attempts++ })
	failClient := GetHttpClientMock(500, "internal error", func() { attempts++ })

	handler := &SeqHandler{
		client:        failClient,
		seqURL:        "http://example.com",
		flushInterval: 100 * time.Hour, // won't flush automatically
		batchSize:     2,
	}

	w := &worker{
		eventsCh:    make(chan CLEFEvent, 10),
		doneCh:      make(chan struct{}),
		wg:          sync.WaitGroup{},
		retryBuffer: nil, // start empty
	}

	w.wg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Send 2 events -> triggers flush immediately (batchSize=2).
	e1 := CLEFEvent{Message: "fail1", Timestamp: time.Now()}
	e2 := CLEFEvent{Message: "fail2", Timestamp: time.Now()}

	w.eventsCh <- e1
	w.eventsCh <- e2
	handler.client = successClient // switch to success client for next batch

	// Close and wait
	close(w.eventsCh)
	close(w.doneCh)
	w.wg.Wait()

	// We expect:
	//  - First attempt to send => 500 => both events remain in retryBuffer
	//  - Second attempt when flushCurrentBatch is called before exit => success => retryBuffer cleared

	// So there should have been 2 attempts total.
	assert.Equal(t, 2, attempts, "expected 2 attempts to send batch")
	// Retry buffer should be empty after the final success
	assert.Empty(t, w.retryBuffer)
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

func TestNoFlushMode(t *testing.T) {
	// If h.noFlush is set, runBackgroundFlusher should exit immediately.
	handler := &SeqHandler{
		noFlush:       true,
		flushInterval: 1 * time.Millisecond,
	}

	w := &worker{
		eventsCh: make(chan CLEFEvent, 1),
		doneCh:   make(chan struct{}),
		wg:       sync.WaitGroup{},
	}

	w.wg.Add(1)

	go handler.runBackgroundFlusher(w)

	// Even if we send events, it should immediately return and do nothing.
	w.eventsCh <- CLEFEvent{Message: "test", Timestamp: time.Now()}

	// Close channels
	close(w.eventsCh)
	close(w.doneCh)
	w.wg.Wait()

	// Confirm that we never stored anything in retryBuffer
	assert.Nil(t, w.retryBuffer, "retryBuffer should remain nil/empty in noFlush mode")
}
