package slogseq

import (
	"errors"
	"log/slog"
	"maps"
	"testing"
	"testing/slogtest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests send events through a fake Seq server and check the CLEF
// output that arrives.

// logAndCollect runs log against a logger backed by a fake Seq server, closes
// the handler and returns the events that arrived.
func logAndCollect(t *testing.T, log func(*slog.Logger), opts ...SeqOption) []map[string]any {
	t.Helper()
	srv := newSeqServer(t, nil)
	logger, handler := newServerLogger(t, srv, opts...)
	log(logger)
	require.NoError(t, handler.Close())
	return srv.Events()
}

func TestSlogtest(t *testing.T) {
	var srv *seqServer
	var handler *SeqHandler
	slogtest.Run(t,
		func(t *testing.T) slog.Handler {
			srv = newSeqServer(t, nil)
			_, handler = newServerLogger(t, srv)
			return handler
		},
		func(t *testing.T) map[string]any {
			require.NoError(t, handler.Close())
			events := srv.Events()
			require.Len(t, events, 1)

			// slogtest expects the built-in fields under slog's own keys.
			out := maps.Clone(events[0])
			for clefKey, slogKey := range map[string]string{
				"@t": slog.TimeKey,
				"@l": slog.LevelKey,
				"@m": slog.MessageKey,
			} {
				if v, ok := out[clefKey]; ok {
					delete(out, clefKey)
					out[slogKey] = v
				}
			}
			return out
		},
	)
}

func TestDelivery_Levels(t *testing.T) {
	levels := []slog.Level{
		slog.LevelDebug - 4,
		slog.LevelDebug,
		slog.LevelInfo + 2,
		slog.LevelWarn + 1,
		slog.LevelError,
		slog.LevelError + 4,
	}
	events := logAndCollect(t, func(l *slog.Logger) {
		for _, level := range levels {
			l.Log(t.Context(), level, level.String())
		}
	}, WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelDebug - 4}))

	got := make(map[any]any)
	for _, e := range events {
		got[e["@m"]] = e["@l"]
	}
	assert.Equal(t, map[any]any{
		"DEBUG-4": "Verbose",
		"DEBUG":   "Debug",
		"INFO+2":  "Information",
		"WARN+1":  "Warning",
		"ERROR":   "Error",
		"ERROR+4": "Fatal",
	}, got)
}

func TestDelivery_ErrorValues(t *testing.T) {
	events := logAndCollect(t, func(l *slog.Logger) {
		l.With("err", errors.New("boom")).Info("with error")
	})

	require.Len(t, events, 1)
	assert.Equal(t, "boom", events[0]["err"])
}

func TestDelivery_ReservedKeysAreEscaped(t *testing.T) {
	events := logAndCollect(t, func(l *slog.Logger) {
		l.Info("real", "@m", "x", "@l", "Fatal")
	})

	require.Len(t, events, 1)
	assert.Equal(t, "real", events[0]["@m"])
	assert.Equal(t, "Information", events[0]["@l"])
	assert.Equal(t, "x", events[0]["@@m"])
	assert.Equal(t, "Fatal", events[0]["@@l"])
}

func TestDelivery_EmptyFieldsAreLeftOut(t *testing.T) {
	srv := newSeqServer(t, nil)
	_, handler := newServerLogger(t, srv)

	// Span events have neither a level nor necessarily a time.
	handler.HandleCLEFEvent(CLEFEvent{Message: "span event"})
	require.NoError(t, handler.Close())

	events := srv.Events()
	require.Len(t, events, 1)
	assert.Equal(t, map[string]any{"@m": "span event"}, events[0])
}
