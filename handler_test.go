package slogseq

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSeqHandler tests constructing a new handler with various config.
func TestNewSeqHandler(t *testing.T) {
	_, handler := NewLogger("http://localhost:5341",
		WithAPIKey("test-key"),
		WithBatchSize(50),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}),
	)
	defer handler.Close()

	assert.Equal(t, "http://localhost:5341", handler.seqURL)
	assert.Equal(t, "test-key", handler.apiKey)
	assert.Equal(t, 50, handler.batchSize)
	assert.Equal(t, 5*time.Second, handler.flushInterval)
	assert.Equal(t, slog.LevelWarn, handler.options.Level.Level())
}

// TestNewSeqHandler_InvalidOptions checks that invalid options fall back to the defaults instead of panicking.
func TestNewSeqHandler_InvalidOptions(t *testing.T) {
	srv := newSeqServer(t, nil)
	for _, n := range []int{0, -1} {
		logger, handler := newServerLogger(t, srv,
			WithBatchSize(n),
			WithFlushInterval(time.Duration(n)),
			WithWorkers(n),
			WithHandlerOptions(nil),
		)

		assert.Equal(t, defaultBatchSize, handler.batchSize, "n=%d", n)
		assert.Equal(t, defaultFlushInterval, handler.flushInterval, "n=%d", n)
		assert.Equal(t, defaultWorkerCount, handler.workerCount, "n=%d", n)
		assert.Nil(t, handler.options.Level, "n=%d", n)

		logger.Info("should not panic")
		handler.Close()
	}
	assert.Len(t, srv.Events(), 2)
}

// TestSeqHandler_Handle checks that Handle() sends events with correct properties.
func TestSeqHandler_Handle(t *testing.T) {
	handler := newUnstartedHandler()
	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	evt := nextEvent(t, handler)
	assert.Equal(t, "Hello, slog-seq!", evt.Message)
	assert.Equal(t, "Information", evt.Level)
	assert.Equal(t, "alice", evt.Properties["user"])
	assert.Equal(t, int64(123), evt.Properties["count"])
}

// TestSeqHandler_Enabled checks that level filtering via HandlerOptions works.
func TestSeqHandler_Enabled(t *testing.T) {
	handler := newUnstartedHandler(WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()

	assert.False(t, handler.Enabled(ctx, slog.LevelDebug), "Debug level should be disabled")
	assert.False(t, handler.Enabled(ctx, slog.LevelInfo), "Info level should be disabled")
	assert.True(t, handler.Enabled(ctx, slog.LevelWarn), "Warn level should be enabled")
	assert.True(t, handler.Enabled(ctx, slog.LevelError), "Error level should be enabled")
}

// TestSeqHandler_WithAttrs checks that WithAttrs merges attributes into subsequent logs.
func TestSeqHandler_WithAttrs(t *testing.T) {
	handler := newUnstartedHandler()
	logger := slog.New(handler).With("service", "testsvc")

	logger.Info("WithAttrs test", "version", "1.2.3")

	evt := nextEvent(t, handler)
	assert.Equal(t, "testsvc", evt.Properties["service"])
	assert.Equal(t, "1.2.3", evt.Properties["version"])
}

// TestSeqHandler_WithGroup checks that WithGroup nests attributes.
func TestSeqHandler_WithGroup(t *testing.T) {
	handler := newUnstartedHandler()
	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	evt := nextEvent(t, handler)
	request, ok := evt.Properties["request"].(map[string]any)
	require.True(t, ok, "expected group request, got %#v", evt.Properties["request"])
	headers, ok := request["headers"].(map[string]any)
	require.True(t, ok, "expected group request.headers, got %#v", request["headers"])
	assert.Equal(t, "1234", request["id"])
	assert.Equal(t, "application/json", headers["Accept"])
}

// TestSeqHandler_WithEmptyGroup checks that WithGroup("") returns the handler unchanged.
func TestSeqHandler_WithEmptyGroup(t *testing.T) {
	handler := newUnstartedHandler()

	assert.Same(t, handler, handler.WithGroup(""))

	logger := slog.New(handler)
	logger.WithGroup("").With("id", "1234").Info("empty group", "k", "v")

	evt := nextEvent(t, handler)
	assert.Equal(t, "1234", evt.Properties["id"])
	assert.Equal(t, "v", evt.Properties["k"])
	assert.NotContains(t, evt.Properties, "")
}

// TestSeqHandler_Close checks that Close() sends the buffered events.
func TestSeqHandler_Close(t *testing.T) {
	srv := newSeqServer(t, nil)
	logger, handler := newServerLogger(t, srv, WithFlushInterval(time.Hour))

	logger.Info("one")
	logger.Info("two")
	logger.Info("three")
	require.NoError(t, handler.Close())

	var messages []any
	for _, e := range srv.Events() {
		messages = append(messages, e["@m"])
	}
	assert.Equal(t, []any{"one", "two", "three"}, messages)
}

// TestSeqHandler_CloseTwice checks that a second Close() doesn't panic.
func TestSeqHandler_CloseTwice(t *testing.T) {
	_, handler := NewLogger("http://fake")

	assert.NoError(t, handler.Close(), "first Close")
	assert.NoError(t, handler.Close(), "second Close")
}

// TestSeqHandler_LogAfterClose checks that logging after Close() is a no-op, also through derived handlers.
func TestSeqHandler_LogAfterClose(t *testing.T) {
	srv := newSeqServer(t, nil)
	for _, nonBlocking := range []bool{true, false} {
		logger, handler := newServerLogger(t, srv, WithNonBlocking(nonBlocking))
		derived := logger.With("err", errors.New("boom")).WithGroup("g")

		handler.Close()

		logger.Info("after close")
		derived.Info("after close", "k", "v")
		handler.HandleCLEFEvent(CLEFEvent{Message: "after close"})
	}
	assert.Empty(t, srv.Events())
}

// TestSeqHandler_LogDuringClose checks that logging concurrently with Close() doesn't panic.
func TestSeqHandler_LogDuringClose(t *testing.T) {
	srv := newSeqServer(t, nil)
	logger, handler := newServerLogger(t, srv, WithWorkers(4))

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 1000 {
				logger.Info("racing close")
			}
		}()
	}
	handler.Close()
	wg.Wait()
}

// TestSeqHandler_CloseWithSlowServer checks that Close() sends every buffered event, even when the server is slow.
func TestSeqHandler_CloseWithSlowServer(t *testing.T) {
	srv := newSeqServer(t, func(int) int {
		time.Sleep(20 * time.Millisecond)
		return http.StatusCreated
	})
	logger, handler := newServerLogger(t, srv, WithNonBlocking(false), WithFlushInterval(time.Hour))

	for i := range 1000 {
		logger.Info("event", "i", i)
	}
	require.NoError(t, handler.Close())

	assert.Equal(t, 1000, len(srv.Events()))
}

// TestSeqHandler_CloseReportsUnsentEvents checks that events still unsent at shutdown are reported, not silently lost.
func TestSeqHandler_CloseReportsUnsentEvents(t *testing.T) {
	srv := newSeqServer(t, func(int) int { return http.StatusInternalServerError })
	var errs errorRecorder
	logger, handler := newServerLogger(t, srv, WithFlushInterval(time.Hour), WithErrorHandlerFunc(errs.handle))

	logger.Info("one")
	logger.Info("two")
	logger.Info("three")
	err := handler.Close()

	require.ErrorContains(t, err, "dropped 3 events")
	reported := errs.Errors()
	require.NotEmpty(t, reported)
	assert.ErrorIs(t, err, reported[len(reported)-1], "the drop should also go to the error handler")
	assert.Empty(t, srv.Events())
}

// TestSeqHandler_ShutdownDeadline checks that Shutdown() returns when its context expires, even with a hung
// server and senders blocked in blocking mode.
func TestSeqHandler_ShutdownDeadline(t *testing.T) {
	release := make(chan struct{})
	srv := newSeqServer(t, func(int) int {
		<-release
		return http.StatusCreated
	})
	// Registered after the server, so it runs first and lets the server close.
	t.Cleanup(func() { close(release) })

	var errs errorRecorder
	logger, handler := newServerLogger(t, srv,
		WithNonBlocking(false),
		WithBatchSize(1),
		WithErrorHandlerFunc(errs.handle),
	)

	logged := make(chan struct{})
	go func() {
		defer close(logged)
		// More than the channel holds, so the sender blocks.
		for range 2000 {
			logger.Info("stuck")
		}
	}()
	require.Eventually(t, func() bool { return len(handler.workers[0].eventsCh) == cap(handler.workers[0].eventsCh) },
		time.Second, 5*time.Millisecond, "expected the channel to fill up")

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := handler.Shutdown(ctx)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), time.Second)

	// The workers give up on the hung request, so the blocked sender is released and the drop is reported.
	select {
	case <-logged:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "the blocked sender was never released")
	}
	require.Eventually(t, func() bool {
		for _, err := range errs.Errors() {
			if strings.Contains(err.Error(), "dropped") {
				return true
			}
		}
		return false
	}, 5*time.Second, 5*time.Millisecond, "expected the dropped events to be reported")
	for _, err := range errs.Errors() {
		assert.NotErrorIs(t, err, context.Canceled, "cancelled requests shouldn't be reported on their own")
	}
}

// TestSeqHandler_Flush checks that Flush() sends the queued events and leaves the handler running.
func TestSeqHandler_Flush(t *testing.T) {
	srv := newSeqServer(t, nil)
	logger, handler := newServerLogger(t, srv, WithWorkers(3), WithFlushInterval(time.Hour))

	for range 10 {
		logger.Info("before flush")
	}
	require.NoError(t, handler.Flush(t.Context()))
	assert.Len(t, srv.Events(), 10)

	logger.Info("after flush")
	require.NoError(t, handler.Close())
	assert.Len(t, srv.Events(), 11)
}

// TestSeqHandler_FlushFailure checks that Flush() reports events that couldn't be sent, and keeps them for retry.
func TestSeqHandler_FlushFailure(t *testing.T) {
	// The first request fails, the rest succeed.
	srv := newSeqServer(t, func(n int) int {
		if n == 1 {
			return http.StatusInternalServerError
		}
		return http.StatusCreated
	})
	logger, handler := newServerLogger(t, srv, WithFlushInterval(time.Hour))

	logger.Info("one")
	logger.Info("two")
	require.ErrorContains(t, handler.Flush(t.Context()), "2 events could not be sent")
	assert.Empty(t, srv.Events())

	require.NoError(t, handler.Flush(t.Context()))
	assert.Len(t, srv.Events(), 2)
}

// TestSeqHandler_FlushAfterClose checks that Flush() returns straight away once the handler is closed.
func TestSeqHandler_FlushAfterClose(t *testing.T) {
	srv := newSeqServer(t, nil)
	_, handler := newServerLogger(t, srv)
	require.NoError(t, handler.Close())

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	assert.NoError(t, handler.Flush(ctx))
}

// TestSeqHandler_convertLevel ensures level conversion matches expectations.
func TestSeqHandler_convertLevel(t *testing.T) {
	cases := []struct {
		in       slog.Level
		expected string
	}{
		{slog.LevelDebug - 4, "Verbose"},
		{slog.LevelDebug - 1, "Verbose"},
		{slog.LevelDebug, "Debug"},
		{slog.LevelDebug + 2, "Debug"},
		{slog.LevelInfo, "Information"},
		{slog.LevelInfo + 2, "Information"},
		{slog.LevelWarn, "Warning"},
		{slog.LevelWarn + 1, "Warning"},
		{slog.LevelError, "Error"},
		{slog.LevelError + 3, "Error"},
		{slog.LevelError + 4, "Fatal"},
		{42, "Fatal"},
	}

	for _, c := range cases {
		assert.Equal(t, c.expected, convertLevel(c.in), "convertLevel(%v)", c.in)
	}
}

// TestSeqHandler_addSource ensures source information is added to log events.
func TestSeqHandler_addSource(t *testing.T) {
	handler := newUnstartedHandler(
		WithSourceKey("gosource"),
		WithHandlerOptions(&slog.HandlerOptions{AddSource: true}),
	)
	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	evt := nextEvent(t, handler)
	source, ok := evt.Properties["gosource"].(*slog.Source)
	require.True(t, ok, "expected gosource to be a *slog.Source, got %#v", evt.Properties["gosource"])
	assert.NotEmpty(t, source.File)
	assert.NotZero(t, source.Line)
	assert.Contains(t, source.Function, "TestSeqHandler_addSource")
}

// TestSeqHandler_grouping ensures that grouping works as expected.
// test case from comments in slog.Handler
func TestSeqHandler_grouping(t *testing.T) {
	handler := newUnstartedHandler()
	ctx := context.Background()
	logger := slog.New(handler)

	logger.WithGroup("s").LogAttrs(ctx, slog.LevelInfo, "huba", slog.Int("a", 1), slog.Int("b", 2))
	logger.LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	event1 := nextEvent(t, handler)
	event2 := nextEvent(t, handler)

	event2.Timestamp = event1.Timestamp
	assert.Equal(t, event1, event2)
}

func TestSeqHandler_replaceAttr(t *testing.T) {
	opts := &slog.HandlerOptions{
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == "password" {
				a.Value = slog.StringValue("*****")
			}
			return a
		},
	}
	handler := newUnstartedHandler(WithHandlerOptions(opts))
	logger := slog.New(handler)

	logger.Info("Super secret info", "password", "2Fat2Fly")
	logger.WithGroup("secret_info").Info("Wohoo", "password", "secret")

	event1 := nextEvent(t, handler)
	event2 := nextEvent(t, handler)

	assert.Equal(t, "*****", event1.Properties["password"])
	secretInfo, ok := event2.Properties["secret_info"].(map[string]any)
	require.True(t, ok, "expected group secret_info, got %#v", event2.Properties["secret_info"])
	assert.Equal(t, "*****", secretInfo["password"])
}

// A tiny payload that implements slog.LogValuer.
type payload struct {
	ID   int64
	Name string
}

func (p payload) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Int64("id", p.ID),
		slog.String("name", p.Name),
	)
}

func TestSeqHandler_AnonymousGroup(t *testing.T) {
	handler := newUnstartedHandler()
	logger := slog.New(handler)

	// 1. Argument-style anonymous group.
	logger.Info("anon-group-arg",
		slog.Any("", payload{ID: 42, Name: "keyname"}))

	// 2. With-style anonymous group.
	logger.With("", payload{ID: 42, Name: "keyname"}).
		Info("anon-group-with")

	evt1 := nextEvent(t, handler)
	evt2 := nextEvent(t, handler)

	assert.Equal(t, int64(42), evt1.Properties["id"], "argument style")
	assert.Equal(t, "keyname", evt1.Properties["name"], "argument style")
	assert.Equal(t, int64(42), evt2.Properties["id"], "With style")
	assert.Equal(t, "keyname", evt2.Properties["name"], "With style")

	// The two events should differ only in Timestamp and Message.
	evt2.Timestamp = evt1.Timestamp
	evt2.Message = evt1.Message
	assert.Equal(t, evt1, evt2)
}

// TestSeqHandler_errorValues checks that error values are sent as strings no
// matter how they reach the handler.
func TestSeqHandler_errorValues(t *testing.T) {
	handler := newUnstartedHandler(WithGlobalAttrs(slog.Any("global_err", errors.New("global"))))
	logger := slog.New(handler)

	logger.With("with_err", errors.New("with")).
		WithGroup("g").
		Info("errors",
			"record_err", errors.New("record"),
			slog.Group("inner", slog.Any("group_err", errors.New("group"))),
		)

	evt := nextEvent(t, handler)
	assert.Equal(t, "global", evt.Properties["global_err"], "global attrs")
	assert.Equal(t, "with", evt.Properties["with_err"], "With()")
	g, ok := evt.Properties["g"].(map[string]any)
	require.True(t, ok, "expected group g, got %#v", evt.Properties["g"])
	assert.Equal(t, "record", g["record_err"], "record attr")
	inner, ok := g["inner"].(map[string]any)
	require.True(t, ok, "expected group g.inner, got %#v", g["inner"])
	assert.Equal(t, "group", inner["group_err"], "group attr")
}
