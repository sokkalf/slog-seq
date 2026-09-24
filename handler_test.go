package slogseq

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

// TestNewSeqHandler tests constructing a new handler with various config.
func TestNewSeqHandler(t *testing.T) {
	_, handler := NewLogger("http://localhost:5341",
		WithAPIKey("test-key"),
		WithBatchSize(50),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(&slog.HandlerOptions{Level: slog.LevelWarn}),
	)

	if handler.seqURL != "http://localhost:5341" {
		t.Errorf("expected seqURL to be http://localhost:5341, got %s", handler.seqURL)
	}
	if handler.apiKey != "test-key" {
		t.Errorf("expected apiKey to be test-key, got %s", handler.apiKey)
	}
	if handler.batchSize != 50 {
		t.Errorf("expected batchSize = 50, got %d", handler.batchSize)
	}
	if handler.flushInterval != 5*time.Second {
		t.Errorf("expected flushInterval = 5s, got %v", handler.flushInterval)
	}
	if handler.options.Level.Level() != slog.LevelWarn {
		t.Errorf("expected level = Warn, got %v", handler.options.Level)
	}

	// Clean up
	handler.Close()
}

// TestNewSeqHandler_InvalidOptions checks that invalid options fall back to the defaults instead of panicking.
func TestNewSeqHandler_InvalidOptions(t *testing.T) {
	for _, n := range []int{0, -1} {
		logger, handler := NewLogger("http://fake",
			WithBatchSize(n),
			WithFlushInterval(time.Duration(n)),
			WithWorkers(n),
			WithHandlerOptions(nil),
		)

		if handler.batchSize != defaultBatchSize {
			t.Errorf("n=%d: expected batchSize = %d, got %d", n, defaultBatchSize, handler.batchSize)
		}
		if handler.flushInterval != defaultFlushInterval {
			t.Errorf("n=%d: expected flushInterval = %v, got %v", n, defaultFlushInterval, handler.flushInterval)
		}
		if handler.workerCount != defaultWorkerCount {
			t.Errorf("n=%d: expected workerCount = %d, got %d", n, defaultWorkerCount, handler.workerCount)
		}
		if handler.options.Level != nil {
			t.Errorf("n=%d: expected default handler options, got %+v", n, handler.options)
		}

		logger.Info("should not panic")
		handler.Close()
	}
}

// TestSeqHandler_Handle checks that Handle() sends events with correct properties.
func TestSeqHandler_Handle(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
	)
	handler.noFlush = true // Disable flushing for this test
	defer handler.Close()

	logger := slog.New(handler)

	// Log something at Info level
	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
		if evt.Message != "Hello, slog-seq!" {
			t.Errorf("Expected message 'Hello, slog-seq!', got '%s'", evt.Message)
		}
		if evt.Level != "Information" {
			t.Errorf("Expected level = Information, got '%s'", evt.Level)
		}
		if evt.Properties["user"] != "alice" {
			t.Errorf("Expected user=alice, got %v", evt.Properties["user"])
		}
		if evt.Properties["count"].(int64) != 123 {
			t.Errorf("Expected count=123, got %v", evt.Properties["count"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for log event in eventsCh")
	}
}

// TestSeqHandler_Enabled checks that level filtering via HandlerOptions works.
func TestSeqHandler_Enabled(t *testing.T) {
	opts := &slog.HandlerOptions{Level: slog.LevelWarn}
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithHandlerOptions(opts),
	)
	defer handler.Close()

	// Debug/Info should be disabled
	if handler.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("Debug level should be disabled")
	}
	if handler.Enabled(context.Background(), slog.LevelInfo) {
		t.Error("Info level should be disabled")
	}
	// Warn and above should be enabled
	if !handler.Enabled(context.Background(), slog.LevelWarn) {
		t.Error("Warn level should be enabled")
	}
	if !handler.Enabled(context.Background(), slog.LevelError) {
		t.Error("Error level should be enabled")
	}
}

// TestSeqHandler_WithAttrs checks that WithAttrs merges attributes into subsequent logs.
func TestSeqHandler_WithAttrs(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	logger := slog.New(handler)
	logger2 := logger.With("service", "testsvc")

	logger2.Info("WithAttrs test", "version", "1.2.3")

	select {
	case evt := <-handler.workers[0].eventsCh:
		// Should have both service=testsvc and version=1.2.3
		if evt.Properties["service"] != "testsvc" {
			t.Errorf("Expected service=testsvc, got %v", evt.Properties["service"])
		}
		if evt.Properties["version"] != "1.2.3" {
			t.Errorf("Expected version=1.2.3, got %v", evt.Properties["version"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for WithAttrs event")
	}
}

// TestSeqHandler_WithGroup checks that WithGroup prefixes attribute keys.
func TestSeqHandler_WithGroup(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	select {
	case evt := <-handler.workers[0].eventsCh:
		// We expect keys to be "request.id" and "request.headers.Accept"
		request := evt.Properties["request"].(map[string]any)
		headers := request["headers"].(map[string]any)
		if request["id"] != "1234" {
			t.Errorf("Expected request.id=1234, got %v", request["id"])
		}
		if headers["Accept"] != "application/json" {
			t.Errorf("Expected request.headers.Accept=application/json, got %v", headers["Accept"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for grouped event")
	}
}

// TestSeqHandler_Close checks that Close() completes without error and presumably flushes.
func TestSeqHandler_Close(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
	)

	if err := handler.Close(); err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Optionally, you might check that the background goroutine is done
	// but we can't do that directly without instrumentation or reflection.
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
		out := convertLevel(c.in)
		if out != c.expected {
			t.Errorf("convertLevel(%v) = %s, want %s", c.in, out, c.expected)
		}
	}
}

// TestSeqHandler_addSource ensures source information is added to log events.
func TestSeqHandler_addSource(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithSourceKey("gosource"),
		WithHandlerOptions(&slog.HandlerOptions{AddSource: true}),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	logger := slog.New(handler)

	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.workers[0].eventsCh:
		if evt.Properties["gosource"] == nil {
			t.Error("Expected gosource to be set")
		}
		source := evt.Properties["gosource"].(*slog.Source)
		if source.File == "" {
			t.Error("Expected source file to be set")
		}
		if source.Line == 0 {
			t.Error("Expected source line to be set")
		}
		if source.Function == "" {
			t.Error("Expected source function to be set")
		}
		if !strings.Contains(source.Function, "TestSeqHandler_addSource") {
			t.Errorf("Expected source function to contain TestSeqHandler_addSource, got %s", source.Function)
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for log event in eventsCh")
	default:
		t.Error("Expected event to be sent")
	}
}

// TestSeqHandler_grouping ensures that grouping works as expected.
// test case from comments in slog.Handler
func TestSeqHandler_grouping(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	ctx := context.Background()
	logger := slog.New(handler)
	logger.WithGroup("s").LogAttrs(ctx, slog.LevelInfo, "huba", slog.Int("a", 1), slog.Int("b", 2))
	logger.LogAttrs(ctx, slog.LevelInfo, "huba", slog.Group("s", slog.Int("a", 1), slog.Int("b", 2)))

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	if diff := cmp.Diff(event1, event2, cmpopts.IgnoreFields(CLEFEvent{}, "Timestamp")); diff != "" {
		t.Errorf("events differ: (-got +want)\n%s", diff)
	}
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
	_, handler := NewLogger("http://fake",
		WithAPIKey(""),
		WithBatchSize(10),
		WithFlushInterval(5*time.Second),
		WithWorkers(1),
		WithHandlerOptions(opts),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	logger := slog.New(handler)
	logger.Info("Super secret info", "password", "2Fat2Fly")
	logger.WithGroup("secret_info").Info("Wohoo", "password", "secret")

	event1 := <-handler.workers[0].eventsCh
	event2 := <-handler.workers[0].eventsCh

	if event1.Properties["password"] != "*****" {
		t.Errorf("Expected password=*****, got %v", event1.Properties["password"])
	}

	secret_info := event2.Properties["secret_info"].(map[string]any)
	if secret_info["password"] != "*****" {
		t.Errorf("Expected password=*****, got %v", secret_info["password"])
	}
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

	_, handler := NewLogger("http://fake",
		WithWorkers(1),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for the test

	logger := slog.New(handler)

	// 1. Argument-style anonymous group.
	logger.Info("anon-group-arg",
		slog.Any("", payload{ID: 42, Name: "keyname"}))

	// 2. With-style anonymous group.
	logger.With("", payload{ID: 42, Name: "keyname"}).
		Info("anon-group-with")

	evt1 := <-handler.workers[0].eventsCh
	evt2 := <-handler.workers[0].eventsCh

	// --- Assertions for the first event (argument style) -----------
	if got := evt1.Properties["id"]; got != int64(42) {
		t.Errorf("argument style: expected id=42, got %v", got)
	}
	if got := evt1.Properties["name"]; got != "keyname" {
		t.Errorf("argument style: expected name=arg, got %v", got)
	}

	// --- Assertions for the second event (With style) --------------
	if got := evt2.Properties["id"]; got != int64(42) {
		t.Errorf("With style: expected id=42, got %v", got)
	}
	if got := evt2.Properties["name"]; got != "keyname" {
		t.Errorf("With style: expected name=with, got %v", got)
	}

	// The two events should differ only in Timestamp and Message.
	if diff := cmp.Diff(evt1, evt2,
		cmpopts.IgnoreFields(CLEFEvent{}, "Timestamp", "Message"),
	); diff != "" {
		t.Errorf("events differ: (-arg +with)\n%s", diff)
	}
}

// TestSeqHandler_errorValues checks that error values are sent as strings no
// matter how they reach the handler.
func TestSeqHandler_errorValues(t *testing.T) {
	_, handler := NewLogger("http://fake",
		WithWorkers(1),
		WithGlobalAttrs(slog.Any("global_err", errors.New("global"))),
	)
	defer handler.Close()
	handler.noFlush = true // Disable flushing for this test

	logger := slog.New(handler)
	logger.With("with_err", errors.New("with")).
		WithGroup("g").
		Info("errors",
			"record_err", errors.New("record"),
			slog.Group("inner", slog.Any("group_err", errors.New("group"))),
		)

	evt := <-handler.workers[0].eventsCh

	if got := evt.Properties["global_err"]; got != "global" {
		t.Errorf("global attrs: expected global_err=\"global\", got %#v", got)
	}
	if got := evt.Properties["with_err"]; got != "with" {
		t.Errorf("With(): expected with_err=\"with\", got %#v", got)
	}
	g, ok := evt.Properties["g"].(map[string]any)
	if !ok {
		t.Fatalf("expected group g, got %#v", evt.Properties["g"])
	}
	if got := g["record_err"]; got != "record" {
		t.Errorf("record attr: expected g.record_err=\"record\", got %#v", got)
	}
	inner, ok := g["inner"].(map[string]any)
	if !ok {
		t.Fatalf("expected group g.inner, got %#v", g["inner"])
	}
	if got := inner["group_err"]; got != "group" {
		t.Errorf("group attr: expected g.inner.group_err=\"group\", got %#v", got)
	}
}
