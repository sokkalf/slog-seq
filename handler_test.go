package slogseq

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// TestNewSeqHandler tests constructing a new handler with various config.
func TestNewSeqHandler(t *testing.T) {
	handler := newSeqHandler(
		"http://localhost:5341",
		"test-key",
		50,
		2*time.Second,
		&slog.HandlerOptions{Level: slog.LevelWarn},
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
	if handler.flushInterval != 2*time.Second {
		t.Errorf("expected flushInterval = 2s, got %v", handler.flushInterval)
	}
	if handler.options.Level.Level() != slog.LevelWarn {
		t.Errorf("expected level = Warn, got %v", handler.options.Level)
	}

	// Clean up
	handler.Close()
}

// TestSeqHandler_Handle checks that Handle() sends events with correct properties.
func TestSeqHandler_Handle(t *testing.T) {
	handler := newSeqHandler(
		"http://fake",
		"",
		10,
		1*time.Second,
		nil,
	)
	defer handler.Close()

	logger := slog.New(handler)

	// Log something at Info level
	logger.Info("Hello, slog-seq!", "user", "alice", "count", 123)

	select {
	case evt := <-handler.state.eventsCh:
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
	handler := newSeqHandler("http://fake", "", 10, 1*time.Second, opts)
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
	handler := newSeqHandler("http://fake", "", 10, 1*time.Second, nil)
	defer handler.Close()

	logger := slog.New(handler)
	logger2 := logger.With("service", "testsvc")

	logger2.Info("WithAttrs test", "version", "1.2.3")

	select {
	case evt := <-handler.state.eventsCh:
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
	handler := newSeqHandler("http://fake", "", 10, 1*time.Second, nil)
	defer handler.Close()

	logger := slog.New(handler)
	grouped := logger.WithGroup("request").With("id", "1234").WithGroup("headers").With("Accept", "application/json")

	grouped.Info("Grouped log")

	select {
	case evt := <-handler.state.eventsCh:
		// We expect keys to be "request.id" and "request.headers.Accept"
		if evt.Properties["request.id"] != "1234" {
			t.Errorf("Expected request.id=1234, got %v", evt.Properties["request.id"])
		}
		if evt.Properties["request.headers.Accept"] != "application/json" {
			t.Errorf("Expected request.headers.Accept=application/json, got %v", evt.Properties["request.headers.Accept"])
		}
	case <-time.After(2000 * time.Millisecond):
		t.Error("Timed out waiting for grouped event")
	}
}

// TestSeqHandler_Close checks that Close() completes without error and presumably flushes.
func TestSeqHandler_Close(t *testing.T) {
	handler := newSeqHandler("http://fake", "", 10, 1*time.Second, nil)

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
		{slog.LevelDebug, "Debug"},
		{slog.LevelInfo, "Information"},
		{slog.LevelWarn, "Warning"},
		{slog.LevelError, "Error"},
		{42, "Unknown"}, // Something out of range
	}

	for _, c := range cases {
		out := convertLevel(c.in)
		if out != c.expected {
			t.Errorf("convertLevel(%v) = %s, want %s", c.in, out, c.expected)
		}
	}
}
