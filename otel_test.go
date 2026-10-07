package slogseq

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestOnEnd_WithException(t *testing.T) {
	handler := newUnstartedHandler()
	processor := &LoggingSpanProcessor{Handler: handler}

	// OnEnd runs inside span.End(), so the event is queued without a flush.
	// The workers aren't running, so tp.Shutdown() would wait for a flush forever.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))

	// Obtain a tracer from the provider.
	tracer := tp.Tracer("test-tracer")

	// Start a span, add an event with exception attributes, then end the span.
	ctx := context.Background()
	_, span := tracer.Start(ctx, "testSpan", trace.WithSpanKind(trace.SpanKindServer))
	span.AddEvent("originalEventName", trace.WithAttributes(
		attribute.String("exception.message", "error occurred"),
		attribute.Int("code", 500),
	))
	span.End()

	evt := nextEvent(t, handler)

	// Check that the exception message overwrote the event's original name.
	assert.Equal(t, "error occurred", evt.Message)
	// Check that the level was set to error.
	assert.Equal(t, CLEFLevelError.String(), evt.Level)
	// Check that additional properties (like code) are present.
	assert.Equal(t, int64(500), evt.Properties["code"])
}

func TestOnEnd_NonStringExceptionMessage(t *testing.T) {
	handler := newUnstartedHandler()
	processor := &LoggingSpanProcessor{Handler: handler}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))

	tracer := tp.Tracer("test-tracer")
	_, span := tracer.Start(context.Background(), "testSpan")
	span.AddEvent("originalEventName", trace.WithAttributes(
		attribute.Int("exception.message", 42),
	))

	// A non-string exception.message used to panic inside span.End().
	assert.NotPanics(t, func() { span.End() })

	evt := nextEvent(t, handler)
	assert.Equal(t, "42", evt.Message)
	assert.Equal(t, CLEFLevelError.String(), evt.Level)
}

func TestOnEnd_PropagatesResourceAttributes(t *testing.T) {
	handler := newUnstartedHandler()
	processor := &LoggingSpanProcessor{Handler: handler}

	res := resource.NewSchemaless(
		attribute.String("service.name", "testsvc"),
		attribute.String("service.version", "1.2.3"),
	)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(processor),
		sdktrace.WithResource(res),
	)

	tracer := tp.Tracer("test-tracer")
	_, span := tracer.Start(context.Background(), "rootSpan")
	span.AddEvent("anEvent")
	span.End()

	// One event emitted from AddEvent, one from span end.
	for _, evt := range []CLEFEvent{nextEvent(t, handler), nextEvent(t, handler)} {
		assert.Equal(t, "testsvc", evt.ResourceAttributes["service.name"], "@ra service.name")
		assert.Equal(t, "1.2.3", evt.ResourceAttributes["service.version"], "@ra service.version")
	}
}

func TestLoggingSpanProcessor_ForceFlush(t *testing.T) {
	srv := newSeqServer(t, nil)
	_, handler := newServerLogger(t, srv, WithFlushInterval(time.Hour))
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(&LoggingSpanProcessor{Handler: handler}))

	_, span := tp.Tracer("test-tracer").Start(context.Background(), "testSpan")
	span.End()
	require.NoError(t, tp.ForceFlush(t.Context()))

	events := srv.Events()
	require.Len(t, events, 1)
	assert.Equal(t, "testSpan", events[0]["@m"])
}

// TestLoggingSpanProcessor_Shutdown checks that shutting down the tracer provider sends the spans, but leaves the
// handler running for the logger.
func TestLoggingSpanProcessor_Shutdown(t *testing.T) {
	srv := newSeqServer(t, nil)
	logger, handler := newServerLogger(t, srv, WithFlushInterval(time.Hour))
	// Used as an exporter, as the README shows.
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(
		sdktrace.NewSimpleSpanProcessor(&LoggingSpanProcessor{Handler: handler}),
	))

	_, span := tp.Tracer("test-tracer").Start(context.Background(), "testSpan")
	span.End()
	require.NoError(t, tp.Shutdown(t.Context()))
	assert.Equal(t, []any{"testSpan"}, messages(srv.Events()))

	logger.Info("after tracer shutdown")
	require.NoError(t, handler.Close())
	assert.Equal(t, []any{"testSpan", "after tracer shutdown"}, messages(srv.Events()))
}
