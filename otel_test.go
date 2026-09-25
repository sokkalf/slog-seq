package slogseq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestOnEnd_WithException(t *testing.T) {
	handler := newUnstartedHandler()
	processor := &LoggingSpanProcessor{Handler: handler}

	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
	// Ensure spans are processed before the test exits.
	defer func() { _ = tp.Shutdown(context.Background()) }()

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
	defer func() { _ = tp.Shutdown(context.Background()) }()

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
