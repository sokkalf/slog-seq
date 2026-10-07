package slogseq

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace"
	tr "go.opentelemetry.io/otel/trace"
)

type LoggingSpanProcessor struct {
	Handler *SeqHandler
}

func (p *LoggingSpanProcessor) OnStart(ctx context.Context, s trace.ReadWriteSpan) {
	// noop
}

func (p *LoggingSpanProcessor) OnEnd(s trace.ReadOnlySpan) {
	_ = p.ExportSpans(context.Background(), []trace.ReadOnlySpan{s})
}

// ForceFlush sends the spans that have ended to Seq, along with any log
// events the handler holds.
func (p *LoggingSpanProcessor) ForceFlush(ctx context.Context) error {
	return p.Handler.Flush(ctx)
}

// Shutdown flushes the handler like ForceFlush. It doesn't shut the handler
// down, because the handler is usually shared with a slog.Logger that may
// still be in use. Close the handler separately.
func (p *LoggingSpanProcessor) Shutdown(ctx context.Context) error {
	return p.Handler.Flush(ctx)
}

func (p *LoggingSpanProcessor) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	for _, s := range spans {
		for _, e := range s.Events() {
			p.logOtelEventAsCLEF(s, e)
		}
		p.logOtelSpanAsCLEF(s)
	}
	return nil
}

func (p *LoggingSpanProcessor) logOtelSpanAsCLEF(span trace.ReadOnlySpan) {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return
	}

	spanKind := tr.ValidateSpanKind(span.SpanKind()).String()
	event := &CLEFEvent{
		Timestamp:          span.EndTime(),
		Message:            span.Name(),
		TraceID:            sc.TraceID().String(),
		SpanID:             sc.SpanID().String(),
		SpanStart:          span.StartTime(),
		SpanKind:           spanKind,
		ResourceAttributes: resourceAttrs(span),
		Properties:         map[string]any{"SpanName": span.Name()},
	}

	if parent := span.Parent(); parent.IsValid() {
		event.ParentSpanID = parent.SpanID().String()
	}

	// Include span attributes as properties
	for _, attr := range span.Attributes() {
		event.Properties[string(attr.Key)] = attr.Value.AsInterface()
	}

	// Set level based on span status
	status := span.Status()
	if status.Code == codes.Error {
		event.Level = CLEFLevelError.String()
		if status.Description != "" {
			event.Message = status.Description
		}
	}

	p.Handler.HandleCLEFEvent(*event)
}

func (p *LoggingSpanProcessor) logOtelEventAsCLEF(span trace.ReadOnlySpan, e trace.Event) {
	sc := span.SpanContext()
	if !sc.IsValid() {
		return
	}

	spanKind := tr.ValidateSpanKind(span.SpanKind()).String()
	event := &CLEFEvent{
		Timestamp:          e.Time,
		Message:            e.Name,
		TraceID:            sc.TraceID().String(),
		SpanID:             sc.SpanID().String(),
		SpanStart:          span.StartTime(),
		SpanKind:           spanKind,
		ResourceAttributes: resourceAttrs(span),
		Properties:         map[string]any{"SpanName": span.Name()},
	}

	if parent := span.Parent(); parent.IsValid() {
		event.ParentSpanID = parent.SpanID().String()
	}

	for _, attr := range e.Attributes {
		k := string(attr.Key)
		v := attr.Value.AsInterface()
		event.Properties[k] = v
		if k == "exception.message" {
			event.Level = CLEFLevelError.String()
			event.Message = attr.Value.Emit()
		}
	}

	p.Handler.HandleCLEFEvent(*event)
}

// resourceAttrs flattens the span's OTel Resource into a map suitable for
// CLEF's @ra field. Returns nil when the resource is empty so the field is
// omitted from the JSON output.
func resourceAttrs(span trace.ReadOnlySpan) map[string]any {
	res := span.Resource()
	if res == nil || res.Len() == 0 {
		return nil
	}
	attrs := res.Attributes()
	out := make(map[string]any, len(attrs))
	for _, kv := range attrs {
		out[string(kv.Key)] = kv.Value.AsInterface()
	}
	return out
}
