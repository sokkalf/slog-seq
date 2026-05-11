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
	events := s.Events()
	for _, e := range events {
		p.logOtelEventAsCLEF(s, e)
	}
	p.logOtelSpanAsCLEF(s)
}

func (p *LoggingSpanProcessor) ForceFlush(ctx context.Context) error {
	return nil
}

func (p *LoggingSpanProcessor) Shutdown(ctx context.Context) error {
	return nil
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
		Timestamp:  span.EndTime(),
		Message:    span.Name(),
		TraceID:    sc.TraceID().String(),
		SpanID:     sc.SpanID().String(),
		SpanStart:  span.StartTime(),
		SpanKind:   spanKind,
		Properties: map[string]any{"SpanName": span.Name()},
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
		Timestamp:  e.Time,
		Message:    e.Name,
		TraceID:    sc.TraceID().String(),
		SpanID:     sc.SpanID().String(),
		SpanStart:  span.StartTime(),
		SpanKind:   spanKind,
		Properties: map[string]any{"SpanName": span.Name()},
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
			event.Message = v.(string)
		}
	}

	p.Handler.HandleCLEFEvent(*event)
}
