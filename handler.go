package slogseq

import (
	"context"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/trace"
)

type concurrencyState struct {
	eventsCh chan CLEFEvent
	doneCh   chan struct{}
	wg       sync.WaitGroup
}

type SeqHandler struct {
	// config
	seqURL           string
	apiKey           string
	batchSize        int
	flushInterval    time.Duration
	disableTLSVerify bool

	// retry buffer
	retryBuffer []CLEFEvent

	// http client
	client *http.Client

	// concurrency
	state *concurrencyState

	// Other fields for global attrs, grouping, etc.
	attrs   []slog.Attr
	groups  []string
	options slog.HandlerOptions
}

func newSeqHandler(seqURL string) *SeqHandler {
	h := &SeqHandler{
		seqURL: seqURL,
		// sane defaults
		batchSize: 50,
		flushInterval: 2 * time.Second,
		state: &concurrencyState{
			eventsCh: make(chan CLEFEvent, 1000), // some buffer size
			doneCh:   make(chan struct{}),
		},
		options: slog.HandlerOptions{},
	}

	return h
}

func (h *SeqHandler) start() {
	if h.client == nil {
		h.client = newHttpClient(h.disableTLSVerify)
	}
	// Start background flusher
	h.state.wg.Add(1)
	go h.runBackgroundFlusher()
}

func (h *SeqHandler) Handle(ctx context.Context, r slog.Record) error {
	// Convert slog.Level to text
	levelString := convertLevel(r.Level)

	spanCtx := trace.SpanContextFromContext(ctx)

	// Collect attributes into a map
	props := make(map[string]interface{})

	if h.options.AddSource {
		pc := r.PC
		caller := runtime.CallersFrames([]uintptr{pc})
		frame, _ := caller.Next()
		source := slog.Source{File: frame.File, Line: frame.Line, Function: frame.Function}
		sourceAttr := slog.Any(slog.SourceKey, &source)
		r.AddAttrs(sourceAttr)
	}
	h.addAttrs(props, h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		if h.options.ReplaceAttr != nil {
			a = h.options.ReplaceAttr(h.groups, a)
			if a.Key == "" {
				return true
			}
		}
		h.addAttr(props, a)
		return true
	})

	// Create CLEF event
	event := CLEFEvent{
		Timestamp:  r.Time,
		Message:    r.Message,
		Level:      levelString,
		Properties: props,
	}
	if spanCtx.IsValid() {
		event.TraceID = spanCtx.TraceID().String()
		event.SpanID = spanCtx.SpanID().String()
	}
	h.HandleCLEFEvent(event)

	return nil
}

func (h *SeqHandler) HandleCLEFEvent(event CLEFEvent) {
	// Send to channel (non-blocking or minimal blocking)
	select {
	case h.state.eventsCh <- event:
		// success
	default:
		// channel is full -> decide if we drop or block
		// for non-blocking, you might drop
		// or you can block (which might block the application)
		// or consider a better queue strategy
	}
}

func (h *SeqHandler) Enabled(ctx context.Context, l slog.Level) bool {
	if h.options.Level != nil {
		return l >= h.options.Level.Level()
	}
	return true
}

func (h *SeqHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = append([]slog.Attr(nil), h.attrs...)
	for _, a := range attrs {
		if len(h.groups) > 0 {
			a.Key = strings.Join(h.groups, ".") + "." + a.Key
		}
		h2.attrs = append(h2.attrs, a)
	}

	return &h2
}

func (h *SeqHandler) WithGroup(name string) slog.Handler {
	h2 := *h
	h2.groups = append([]string(nil), h.groups...)
	h2.groups = append(h2.groups, name)

	return &h2
}

func (h *SeqHandler) Close() error {
	// this is ugly, but we need to give all the events a chance to be sent
	time.Sleep(50 * time.Millisecond)
	close(h.state.eventsCh)
	close(h.state.doneCh)
	h.state.wg.Wait()
	return nil
}

func (h *SeqHandler) addAttrs(dst map[string]any, attrs []slog.Attr) {
	for _, a := range attrs {
		h.addAttr(dst, a)
	}
}

func (h *SeqHandler) addAttr(dst map[string]any, a slog.Attr) {
	if a.Key == "" {
		return
	}
	dst[a.Key] = a.Value.Any()
}

func convertLevel(l slog.Level) string {
	switch l {
	case slog.LevelDebug:
		return "Debug"
	case slog.LevelInfo:
		return "Information"
	case slog.LevelWarn:
		return "Warning"
	case slog.LevelError:
		return "Error"
	default:
		return "Unknown"
	}
}
