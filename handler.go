package slogseq

import (
	"context"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
	"slices"
)

type worker struct {
	eventsCh chan CLEFEvent
	doneCh   chan struct{}
	wg       sync.WaitGroup
	// retry buffer
	retryBuffer []CLEFEvent
	purgeTicker *time.Ticker
}

type SeqHandler struct {
	// config
	seqURL           string
	apiKey           string
	batchSize        int
	flushInterval    time.Duration
	disableTLSVerify bool
	sourceKey        string
	workerCount      int
	nonBlocking      bool
	noFlush          bool // Used in tests

	// http client
	client *http.Client

	// concurrency
	workers []worker
	next    uint32

	// optional function that will get called on errors
	errorHandlerFunc func(error)

	// Other fields for global attrs, grouping, etc.
	attrs   []slog.Attr
	groups  []string
	options slog.HandlerOptions
}

func newSeqHandler(seqURL string) *SeqHandler {
	h := &SeqHandler{
		seqURL: seqURL,
		// sane defaults
		batchSize:     50,
		flushInterval: 2 * time.Second,
		workerCount:   1,
		nonBlocking:   true,
		noFlush:       false,
		sourceKey:     slog.SourceKey,
		options:       slog.HandlerOptions{},
	}

	return h
}

func (h *SeqHandler) start() {
	if h.client == nil {
		h.client = newHttpClient(h.disableTLSVerify)
	}
	if h.errorHandlerFunc == nil {
		h.errorHandlerFunc = func(err error) {
			// by default we do nothing
		}
	}
	h.workers = make([]worker, h.workerCount)
	// Start background workers
	for i := range h.workerCount {
		h.workers[i].eventsCh = make(chan CLEFEvent, 1000)
		h.workers[i].doneCh = make(chan struct{})
		h.workers[i].wg.Add(1)
		go h.runBackgroundFlusher(&h.workers[i])
	}
}

func (h *SeqHandler) Handle(ctx context.Context, r slog.Record) error {
	// Convert slog.Level to text
	levelString := convertLevel(r.Level)

	spanCtx := trace.SpanContextFromContext(ctx)

	// Collect attributes into a map
	props := make(map[string]any)

	if h.options.AddSource {
		pc := r.PC
		caller := runtime.CallersFrames([]uintptr{pc})
		frame, _ := caller.Next()
		source := slog.Source{File: frame.File, Line: frame.Line, Function: frame.Function}
		sourceAttr := slog.Any(h.sourceKey, &source)
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

		if len(h.groups) > 0 && a.Key != h.sourceKey {
			a.Key = strings.Join(h.groups, ".") + "." + a.Key
		}

		if v, ok := a.Value.Any().(error); ok {
			a.Value = slog.StringValue(v.Error())
		}
		h.addAttr(props, a)
		return true
	})

	// split multi-line messages into a message (first line) and 'exception' (rest)
	msg := strings.SplitN(r.Message, "\n", 2)

	var exception string
	if len(msg) == 1 {
		exception = ""
	} else {
		exception = msg[1]
	}

	// Create CLEF event
	event := CLEFEvent{
		Timestamp:  r.Time,
		Message:    msg[0],
		Exception:  exception,
		Level:      levelString,
		Properties: dottedToNested(props),
	}
	if spanCtx.IsValid() {
		event.TraceID = spanCtx.TraceID().String()
		event.SpanID = spanCtx.SpanID().String()
	}
	h.HandleCLEFEvent(event)

	return nil
}

func (h *SeqHandler) HandleCLEFEvent(event CLEFEvent) {
	idx := atomic.AddUint32(&h.next, 1) % uint32(len(h.workers))
	if h.nonBlocking {
		// send to channel, drop if full
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		default:
			// channel full, drop event
		}
	} else {
		// blocking send
		select {
		case h.workers[idx].eventsCh <- event:
			// success
		}
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
	h2.attrs = slices.Clone(h.attrs)
	for _, a := range attrs {
		a.Value = a.Value.Resolve()

		if a.Key == "" {
			h2.attrs = append(h2.attrs, a)
			continue
		}

		if len(h2.groups) > 0 && a.Key != h2.sourceKey {
			a.Key = strings.Join(h2.groups, ".") + "." + a.Key
		}

		h2.attrs = append(h2.attrs, a)
	}

	return &h2
}

func (h *SeqHandler) WithGroup(name string) slog.Handler {
	h2 := *h
	h2.groups = slices.Clone(h.groups)
	h2.groups = append(h2.groups, name)

	return &h2
}

func (h *SeqHandler) Close() error {
	// this is ugly, but we need to give all the events a chance to be sent
	time.Sleep(50 * time.Millisecond)
	for i := range h.workerCount {
		close(h.workers[i].eventsCh)
		close(h.workers[i].doneCh)
		h.workers[i].wg.Wait()
	}
	return nil
}

// SourceKey returns the key used when AddSource is enabled.
func (h *SeqHandler) SourceKey() string {
	return h.sourceKey
}

func (h *SeqHandler) addAttrs(dst map[string]any, attrs []slog.Attr) {
	for _, a := range attrs {
		h.addAttr(dst, a)
	}
}

func (h *SeqHandler) addAttr(dst map[string]any, a slog.Attr) {
	a.Value = a.Value.Resolve()

	if a.Key == "" {
		// Anonymous group, inline
		if a.Value.Kind() == slog.KindGroup {
			for _, ga := range a.Value.Group() {
				h.addAttr(dst, ga)
			}
		}
		return
	}

	switch a.Value.Kind() {
	case slog.KindGroup:
		groupMap, ok := dst[a.Key].(map[string]any)
		if !ok {
			groupMap = make(map[string]any)
			dst[a.Key] = groupMap
		}
		for _, ga := range a.Value.Group() {
			h.addAttr(groupMap, ga)
		}
	default:
		dst[a.Key] = a.Value.Any()
	}
}

func dottedToNested(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))
	for k, v := range props {
		path := strings.Split(k, ".")
		addNested(out, path, v)
	}
	return out
}

func addNested(dst map[string]any, path []string, val any) {
	if len(path) == 1 {
		dst[path[0]] = val
		return
	}

	head := path[0]
	child, ok := dst[head].(map[string]any)
	if !ok {
		child = make(map[string]any)
		dst[head] = child
	}

	addNested(child, path[1:], val)
}

func convertLevel(l slog.Level) string {
	switch l {
	case slog.LevelDebug:
		return CLEFLevelDebug.String()
	case slog.LevelInfo:
		return CLEFLevelInformation.String()
	case slog.LevelWarn:
		return CLEFLevelWarning.String()
	case slog.LevelError:
		return CLEFLevelError.String()
	default:
		return CLEFLevelInformation.String()
	}
}
