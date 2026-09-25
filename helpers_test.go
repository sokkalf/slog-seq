package slogseq

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// seqServer is a fake Seq server that records the CLEF events it accepts.
type seqServer struct {
	*httptest.Server
	t *testing.T

	// status returns the status code for the nth request, counting from 1.
	// If nil, every request succeeds.
	status func(n int) int

	mu       sync.Mutex
	requests int
	events   []map[string]any
}

func newSeqServer(t *testing.T, status func(n int) int) *seqServer {
	s := &seqServer{t: t, status: status}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(s.Close)
	return s
}

func (s *seqServer) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.requests++
	code := http.StatusCreated
	if s.status != nil {
		code = s.status(s.requests)
	}
	if code < 200 || code > 299 {
		w.WriteHeader(code)
		return
	}

	// The body is newline-delimited JSON, one event per line.
	var events []map[string]any
	dec := json.NewDecoder(r.Body)
	for {
		var e map[string]any
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			s.t.Errorf("invalid CLEF body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		events = append(events, e)
	}
	s.events = append(s.events, events...)
	w.WriteHeader(code)
}

// Requests returns the number of requests received so far.
func (s *seqServer) Requests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests
}

// Events returns the events accepted so far, in the order they arrived.
func (s *seqServer) Events() []map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.events)
}

// newServerLogger returns a logger that sends to srv. Close the handler to
// flush the events. The handler is also closed when the test ends.
func newServerLogger(t *testing.T, srv *seqServer, opts ...SeqOption) (*slog.Logger, *SeqHandler) {
	opts = append([]SeqOption{WithHTTPClient(srv.Client())}, opts...)
	logger, handler := NewLogger(srv.URL+"/ingest/clef", opts...)
	t.Cleanup(func() { handler.Close() })
	return logger, handler
}

// newUnstartedHandler returns a handler whose workers aren't running, so the
// events it builds stay queued for the test to inspect with nextEvent.
func newUnstartedHandler(opts ...SeqOption) *SeqHandler {
	handler := newSeqHandler("http://fake")
	for _, opt := range opts {
		handler = opt.apply(handler)
	}
	handler.applyDefaults()
	handler.setup()
	return handler
}

// nextEvent returns the next event queued on the handler's first worker.
func nextEvent(t *testing.T, h *SeqHandler) CLEFEvent {
	t.Helper()
	select {
	case evt := <-h.workers[0].eventsCh:
		return evt
	default:
		require.FailNow(t, "no event was queued")
		return CLEFEvent{}
	}
}
