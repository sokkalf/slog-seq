package slogseq

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/http"
	"strings"
	"time"
)

func (h *SeqHandler) runBackgroundFlusher(w *worker) {
	defer w.wg.Done()
	if h.noFlush { // Used in tests
		return
	}

	ticker := time.NewTicker(h.flushInterval)
	defer ticker.Stop()

	purgeInterval := h.flushInterval * 60
	w.purgeTicker = time.NewTicker(purgeInterval)
	defer w.purgeTicker.Stop()

	events := make([]CLEFEvent, 0, h.batchSize)

	for {
		select {
		case e, ok := <-w.eventsCh:
			if !ok {
				if len(events) > 0 {
					if len(w.retryBuffer) > 0 {
						leftover := h.sendWithRetry(w.retryBuffer)
						w.retryBuffer = leftover
					}
					leftover := h.sendWithRetry(events)
					if leftover != nil {
						w.retryBuffer = append(w.retryBuffer, leftover...)
					}
				}
				return
			}
			events = append(events, e)
			if len(events) >= h.batchSize {
				h.flushCurrentBatch(w, &events)
			}

		case <-ticker.C:
			if len(events) > 0 {
				h.flushCurrentBatch(w, &events)
			}

		case <-w.purgeTicker.C:
			// Purge events older than 5 minutes from retry buffer
			cutoff := time.Now().Add(-5 * time.Minute)
			h.purgeOldEvents(w, cutoff)

		case <-w.doneCh:
			if len(events) > 0 {
				h.flushCurrentBatch(w, &events)
			}
			return
		}
	}
}

func (h *SeqHandler) flushCurrentBatch(w *worker, events *[]CLEFEvent) {
	if len(w.retryBuffer) > 0 {
		leftover := h.sendWithRetry(w.retryBuffer)
		w.retryBuffer = leftover
	}
	leftover := h.sendWithRetry(*events)

	if leftover != nil {
		w.retryBuffer = append(w.retryBuffer, leftover...)
	}
	*events = (*events)[:0]
}

func encodeEvent(e CLEFEvent) map[string]any {
	topLevel := map[string]any{
		"@t": e.Timestamp.Format(time.RFC3339Nano),
		"@m": e.Message,
		"@l": e.Level,
	}
	if e.Exception != "" {
		topLevel["@x"] = e.Exception
	}
	if !e.SpanStart.IsZero() {
		topLevel["@st"] = e.SpanStart.Format(time.RFC3339Nano)
	}
	if e.TraceID != "" {
		topLevel["@tr"] = e.TraceID
	}
	if e.SpanID != "" {
		topLevel["@sp"] = e.SpanID
	}
	if e.ParentSpanID != "" {
		topLevel["@ps"] = e.ParentSpanID
	}
	if len(e.ResourceAttributes) > 0 {
		topLevel["@ra"] = e.ResourceAttributes
	}
	if e.SpanKind != "" {
		topLevel["@sk"] = e.SpanKind
	}
	maps.Copy(topLevel, e.Properties)
	return topLevel
}

func (h *SeqHandler) attemptSendBatch(events []CLEFEvent) bool {
	if len(events) == 0 {
		return true
	}

	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, e := range events {
		topLevel := encodeEvent(e)
		if err := enc.Encode(topLevel); err != nil {
			// Return false => indicates we should retry
			return false
		}
	}

	req, err := http.NewRequest("POST", h.seqURL, strings.NewReader(sb.String()))
	if err != nil {
		h.errorHandlerFunc(err)
		return false
	}
	req.Header.Set("Content-Type", "application/vnd.serilog.clef")
	if h.apiKey != "" {
		req.Header.Set("X-Seq-ApiKey", h.apiKey)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		h.errorHandlerFunc(err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		h.errorHandlerFunc(fmt.Errorf("Seq server returned status code %d", resp.StatusCode))
		return false
	}

	// Success
	return true
}

func (h *SeqHandler) sendWithRetry(events []CLEFEvent) []CLEFEvent {
	if len(events) == 0 {
		return nil
	}
	success := h.attemptSendBatch(events)
	if success {
		return nil // nothing left to retry
	}
	return events
}

func (h *SeqHandler) purgeOldEvents(w *worker, olderThan time.Time) {
	newBuf := w.retryBuffer[:0]
	for _, e := range w.retryBuffer {
		if e.Timestamp.After(olderThan) {
			newBuf = append(newBuf, e)
		}
	}
	w.retryBuffer = newBuf
}

func newHttpClient(skipVerify bool) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
			}).DialContext,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: skipVerify,
			},
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
		},
	}
}
