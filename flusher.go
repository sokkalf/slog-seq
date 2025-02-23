package slogseq

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func (h *SeqHandler) runBackgroundFlusher() {
    defer h.state.wg.Done()

    ticker := time.NewTicker(h.flushInterval)
    defer ticker.Stop()

    events := make([]CLEFEvent, 0, h.batchSize)

    for {
        select {
        case e, ok := <-h.state.eventsCh:
            if !ok {
                if len(events) > 0 {
                    if len(h.retryBuffer) > 0 {
                        leftover := h.sendWithRetry(h.retryBuffer)
                        h.retryBuffer = leftover
                    }
                    leftover := h.sendWithRetry(events)
                    if leftover != nil {
                        h.retryBuffer = append(h.retryBuffer, leftover...)
                    }
                }
                return
            }
            events = append(events, e)
            if len(events) >= h.batchSize {
                h.flushCurrentBatch(&events)
            }

        case <-ticker.C:
            if len(events) > 0 {
                h.flushCurrentBatch(&events)
            }

        case <-h.state.doneCh:
            if len(events) > 0 {
                h.flushCurrentBatch(&events)
            }
            return
        }
    }
}

func (h *SeqHandler) flushCurrentBatch(events *[]CLEFEvent) {
    if len(h.retryBuffer) > 0 {
        leftover := h.sendWithRetry(h.retryBuffer)
        h.retryBuffer = leftover
    }
    leftover := h.sendWithRetry(*events)

    if leftover != nil {
        h.retryBuffer = append(h.retryBuffer, leftover...)
    }
    *events = (*events)[:0]
}

func (h *SeqHandler) attemptSendBatch(events []CLEFEvent) bool {
	if len(events) == 0 {
		return true
	}

	var sb strings.Builder
	enc := json.NewEncoder(&sb)
	for _, e := range events {
		topLevel := map[string]interface{}{
			"@t": e.Timestamp.Format(time.RFC3339Nano),
			"@m": e.Message,
			"@l": e.Level,
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
		for k, v := range e.Properties {
			topLevel[k] = v
		}
		if err := enc.Encode(topLevel); err != nil {
			// Return false => indicates we should retry
			return false
		}
	}

	req, err := http.NewRequest("POST", h.seqURL, strings.NewReader(sb.String()))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/vnd.serilog.clef")
	if h.apiKey != "" {
		req.Header.Set("X-Seq-ApiKey", h.apiKey)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
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
