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
        case <-h.state.doneCh:
            // flush any remaining events before exiting
            if len(events) > 0 {
                h.sendBatch(events)
            }
            return
        case e := <-h.state.eventsCh:
            events = append(events, e)
            if len(events) >= h.batchSize {
                h.sendBatch(events)
                events = events[:0]
            }
        case <-ticker.C:
            if len(events) > 0 {
                h.sendBatch(events)
                events = events[:0]
            }
        }
    }
}

func (h *SeqHandler) sendBatch(events []CLEFEvent) {
    if len(events) == 0 {
        return
    }

    var sb strings.Builder
    encoder := json.NewEncoder(&sb)

    for _, e := range events {
		// create CLEF data
        topLevel := map[string]interface{}{
            "@t": e.Timestamp.Format(time.RFC3339Nano),
            "@m": e.Message,
            "@l": e.Level,
        }
        for k, v := range e.Properties {
            topLevel[k] = v
        }
        if err := encoder.Encode(topLevel); err != nil {
            // handle error
        }
    }

    req, err := http.NewRequest("POST", h.seqURL, strings.NewReader(sb.String()))
    if err != nil {
        // handle error
        return
    }
    req.Header.Set("Content-Type", "application/vnd.serilog.clef")
    if h.apiKey != "" {
        req.Header.Set("X-Seq-ApiKey", h.apiKey)
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        // handle error, maybe retry or drop
        return
    }
    defer resp.Body.Close()

    if resp.StatusCode < 200 || resp.StatusCode > 299 {
        // handle non-2xx response
    }
}
