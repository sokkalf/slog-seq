package slogseq

import "time"

// Compact Log Event Format (CLEF) is a JSON-based log event format that Seq uses.
// https://clef-json.org
type CLEFEvent struct {
	Timestamp          time.Time      `json:"@t,omitzero"`
	Message            string         `json:"@m,omitempty"`
	Exception          string         `json:"@x,omitempty"`
	Level              string         `json:"@l,omitempty"`
	Properties         map[string]any `json:"-"`
	TraceID            string         `json:"@tr,omitempty"`
	SpanID             string         `json:"@sp,omitempty"`
	SpanStart          time.Time      `json:"@st,omitzero"`
	SpanKind           string         `json:"@sk,omitempty"`
	ResourceAttributes map[string]any `json:"@ra,omitempty,omitzero"`
	ParentSpanID       string         `json:"@ps,omitempty"`
}

type CLEFLevel string

const (
	CLEFLevelDebug       CLEFLevel = "Debug"
	CLEFLevelVerbose     CLEFLevel = "Verbose"
	CLEFLevelInformation CLEFLevel = "Information"
	CLEFLevelWarning     CLEFLevel = "Warning"
	CLEFLevelError       CLEFLevel = "Error"
	CLEFLevelFatal       CLEFLevel = "Fatal"
)

func (l CLEFLevel) String() string {
	return string(l)
}
