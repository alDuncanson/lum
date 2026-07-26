// Package apiv1 owns the stable JSON contract exposed by lumd.
package apiv1

import "time"

type Source struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	URI       string    `json:"uri"`
	CreatedAt time.Time `json:"created_at"`
}

type SearchResult struct {
	DocumentID string  `json:"document_id"`
	SourceID   string  `json:"source_id"`
	URI        string  `json:"uri"`
	ChunkIndex uint32  `json:"chunk_index"`
	Score      float32 `json:"score"`
	Text       string  `json:"text"`
	StartLine  uint32  `json:"start_line"`
	EndLine    uint32  `json:"end_line"`
}

type SearchEnvelope struct {
	Query   string         `json:"query"`
	Results []SearchResult `json:"results"`
}

type AddSourceRequest struct {
	URI string `json:"uri"`
}
type AddSourceResponse struct {
	Source  Source `json:"source"`
	Created bool   `json:"created"`
}
type Stats struct {
	Sources   int `json:"sources"`
	Documents int `json:"documents"`
	Chunks    int `json:"chunks"`
	Failures  int `json:"failures"`
}
type IngestFailure struct {
	SourceID string    `json:"source_id"`
	URI      string    `json:"uri"`
	Attempts int       `json:"attempts"`
	Error    string    `json:"error"`
	FailedAt time.Time `json:"failed_at"`
}
type Status struct {
	Daemon   string          `json:"daemon"`
	Worker   string          `json:"worker"`
	Detail   string          `json:"detail,omitempty"`
	Stats    Stats           `json:"stats"`
	Failures []IngestFailure `json:"failures"`
	// What is happening right now, so `lum status` can answer "is it doing
	// anything?" — during a first index every count above reads zero for a
	// minute, which is indistinguishable from being stuck.
	Activity Activity `json:"activity"`
}

// Activity is the in-flight work at the moment status was asked.
type Activity struct {
	PendingScans     int    `json:"pending_scans"`
	PendingDocuments int    `json:"pending_documents"`
	Document         string `json:"document,omitempty"`
	Stage            string `json:"stage,omitempty"`
}
type Error struct {
	Error string `json:"error"`
}
type StatusResponse struct {
	Status string `json:"status"`
}
