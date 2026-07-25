package apiv1

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSearchEnvelopeJSONContract(t *testing.T) {
	raw, err := json.Marshal(SearchEnvelope{Query: "needle", Results: []SearchResult{{
		DocumentID: "doc", SourceID: "source", URI: "/repo/main.go", ChunkIndex: 2,
		Score: .75, Text: "match", StartLine: 11, EndLine: 19,
	}}})
	if err != nil {
		t.Fatal(err)
	}
	got := string(raw)
	for _, field := range []string{`"query":"needle"`, `"document_id":"doc"`, `"source_id":"source"`, `"chunk_index":2`, `"start_line":11`, `"end_line":19`} {
		if !strings.Contains(got, field) {
			t.Errorf("JSON %s does not contain %s", got, field)
		}
	}
}
