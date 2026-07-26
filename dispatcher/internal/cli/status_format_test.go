package cli

import (
	"testing"

	"github.com/alDuncanson/lum/dispatcher/internal/apiv1"
)

func TestActivityLineIsEmptyWhenNothingIsHappening(t *testing.T) {
	// Silence is the common case — a warm daemon with nothing to do — and
	// printing "indexing:" with nothing after it would be worse than nothing.
	if got := activityLine(apiv1.Activity{}); got != "" {
		t.Fatalf("activityLine(zero) = %q, want empty", got)
	}
}

func TestActivityLineReportsWorkInFlight(t *testing.T) {
	// The reason this exists: during a first index every count in `lum status`
	// reads zero for a minute, which looks exactly like being stuck.
	got := activityLine(apiv1.Activity{
		Document:         "/repo/README.md",
		Stage:            "embedding",
		PendingDocuments: 4,
		PendingScans:     1,
	})
	want := "embedding /repo/README.md, 4 documents queued, 1 scan queued"
	if got != want {
		t.Fatalf("activityLine() = %q, want %q", got, want)
	}
}

func TestActivityLineHandlesAQueueWithNoActiveDocument(t *testing.T) {
	got := activityLine(apiv1.Activity{PendingScans: 2})
	if got != "2 scans queued" {
		t.Fatalf("activityLine() = %q", got)
	}
}
