// Package snapshot assembles the one systemwide gauge reading used by
// both the periodic event-bus heartbeat (cli) and a newly connecting SSE
// subscriber (api), which is why it lives apart from either.
package snapshot

import (
	"context"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
)

// Build assembles the current point-in-time reading: scan queue depth,
// the document worker's current document and stage, data plane state,
// and index totals. It never reaches into lumen for extra detail — Health
// is the one side-effect-free call already available (dataplane.Manager
// never wakes a shed data plane just to be observed).
func Build(ctx context.Context, cat *catalog.Catalog, ing *ingest.Ingestor, dp dataplane.DataPlane) events.Event {
	health, _ := dp.Health(ctx)
	pendingScans, pendingDocuments := ing.QueueDepth()
	activeDocument, activeStage := ing.ActiveWork()
	stats, _ := cat.Stats(ctx)
	return events.Event{
		Kind:             events.KindSnapshot,
		DataPlaneState:   string(health.State),
		Detail:           health.Detail,
		PendingScans:     pendingScans,
		PendingDocuments: pendingDocuments,
		ActiveDocument:   activeDocument,
		ActiveStage:      activeStage,
		Sources:          stats.Sources,
		Documents:        stats.Documents,
		Chunks:           stats.Chunks,
		IngestFailures:   stats.Failures,
	}
}
