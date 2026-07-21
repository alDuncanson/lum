package cli

import (
	"context"
	"time"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
)

const snapshotInterval = 2 * time.Second

// runSnapshotLoop is the one place that assembles a systemwide gauge
// reading: it doesn't reach into lumen for extra detail (see events'
// KindRPCCompleted doc comment) — everything here is either already
// tracked by the ingestor or a side-effect-free Health call. It also
// detects data-plane readiness transitions, since nothing else watches
// for those between requests.
func runSnapshotLoop(ctx context.Context, bus *events.Bus, cat *catalog.Catalog, ing *ingest.Ingestor, dp dataplane.DataPlane) {
	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	var lastState string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lastState = publishSnapshot(ctx, bus, cat, ing, dp, lastState)
		}
	}
}

func publishSnapshot(
	ctx context.Context, bus *events.Bus, cat *catalog.Catalog, ing *ingest.Ingestor, dp dataplane.DataPlane, lastState string,
) string {
	health, _ := dp.Health(ctx)
	if lastState != "" && string(health.State) != lastState {
		bus.Publish(events.Event{
			Kind:      events.KindDataPlaneStateChanged,
			FromState: lastState, DataPlaneState: string(health.State), Detail: health.Detail,
		})
	}

	pendingScans, pendingDocuments := ing.QueueDepth()
	activeDocument, activeStage := ing.ActiveWork()
	stats, _ := cat.Stats(ctx)
	bus.Publish(events.Event{
		Kind:           events.KindSnapshot,
		DataPlaneState: string(health.State), Detail: health.Detail,
		PendingScans: pendingScans, PendingDocuments: pendingDocuments,
		ActiveDocument: activeDocument, ActiveStage: activeStage,
		Sources: stats.Sources, Documents: stats.Documents, Chunks: stats.Chunks, IngestFailures: stats.Failures,
	})
	return string(health.State)
}
