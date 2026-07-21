package cli

import (
	"context"
	"time"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
	"github.com/alDuncanson/lum/control-plane/internal/snapshot"
)

const snapshotInterval = 2 * time.Second

// runSnapshotLoop periodically publishes a systemwide gauge reading and
// detects data-plane readiness transitions between requests — nothing
// else watches for those while the pipeline is otherwise idle.
func runSnapshotLoop(ctx context.Context, bus *events.Bus, cat *catalog.Catalog, ing *ingest.Ingestor, dp dataplane.DataPlane) {
	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	var lastState string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := snapshot.Build(ctx, cat, ing, dp)
			if lastState != "" && event.DataPlaneState != lastState {
				bus.Publish(events.Event{
					Kind:      events.KindDataPlaneStateChanged,
					FromState: lastState, DataPlaneState: event.DataPlaneState, Detail: event.Detail,
				})
			}
			lastState = event.DataPlaneState
			bus.Publish(event)
		}
	}
}
