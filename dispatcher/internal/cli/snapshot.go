package cli

import (
	"context"
	"time"

	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
	"github.com/alDuncanson/lum/dispatcher/internal/ingest"
	"github.com/alDuncanson/lum/dispatcher/internal/snapshot"
	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

const snapshotInterval = 2 * time.Second

// runSnapshotLoop periodically publishes a systemwide gauge reading and
// detects worker readiness transitions between requests — nothing
// else watches for those while the pipeline is otherwise idle.
func runSnapshotLoop(ctx context.Context, bus *events.Bus, cat *catalog.Catalog, ing *ingest.Ingestor, dp worker.Interface) {
	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()
	var lastState string
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			event := snapshot.Build(ctx, cat, ing, dp)
			if lastState != "" && event.WorkerState != lastState {
				bus.Publish(events.Event{
					Kind:      events.KindWorkerStateChanged,
					FromState: lastState, WorkerState: event.WorkerState, Detail: event.Detail,
				})
			}
			lastState = event.WorkerState
			bus.Publish(event)
		}
	}
}
