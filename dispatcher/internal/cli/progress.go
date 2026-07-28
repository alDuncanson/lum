package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/alDuncanson/lum/dispatcher/internal/apiclient"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
)

// Some commands block for a long time on a first run and, until now, said
// nothing while they did. `lum search --root .` on an unindexed repository
// waits for a model download and a full embed — twenty seconds on a small
// repository, minutes on a large one — with no output at all, which is
// indistinguishable from a hang. `lum remove` on a large source spends ten
// seconds deleting vectors, equally silently.
//
// The daemon already publishes exactly what is happening, because the Neovim
// integration needed it. This subscribes to the same stream and draws one
// self-erasing line.
//
// Two rules keep it from breaking anything:
//
//   - It writes to stderr, never stdout. `--json` and `--jsonl` are parsed by
//     other programs, and a spinner in that stream would corrupt it.
//   - It draws only to a terminal. Redirected into a file or a pipe, a
//     progress bar is noise, and \r animation is worse than noise.

const (
	spinnerInterval = 100 * time.Millisecond
	barWidth        = 14
)

var spinnerFrames = []rune{'⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'}

// activity is what the event stream has said so far.
type activity struct {
	workerState string
	phase       string
	phaseDone   uint64
	phaseTotal  uint64
	phaseUnit   string
	queued      int
	deleted     int
	failed      int
}

// line composes the current activity into one line, or "" when there is
// nothing worth saying. Exposed for testing: these rules are the design.
func (a activity) line() string {
	var text string
	switch {
	case a.workerState == "downloading-model":
		// The one that takes minutes on a first run, and the one a user is
		// most likely to read as a hang.
		text = "downloading the embedding model (~70 MB, first run)"
	case a.phase != "" && a.phaseTotal > 0:
		text = fmt.Sprintf("%s %s %d/%d %s",
			a.phase, bar(a.phaseDone, a.phaseTotal), a.phaseDone, a.phaseTotal, a.phaseUnit)
	case a.phase != "":
		text = a.phase
	case a.deleted > 0:
		text = fmt.Sprintf("removed %s", plural(a.deleted, "document"))
	case a.queued > 0:
		text = fmt.Sprintf("indexing %s", plural(a.queued, "file"))
	case a.workerState == "starting":
		text = "starting the worker"
	default:
		return ""
	}
	if a.failed > 0 {
		text += fmt.Sprintf(" · %s failed", plural(a.failed, "file"))
	}
	return strings.TrimSpace(text)
}

func bar(done, total uint64) string {
	if total == 0 {
		return ""
	}
	filled := int(float64(done) / float64(total) * barWidth)
	if filled > barWidth {
		filled = barWidth
	}
	return "▕" + strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled) + "▏"
}

// reporter draws one line and erases it when finished.
type reporter struct {
	out     io.Writer
	mu      sync.Mutex
	last    string
	stopped bool
}

func (r *reporter) draw(frame rune, text string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped || text == "" {
		return
	}
	rendered := fmt.Sprintf("%c %s", frame, text)
	// \r to the start, then erase to end of line: without the erase, a shorter
	// line leaves the tail of a longer one behind.
	fmt.Fprintf(r.out, "\r\033[K%s", rendered)
	r.last = rendered
}

// clear removes the line, leaving the terminal as it was. The command's own
// output should not have to share a line with a spinner that is now stale.
func (r *reporter) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stopped = true
	if r.last != "" {
		fmt.Fprint(r.out, "\r\033[K")
		r.last = ""
	}
}

// progressEnabled reports whether a progress line should be drawn at all.
func progressEnabled(stderr *os.File, quiet bool) bool {
	if quiet {
		return false
	}
	// A terminal, rather than a pipe or a file.
	info, err := stderr.Stat()
	if err != nil || info.Mode()&os.ModeCharDevice == 0 {
		return false
	}
	// TERM=dumb means "no cursor control", which is exactly what this needs.
	if term := os.Getenv("TERM"); term == "" || term == "dumb" {
		return false
	}
	return true
}

// startProgress draws lum's activity on stderr until the returned function is
// called. Safe to call when nothing will be drawn; the returned function is
// always non-nil and idempotent.
//
// Subscribing starts the daemon if it is not running, which is wanted here:
// every caller is about to make it work anyway, and having it up first means
// the download or the scan is visible from its beginning rather than joined
// halfway through.
func startProgress(ctx context.Context, quiet bool, kinds []events.Kind) func() {
	if !progressEnabled(os.Stderr, quiet) {
		return func() {}
	}
	types := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		types = append(types, string(kind))
	}

	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := apiclient.New().EventsLive(streamCtx, types)
	if err != nil {
		// Progress is advisory. A command that cannot subscribe should still
		// run, silently, rather than fail over the absence of a spinner.
		cancel()
		return func() {}
	}

	rep := &reporter{out: os.Stderr}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(spinnerInterval)
		defer ticker.Stop()
		var state activity
		frame := 0
		for {
			select {
			case <-streamCtx.Done():
				return
			case event, ok := <-stream:
				if !ok {
					return
				}
				state.apply(event)
			case <-ticker.C:
				frame = (frame + 1) % len(spinnerFrames)
				rep.draw(spinnerFrames[frame], state.line())
			}
		}
	}()

	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			wg.Wait()
			rep.clear()
		})
	}
}

// apply folds one event into the running state.
func (a *activity) apply(event events.Event) {
	switch event.Kind {
	case events.KindSnapshot, events.KindWorkerStateChanged:
		a.workerState = event.WorkerState
	case events.KindWorkerProgress:
		a.phase, a.phaseDone, a.phaseTotal, a.phaseUnit = event.Phase, event.Done, event.Total, event.Unit
	case events.KindDocumentQueued:
		a.queued++
	case events.KindDocumentDeleted:
		a.deleted++
	case events.KindDocumentFailed:
		a.failed++
	case events.KindScanFinished:
		// The scan this was reporting on is over; anything still on screen
		// describes work that has finished.
		a.phase, a.phaseDone, a.phaseTotal, a.phaseUnit = "", 0, 0, ""
		a.queued = 0
	}
}

// indexKinds is what a command that waits for indexing needs to see.
var indexKinds = []events.Kind{
	events.KindSnapshot,
	events.KindWorkerStateChanged,
	events.KindWorkerProgress,
	events.KindDocumentQueued,
	events.KindDocumentFailed,
	events.KindScanFinished,
}

// deleteKinds is what a command that waits for a source deletion needs.
var deleteKinds = []events.Kind{
	events.KindSnapshot,
	events.KindWorkerStateChanged,
	events.KindDocumentDeleted,
	events.KindDocumentFailed,
}
