package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/alDuncanson/lum/dispatcher/internal/events"
)

func TestProgressLineIsEmptyWhenNothingIsHappening(t *testing.T) {
	// Silence must stay silent: a spinner with no text next to it is worse
	// than no spinner.
	if got := (activity{}).line(); got != "" {
		t.Fatalf("line() = %q, want empty", got)
	}
}

func TestProgressLineLeadsWithTheModelDownload(t *testing.T) {
	// The wait most likely to be read as a hang, so it wins over anything
	// else the stream happens to be saying.
	a := activity{workerState: "downloading-model", queued: 40}
	got := a.line()
	if !strings.Contains(got, "downloading the embedding model") {
		t.Fatalf("line() = %q", got)
	}
}

func TestProgressLineShowsPhaseWithABar(t *testing.T) {
	a := activity{phase: "embedding", phaseDone: 128, phaseTotal: 256, phaseUnit: "chunks"}
	got := a.line()
	if !strings.HasPrefix(got, "embedding ▕") || !strings.HasSuffix(got, "128/256 chunks") {
		t.Fatalf("line() = %q", got)
	}
	// Half done should be half filled.
	if strings.Count(got, "█") != barWidth/2 {
		t.Fatalf("bar not half full: %q", got)
	}
}

func TestProgressLineFallsBackToCoarserFacts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state activity
		want  string
	}{
		{"queued files", activity{queued: 3}, "indexing 3 files"},
		{"one file", activity{queued: 1}, "indexing 1 file"},
		{"deletions", activity{deleted: 12}, "removed 12 documents"},
		{"worker starting", activity{workerState: "starting"}, "starting the worker"},
		{"phase with no total", activity{phase: "storing"}, "storing"},
	} {
		if got := tc.state.line(); got != tc.want {
			t.Errorf("%s: line() = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestProgressLineAppendsFailures(t *testing.T) {
	a := activity{queued: 5, failed: 2}
	if got := a.line(); !strings.HasSuffix(got, "· 2 files failed") {
		t.Fatalf("line() = %q", got)
	}
}

func TestScanFinishedClearsWorkThatIsOver(t *testing.T) {
	// Otherwise the last thing drawn describes finished work and stays on
	// screen looking live.
	a := activity{phase: "embedding", phaseDone: 10, phaseTotal: 10, queued: 4}
	a.apply(events.Event{Kind: events.KindScanFinished})
	if got := a.line(); got != "" {
		t.Fatalf("line() = %q after scan_finished, want empty", got)
	}
}

func TestApplyFoldsTheStream(t *testing.T) {
	var a activity
	for _, event := range []events.Event{
		{Kind: events.KindWorkerStateChanged, WorkerState: "downloading-model"},
		{Kind: events.KindWorkerStateChanged, WorkerState: "ready"},
		{Kind: events.KindDocumentQueued},
		{Kind: events.KindDocumentQueued},
		{Kind: events.KindDocumentFailed},
		{Kind: events.KindWorkerProgress, Phase: "embedding", Done: 5, Total: 20, Unit: "chunks"},
	} {
		a.apply(event)
	}
	if a.workerState != "ready" || a.queued != 2 || a.failed != 1 {
		t.Fatalf("unexpected state: %+v", a)
	}
	if got := a.line(); !strings.Contains(got, "embedding") || !strings.Contains(got, "5/20 chunks") {
		t.Fatalf("line() = %q", got)
	}
}

func TestProgressIsDisabledWhereItWouldCorruptOutput(t *testing.T) {
	// The important case: a pipe. `lum search --jsonl | jq` must not receive
	// cursor-control sequences, and a progress bar in a log file is noise.
	pipeRead, pipeWrite, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer pipeRead.Close()
	defer pipeWrite.Close()

	t.Setenv("TERM", "xterm-256color")
	if progressEnabled(pipeWrite, false) {
		t.Error("a pipe is not a terminal")
	}

	file, err := os.CreateTemp(t.TempDir(), "stderr")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if progressEnabled(file, false) {
		t.Error("a regular file is not a terminal")
	}
}

func TestQuietAndDumbTerminalsDisableProgress(t *testing.T) {
	// Not a terminal either way here, but assert the guards independently so
	// they cannot rot: --quiet must win, and TERM=dumb has no cursor control.
	device, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Close()

	t.Setenv("TERM", "xterm-256color")
	if progressEnabled(device, true) {
		t.Error("--quiet must disable progress")
	}
	t.Setenv("TERM", "dumb")
	if progressEnabled(device, false) {
		t.Error("TERM=dumb must disable progress")
	}
}

func TestBarSaturates(t *testing.T) {
	// done can exceed total if a report arrives out of order; the bar must
	// not run off the end.
	if got := bar(30, 10); strings.Count(got, "█") != barWidth {
		t.Fatalf("bar(30,10) = %q", got)
	}
	if got := bar(0, 0); got != "" {
		t.Fatalf("bar(0,0) = %q, want empty", got)
	}
}
