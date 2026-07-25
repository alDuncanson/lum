package cli

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/alDuncanson/lum/dispatcher/internal/apiclient"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
)

const topRecentEventsShown = 15

// minRateWindow avoids reporting an absurd rate the instant lum top
// starts (one document ingested in 20ms is not "3000 docs/min").
const minRateWindow = 5 * time.Second

// topCmd is a real-time, htop-style view of the ingest pipeline. It is a
// pure REST client like every other command — a plain SSE consumer of
// GET /v1/events through internal/apiclient — so anything it can show, a
// curl user watching the same endpoint could compute too. All semantics
// live in the event schema (#19); this file only renders it.
func topCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "top",
		Short: "Real-time view of the ingest pipeline (scans, documents, worker)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ch, err := apiclient.New().Events(cmd.Context(), nil)
			if err != nil {
				return fmt.Errorf("connecting to lum daemon: %w", err)
			}
			program := tea.NewProgram(newTopModel(ch), tea.WithAltScreen())
			_, err = program.Run()
			return err
		},
	}
}

type topEventMsg events.Event
type topStreamClosedMsg struct{}
type topTickMsg time.Time

// topModel holds only what's needed to render the last known state; it
// never computes anything the server hasn't already told it (see the
// package doc comment above).
type topModel struct {
	events <-chan events.Event
	start  time.Time

	workerState    string
	workerDetail   string
	sources        int
	documents      int
	chunks         int
	ingestFailures int
	pendingScans   int
	pendingDocs    int
	activeDocument string
	activeStage    string
	// sawLiveDocumentActivity switches pendingDocs from snapshot-fed to
	// event-fed once any document lifecycle event arrives: a live count
	// tracks batches that start and finish faster than the periodic
	// snapshot's 2s cadence, which would otherwise be missed entirely.
	sawLiveDocumentActivity bool

	lastBatchTookMS int64
	lastBatchDocs   int
	lastScanTookMS  int64
	lastError       string

	ingestedTotal int
	chunksTotal   int
	failedTotal   int

	recent []events.Event

	quitting bool
}

func newTopModel(ch <-chan events.Event) topModel {
	return topModel{events: ch, start: time.Now()}
}

func (m topModel) Init() tea.Cmd {
	return tea.Batch(waitForTopEvent(m.events), tickTop())
}

func waitForTopEvent(ch <-chan events.Event) tea.Cmd {
	return func() tea.Msg {
		e, ok := <-ch
		if !ok {
			return topStreamClosedMsg{}
		}
		return topEventMsg(e)
	}
}

func tickTop() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return topTickMsg(t) })
}

func (m topModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			m.quitting = true
			return m, tea.Quit
		}
	case topStreamClosedMsg:
		m.quitting = true
		m.lastError = "connection to lum daemon closed"
		return m, tea.Quit
	case topTickMsg:
		return m, tickTop()
	case topEventMsg:
		m.apply(events.Event(msg))
		return m, waitForTopEvent(m.events)
	}
	return m, nil
}

// apply folds one event into display state. It is the only place that
// interprets Kind, kept separate from Update so it's trivially testable
// without a running tea.Program.
func (m *topModel) apply(e events.Event) {
	switch e.Kind {
	case events.KindSnapshot:
		m.workerState = e.WorkerState
		m.workerDetail = e.Detail
		m.sources, m.documents, m.chunks, m.ingestFailures = e.Sources, e.Documents, e.Chunks, e.IngestFailures
		m.pendingScans = e.PendingScans
		// Documents in flight and the active document are tracked live
		// below (queued/resolved events), since a periodic 2s snapshot can
		// miss a batch that starts and finishes well inside that window.
		// The snapshot only seeds these before the first live event
		// arrives — e.g. connecting mid-scan, where it's the only signal
		// available for whatever was already in flight.
		if !m.sawLiveDocumentActivity {
			m.pendingDocs = e.PendingDocuments
			m.activeDocument, m.activeStage = e.ActiveDocument, e.ActiveStage
		}
	case events.KindWorkerStateChanged:
		m.workerState = e.WorkerState
		m.workerDetail = e.Detail
	case events.KindDocumentQueued:
		m.sawLiveDocumentActivity = true
		m.pendingDocs++
	case events.KindDocumentReading:
		m.activeDocument, m.activeStage = e.URI, "reading"
	case events.KindDocumentEmbedding:
		m.activeDocument, m.activeStage = e.URI, "embedding"
	case events.KindDocumentIngested:
		m.ingestedTotal++
		m.chunksTotal += e.ChunkCount
		m.resolveDocument()
	case events.KindDocumentFailed:
		m.failedTotal++
		m.lastError = fmt.Sprintf("%s: %s", e.URI, e.Error)
		m.resolveDocument()
	case events.KindDocumentDeleted:
		m.resolveDocument()
	case events.KindScanFinished:
		m.lastScanTookMS = e.TookMS
		if e.Error != "" {
			m.lastError = fmt.Sprintf("scan %s: %s", e.SourceID, e.Error)
		}
	case events.KindRPCCompleted:
		if e.Transport == "grpc" && e.Method == "IngestBatch" {
			m.lastBatchTookMS = e.TookMS
		}
	}

	switch e.Kind {
	case events.KindScanStarted, events.KindScanFinished,
		events.KindDocumentQueued, events.KindDocumentReading, events.KindDocumentEmbedding,
		events.KindDocumentIngested, events.KindDocumentFailed, events.KindDocumentDeleted,
		events.KindWorkerStateChanged:
		m.recent = append(m.recent, e)
		if len(m.recent) > topRecentEventsShown {
			m.recent = m.recent[len(m.recent)-topRecentEventsShown:]
		}
	}
}

// resolveDocument accounts for one document leaving the pipeline
// (ingested, failed, or deleted): decrements the live in-flight count and
// clears the active indicator once nothing is left, rather than waiting
// on the next periodic snapshot to notice the pipeline went idle.
func (m *topModel) resolveDocument() {
	if m.pendingDocs > 0 {
		m.pendingDocs--
	}
	if m.pendingDocs == 0 {
		m.activeDocument, m.activeStage = "", ""
	}
}

var (
	topHeaderStyle = lipgloss.NewStyle().Bold(true)
	topDimStyle    = lipgloss.NewStyle().Faint(true)
	topErrorStyle  = lipgloss.NewStyle().Bold(true)
)

func (m topModel) View() string {
	if m.quitting {
		return ""
	}
	var b strings.Builder

	fmt.Fprintf(&b, "%s  %s\n\n", topHeaderStyle.Render("lum top"), topDimStyle.Render("q to quit"))

	workerLine := m.workerState
	if m.workerDetail != "" {
		workerLine += " (" + m.workerDetail + ")"
	}
	fmt.Fprintf(&b, "worker:      %s\n", workerLine)
	fmt.Fprintf(&b, "index:       %d sources, %d documents, %d chunks, %d failures\n",
		m.sources, m.documents, m.chunks, m.ingestFailures)
	fmt.Fprintf(&b, "queue:       %d scans pending, %d documents pending\n", m.pendingScans, m.pendingDocs)

	active := "(idle)"
	if m.activeDocument != "" {
		active = fmt.Sprintf("%s (%s)", m.activeDocument, m.activeStage)
	}
	fmt.Fprintf(&b, "active:      %s\n\n", active)

	elapsed := time.Since(m.start)
	if elapsed < minRateWindow {
		fmt.Fprintf(&b, "rates (since lum top started): warming up..., %d failed\n", m.failedTotal)
	} else {
		docsPerMin := float64(m.ingestedTotal) / elapsed.Minutes()
		chunksPerMin := float64(m.chunksTotal) / elapsed.Minutes()
		fmt.Fprintf(&b, "rates (since lum top started): %.1f docs/min, %.1f chunks/min, %d failed\n",
			docsPerMin, chunksPerMin, m.failedTotal)
	}
	if m.lastBatchTookMS > 0 {
		fmt.Fprintf(&b, "last embed batch: %dms\n", m.lastBatchTookMS)
	}
	if m.lastScanTookMS > 0 {
		fmt.Fprintf(&b, "last scan: %dms\n", m.lastScanTookMS)
	}
	if m.lastError != "" {
		fmt.Fprintf(&b, "last error: %s\n", topErrorStyle.Render(m.lastError))
	}

	b.WriteString("\nrecent events\n")
	if len(m.recent) == 0 {
		b.WriteString(topDimStyle.Render("  (none yet)\n"))
	}
	for i := len(m.recent) - 1; i >= 0; i-- {
		e := m.recent[i]
		b.WriteString(topDimStyle.Render(e.Time.Format("15:04:05")))
		fmt.Fprintf(&b, "  %-22s %s\n", e.Kind, recentEventDetail(e))
	}
	return b.String()
}

func recentEventDetail(e events.Event) string {
	switch e.Kind {
	case events.KindScanStarted:
		return "source " + e.SourceID
	case events.KindScanFinished:
		detail := fmt.Sprintf("source %s (ingested=%d unchanged=%d removed=%d failed=%d)",
			e.SourceID, e.Ingested, e.Unchanged, e.Removed, e.Failed)
		if e.Error != "" {
			detail += ": " + e.Error
		}
		return detail
	case events.KindWorkerStateChanged:
		return fmt.Sprintf("%s -> %s", e.FromState, e.WorkerState)
	case events.KindDocumentFailed:
		return e.URI + ": " + e.Error
	default:
		if e.URI != "" {
			return e.URI
		}
		return e.DocumentID
	}
}
