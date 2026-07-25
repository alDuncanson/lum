// Package api implements lumd's REST API — the single front door of the
// system. The CLI, the MCP server, curl, and any future client all go
// through these endpoints; none of them get a privileged side channel.
//
// Endpoints (all JSON):
//
//	POST   /v1/sources        {"uri": "~/Documents"}     register + scan
//	GET    /v1/sources                                   list sources
//	POST   /v1/sources/{id}/scan                         trigger rescan
//	DELETE /v1/sources/{id}                              remove source + its vectors
//	GET    /v1/search?q=...&limit=10&source=<id>          semantic search
//	GET    /v1/status                                    daemon + worker health
//	GET    /v1/events[?types=k1,k2]                       SSE event stream
//	POST   /v1/shutdown                                   graceful daemon shutdown
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/alDuncanson/lum/dispatcher/internal/apiv1"
	"github.com/alDuncanson/lum/dispatcher/internal/catalog"
	"github.com/alDuncanson/lum/dispatcher/internal/events"
	"github.com/alDuncanson/lum/dispatcher/internal/ingest"
	"github.com/alDuncanson/lum/dispatcher/internal/requestid"
	"github.com/alDuncanson/lum/dispatcher/internal/snapshot"
	"github.com/alDuncanson/lum/dispatcher/internal/source"
	"github.com/alDuncanson/lum/dispatcher/internal/worker"
)

// Server wires HTTP handlers to the dispatcher's components.
type Server struct {
	catalog    *catalog.Catalog
	dp         worker.Interface
	ingestor   *ingest.Ingestor
	bus        *events.Bus
	onRequest  func()
	shutdownCh chan struct{}
	sourceMu   sync.Mutex
}

// New wires a Server. bus may be nil, in which case no events are published
// and GET /v1/events reports 503.
func New(cat *catalog.Catalog, dp worker.Interface, ing *ingest.Ingestor, bus *events.Bus) *Server {
	return &Server{catalog: cat, dp: dp, ingestor: ing, bus: bus, shutdownCh: make(chan struct{}, 1)}
}

// ShutdownRequested fires once a client POSTs /v1/shutdown. The daemon's
// main loop selects on it alongside the idle timer and OS signals so a
// requested shutdown goes through the exact same ordered teardown.
func (s *Server) ShutdownRequested() <-chan struct{} {
	return s.shutdownCh
}

// Handler builds the route table (Go 1.22+ method-aware patterns). onRequest
// reports activity to the daemon's idle timer before dispatch, and again
// periodically for as long as an /v1/events stream stays open (#13/#20): a
// connected observability client is exactly the kind of activity that
// idle timer exists to detect.
func (s *Server) Handler(onRequest func()) http.Handler {
	s.onRequest = onRequest
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/sources", s.handleAddSource)
	mux.HandleFunc("GET /v1/sources", s.handleListSources)
	mux.HandleFunc("POST /v1/sources/{id}/scan", s.handleScanSource)
	mux.HandleFunc("DELETE /v1/sources/{id}", s.handleDeleteSource)
	mux.HandleFunc("GET /v1/search", s.handleSearch)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
	mux.HandleFunc("POST /v1/shutdown", s.handleShutdown)
	return s.withRequestID(mux, onRequest)
}

// statusRecorder captures the response status for logging and the
// rpc_completed event; http.ResponseWriter has no accessor of its own.
// Flush is forwarded explicitly because embedding the http.ResponseWriter
// interface only promotes that interface's own methods — Flush belongs to
// the separate http.Flusher interface, so handleEvents' type assertion
// would otherwise fail on every request wrapped by this middleware.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *statusRecorder) Flush() {
	if flusher, ok := r.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (s *Server) withRequestID(next http.Handler, onRequest func()) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if onRequest != nil {
			onRequest()
		}
		ctx, id := requestid.New(r.Context())
		w.Header().Set(requestid.Header, id)
		started := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(recorder, r.WithContext(ctx))
		took := time.Since(started)
		slog.Info("HTTP request",
			"request_id", id,
			"method", r.Method,
			"path", r.URL.Path,
			"took", took.Round(time.Millisecond),
		)
		if s.bus != nil {
			s.bus.Publish(events.Event{
				Kind: events.KindRPCCompleted, RequestID: id, Transport: "http",
				Method: r.Method + " " + r.URL.Path, Code: strconv.Itoa(recorder.status),
				TookMS: took.Milliseconds(),
			})
		}
	})
}

// ---- handlers ----

func (s *Server) handleAddSource(w http.ResponseWriter, r *http.Request) {
	var req apiv1.AddSourceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URI == "" {
		httpError(w, http.StatusBadRequest, "body must be JSON like {\"uri\": \"~/Documents\"}")
		return
	}

	// Resolve validates the URI and canonicalizes it (absolute path),
	// so the same directory can't be registered twice under two names.
	src, canonicalURI, err := source.Resolve(req.URI)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Serialize the catalog insert with initial-attempt registration. Without
	// this, a concurrent ensure can observe the new row just before its creator
	// registers the daemon-owned attempt and incorrectly treat it as established.
	s.sourceMu.Lock()
	row, created, err := s.catalog.AddSource(r.Context(), catalog.Source{
		ID:        uuid.NewString(),
		Type:      src.Type(),
		URI:       canonicalURI,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		s.sourceMu.Unlock()
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// The initial attempt is registered before it is queued, so concurrent
	// ensure callers can reliably join it.
	if created {
		s.dp.EnsureRunning()
		s.ingestor.EnqueueInitialScan(r.Context(), row.ID)
		s.ingestor.WatchSource(row.ID)
	}
	s.sourceMu.Unlock()

	if r.URL.Query().Get("wait") == "initial" {
		if err := s.ingestor.WaitInitialScan(r.Context(), row.ID); err != nil {
			httpError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	writeJSON(w, http.StatusAccepted, apiv1.AddSourceResponse{Source: sourceDTO(row), Created: created})
}

func (s *Server) handleListSources(w http.ResponseWriter, r *http.Request) {
	sources, err := s.catalog.ListSources(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if sources == nil {
		sources = []catalog.Source{} // JSON [] instead of null
	}
	out := make([]apiv1.Source, 0, len(sources))
	for _, item := range sources {
		out = append(out, sourceDTO(item))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleScanSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.catalog.GetSource(r.Context(), id); err != nil {
		httpError(w, http.StatusNotFound, "no source with id "+id)
		return
	}
	s.ingestor.EnqueueScan(r.Context(), id)
	s.dp.EnsureRunning()
	writeJSON(w, http.StatusAccepted, apiv1.StatusResponse{Status: "scan queued"})
}

// handleDeleteSource removes a source and every vector it produced.
// Synchronous, unlike add/scan: a client that gets 200 back knows
// cleanup actually finished, and a partial failure (worker
// temporarily unavailable) leaves the source in place with a clear
// error rather than reporting success while vectors linger (#4).
func (s *Server) handleDeleteSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.catalog.GetSource(r.Context(), id); err != nil {
		httpError(w, http.StatusNotFound, "no source with id "+id)
		return
	}
	if err := s.ingestor.DeleteSource(r.Context(), id); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, apiv1.StatusResponse{Status: "deleted"})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query().Get("q")
	if query == "" {
		httpError(w, http.StatusBadRequest, "missing query parameter q")
		return
	}
	limit := 10
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			httpError(w, http.StatusBadRequest, "limit must be an integer in [1, 100]")
			return
		}
		limit = parsed
	}
	sourceID := r.URL.Query().Get("source")

	results, err := s.dp.Search(r.Context(), query, uint32(limit), sourceID)
	if err != nil {
		httpError(w, http.StatusBadGateway, "worker search failed: "+err.Error())
		return
	}
	out := make([]apiv1.SearchResult, 0, len(results))
	for _, item := range results {
		out = append(out, searchResultDTO(item))
	}
	writeJSON(w, http.StatusOK, apiv1.SearchEnvelope{Query: query, Results: out})
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	stats, err := s.catalog.Stats(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	failures, err := s.catalog.IngestFailures(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if failures == nil {
		failures = []catalog.IngestFailure{}
	}
	stats.Failures = len(failures)
	failureDTOs := make([]apiv1.IngestFailure, 0, len(failures))
	for _, f := range failures {
		failureDTOs = append(failureDTOs, apiv1.IngestFailure{SourceID: f.SourceID, URI: f.URI, Attempts: f.Attempts, Error: f.Error, FailedAt: f.FailedAt})
	}
	resp := apiv1.Status{Daemon: "ok", Stats: apiv1.Stats{Sources: stats.Sources, Documents: stats.Documents, Chunks: stats.Chunks, Failures: stats.Failures}, Failures: failureDTOs}
	health, _ := s.dp.Health(r.Context())
	resp.Worker = string(health.State)
	resp.Detail = health.Detail
	writeJSON(w, http.StatusOK, resp)
}

// handleShutdown requests a graceful daemon shutdown. It responds before
// signaling ShutdownRequested, so the client always sees the 202 even
// though the daemon starts tearing down (server, worker, catalog, in
// that order, per serve.go) as soon as the main loop observes the signal.
func (s *Server) handleShutdown(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "shutting down"})
	select {
	case s.shutdownCh <- struct{}{}:
	default: // already requested; no-op
	}
}

// eventsHeartbeatInterval is a var, not a const, so tests can shrink it
// rather than waiting out a real 15 seconds.
var eventsHeartbeatInterval = 15 * time.Second

// handleEvents streams the internal event bus (#19) as Server-Sent Events:
// one-directional, plain HTTP, so `curl -N localhost:7420/v1/events` works
// with zero client-side dependencies. On connect it replays the ring
// buffer plus one fresh snapshot, then streams live; an optional
// ?types=k1,k2 filter narrows which Kinds are sent.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if s.bus == nil {
		httpError(w, http.StatusServiceUnavailable, "event bus is not available")
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		httpError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	var typeFilter map[events.Kind]bool
	if raw := r.URL.Query().Get("types"); raw != "" {
		typeFilter = make(map[events.Kind]bool)
		for _, kind := range strings.Split(raw, ",") {
			typeFilter[events.Kind(strings.TrimSpace(kind))] = true
		}
	}

	// ?replay=false subscribes to live events only. The default replays the
	// ring buffer, which gives a fresh `lum top` immediate context — but a
	// client that reacts to events rather than displaying them (a Neovim
	// notifier, a hook) would announce work that finished long before it
	// connected. Skipping the backlog is the honest fix; the alternative is
	// every such client reimplementing "is this event stale".
	ch, backlog, unsubscribe := s.bus.Subscribe(64)
	defer unsubscribe()
	if r.URL.Query().Get("replay") == "false" {
		backlog = nil
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	writeEvent := func(e events.Event) bool {
		if typeFilter != nil && !typeFilter[e.Kind] {
			return true
		}
		data, err := json.Marshal(e)
		if err != nil {
			slog.Error("marshaling event for SSE", "error", err)
			return true
		}
		if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.Kind, data); err != nil {
			return false
		}
		flusher.Flush()
		return true
	}

	for _, e := range backlog {
		if !writeEvent(e) {
			return
		}
	}
	// Publish (rather than write directly) so this snapshot gets a real
	// Seq and Time stamped by the bus like any other event; ch is already
	// registered, so it arrives back through the normal loop below.
	s.bus.Publish(snapshot.Build(r.Context(), s.catalog, s.ingestor, s.dp))

	heartbeat := time.NewTicker(eventsHeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case e := <-ch:
			if !writeEvent(e) {
				return
			}
		case <-heartbeat.C:
			// A connected subscriber is real activity against the daemon's
			// idle timer (#13), not just the request that opened the stream.
			if s.onRequest != nil {
				s.onRequest()
			}
			if _, err := fmt.Fprint(w, ": heartbeat\n\n"); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}

// ---- helpers ----

func sourceDTO(s catalog.Source) apiv1.Source {
	return apiv1.Source{ID: s.ID, Type: s.Type, URI: s.URI, CreatedAt: s.CreatedAt}
}
func searchResultDTO(r worker.SearchResult) apiv1.SearchResult {
	return apiv1.SearchResult{DocumentID: r.DocumentID, SourceID: r.SourceID, URI: r.URI, ChunkIndex: r.ChunkIndex, Score: r.Score, Text: r.Text, StartLine: r.StartLine, EndLine: r.EndLine}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response", "error", err)
	}
}

func httpError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, apiv1.Error{Error: message})
}
