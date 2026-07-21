// Package api implements lumd's REST API — the single front door of the
// system. The CLI, the MCP server, curl, and any future client all go
// through these endpoints; none of them get a privileged side channel.
//
// Endpoints (all JSON):
//
//	POST   /v1/sources        {"uri": "~/Documents"}     register + scan
//	GET    /v1/sources                                   list sources
//	POST   /v1/sources/{id}/scan                         trigger rescan
//	GET    /v1/search?q=...&limit=10                     semantic search
//	GET    /v1/status                                    daemon + data plane health
//	GET    /v1/events[?types=k1,k2]                       SSE event stream
package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/alDuncanson/lum/control-plane/internal/catalog"
	"github.com/alDuncanson/lum/control-plane/internal/dataplane"
	"github.com/alDuncanson/lum/control-plane/internal/events"
	"github.com/alDuncanson/lum/control-plane/internal/ingest"
	"github.com/alDuncanson/lum/control-plane/internal/requestid"
	"github.com/alDuncanson/lum/control-plane/internal/snapshot"
	"github.com/alDuncanson/lum/control-plane/internal/source"
)

// Server wires HTTP handlers to the control plane's components.
type Server struct {
	catalog   *catalog.Catalog
	dp        dataplane.DataPlane
	ingestor  *ingest.Ingestor
	bus       *events.Bus
	onRequest func()
}

// New wires a Server. bus may be nil, in which case no events are published
// and GET /v1/events reports 503.
func New(cat *catalog.Catalog, dp dataplane.DataPlane, ing *ingest.Ingestor, bus *events.Bus) *Server {
	return &Server{catalog: cat, dp: dp, ingestor: ing, bus: bus}
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
	mux.HandleFunc("GET /v1/search", s.handleSearch)
	mux.HandleFunc("GET /v1/status", s.handleStatus)
	mux.HandleFunc("GET /v1/events", s.handleEvents)
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

type addSourceRequest struct {
	URI string `json:"uri"`
}

type addSourceResponse struct {
	Source  catalog.Source `json:"source"`
	Created bool           `json:"created"` // false if URI was already registered
}

func (s *Server) handleAddSource(w http.ResponseWriter, r *http.Request) {
	if !s.requireDataPlaneReady(w, r) {
		return
	}
	var req addSourceRequest
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

	row, created, err := s.catalog.AddSource(r.Context(), catalog.Source{
		ID:        uuid.NewString(),
		Type:      src.Type(),
		URI:       canonicalURI,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Scan asynchronously: registering a big directory shouldn't hold
	// the HTTP request open. 202 tells the client work is in progress;
	// progress is observable via `lum status`.
	s.ingestor.WatchSource(row.ID)
	s.ingestor.EnqueueScan(r.Context(), row.ID)

	writeJSON(w, http.StatusAccepted, addSourceResponse{Source: row, Created: created})
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
	writeJSON(w, http.StatusOK, sources)
}

func (s *Server) handleScanSource(w http.ResponseWriter, r *http.Request) {
	if !s.requireDataPlaneReady(w, r) {
		return
	}
	id := r.PathValue("id")
	if _, err := s.catalog.GetSource(r.Context(), id); err != nil {
		httpError(w, http.StatusNotFound, "no source with id "+id)
		return
	}
	s.ingestor.EnqueueScan(r.Context(), id)
	writeJSON(w, http.StatusAccepted, map[string]string{"status": "scan queued"})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if !s.requireDataPlaneReady(w, r) {
		return
	}
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

	results, err := s.dp.Search(r.Context(), query, uint32(limit))
	if err != nil {
		httpError(w, http.StatusBadGateway, "data plane search failed: "+err.Error())
		return
	}
	if results == nil {
		results = []dataplane.SearchResult{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"query": query, "results": results})
}

type statusResponse struct {
	Daemon    string                  `json:"daemon"`
	DataPlane string                  `json:"data_plane"`
	Detail    string                  `json:"detail,omitempty"`
	Stats     catalog.Stats           `json:"stats"`
	Failures  []catalog.IngestFailure `json:"failures"`
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
	resp := statusResponse{Daemon: "ok", Stats: stats, Failures: failures}
	health, _ := s.dp.Health(r.Context())
	resp.DataPlane = string(health.State)
	resp.Detail = health.Detail
	writeJSON(w, http.StatusOK, resp)
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

	ch, backlog, unsubscribe := s.bus.Subscribe(64)
	defer unsubscribe()

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

func (s *Server) requireDataPlaneReady(w http.ResponseWriter, r *http.Request) bool {
	// Health alone never wakes a shed data plane (see DataPlane's doc
	// comment) — a real request like this one is what's supposed to.
	s.dp.EnsureRunning()
	health, err := s.dp.Health(r.Context())
	if err == nil && health.State == dataplane.StateReady {
		return true
	}
	httpError(w, http.StatusServiceUnavailable, "data plane is "+string(health.State)+": "+health.Detail)
	return false
}

// ---- helpers ----

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("writing response", "error", err)
	}
}

func httpError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
