package dashboard

import (
	"embed"
	"fmt"
	"io/fs"
	"net/http"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
)

//go:embed web/*
var embeddedWeb embed.FS

// Server exposes the observability runtime over REST/SSE and serves the web UI.
type Server struct {
	// obs is the shared in-memory observability runtime (ledger, hub, tracer, etc.).
	obs *squads.ObservabilityRuntime
	// mux routes both API and static UI requests.
	mux *http.ServeMux
	// assets serves the embedded dashboard frontend.
	assets http.Handler
	// loader is optional and used only when replaying traces from JSONL.
	loader *loaderState
}

type Option func(*Server)

type loaderState struct {
	jsonl *observability.JSONFileLoader
}

func WithTraceFile(path string) Option {
	return func(server *Server) {
		if path == "" {
			return
		}
		// Enable incremental replay mode from a JSONL trace file.
		server.loader = &loaderState{jsonl: &observability.JSONFileLoader{Path: path}}
	}
}

func NewServer(obs *squads.ObservabilityRuntime, options ...Option) *Server {
	if obs == nil {
		// Keep constructor safe for callers that only need a standalone dashboard.
		obs = squads.NewObservabilityRuntime()
	}
	webFS, err := fs.Sub(embeddedWeb, "web")
	if err != nil {
		panic(fmt.Sprintf("dashboard embedded assets: %v", err))
	}
	server := &Server{
		obs:    obs,
		mux:    http.NewServeMux(),
		assets: http.FileServer(http.FS(webFS)),
	}
	for _, option := range options {
		option(server)
	}
	// Register API and static routes once the server is fully configured.
	server.routes()
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	// REST endpoints consumed by the dashboard web app.
	s.mux.HandleFunc("/api/queries", s.handleQueries)
	s.mux.HandleFunc("/api/queries/", s.handleQueryResource)
	s.mux.HandleFunc("/api/workflow/", s.handleWorkflowResource)
	s.mux.HandleFunc("/api/metrics/summary", s.handleMetricsSummary)
	// SSE stream for live step updates.
	s.mux.HandleFunc("/api/stream", s.handleStream)
	// Fallback to static assets for dashboard UI routes.
	s.mux.Handle("/", s.assets)
}

func (s *Server) syncTraceFile() {
	if s == nil || s.loader == nil || s.loader.jsonl == nil || s.obs == nil || s.obs.Ledger == nil {
		return
	}
	// Best-effort sync keeps replay mode fresh without breaking API reads on transient IO errors.
	_ = s.loader.jsonl.Sync(s.obs.Ledger)
}
