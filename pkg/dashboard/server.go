package dashboard

import (
	"embed"
	"io/fs"
	"net/http"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
)

//go:embed web/*
var embeddedWeb embed.FS

// Server exposes the observability runtime over REST/SSE and serves the web UI.
type Server struct {
	obs    *squads.ObservabilityRuntime
	mux    *http.ServeMux
	assets http.Handler
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
		server.loader = &loaderState{jsonl: &observability.JSONFileLoader{Path: path}}
	}
}

func NewServer(obs *squads.ObservabilityRuntime, options ...Option) *Server {
	if obs == nil {
		obs = squads.NewObservabilityRuntime()
	}
	webFS, _ := fs.Sub(embeddedWeb, "web")
	server := &Server{
		obs:    obs,
		mux:    http.NewServeMux(),
		assets: http.FileServer(http.FS(webFS)),
	}
	for _, option := range options {
		option(server)
	}
	server.routes()
	return server
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("/api/queries", s.handleQueries)
	s.mux.HandleFunc("/api/queries/", s.handleQueryResource)
	s.mux.HandleFunc("/api/metrics/summary", s.handleMetricsSummary)
	s.mux.HandleFunc("/api/stream", s.handleStream)
	s.mux.Handle("/", s.assets)
}

func (s *Server) syncTraceFile() {
	if s == nil || s.loader == nil || s.loader.jsonl == nil || s.obs == nil || s.obs.Ledger == nil {
		return
	}
	_ = s.loader.jsonl.Sync(s.obs.Ledger)
}
