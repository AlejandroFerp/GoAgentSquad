package dashboard

import (
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/embention/agent-squad-go/pkg/observability"
)

func (s *Server) handleQueries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Keep file-backed replay and in-memory ledger aligned before serving reads.
	s.syncTraceFile()
	queries := s.obs.Ledger.Queries()
	// Newest queries first so the UI can show the most recent activity at the top.
	sort.Slice(queries, func(i, j int) bool { return queries[i].StartedAt.After(queries[j].StartedAt) })
	snapshots := make([]QuerySnapshot, 0, len(queries))
	for _, query := range queries {
		timeline := s.obs.Ledger.Timeline(query.CorrelationID)
		snapshots = append(snapshots, QuerySnapshot{
			Summary: query,
			Metrics: BuildMetricsSummary(query.CorrelationID, timeline),
		})
	}
	writeJSON(w, snapshots)
}

func (s *Server) handleQueryResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.syncTraceFile()
	// Route shape: /api/queries/{correlationID}/{timeline|graph}
	resourcePath := strings.TrimPrefix(r.URL.Path, "/api/queries/")
	parts := strings.Split(resourcePath, "/")
	if len(parts) != 2 || parts[0] == "" {
		http.NotFound(w, r)
		return
	}
	correlationID := parts[0]
	resource := parts[1]
	timeline := s.obs.Ledger.Timeline(correlationID)
	switch resource {
	case "timeline":
		writeJSON(w, timeline)
	case "graph":
		writeJSON(w, BuildGraph(correlationID, timeline))
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleMetricsSummary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.syncTraceFile()
	// Accept both names for backwards compatibility with existing callers.
	correlationID := r.URL.Query().Get("query")
	if correlationID == "" {
		correlationID = r.URL.Query().Get("correlation_id")
	}
	if correlationID == "" {
		writeJSON(w, MetricsSummary{})
		return
	}
	writeJSON(w, BuildMetricsSummary(correlationID, s.obs.Ledger.Timeline(correlationID)))
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	encoder := json.NewEncoder(w)
	// Preserve raw characters in summaries/messages to keep dashboard output readable.
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

var _ = observability.QuerySummary{}
