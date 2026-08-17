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
			Metrics: addHubMetrics(BuildMetricsSummary(query.CorrelationID, timeline), s.obs.Hub),
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

func (s *Server) handleWorkflowResource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.syncTraceFile()
	resource := strings.TrimPrefix(r.URL.Path, "/api/workflow/")
	if resource == "" || strings.Contains(resource, "/") {
		http.NotFound(w, r)
		return
	}
	stages := s.workflowStages()
	timeline := workflowTimeline(stages)
	switch resource {
	case "timeline":
		writeJSON(w, timeline)
	case "graph":
		writeJSON(w, BuildWorkflowGraph(stages))
	case "metrics":
		writeJSON(w, addHubMetrics(BuildMetricsSummary(WorkflowCorrelationID, timeline), s.obs.Hub))
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
	writeJSON(w, addHubMetrics(BuildMetricsSummary(correlationID, s.obs.Ledger.Timeline(correlationID)), s.obs.Hub))
}

func (s *Server) workflowStages() []WorkflowStage {
	queries := s.obs.Ledger.Queries()
	sort.Slice(queries, func(i, j int) bool {
		if queries[i].StartedAt.Equal(queries[j].StartedAt) {
			return queries[i].CorrelationID < queries[j].CorrelationID
		}
		return queries[i].StartedAt.Before(queries[j].StartedAt)
	})
	stages := make([]WorkflowStage, 0, len(queries))
	for _, query := range queries {
		stages = append(stages, WorkflowStage{
			Summary:  query,
			Timeline: s.obs.Ledger.Timeline(query.CorrelationID),
		})
	}
	return stages
}

func workflowTimeline(stages []WorkflowStage) []observability.AgentStep {
	timeline := []observability.AgentStep{}
	for _, stage := range stages {
		timeline = append(timeline, stage.Timeline...)
	}
	sort.SliceStable(timeline, func(i, j int) bool {
		if timeline[i].StartedAt.Equal(timeline[j].StartedAt) {
			return timeline[i].StepID < timeline[j].StepID
		}
		return timeline[i].StartedAt.Before(timeline[j].StartedAt)
	})
	return timeline
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	encoder := json.NewEncoder(w)
	// Preserve raw characters in summaries/messages to keep dashboard output readable.
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
}

var _ = observability.QuerySummary{}
