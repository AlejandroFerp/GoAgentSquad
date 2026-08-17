package dashboard

import "github.com/embention/agent-squad-go/pkg/observability"

const WorkflowCorrelationID = "workflow"

// GraphModel is the graph projection of a query timeline consumed by the web UI.
type GraphModel struct {
	CorrelationID string      `json:"correlation_id"`
	Nodes         []GraphNode `json:"nodes"`
	Edges         []GraphEdge `json:"edges"`
}

type GraphNode struct {
	ID        string `json:"id"`
	Label     string `json:"label"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	TokensIn  int    `json:"tokens_in"`
	TokensOut int    `json:"tokens_out"`
	Calls     int    `json:"calls"`
}

type GraphEdge struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Target string `json:"target"`
	Kind   string `json:"kind"`
	Label  string `json:"label"`
	Count  int    `json:"count"`
}

type MetricsSummary struct {
	CorrelationID  string `json:"correlation_id"`
	DurationMS     int64  `json:"duration_ms"`
	TotalSteps     int    `json:"total_steps"`
	TotalTokensIn  int    `json:"total_tokens_in"`
	TotalTokensOut int    `json:"total_tokens_out"`
	LLMCalls       int    `json:"llm_calls"`
	ToolCalls      int    `json:"tool_calls"`
	UniqueAgents   int    `json:"unique_agents"`
	Errors         int    `json:"errors"`
	SSEDropped     int    `json:"sse_dropped_events"`
	SSESubscribers int    `json:"sse_subscribers"`
	SSEMaxClients  int    `json:"sse_max_clients"`
}

type QuerySnapshot struct {
	Summary observability.QuerySummary `json:"summary"`
	Metrics MetricsSummary             `json:"metrics"`
}

// WorkflowStage is one query execution included in the aggregate dashboard view.
type WorkflowStage struct {
	Summary  observability.QuerySummary
	Timeline []observability.AgentStep
}
