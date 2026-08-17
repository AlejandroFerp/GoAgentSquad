package dashboard_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
)

func newObservedRuntime() *squads.ObservabilityRuntime {
	return squads.NewObservabilityRuntime()
}

func seedTimeline(obs *squads.ObservabilityRuntime, correlationID string) {
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: correlationID,
		TraceID:       "trace-1",
		SpanID:        "span-1",
		Kind:          observability.StepQueryReceived,
		ThreadID:      correlationID,
		Summary:       "Need pastoral help",
		StartedAt:     time.Now().Add(-2 * time.Second),
		FinishedAt:    time.Now().Add(-2 * time.Second),
	})
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: correlationID,
		TraceID:       "trace-1",
		SpanID:        "span-2",
		Kind:          observability.StepRouted,
		ThreadID:      correlationID,
		Summary:       "routed to squads: squad-pastoral",
		StartedAt:     time.Now().Add(-1800 * time.Millisecond),
		FinishedAt:    time.Now().Add(-1800 * time.Millisecond),
	})
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: correlationID,
		TraceID:       "trace-1",
		SpanID:        "span-3",
		Kind:          observability.StepAgentStarted,
		ThreadID:      correlationID,
		SquadID:       "squad-pastoral",
		AgentID:       "comfort-agent",
		AgentType:     "ComfortAgent",
		Summary:       "comfort execution",
		StartedAt:     time.Now().Add(-1500 * time.Millisecond),
		FinishedAt:    time.Now().Add(-1500 * time.Millisecond),
	})
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: correlationID,
		TraceID:       "trace-1",
		SpanID:        "span-4",
		Kind:          observability.StepLLMCall,
		ThreadID:      correlationID,
		SquadID:       "squad-pastoral",
		AgentID:       "comfort-agent",
		AgentType:     "ComfortAgent",
		Model:         "mock",
		TokensIn:      30,
		TokensOut:     20,
		Summary:       "llm call",
		LLMTrace: &observability.LLMTrace{
			SystemPrompt: "Provide a helpful answer.",
			Messages: []observability.LLMMessage{
				{Role: "user", Content: "Need pastoral help"},
			},
			Completion:   "Theological comfort answer.",
			Provider:     "mock",
			GenerationID: "generation-1",
			FinishReason: "stop",
			CostUSD:      0.001,
		},
		StartedAt:  time.Now().Add(-1400 * time.Millisecond),
		FinishedAt: time.Now().Add(-1200 * time.Millisecond),
	})
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: correlationID,
		TraceID:       "trace-1",
		SpanID:        "span-5",
		Kind:          observability.StepResponded,
		ThreadID:      correlationID,
		SquadID:       "squad-pastoral",
		AgentID:       "comfort-agent",
		AgentType:     "ComfortAgent",
		Summary:       "Theological comfort answer.",
		StartedAt:     time.Now().Add(-500 * time.Millisecond),
		FinishedAt:    time.Now().Add(-300 * time.Millisecond),
	})
}

func TestBuildGraph(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "thread-1")
	graph := dashboard.BuildGraph("thread-1", obs.Ledger.Timeline("thread-1"))
	if len(graph.Nodes) < 3 {
		t.Fatalf("expected graph nodes, got %d", len(graph.Nodes))
	}
	if len(graph.Edges) == 0 {
		t.Fatal("expected graph edges")
	}
	wantEdges := map[string]bool{
		"squad:squad-pastoral|agent:comfort-agent|message|runs":   true,
		"agent:comfort-agent|squad:squad-pastoral|message|result": true,
	}
	for _, edge := range graph.Edges {
		delete(wantEdges, edge.ID)
	}
	if len(wantEdges) != 0 {
		t.Fatalf("missing communication edges: %#v", wantEdges)
	}
}

func TestBuildGraphShowsCompletedSquadSummary(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "thread-coordination")
	completedAt := time.Now()
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "thread-coordination",
		Kind:          observability.StepSynthesis,
		ThreadID:      "thread-coordination",
		SquadID:       "squad-pastoral",
		AgentID:       "squad-pastoral-coordinator",
		Summary:       "coordinated comfort response",
		StartedAt:     completedAt,
		FinishedAt:    completedAt,
	})

	graph := dashboard.BuildGraph("thread-coordination", obs.Ledger.Timeline("thread-coordination"))
	var squadStatus string
	summaryFound := false
	for _, node := range graph.Nodes {
		if node.ID == "squad:squad-pastoral" {
			squadStatus = node.Status
		}
	}
	for _, edge := range graph.Edges {
		if edge.ID == "agent:squad-pastoral-coordinator|user:thread-coordination|summary|summary" {
			summaryFound = true
		}
	}
	if squadStatus != "done" {
		t.Fatalf("squad status = %q, want done", squadStatus)
	}
	if !summaryFound {
		t.Fatal("expected squad summary edge to the query phase")
	}
}

func TestBuildGraphClosesSingleAgentSquadAfterRootTerminalEvent(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "thread-single-agent")
	completedAt := time.Now()
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "thread-single-agent",
		Kind:          observability.StepQuiesced,
		ThreadID:      "thread-single-agent",
		Summary:       "execution tree reached quiescence",
		StartedAt:     completedAt,
		FinishedAt:    completedAt,
	})

	graph := dashboard.BuildGraph("thread-single-agent", obs.Ledger.Timeline("thread-single-agent"))
	for _, node := range graph.Nodes {
		if node.ID == "squad:squad-pastoral" {
			if node.Status != "done" {
				t.Fatalf("single-agent squad status = %q, want done", node.Status)
			}
			return
		}
	}
	t.Fatal("expected single-agent squad graph node")
}

func TestBuildWorkflowGraphAddsTerminalNodeAfterAllStagesFinish(t *testing.T) {
	startedAt := time.Now().Add(-2 * time.Second)
	stages := []dashboard.WorkflowStage{
		{
			Summary: observability.QuerySummary{
				CorrelationID: "research-query",
				StartedAt:     startedAt,
				Status:        "done",
			},
		},
		{
			Summary: observability.QuerySummary{
				CorrelationID: "article-query",
				StartedAt:     startedAt.Add(time.Second),
				Status:        "done",
			},
		},
	}

	graph := dashboard.BuildWorkflowGraph(stages)
	terminalFound := false
	completionFound := false
	for _, node := range graph.Nodes {
		if node.ID == "terminal:workflow" {
			terminalFound = node.Type == "terminal" && node.Status == "done" && node.Label == "Workflow complete"
		}
	}
	for _, edge := range graph.Edges {
		if edge.ID == "phase:article-query|terminal:workflow|completion|complete" {
			completionFound = true
		}
	}
	if !terminalFound {
		t.Fatal("expected completed workflow terminal node")
	}
	if !completionFound {
		t.Fatal("expected completion edge from final phase")
	}
}

func TestBuildGraphClassifiesCoordinatorNodes(t *testing.T) {
	obs := newObservedRuntime()
	completedAt := time.Now()
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "thread-coordinator",
		Kind:          observability.StepLLMCall,
		ThreadID:      "thread-coordinator",
		SquadID:       "squad-pastoral",
		AgentID:       "squad-pastoral-coordinator",
		AgentType:     "COORDINATOR",
		StartedAt:     completedAt,
		FinishedAt:    completedAt,
	})

	graph := dashboard.BuildGraph("thread-coordinator", obs.Ledger.Timeline("thread-coordinator"))
	for _, node := range graph.Nodes {
		if node.ID == "agent:squad-pastoral-coordinator" {
			if node.Type != "coordinator" {
				t.Fatalf("coordinator node type = %q, want coordinator", node.Type)
			}
			return
		}
	}
	t.Fatal("expected coordinator graph node")
}

func TestDashboardEndpoints(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "thread-2")
	server := httptest.NewServer(dashboard.NewServer(obs))
	defer server.Close()

	for _, path := range []string{
		"/api/queries",
		"/api/queries/thread-2/timeline",
		"/api/queries/thread-2/graph",
		"/api/metrics/summary?query=thread-2",
		"/",
	} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s returned %d", path, resp.StatusCode)
		}
		_ = resp.Body.Close()
	}
}

func TestDashboardTimelineSerializesLLMTrace(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "trace-query")
	server := httptest.NewServer(dashboard.NewServer(obs))
	defer server.Close()

	response, err := http.Get(server.URL + "/api/queries/trace-query/timeline")
	if err != nil {
		t.Fatalf("GET timeline: %v", err)
	}
	defer response.Body.Close()

	var timeline []observability.AgentStep
	if err := json.NewDecoder(response.Body).Decode(&timeline); err != nil {
		t.Fatalf("decode timeline: %v", err)
	}
	for _, step := range timeline {
		if step.Kind != observability.StepLLMCall {
			continue
		}
		if step.LLMTrace == nil || step.LLMTrace.SystemPrompt != "Provide a helpful answer." {
			t.Fatalf("serialized LLM trace = %#v", step.LLMTrace)
		}
		if step.LLMTrace.Completion != "Theological comfort answer." || step.LLMTrace.GenerationID != "generation-1" {
			t.Fatalf("serialized LLM trace content = %#v", step.LLMTrace)
		}
		return
	}
	t.Fatal("expected LLM call in timeline")
}

func TestDashboardWorkflowEndpointsAggregateAllQueries(t *testing.T) {
	obs := newObservedRuntime()
	seedTimeline(obs, "research-query")
	seedTimeline(obs, "review-query")
	server := httptest.NewServer(dashboard.NewServer(obs))
	defer server.Close()

	response, err := http.Get(server.URL + "/api/workflow/timeline")
	if err != nil {
		t.Fatalf("GET workflow timeline: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET workflow timeline returned %d", response.StatusCode)
	}
	var timeline []observability.AgentStep
	if err := json.NewDecoder(response.Body).Decode(&timeline); err != nil {
		t.Fatalf("decode workflow timeline: %v", err)
	}
	if len(timeline) != 10 {
		t.Fatalf("workflow timeline length = %d, want 10", len(timeline))
	}

	response, err = http.Get(server.URL + "/api/workflow/graph")
	if err != nil {
		t.Fatalf("GET workflow graph: %v", err)
	}
	defer response.Body.Close()
	var graph dashboard.GraphModel
	if err := json.NewDecoder(response.Body).Decode(&graph); err != nil {
		t.Fatalf("decode workflow graph: %v", err)
	}
	if graph.CorrelationID != "workflow" {
		t.Fatalf("workflow graph correlation ID = %q, want workflow", graph.CorrelationID)
	}
	phaseCount := 0
	for _, node := range graph.Nodes {
		if node.Type == "phase" {
			phaseCount++
		}
	}
	if phaseCount != 2 {
		t.Fatalf("workflow phase count = %d, want 2", phaseCount)
	}

	response, err = http.Get(server.URL + "/api/workflow/metrics")
	if err != nil {
		t.Fatalf("GET workflow metrics: %v", err)
	}
	defer response.Body.Close()
	var metrics dashboard.MetricsSummary
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode workflow metrics: %v", err)
	}
	if metrics.TotalSteps != 10 {
		t.Fatalf("workflow total steps = %d, want 10", metrics.TotalSteps)
	}
	if metrics.LLMCalls != 2 {
		t.Fatalf("workflow LLM calls = %d, want 2", metrics.LLMCalls)
	}
}

func TestDashboardQueryStatusRequiresRootTerminalStep(t *testing.T) {
	obs := newObservedRuntime()
	startedAt := time.Now().Add(-time.Second)
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "root-query",
		Kind:          observability.StepQueryReceived,
		ThreadID:      "root-query",
		Summary:       "multi-squad query",
		StartedAt:     startedAt,
		FinishedAt:    startedAt,
	})
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "root-query",
		Kind:          observability.StepResponded,
		ThreadID:      "child-agent-thread",
		AgentID:       "researcher",
		Summary:       "one agent responded",
		StartedAt:     time.Now(),
		FinishedAt:    time.Now(),
	})

	queries := obs.Ledger.Queries()
	if len(queries) != 1 {
		t.Fatalf("query count = %d, want 1", len(queries))
	}
	if queries[0].Status != "running" {
		t.Fatalf("status after child response = %q, want running", queries[0].Status)
	}

	completedAt := time.Now()
	obs.Ledger.Record(observability.AgentStep{
		CorrelationID: "root-query",
		Kind:          observability.StepQuiesced,
		ThreadID:      "root-query",
		Summary:       "execution tree reached quiescence",
		StartedAt:     completedAt,
		FinishedAt:    completedAt,
	})

	queries = obs.Ledger.Queries()
	if queries[0].Status != "done" {
		t.Fatalf("status after root quiescence = %q, want done", queries[0].Status)
	}
}

func TestDashboardMetricsExposeSSEDropsAndSubscriberCapacity(t *testing.T) {
	obs := newObservedRuntime()
	id, _, ok := obs.Hub.TrySubscribe()
	if !ok {
		t.Fatal("failed to subscribe to observability hub")
	}
	defer obs.Hub.Unsubscribe(id)
	for i := 0; i < 129; i++ {
		obs.Hub.Broadcast(observability.AgentStep{StepID: "hub-step"})
	}

	server := httptest.NewServer(dashboard.NewServer(obs))
	defer server.Close()
	response, err := http.Get(server.URL + "/api/metrics/summary?query=missing")
	if err != nil {
		t.Fatalf("GET metrics summary: %v", err)
	}
	defer response.Body.Close()

	var metrics dashboard.MetricsSummary
	if err := json.NewDecoder(response.Body).Decode(&metrics); err != nil {
		t.Fatalf("decode metrics summary: %v", err)
	}
	if metrics.SSEDropped != 1 || metrics.SSESubscribers != 1 || metrics.SSEMaxClients != observability.DefaultHubMaxSubscribers {
		t.Fatalf("SSE metrics = %+v", metrics)
	}
}

func TestDashboardStream(t *testing.T) {
	obs := newObservedRuntime()
	server := httptest.NewServer(dashboard.NewServer(obs))
	defer server.Close()

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL+"/api/stream", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("stream request: %v", err)
	}
	defer resp.Body.Close()

	step := observability.AgentStep{
		CorrelationID: "thread-stream",
		TraceID:       "trace-stream",
		SpanID:        "span-stream",
		Kind:          observability.StepResponded,
		ThreadID:      "thread-stream",
		Summary:       "stream response",
		StartedAt:     time.Now(),
		FinishedAt:    time.Now(),
	}
	obs.Ledger.Record(step)

	reader := bufio.NewReader(resp.Body)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatalf("read stream: %v", err)
		}
		if strings.HasPrefix(line, "data: ") {
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			var got observability.AgentStep
			if err := json.Unmarshal([]byte(payload), &got); err != nil {
				t.Fatalf("unmarshal stream payload: %v", err)
			}
			if got.CorrelationID != step.CorrelationID {
				t.Fatalf("expected correlation %s, got %s", step.CorrelationID, got.CorrelationID)
			}
			return
		}
	}
	t.Fatal("timed out waiting for SSE payload")
}

func TestDashboardLoadsTraceFileWithoutDuplicatingSteps(t *testing.T) {
	traceDir := t.TempDir()
	tracePath := filepath.Join(traceDir, "trace.jsonl")
	step := observability.AgentStep{
		StepID:        "step-file-1",
		CorrelationID: "thread-file",
		TraceID:       "trace-file",
		SpanID:        "span-file",
		Kind:          observability.StepResponded,
		ThreadID:      "thread-file",
		Summary:       "persisted response",
		StartedAt:     time.Now(),
		FinishedAt:    time.Now(),
	}
	bytes, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("marshal step: %v", err)
	}
	if err := os.WriteFile(tracePath, append(bytes, '\n'), 0o600); err != nil {
		t.Fatalf("write trace file: %v", err)
	}

	obs := newObservedRuntime()
	server := httptest.NewServer(dashboard.NewServer(obs, dashboard.WithTraceFile(tracePath)))
	defer server.Close()

	for i := 0; i < 2; i++ {
		resp, err := http.Get(server.URL + "/api/queries/thread-file/timeline")
		if err != nil {
			t.Fatalf("GET timeline attempt %d: %v", i+1, err)
		}
		var timeline []observability.AgentStep
		if err := json.NewDecoder(resp.Body).Decode(&timeline); err != nil {
			_ = resp.Body.Close()
			t.Fatalf("decode timeline attempt %d: %v", i+1, err)
		}
		_ = resp.Body.Close()
		if len(timeline) != 1 {
			t.Fatalf("expected 1 deduplicated step on attempt %d, got %d", i+1, len(timeline))
		}
	}
}
