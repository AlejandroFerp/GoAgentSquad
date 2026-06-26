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
		StartedAt:     time.Now().Add(-1400 * time.Millisecond),
		FinishedAt:    time.Now().Add(-1200 * time.Millisecond),
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
