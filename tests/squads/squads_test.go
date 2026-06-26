package squads_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

// mockLLMCall returns a canned response for testing.
func mockLLMCall(ctx context.Context, model, systemPrompt string, messages []map[string]any) (map[string]any, error) {
	if strings.Contains(systemPrompt, "correction helper") {
		return map[string]any{
			"content":           `{"query": "healed"}`,
			"prompt_tokens":     10,
			"completion_tokens": 5,
			"total_tokens":      15,
		}, nil
	}
	if strings.Contains(systemPrompt, "Integrate the tool result") {
		return map[string]any{
			"content":           "Integrated final answer based on tool output.",
			"prompt_tokens":     20,
			"completion_tokens": 15,
			"total_tokens":      35,
		}, nil
	}
	return map[string]any{
		"content":           "Mock theological analysis: the passage reveals divine love.",
		"prompt_tokens":     30,
		"completion_tokens": 20,
		"total_tokens":      50,
	}, nil
}

func setupPipeline(t *testing.T) (*squads.SquadsPipeline, *squads.SynapseBlackboardBus, *synapse.SynapseService) {
	t.Helper()
	svc := synapse.NewSynapseService(50, nil)
	if err := svc.Connect(context.Background()); err != nil {
		t.Fatalf("synapse connect: %v", err)
	}
	t.Cleanup(func() { _ = svc.Close() })

	bb := squads.NewSynapseBlackboardBus(svc)
	pipeline := squads.NewSquadsPipeline(bb, nil, 15)
	return pipeline, bb, svc
}

func TestBoundedMap(t *testing.T) {
	m := squads.NewBoundedMap(3)
	m.Set("a", 1)
	m.Set("b", 2)
	m.Set("c", 3)
	m.Set("d", 4) // should evict "a"

	if _, ok := m.Get("a"); ok {
		t.Error("expected 'a' to be evicted")
	}
	if v, ok := m.Get("d"); !ok || v.(int) != 4 {
		t.Errorf("expected d=4, got %v", v)
	}
}

func TestParentThreadMap(t *testing.T) {
	ptm := squads.NewParentThreadMap()
	ptm.Set("child-1", "parent-1")
	if ptm.Get("child-1") != "parent-1" {
		t.Error("expected parent-1")
	}
	if !ptm.Has("child-1") {
		t.Error("expected Has=true")
	}
	if ptm.Has("child-2") {
		t.Error("expected Has=false for child-2")
	}
}

func TestSubAgentRespond(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)

	agent := squads.NewSubAgent("test-agent", "TestAgent", "desc", "system", bb, "squad-test")
	agent.LLMCall = mockLLMCall
	agent.Model = "mock"

	ctx := context.Background()
	_, err := agent.Respond(ctx, "thread-1", "Hello world", nil)
	if err != nil {
		t.Fatalf("Respond: %v", err)
	}

	msgs, err := bb.FetchContext(ctx, "thread-1", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Content() != "Hello world" {
		t.Errorf("expected 'Hello world', got %s", msgs[0].Content())
	}
	if msgs[0].Role != synapse.RoleAssistant {
		t.Errorf("expected assistant role, got %s", msgs[0].Role)
	}

	_ = pipeline // suppress unused
}

func TestReferenceExpansionObserver(t *testing.T) {
	_, bb, _ := setupPipeline(t)

	bibleDB := map[string]string{
		"Juan 3:16": "Porque de tal manera amó Dios al mundo...",
	}
	observer := squads.NewReferenceExpansionObserver("*", bb, func(ref string) string {
		return bibleDB[ref]
	})
	observer.Start()
	defer observer.Stop()

	ctx := context.Background()
	msg := synapse.NewContextMessage("thread-obs", "agent-1", synapse.RoleUser, "Please explain [Juan 3:16]", "", nil, 3600)
	sent, err := bb.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent == nil {
		t.Fatal("SendMessage returned nil")
	}
	content := sent.Content()
	if !strings.Contains(content, "Expanded References") {
		t.Errorf("expected expanded references in content, got: %s", content)
	}
	if !strings.Contains(content, "Porque de tal manera") {
		t.Errorf("expected scripture text in content, got: %s", content)
	}
}

func TestTransversalAgentProcessTask(t *testing.T) {
	_, bb, _ := setupPipeline(t)

	agent := squads.NewTransversalAgent("trans-test", "TestTransversal", []string{"lookup"}, bb)
	agent.ExecuteTask = func(ctx context.Context, taskMsg *synapse.SynapseMessage) (string, error) {
		return "lookup result", nil
	}

	ctx := context.Background()
	taskMsg := synapse.NewTaskMessage("thread-trans", "agent-1", "lookup", "reply-trans", map[string]any{"q": "test"}, "", 3600, 1)
	agent.ProcessTask(ctx, &taskMsg)

	// Wait for the reply to be posted.
	time.Sleep(100 * time.Millisecond)
	msgs, err := bb.FetchContext(ctx, "reply-trans", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 reply message, got %d", len(msgs))
	}
	if msgs[0].Content() != "lookup result" {
		t.Errorf("expected 'lookup result', got %s", msgs[0].Content())
	}
}

func TestSquadRunConcurrent(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)

	agent1 := squads.NewSubAgent("agent-1", "Agent1", "desc1", "system1", bb, "squad-1")
	agent1.LLMCall = mockLLMCall
	agent1.Model = "mock"

	agent2 := squads.NewSubAgent("agent-2", "Agent2", "desc2", "system2", bb, "squad-1")
	agent2.LLMCall = mockLLMCall
	agent2.Model = "mock"

	squad := squads.NewSquad("squad-1", "Test Squad", "desc", bb)
	squad.LLMCall = mockLLMCall
	squad.Model = "mock"
	squad.RegisterSubAgent(agent1)
	squad.RegisterSubAgent(agent2)
	pipeline.RegisterSquad(squad)

	ctx := context.Background()
	triggerMsg := synapse.NewContextMessage("thread-squad-1", "user-client", synapse.RoleUser, "test query", "squad-1", nil, 3600)
	squad.Start()
	defer squad.Stop()

	err := squad.Run(ctx, "thread-squad-1", &triggerMsg)
	if err != nil {
		t.Fatalf("Squad.Run: %v", err)
	}

	// Both agents should have posted assistant messages on the squad thread.
	msgs, err := bb.FetchContext(ctx, "thread-squad-1", 100)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	assistantCount := 0
	for _, m := range msgs {
		if m.Role == synapse.RoleAssistant {
			assistantCount++
		}
	}
	if assistantCount < 2 {
		t.Errorf("expected at least 2 assistant messages, got %d", assistantCount)
	}
}

func TestMetricsThreadSafety(t *testing.T) {
	metrics := squads.NewExecutionMetrics("thread-metrics")
	metrics.RegisterSquad("squad-1", &squads.Squad{SquadID: "squad-1", Name: "Test", SubAgents: map[string]*squads.SubAgent{}})
	metrics.RegisterTransversal("trans-1", "Transversal")

	var wg sync.WaitGroup
	var counter atomic.Int32
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			metrics.RecordSubagentStart("squad-1", "agent-1")
			metrics.RecordLLMUsage("squad-1", "agent-1", 10, 5, 15, 0.1)
			metrics.RecordSubagentEnd("squad-1", "agent-1", 0.1)
			counter.Add(1)
		}(i)
	}
	wg.Wait()

	if counter.Load() != 100 {
		t.Errorf("expected 100 iterations, got %d", counter.Load())
	}
	dict := metrics.ToDict()
	if dict["status"] != "Running" {
		t.Errorf("expected Running status, got %v", dict["status"])
	}
}

func TestFetchSlicedContext(t *testing.T) {
	_, bb, _ := setupPipeline(t)
	ctx := context.Background()

	// Post 3 regular messages and 1 synthesis checkpoint.
	for i := 0; i < 3; i++ {
		msg := synapse.NewContextMessage("thread-slice", "agent-1", synapse.RoleUser, fmt.Sprintf("msg-%d", i), "", nil, 3600)
		_, _ = bb.SendMessage(ctx, msg)
	}
	synthMsg := synapse.NewContextMessage("thread-slice", "synthesizer", synapse.RoleSystem, "[SYNTHESIS] checkpoint", "", nil, 3600)
	synthMsg.Payload["is_synthesis"] = true
	_, _ = bb.SendMessage(ctx, synthMsg)

	// Post 2 more messages after the checkpoint.
	for i := 3; i < 5; i++ {
		msg := synapse.NewContextMessage("thread-slice", "agent-1", synapse.RoleUser, fmt.Sprintf("msg-%d", i), "", nil, 3600)
		_, _ = bb.SendMessage(ctx, msg)
	}

	sliced, err := squads.FetchSlicedContext(ctx, bb, "thread-slice", 100)
	if err != nil {
		t.Fatalf("FetchSlicedContext: %v", err)
	}
	// Should include the synthesis message + 2 post-synthesis messages = 3.
	if len(sliced) != 3 {
		t.Errorf("expected 3 messages after synthesis, got %d", len(sliced))
	}
}

func TestQueryPropagatesSingleTraceAcrossConcurrentSpans(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	tracer := &observability.RecorderTracer{}
	bb.Observability().Tracer = tracer

	agent1 := squads.NewSubAgent("agent-a", "AgentA", "desc", "system", bb, "squad-1")
	agent1.LLMCall = mockLLMCall
	agent1.Model = "mock"

	agent2 := squads.NewSubAgent("agent-b", "AgentB", "desc", "system", bb, "squad-1")
	agent2.LLMCall = mockLLMCall
	agent2.Model = "mock"

	squad := squads.NewSquad("squad-1", "Test Squad", "desc", bb)
	squad.LLMCall = mockLLMCall
	squad.Model = "mock"
	squad.RegisterSubAgent(agent1)
	squad.RegisterSubAgent(agent2)
	pipeline.RegisterSquad(squad)
	pipeline.RouteQueryFn = func(ctx context.Context, content string) (any, error) {
		return "squad-1", nil
	}

	result, err := pipeline.Query(context.Background(), "trace-thread-1", nil, "help me understand this passage", 5)
	if err != nil {
		t.Fatalf("pipeline.Query: %v", err)
	}
	if result.Metadata.TraceID == "" {
		t.Fatal("expected root trace id in query metadata")
	}

	spans := tracer.Spans()
	if len(spans) == 0 {
		t.Fatal("expected recorded spans")
	}
	for _, span := range spans {
		if span.TraceID != result.Metadata.TraceID {
			t.Fatalf("expected all spans to share trace %q, got %q for span %s", result.Metadata.TraceID, span.TraceID, span.Name)
		}
	}

	hasChildSpan := false
	for _, span := range spans {
		if span.ParentID != "" {
			hasChildSpan = true
			break
		}
	}
	if !hasChildSpan {
		t.Fatalf("expected at least one child span with a parent id, spans=%+v", spans)
	}

	if len(result.Timeline) < 4 {
		t.Fatalf("expected an instrumented timeline, got %d steps", len(result.Timeline))
	}
	for _, step := range result.Timeline {
		if step.TraceID != "" && step.TraceID != result.Metadata.TraceID {
			t.Fatalf("expected all steps to share trace %q, got %q for step %s", result.Metadata.TraceID, step.TraceID, step.Kind)
		}
	}
}

func TestLoggerFromContextInjectsTraceFields(t *testing.T) {
	var buffer bytes.Buffer
	logger := observability.NewTextLogger(&buffer, nil)
	ctx := observability.WithTraceContext(context.Background(), observability.TraceContext{
		TraceID:       "trace-123",
		SpanID:        "span-456",
		CorrelationID: "corr-789",
		CausationID:   "cause-000",
	})
	ctx = observability.WithStepID(ctx, "step-321")

	observability.BindLogger(logger, ctx).Info("hello logger")

	output := buffer.String()
	for _, expected := range []string{
		"trace_id=trace-123",
		"span_id=span-456",
		"correlation_id=corr-789",
		"causation_id=cause-000",
		"step_id=step-321",
	} {
		if !strings.Contains(output, expected) {
			t.Fatalf("expected %q in logger output, got %s", expected, output)
		}
	}
}
