package squads_test

import (
	"bytes"
	"context"
	"errors"
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
func mockLLMCall(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
	if strings.Contains(systemPrompt, "correction helper") {
		return squads.LLMResponse{
			Content:          `{"query": "healed"}`,
			PromptTokens:     10,
			CompletionTokens: 5,
			TotalTokens:      15,
		}, nil
	}
	if strings.Contains(systemPrompt, "Integrate the tool result") {
		return squads.LLMResponse{
			Content:          "Integrated final answer based on tool output.",
			PromptTokens:     20,
			CompletionTokens: 15,
			TotalTokens:      35,
		}, nil
	}
	return squads.LLMResponse{
		Content:          "Mock theological analysis: the passage reveals divine love.",
		PromptTokens:     30,
		CompletionTokens: 20,
		TotalTokens:      50,
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

func TestBlackboardPersistsParentThreadFromRegisteredChild(t *testing.T) {
	ctx := context.Background()
	storage := synapse.NewMemoryStorage()
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("synapse connect: %v", err)
	}
	defer svc.Close()

	blackboard := squads.NewSynapseBlackboardBus(svc)
	blackboard.ParentThreads().Set("child-thread", "root-thread")
	message := synapse.NewContextMessage("child-thread", "agent-1", synapse.RoleAssistant, "child response", "squad-1", nil, time.Hour)
	sent, err := blackboard.SendMessage(ctx, message)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.ParentThreadID != "root-thread" {
		t.Fatalf("ParentThreadID = %q, want %q", sent.ParentThreadID, "root-thread")
	}

	reloaded := synapse.NewSynapseService(10, storage)
	if err := reloaded.Connect(ctx); err != nil {
		t.Fatalf("reloaded synapse connect: %v", err)
	}
	defer reloaded.Close()
	contextMessages, err := reloaded.FetchContext(ctx, "child-thread", 1)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(contextMessages) != 1 || contextMessages[0].ParentThreadID != "root-thread" {
		t.Fatalf("reloaded parent metadata = %#v, want root-thread", contextMessages)
	}
}

func TestQueryResultOnlyIncludesCurrentQueryThreads(t *testing.T) {
	pipeline, _, _ := setupPipeline(t)
	squad := squads.NewSquad("squad-1", "Test Squad", "desc", pipeline.Blackboard)
	pipeline.RegisterSquad(squad)
	pipeline.RouteQueryFn = func(ctx context.Context, content string) ([]string, error) {
		return []string{"squad-1"}, nil
	}

	firstResult, err := pipeline.Query(context.Background(), "thread-isolation-1", nil, "query", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("first Query: %v", err)
	}
	if len(firstResult.SquadThreads) == 0 {
		t.Fatal("expected first query to record squad thread mappings")
	}

	secondResult, err := pipeline.Query(context.Background(), "thread-isolation-2", nil, "query", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("second Query: %v", err)
	}
	for firstThreadID := range firstResult.SquadThreads {
		if _, exists := secondResult.SquadThreads[firstThreadID]; exists {
			t.Fatalf("second result included stale thread from first query: %s", firstThreadID)
		}
	}
}

func TestQueryRejectsInvalidInitialSquadIDs(t *testing.T) {
	tests := []struct {
		name    string
		initial []string
		wantErr string
	}{
		{
			name:    "unregistered squad",
			initial: []string{"missing-squad"},
			wantErr: "unregistered squad ID",
		},
		{
			name:    "empty squad ID",
			initial: []string{""},
			wantErr: "empty squad ID",
		},
		{
			name:    "empty squad list",
			initial: []string{},
			wantErr: "at least one squad ID",
		},
		{
			name:    "mixed valid and unregistered list",
			initial: []string{"squad-1", "missing-squad"},
			wantErr: "unregistered squad ID",
		},
	}

	for index, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, bb, _ := setupPipeline(t)
			squad := squads.NewSquad("squad-1", "Test Squad", "desc", bb)
			pipeline.RegisterSquad(squad)

			threadID := fmt.Sprintf("invalid-initial-squad-%d", index)
			var err error
			func() {
				defer func() {
					if recovered := recover(); recovered != nil {
						t.Fatalf("Query panicked for invalid initial squad %v: %v", tt.initial, recovered)
					}
				}()
				_, err = pipeline.Query(context.Background(), threadID, tt.initial, "test query", 100*time.Millisecond)
			}()

			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
			if metrics := bb.GetMetrics(threadID); metrics != nil {
				t.Fatalf("expected invalid query to fail before metrics are registered")
			}
		})
	}
}

func TestQueryRejectsInvalidRoutedInitialSquadIDs(t *testing.T) {
	tests := []struct {
		name        string
		routeResult []string
		wantErr     string
	}{
		{
			name:        "nil squad list",
			routeResult: nil,
			wantErr:     "at least one squad ID",
		},
		{
			name:        "empty squad list",
			routeResult: []string{},
			wantErr:     "at least one squad ID",
		},
		{
			name:        "unregistered squad",
			routeResult: []string{"missing-squad"},
			wantErr:     "unregistered squad ID",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipeline, bb, _ := setupPipeline(t)
			pipeline.RegisterSquad(squads.NewSquad("squad-1", "Test Squad", "desc", bb))
			pipeline.RouteQueryFn = func(ctx context.Context, content string) ([]string, error) {
				return tt.routeResult, nil
			}

			_, err := pipeline.Query(context.Background(), "invalid-routed-initial-squad", nil, "test query", 100*time.Millisecond)
			if err == nil {
				t.Fatalf("expected error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
			if metrics := bb.GetMetrics("invalid-routed-initial-squad"); metrics != nil {
				t.Fatal("expected invalid routed query to fail before metrics are registered")
			}
		})
	}
}

func TestQueryPropagatesCallerCancellation(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	started := make(chan struct{})
	finished := make(chan struct{})

	agent := squads.NewSubAgent("cancel-agent", "CancelAgent", "desc", "system", bb, "cancel-squad")
	agent.LLMCall = func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return squads.LLMResponse{}, ctx.Err()
	}
	squad := squads.NewSquad("cancel-squad", "Cancel Squad", "desc", bb)
	squad.RegisterSubAgent(agent)
	pipeline.RegisterSquad(squad)
	pipeline.RouteQueryFn = func(ctx context.Context, content string) ([]string, error) {
		return []string{"cancel-squad"}, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, err := pipeline.Query(ctx, "cancel-thread", nil, "cancel this", 5*time.Second)
		resultCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not start")
	}
	cancel()

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Query error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Query did not return after caller cancellation")
	}
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not observe caller cancellation")
	}
}

func TestQueryTimeoutCancelsAgentContext(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	started := make(chan struct{})
	canceled := make(chan struct{})

	agent := squads.NewSubAgent("timeout-agent", "TimeoutAgent", "desc", "system", bb, "timeout-squad")
	agent.LLMCall = func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		close(started)
		<-ctx.Done()
		close(canceled)
		return squads.LLMResponse{}, ctx.Err()
	}
	squad := squads.NewSquad("timeout-squad", "Timeout Squad", "desc", bb)
	squad.RegisterSubAgent(agent)
	pipeline.RegisterSquad(squad)

	result, err := pipeline.Query(context.Background(), "timeout-thread", []string{"timeout-squad"}, "wait", 50*time.Millisecond)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Query error = %v, want context.DeadlineExceeded", err)
	}
	if result != nil {
		t.Fatal("expected no result after timeout")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not start")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not observe query timeout")
	}
}

func TestQueryRetainsTraceWhenOneSubagentFails(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	squad := squads.NewSquad("partial-failure-squad", "Partial Failure Squad", "desc", bb)

	failingAgent := squads.NewSubAgent("failing-agent", "FailingAgent", "desc", "system", bb, squad.SquadID)
	failingAgent.LLMCall = func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		return squads.LLMResponse{}, errors.New("simulated subagent failure")
	}

	successfulAgent := squads.NewSubAgent("successful-agent", "SuccessfulAgent", "desc", "system", bb, squad.SquadID)
	successfulAgent.LLMCall = func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		return squads.LLMResponse{Content: "usable sibling result"}, nil
	}

	squad.RegisterSubAgent(failingAgent)
	squad.RegisterSubAgent(successfulAgent)
	pipeline.RegisterSquad(squad)

	result, err := pipeline.Query(context.Background(), "partial-failure-thread", []string{squad.SquadID}, "test query", time.Second)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}

	if result.Metrics["error_count"] != 1 {
		t.Fatalf("error_count = %v, want 1", result.Metrics["error_count"])
	}

	foundFailureTrace := false
	for _, step := range result.Timeline {
		if step.Kind == observability.StepError && strings.Contains(step.Error, "simulated subagent failure") {
			foundFailureTrace = true
			break
		}
	}
	if !foundFailureTrace {
		t.Fatal("expected partial subagent failure in the execution timeline")
	}
}

func TestQueryWaitsForEverySquadCoordinator(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	var completedCoordinators atomic.Int32

	llm := func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		if !strings.Contains(systemPrompt, "You are the coordinator") {
			return squads.LLMResponse{Content: "research finding"}, nil
		}

		delay := 20 * time.Millisecond
		switch {
		case strings.Contains(systemPrompt, "Medium Squad"):
			delay = 60 * time.Millisecond
		case strings.Contains(systemPrompt, "Slow Squad"):
			delay = 100 * time.Millisecond
		}

		select {
		case <-time.After(delay):
			completedCoordinators.Add(1)
			return squads.LLMResponse{Content: "coordinated finding"}, nil
		case <-ctx.Done():
			return squads.LLMResponse{}, ctx.Err()
		}
	}

	for _, definition := range []struct {
		id   string
		name string
	}{
		{id: "fast", name: "Fast Squad"},
		{id: "medium", name: "Medium Squad"},
		{id: "slow", name: "Slow Squad"},
	} {
		squad := squads.NewSquad(definition.id, definition.name, "coordination test", bb)
		squad.LLMCall = llm
		for agentIndex := range 2 {
			agent := squads.NewSubAgent(
				fmt.Sprintf("%s-agent-%d", definition.id, agentIndex),
				"RESEARCHER",
				"Produces a finding.",
				"Research the assigned topic.",
				bb,
				definition.id,
			)
			agent.LLMCall = llm
			squad.RegisterSubAgent(agent)
		}
		pipeline.RegisterSquad(squad)
	}

	startedAt := time.Now()
	result, err := pipeline.Query(
		context.Background(),
		"multi-squad-coordination",
		[]string{"fast", "medium", "slow"},
		"research query",
		time.Second,
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got := completedCoordinators.Load(); got != 3 {
		t.Fatalf("completed coordinators = %d, want 3", got)
	}
	if elapsed := time.Since(startedAt); elapsed < 100*time.Millisecond {
		t.Fatalf("Query returned after %s, before the slow coordinator could complete", elapsed)
	}
	llmCalls := 0
	for _, step := range result.Timeline {
		if step.Kind == observability.StepLLMCall {
			llmCalls++
		}
	}
	if llmCalls != 9 {
		t.Fatalf("observed LLM calls = %d, want 9 including three coordinators", llmCalls)
	}
}

func TestNewRuntimeBuildsDeclarativeSquad(t *testing.T) {
	var calls atomic.Int32
	llm := func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		if model != "test-model" {
			return squads.LLMResponse{}, fmt.Errorf("model = %q, want test-model", model)
		}
		calls.Add(1)
		return squads.LLMResponse{Content: "declarative research result"}, nil
	}

	runtime, err := squads.NewRuntime(context.Background(), squads.RuntimeConfig{
		LLMCall:       llm,
		Model:         "test-model",
		MaxIterations: 15,
		Squads: []squads.SquadDefinition{
			{
				ID:          "research",
				Name:        "Research",
				Description: "Researches the user topic.",
				Agents: []squads.AgentDefinition{
					{
						ID:           "researcher",
						Type:         "RESEARCHER",
						Description:  "Produces evidence-based findings.",
						SystemPrompt: "Research the requested topic.",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	result, err := runtime.Query(
		context.Background(),
		"declarative-runtime",
		[]string{"research"},
		"research this topic",
		time.Second,
	)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("LLM calls = %d, want 1", calls.Load())
	}
	if len(result.History) == 0 {
		t.Fatal("expected the runtime to return query history")
	}
	if len(runtime.Observability().Ledger.Timeline("declarative-runtime")) == 0 {
		t.Fatal("expected the runtime to expose query observability")
	}
}

func TestNewRuntimeRegistersDeclarativeLocalTools(t *testing.T) {
	var toolCalls atomic.Int32
	var receivedValue string
	llm := func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		if strings.Contains(systemPrompt, "Integrate the tool result") {
			return squads.LLMResponse{Content: "tool result integrated"}, nil
		}
		return squads.LLMResponse{Content: `{"call_tool":"echo","arguments":{"value":"from declarative tool"}}`}, nil
	}

	runtime, err := squads.NewRuntime(context.Background(), squads.RuntimeConfig{
		Model:   "tool-model",
		LLMCall: llm,
		Squads: []squads.SquadDefinition{{
			ID: "tool-squad",
			Agents: []squads.AgentDefinition{{
				ID:           "tool-agent",
				Type:         "TOOL_AGENT",
				SystemPrompt: "Use the echo tool.",
				Tools: map[string]squads.LocalTool{
					"echo": {
						Schema: squads.ToolSchema{
							Name:        "echo",
							Description: "Returns the supplied value.",
							Parameters:  map[string]squads.ToolParam{"value": {Type: "string", Required: true}},
						},
						Func: func(arguments map[string]any) (any, error) {
							toolCalls.Add(1)
							receivedValue, _ = arguments["value"].(string)
							return receivedValue, nil
						},
					},
				},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	result, err := runtime.Query(context.Background(), "declarative-tool-runtime", []string{"tool-squad"}, "run the echo tool", time.Second)
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if toolCalls.Load() != 1 || receivedValue != "from declarative tool" {
		t.Fatalf("tool calls = %d, value = %q, want one declarative echo call", toolCalls.Load(), receivedValue)
	}
	historyContainsIntegratedResult := false
	for _, message := range result.History {
		if strings.Contains(message.Content(), "tool result integrated") {
			historyContainsIntegratedResult = true
			break
		}
	}
	if !historyContainsIntegratedResult {
		t.Fatalf("history = %#v, want integrated tool result", result.History)
	}
	toolStepFound := false
	for _, step := range result.Timeline {
		if step.Kind == observability.StepToolCall && step.ToolName == "echo" {
			toolStepFound = true
			break
		}
	}
	if !toolStepFound {
		t.Fatal("expected declarative local tool call in the observed timeline")
	}
}

func TestRuntimeCapturesLLMRequestAndCompletionWhenEnabled(t *testing.T) {
	runtime, err := squads.NewRuntime(context.Background(), squads.RuntimeConfig{
		CaptureLLMContent: true,
		Model:             "trace-model",
		LLMCall: func(context.Context, string, string, []map[string]any) (squads.LLMResponse, error) {
			return squads.LLMResponse{
				Content:          "auditable completion",
				PromptTokens:     11,
				CompletionTokens: 7,
				TotalTokens:      18,
				Provider:         "test-provider",
				GenerationID:     "gen-123",
				FinishReason:     "stop",
				CostUSD:          0.001,
			}, nil
		},
		Squads: []squads.SquadDefinition{
			{
				ID: "trace-squad",
				Agents: []squads.AgentDefinition{
					{
						ID:           "trace-agent",
						Type:         "TRACE",
						SystemPrompt: "Use the auditable system instruction.",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if _, err := runtime.Query(context.Background(), "trace-query", []string{"trace-squad"}, "show the audit trail", time.Second); err != nil {
		t.Fatalf("Query: %v", err)
	}

	for _, step := range runtime.Observability().Ledger.Timeline("trace-query") {
		if step.Kind != observability.StepLLMCall {
			continue
		}
		if step.LLMTrace == nil {
			t.Fatal("expected captured LLM trace")
		}
		if step.LLMTrace.SystemPrompt != "Use the auditable system instruction." {
			t.Fatalf("system prompt = %q", step.LLMTrace.SystemPrompt)
		}
		if len(step.LLMTrace.Messages) == 0 || !strings.Contains(step.LLMTrace.Messages[len(step.LLMTrace.Messages)-1].Content, "show the audit trail") {
			t.Fatalf("captured user messages = %#v", step.LLMTrace.Messages)
		}
		if step.LLMTrace.Completion != "auditable completion" {
			t.Fatalf("completion = %q", step.LLMTrace.Completion)
		}
		if step.LLMTrace.Provider != "test-provider" || step.LLMTrace.GenerationID != "gen-123" || step.LLMTrace.CostUSD != 0.001 {
			t.Fatalf("provider metadata = %#v", step.LLMTrace)
		}
		return
	}
	t.Fatal("expected an LLM call step")
}

func TestRuntimeDoesNotCaptureLLMContentByDefault(t *testing.T) {
	runtime, err := squads.NewRuntime(context.Background(), squads.RuntimeConfig{
		Model: "trace-model",
		LLMCall: func(context.Context, string, string, []map[string]any) (squads.LLMResponse, error) {
			return squads.LLMResponse{Content: "sensitive completion"}, nil
		},
		Squads: []squads.SquadDefinition{
			{
				ID: "trace-squad",
				Agents: []squads.AgentDefinition{
					{
						ID:           "trace-agent",
						Type:         "TRACE",
						SystemPrompt: "Sensitive system instruction.",
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewRuntime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	if _, err := runtime.Query(context.Background(), "private-query", []string{"trace-squad"}, "sensitive user prompt", time.Second); err != nil {
		t.Fatalf("Query: %v", err)
	}

	for _, step := range runtime.Observability().Ledger.Timeline("private-query") {
		if step.Kind == observability.StepLLMCall && step.LLMTrace != nil {
			t.Fatalf("expected LLM trace to remain disabled, got %#v", step.LLMTrace)
		}
	}
}

func TestNewRuntimeRejectsAgentWithoutLLMCall(t *testing.T) {
	runtime, err := squads.NewRuntime(context.Background(), squads.RuntimeConfig{
		Squads: []squads.SquadDefinition{
			{
				ID: "research",
				Agents: []squads.AgentDefinition{
					{
						ID:           "researcher",
						SystemPrompt: "Research the requested topic.",
					},
				},
			},
		},
	})
	if runtime != nil {
		t.Cleanup(func() { _ = runtime.Close() })
		t.Fatal("NewRuntime returned a runtime without an LLM call")
	}
	if err == nil || !strings.Contains(err.Error(), "requires an LLM call") {
		t.Fatalf("NewRuntime error = %v, want missing LLM call error", err)
	}
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

func TestSubAgentHandleReplyRoutesPendingReply(t *testing.T) {
	_, bb, _ := setupPipeline(t)
	ctx := context.Background()
	agent := squads.NewSubAgent("reply-agent", "ReplyAgent", "desc", "system", bb, "reply-squad")

	_, err := agent.CallToolDelegate(
		ctx,
		"original-thread",
		"lookup",
		"reply-thread",
		map[string]any{"query": "test"},
		"destination-squad",
		"response-thread",
	)
	if err != nil {
		t.Fatalf("CallToolDelegate: %v", err)
	}
	if len(agent.PendingReplies) != 1 {
		t.Fatalf("pending replies = %d, want 1", len(agent.PendingReplies))
	}

	reply := synapse.NewContextMessage("reply-thread", "delegate", synapse.RoleAssistant, "lookup result", "destination-squad", nil, time.Hour)
	agent.HandleReply(ctx, "reply-thread", &reply)

	if len(agent.PendingReplies) != 0 {
		t.Fatalf("pending replies = %d, want 0", len(agent.PendingReplies))
	}
	responses, err := bb.FetchContext(ctx, "response-thread", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(responses) != 1 {
		t.Fatalf("responses = %d, want 1", len(responses))
	}
	if responses[0].Content() != "lookup result" {
		t.Errorf("response content = %q, want %q", responses[0].Content(), "lookup result")
	}
}

func TestSubAgentDelegateCreatesDistinctReplyThread(t *testing.T) {
	_, bb, _ := setupPipeline(t)
	originalThreadID := "agent-thread"
	agent := squads.NewSubAgent("delegating-agent", "DelegatingAgent", "desc", "system", bb, "source-squad")

	task, err := agent.Delegate(
		context.Background(),
		originalThreadID,
		"execute_task",
		originalThreadID,
		map[string]any{"query": "delegated work"},
		"target-squad",
		"source-squad-thread",
	)
	if err != nil {
		t.Fatalf("Delegate: %v", err)
	}
	if task.ReplyToThread() == originalThreadID {
		t.Fatal("delegation reused the original thread as its reply thread")
	}
	if parentThreadID := bb.ParentThreads().Get(task.ReplyToThread()); parentThreadID != originalThreadID {
		t.Fatalf("reply thread parent = %q, want %q", parentThreadID, originalThreadID)
	}
}

func TestDelegatedTaskMetricsRecordExecutionFailure(t *testing.T) {
	pipeline, bb, _ := setupPipeline(t)
	targetSquad := squads.NewSquad("target-squad", "Target Squad", "desc", bb)
	failingAgent := squads.NewSubAgent("failing-agent", "FailingAgent", "desc", "system", bb, targetSquad.SquadID)
	failingAgent.LLMCall = func(context.Context, string, string, []map[string]any) (squads.LLMResponse, error) {
		return squads.LLMResponse{}, errors.New("delegated execution failed")
	}
	targetSquad.RegisterSubAgent(failingAgent)
	pipeline.RegisterSquad(targetSquad)
	pipeline.Start()
	t.Cleanup(pipeline.Stop)

	const rootThreadID = "delegated-task-root"
	metrics := squads.NewExecutionMetrics(rootThreadID)
	bb.SetMetrics(rootThreadID, metrics)
	task := synapse.NewTaskMessage(
		rootThreadID,
		"source-agent",
		"execute_task",
		"reply-thread",
		map[string]any{"query": "delegated work"},
		targetSquad.SquadID,
		time.Hour,
		1,
	)
	if _, err := bb.SendMessage(context.Background(), task); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		snapshot := metrics.ToDict()
		if snapshot["error_count"] == 1 {
			tasks := snapshot["tasks"].(map[string]any)
			taskMetrics := tasks["execute_task"].(map[string]any)
			if taskMetrics["started"] != 1 || taskMetrics["failed"] != 1 || taskMetrics["completed"] != 0 {
				t.Fatalf("task metrics = %#v, want started=1 failed=1 completed=0", taskMetrics)
			}
			return
		}

		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("delegated execution did not report its failure: %#v", snapshot)
		}
	}
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
	msg := synapse.NewContextMessage("thread-obs", "agent-1", synapse.RoleUser, "Please explain [Juan 3:16]", "", nil, time.Hour)
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
	metrics := squads.NewExecutionMetrics("thread-trans")
	bb.SetMetrics("thread-trans", metrics)

	agent := squads.NewTransversalAgent("trans-test", "TestTransversal", []string{"lookup"}, bb)
	agent.ExecuteTask = func(ctx context.Context, taskMsg *synapse.SynapseMessage) (string, error) {
		return "lookup result", nil
	}

	ctx := context.Background()
	taskMsg := synapse.NewTaskMessage("thread-trans", "agent-1", "lookup", "reply-trans", map[string]any{"q": "test"}, "", time.Hour, 1)
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
	tasks := metrics.ToDict()["tasks"].(map[string]any)
	lookup := tasks["lookup"].(map[string]any)
	if lookup["started"] != 1 || lookup["completed"] != 1 || lookup["failed"] != 0 {
		t.Errorf("lookup task metrics = %#v, want one completed task", lookup)
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
	triggerMsg := synapse.NewContextMessage("thread-squad-1", "user-client", synapse.RoleUser, "test query", "squad-1", nil, time.Hour)
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
			metrics.RecordLLMUsage("squad-1", "agent-1", 10, 5, 15, 100*time.Millisecond)
			metrics.RecordTaskStarted("lookup")
			metrics.RecordTaskCompleted("lookup")
			metrics.RecordRetry("squad-1", "agent-1", "lookup-tool")
			metrics.RecordError("squad-1", "agent-1", "agent")
			metrics.RecordSubagentEnd("squad-1", "agent-1", 100*time.Millisecond)
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
	if elapsed, ok := dict["elapsed_time"].(time.Duration); !ok || elapsed < 0 {
		t.Errorf("elapsed_time = %T(%v), want non-negative time.Duration", dict["elapsed_time"], dict["elapsed_time"])
	}
	if dict["retry_count"] != 100 {
		t.Errorf("retry_count = %v, want 100", dict["retry_count"])
	}
	if dict["error_count"] != 100 {
		t.Errorf("error_count = %v, want 100", dict["error_count"])
	}
	tasks, ok := dict["tasks"].(map[string]any)
	if !ok {
		t.Fatalf("tasks = %T, want map[string]any", dict["tasks"])
	}
	lookup, ok := tasks["lookup"].(map[string]any)
	if !ok {
		t.Fatalf("lookup task metrics = %T, want map[string]any", tasks["lookup"])
	}
	if lookup["started"] != 100 || lookup["completed"] != 100 {
		t.Errorf("lookup task metrics = %#v, want 100 starts and completions", lookup)
	}
}

func TestFetchSlicedContext(t *testing.T) {
	_, bb, _ := setupPipeline(t)
	ctx := context.Background()

	// Post 3 regular messages and 1 synthesis checkpoint.
	for i := 0; i < 3; i++ {
		msg := synapse.NewContextMessage("thread-slice", "agent-1", synapse.RoleUser, fmt.Sprintf("msg-%d", i), "", nil, time.Hour)
		_, _ = bb.SendMessage(ctx, msg)
	}
	synthMsg := synapse.NewContextMessage("thread-slice", "synthesizer", synapse.RoleSystem, "[SYNTHESIS] checkpoint", "", nil, time.Hour)
	synthMsg.SetPayloadValue("is_synthesis", true)
	_, _ = bb.SendMessage(ctx, synthMsg)

	// Post 2 more messages after the checkpoint.
	for i := 3; i < 5; i++ {
		msg := synapse.NewContextMessage("thread-slice", "agent-1", synapse.RoleUser, fmt.Sprintf("msg-%d", i), "", nil, time.Hour)
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
	pipeline.RouteQueryFn = func(ctx context.Context, content string) ([]string, error) {
		return []string{"squad-1"}, nil
	}

	result, err := pipeline.Query(context.Background(), "trace-thread-1", nil, "help me understand this passage", 5*time.Second)
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
	stepIDs := make(map[string]struct{}, len(result.Timeline))
	var rootStepID string
	hasCausalChild := false
	for _, step := range result.Timeline {
		stepIDs[step.StepID] = struct{}{}
		if step.Kind == observability.StepQueryReceived && step.ThreadID == "trace-thread-1" {
			rootStepID = step.StepID
		}
		if step.ParentStepID != "" {
			hasCausalChild = true
		}
		if step.CorrelationID != "trace-thread-1" {
			t.Fatalf("expected correlation trace-thread-1, got %q for step %s", step.CorrelationID, step.Kind)
		}
		if step.TraceID != "" && step.TraceID != result.Metadata.TraceID {
			t.Fatalf("expected all steps to share trace %q, got %q for step %s", result.Metadata.TraceID, step.TraceID, step.Kind)
		}
	}
	if rootStepID == "" {
		t.Fatal("expected a root query step")
	}
	if !hasCausalChild {
		t.Fatal("expected at least one step with causal parent linkage")
	}
	for _, step := range result.Timeline {
		if step.ParentStepID != "" {
			if _, ok := stepIDs[step.ParentStepID]; !ok {
				t.Fatalf("step %s references missing parent step %s", step.StepID, step.ParentStepID)
			}
		}
	}
	if len(result.History) == 0 {
		t.Fatal("expected persisted Synapse history")
	}
	for _, message := range result.History {
		if message.Trace.CorrelationID != "trace-thread-1" {
			t.Fatalf("message correlation = %q, want trace-thread-1", message.Trace.CorrelationID)
		}
		if message.Trace.TraceID != result.Metadata.TraceID {
			t.Fatalf("message trace = %q, want %q", message.Trace.TraceID, result.Metadata.TraceID)
		}
		if message.ThreadID != "trace-thread-1" && message.ParentThreadID != "trace-thread-1" {
			t.Fatalf("message %s lost parent thread linkage: %+v", message.ID, message)
		}
	}
	if last := result.Timeline[len(result.Timeline)-1]; last.Kind != observability.StepResponded || last.ThreadID != "trace-thread-1" {
		t.Fatalf("root timeline did not finish with responded step: %+v", last)
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
