package squads

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

func TestExecutionBudgetValidate(t *testing.T) {
	tests := []struct {
		name    string
		budget  ExecutionBudget
		wantErr string
	}{
		{
			name:   "disabled limits are valid",
			budget: ExecutionBudget{},
		},
		{
			name:    "negative token limit",
			budget:  ExecutionBudget{MaxTotalTokens: -1},
			wantErr: "max total tokens",
		},
		{
			name:    "negative USD limit",
			budget:  ExecutionBudget{MaxCostUSD: -0.01},
			wantErr: "max cost USD",
		},
		{
			name:    "not a number USD limit",
			budget:  ExecutionBudget{MaxCostUSD: math.NaN()},
			wantErr: "must be finite",
		},
		{
			name:    "positive infinity USD limit",
			budget:  ExecutionBudget{MaxCostUSD: math.Inf(1)},
			wantErr: "must be finite",
		},
		{
			name:    "negative infinity USD limit",
			budget:  ExecutionBudget{MaxCostUSD: math.Inf(-1)},
			wantErr: "max cost USD",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.budget.Validate()
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestSquadsPipelineSetExecutionBudgetRejectsInvalidValues(t *testing.T) {
	pipeline := NewSquadsPipeline(nil, nil, 1)
	if err := pipeline.SetExecutionBudget(ExecutionBudget{MaxTotalTokens: 25, MaxCostUSD: 0.50}); err != nil {
		t.Fatalf("SetExecutionBudget() error = %v", err)
	}
	if got := pipeline.executionBudgetSnapshot(); got.MaxTotalTokens != 25 || got.MaxCostUSD != 0.50 {
		t.Fatalf("configured budget = %+v, want token and USD limits", got)
	}
	if err := pipeline.SetExecutionBudget(ExecutionBudget{MaxTotalTokens: -1}); err == nil {
		t.Fatal("SetExecutionBudget() returned nil for a negative token limit")
	}
}

func TestNewRuntimeRejectsInvalidExecutionBudget(t *testing.T) {
	runtime, err := NewRuntime(context.Background(), RuntimeConfig{
		ExecutionBudget: ExecutionBudget{MaxCostUSD: math.NaN()},
	})
	if runtime != nil {
		t.Cleanup(func() { _ = runtime.Close() })
		t.Fatal("NewRuntime returned a runtime for an invalid execution budget")
	}
	if err == nil || !strings.Contains(err.Error(), "execution budget") {
		t.Fatalf("NewRuntime() error = %v, want execution budget validation error", err)
	}
}

func TestNewRuntimeRejectsInvalidTransversalDefinitions(t *testing.T) {
	executeTask := func(context.Context, *synapse.SynapseMessage) (string, error) {
		return "ok", nil
	}
	tests := []struct {
		name          string
		transversals  []TransversalDefinition
		wantErrorText string
	}{
		{
			name: "empty ID",
			transversals: []TransversalDefinition{{
				Capabilities: []string{"lookup"}, ExecuteTask: executeTask,
			}},
			wantErrorText: "ID must not be empty",
		},
		{
			name: "empty capabilities",
			transversals: []TransversalDefinition{{
				ID: "lookup", ExecuteTask: executeTask,
			}},
			wantErrorText: "requires at least one capability",
		},
		{
			name: "empty capability value",
			transversals: []TransversalDefinition{{
				ID: "lookup", Capabilities: []string{""}, ExecuteTask: executeTask,
			}},
			wantErrorText: "contains an empty capability",
		},
		{
			name: "nil ExecuteTask",
			transversals: []TransversalDefinition{{
				ID: "lookup", Capabilities: []string{"lookup"},
			}},
			wantErrorText: "requires an ExecuteTask function",
		},
		{
			name: "duplicate ID",
			transversals: []TransversalDefinition{
				{ID: "lookup", Capabilities: []string{"lookup"}, ExecuteTask: executeTask},
				{ID: "lookup", Capabilities: []string{"other"}, ExecuteTask: executeTask},
			},
			wantErrorText: "duplicate transversal ID",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, err := NewRuntime(context.Background(), RuntimeConfig{
				Transversals: test.transversals,
			})
			if runtime != nil {
				_ = runtime.Close()
				t.Fatal("NewRuntime returned a runtime for an invalid transversal definition")
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErrorText) {
				t.Fatalf("NewRuntime() error = %v, want substring %q", err, test.wantErrorText)
			}
		})
	}
}

func TestRuntimeConfigExecutionBudgetFailsAnOverrunQuery(t *testing.T) {
	runtime, err := NewRuntime(context.Background(), RuntimeConfig{
		ExecutionBudget: ExecutionBudget{MaxTotalTokens: 5, MaxCostUSD: 0.01},
		LLMCall: func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
			return LLMResponse{Content: "response", PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6, CostUSD: 0.02}, nil
		},
		Squads: []SquadDefinition{{
			ID: "runtime-budget-squad",
			Agents: []AgentDefinition{{
				ID:           "runtime-budget-agent",
				SystemPrompt: "respond",
			}},
		}},
	})
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })

	result, err := runtime.Query(context.Background(), "runtime-budget-query", []string{"runtime-budget-squad"}, "query", time.Second)
	if result != nil {
		t.Fatalf("Query() result = %#v, want nil after budget overrun", result)
	}
	if !errors.Is(err, ErrExecutionBudgetExceeded) {
		t.Fatalf("Query() error = %v, want budget error", err)
	}
	for _, step := range runtime.Observability().Ledger.Timeline("runtime-budget-query") {
		if step.Kind != "llm_call" {
			continue
		}
		if step.LLMTrace != nil {
			t.Fatalf("LLM trace = %#v, want disabled by default", step.LLMTrace)
		}
		if step.CostUSD != 0.02 || step.Budget == nil || step.Budget.Status != string(BudgetStatusExceeded) {
			t.Fatalf("LLM step = %#v, want top-level cost and exceeded budget snapshot", step)
		}
		return
	}
	t.Fatal("expected an observed LLM step")
}

func TestExecutionMetricsRecordsConcurrentTokenAndCostUsage(t *testing.T) {
	metrics, err := NewExecutionMetricsWithBudget("budget-metrics", ExecutionBudget{})
	if err != nil {
		t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
	}
	metrics.RegisterSquad("squad-1", &Squad{SquadID: "squad-1", Name: "Squad", SubAgents: map[string]*SubAgent{}})

	const updates = 100
	var waitGroup sync.WaitGroup
	for range updates {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			if _, err := metrics.recordSubagentLLMUsageWithCost("squad-1", "agent-1", 10, 5, 15, 0.01, time.Millisecond); err != nil {
				t.Errorf("recordSubagentLLMUsageWithCost() error = %v", err)
			}
		}()
	}
	waitGroup.Wait()

	snapshot := metrics.BudgetSnapshot()
	if snapshot.PromptTokens != updates*10 || snapshot.CompletionTokens != updates*5 || snapshot.TotalTokens != updates*15 {
		t.Fatalf("budget snapshot = %+v, want aggregate token usage", snapshot)
	}
	if math.Abs(snapshot.CostUSD-float64(updates)*0.01) > 1e-9 {
		t.Fatalf("cost USD = %.12f, want %.12f", snapshot.CostUSD, float64(updates)*0.01)
	}

	dict := metrics.ToDict()
	if dict["total_tokens"] != updates*15 || dict["cost_usd"] != snapshot.CostUSD {
		t.Fatalf("execution metrics = %#v, want aggregate totals", dict)
	}
	squads := dict["squads"].(map[string]any)
	squad := squads["squad-1"].(map[string]any)
	if squad["cost_usd"] != snapshot.CostUSD {
		t.Fatalf("squad cost USD = %v, want %v", squad["cost_usd"], snapshot.CostUSD)
	}
	agents := squad["subagents"].(map[string]any)
	agent := agents["agent-1"].(map[string]any)
	if agent["cost_usd"] != snapshot.CostUSD {
		t.Fatalf("agent cost USD = %v, want %v", agent["cost_usd"], snapshot.CostUSD)
	}
}

func TestSubAgentLLMCallBlocksAfterExecutionBudgetIsExhausted(t *testing.T) {
	bus := newConnectedBus(t)
	metrics, err := NewExecutionMetricsWithBudget("budget-thread", ExecutionBudget{MaxTotalTokens: 10, MaxCostUSD: 0.05})
	if err != nil {
		t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
	}
	bus.SetMetrics("budget-thread", metrics)

	var calls atomic.Int32
	agent := NewSubAgent("budget-agent", "Agent", "", "system", bus, "budget-squad")
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		calls.Add(1)
		return LLMResponse{Content: "first answer", PromptTokens: 6, CompletionTokens: 4, TotalTokens: 10, CostUSD: 0.05}, nil
	}

	if _, err := agent.callLLMObserved(context.Background(), "budget-thread", "system", nil); err != nil {
		t.Fatalf("first callLLMObserved() error = %v", err)
	}
	_, err = agent.callLLMObserved(context.Background(), "budget-thread", "system", nil)
	if !errors.Is(err, ErrExecutionBudgetExceeded) {
		t.Fatalf("second callLLMObserved() error = %v, want budget error", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("LLM calls = %d, want 1 after the budget is exhausted", calls.Load())
	}
	if snapshot := metrics.BudgetSnapshot(); snapshot.Status != string(BudgetStatusExhausted) {
		t.Fatalf("budget status = %q, want exhausted", snapshot.Status)
	}
}

func TestSubAgentLLMCallReturnsTypedErrorAfterBudgetOverrun(t *testing.T) {
	bus := newConnectedBus(t)
	metrics, err := NewExecutionMetricsWithBudget("overrun-thread", ExecutionBudget{MaxTotalTokens: 9, MaxCostUSD: 0.04})
	if err != nil {
		t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
	}
	bus.SetMetrics("overrun-thread", metrics)

	agent := NewSubAgent("overrun-agent", "Agent", "", "system", bus, "overrun-squad")
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		return LLMResponse{Content: "response", PromptTokens: 6, CompletionTokens: 4, TotalTokens: 10, CostUSD: 0.05}, nil
	}

	response, err := agent.callLLMObserved(context.Background(), "overrun-thread", "system", nil)
	if response.TotalTokens != 10 || response.CostUSD != 0.05 {
		t.Fatalf("response = %+v, want returned provider usage", response)
	}
	if !errors.Is(err, ErrExecutionBudgetExceeded) {
		t.Fatalf("callLLMObserved() error = %v, want budget error", err)
	}
	var budgetErr *BudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("callLLMObserved() error = %v, want *BudgetExceededError", err)
	}
	if budgetErr.Snapshot.Status != string(BudgetStatusExceeded) {
		t.Fatalf("budget error snapshot = %+v, want exceeded", budgetErr.Snapshot)
	}
}

func TestHealingAndCoordinatorLLMCallsObserveExecutionBudget(t *testing.T) {
	tests := []struct {
		name   string
		invoke func(t *testing.T, bus BlackboardBus, threadID string) error
	}{
		{
			name: "tool healing",
			invoke: func(t *testing.T, bus BlackboardBus, threadID string) error {
				agent := NewSubAgent("healing-agent", "Agent", "", "system", bus, "healing-squad")
				agent.LLMCall = budgetOverrunLLMCall
				_, err := agent.askLLMToHeal(context.Background(), threadID, "tool", ToolSchema{}, "tool failed", map[string]any{})
				return err
			},
		},
		{
			name: "squad coordinator",
			invoke: func(t *testing.T, bus BlackboardBus, threadID string) error {
				squad := NewSquad("coordinator-squad", "Coordinator", "", bus)
				squad.LLMCall = budgetOverrunLLMCall
				_, err := squad.callCoordinatorLLMObserved(context.Background(), threadID, "coordinate")
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bus := newConnectedBus(t)
			threadID := "budget-" + test.name
			metrics, err := NewExecutionMetricsWithBudget(threadID, ExecutionBudget{MaxTotalTokens: 5})
			if err != nil {
				t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
			}
			bus.SetMetrics(threadID, metrics)

			err = test.invoke(t, bus, threadID)
			if !errors.Is(err, ErrExecutionBudgetExceeded) {
				t.Fatalf("LLM path error = %v, want budget error", err)
			}
			if snapshot := metrics.BudgetSnapshot(); snapshot.Status != string(BudgetStatusExceeded) {
				t.Fatalf("budget status = %q, want exceeded", snapshot.Status)
			}
		})
	}
}

func TestExecutionBudgetSurvivesMetricsThreadEntryEviction(t *testing.T) {
	bus := newConnectedBus(t)
	pipeline := NewSquadsPipeline(bus, nil, 1)
	if err := pipeline.SetExecutionBudget(ExecutionBudget{MaxTotalTokens: 5}); err != nil {
		t.Fatalf("SetExecutionBudget() error = %v", err)
	}

	var coordinatorCalls atomic.Int32
	squad := NewSquad("fanout-squad", "Fanout Squad", "", bus)
	squad.LLMCall = func(_ context.Context, _ string, systemPrompt string, _ []map[string]any) (LLMResponse, error) {
		if strings.Contains(systemPrompt, "You are the coordinator") {
			coordinatorCalls.Add(1)
			return LLMResponse{Content: "summary", PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6}, nil
		}
		return LLMResponse{Content: "agent response"}, nil
	}
	for agentIndex := range 101 {
		agent := NewSubAgent(
			fmt.Sprintf("fanout-agent-%d", agentIndex),
			"Agent",
			"",
			"respond",
			bus,
			squad.SquadID,
		)
		agent.LLMCall = squad.LLMCall
		squad.RegisterSubAgent(agent)
	}
	pipeline.RegisterSquad(squad)

	result, err := pipeline.Query(context.Background(), "fanout-budget-query", []string{squad.SquadID}, "query", 5*time.Second)
	if result != nil {
		t.Fatalf("Query() result = %#v, want nil after coordinator budget overrun", result)
	}
	if !errors.Is(err, ErrExecutionBudgetExceeded) {
		t.Fatalf("Query() error = %v, want budget error", err)
	}
	if coordinatorCalls.Load() != 1 {
		t.Fatalf("coordinator calls = %d, want 1", coordinatorCalls.Load())
	}
	metrics := pipeline.GetMetrics("fanout-budget-query")
	if metrics["budget_status"] != string(BudgetStatusExceeded) || metrics["total_tokens"] != 6 {
		t.Fatalf("metrics = %#v, want the coordinator overrun accounted after thread entry eviction", metrics)
	}
}

func budgetOverrunLLMCall(context.Context, string, string, []map[string]any) (LLMResponse, error) {
	return LLMResponse{Content: "response", PromptTokens: 4, CompletionTokens: 2, TotalTokens: 6, CostUSD: 0.01}, nil
}

func TestSubAgentLLMCallsWaitForBudgetedUsageToBeRecorded(t *testing.T) {
	bus := newConnectedBus(t)
	metrics, err := NewExecutionMetricsWithBudget("serialized-budget-thread", ExecutionBudget{MaxTotalTokens: 100})
	if err != nil {
		t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
	}
	bus.SetMetrics("serialized-budget-thread", metrics)

	firstCallStarted := make(chan struct{})
	secondCallStarted := make(chan struct{}, 1)
	releaseFirstCall := make(chan struct{})
	var calls atomic.Int32
	agent := NewSubAgent("serialized-agent", "Agent", "", "system", bus, "serialized-squad")
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		callNumber := calls.Add(1)
		if callNumber == 1 {
			close(firstCallStarted)
			<-releaseFirstCall
		} else {
			secondCallStarted <- struct{}{}
		}
		return LLMResponse{Content: "response", PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}, nil
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		if _, callErr := agent.callLLMObserved(context.Background(), "serialized-budget-thread", "system", nil); callErr != nil {
			t.Errorf("first callLLMObserved() error = %v", callErr)
		}
	}()

	select {
	case <-firstCallStarted:
	case <-time.After(time.Second):
		t.Fatal("first LLM call did not start")
	}

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		if _, callErr := agent.callLLMObserved(context.Background(), "serialized-budget-thread", "system", nil); callErr != nil {
			t.Errorf("second callLLMObserved() error = %v", callErr)
		}
	}()

	select {
	case <-secondCallStarted:
		t.Fatal("second LLM call began before the first call recorded budget usage")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseFirstCall)
	waitGroup.Wait()
	if calls.Load() != 2 {
		t.Fatalf("LLM calls = %d, want 2", calls.Load())
	}
}

func TestCanceledLLMCallDoesNotAcquireExecutionBudgetPermit(t *testing.T) {
	bus := newConnectedBus(t)
	metrics, err := NewExecutionMetricsWithBudget("canceled-budget-thread", ExecutionBudget{MaxTotalTokens: 100})
	if err != nil {
		t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
	}
	bus.SetMetrics("canceled-budget-thread", metrics)

	firstCallStarted := make(chan struct{})
	releaseFirstCall := make(chan struct{})
	var calls atomic.Int32
	agent := NewSubAgent("canceled-agent", "Agent", "", "system", bus, "canceled-squad")
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		callNumber := calls.Add(1)
		if callNumber == 1 {
			close(firstCallStarted)
			<-releaseFirstCall
		}
		return LLMResponse{Content: "response", PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}, nil
	}

	firstDone := make(chan error, 1)
	go func() {
		_, callErr := agent.callLLMObserved(context.Background(), "canceled-budget-thread", "system", nil)
		firstDone <- callErr
	}()
	select {
	case <-firstCallStarted:
	case <-time.After(time.Second):
		t.Fatal("first LLM call did not start")
	}

	canceledContext, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = agent.callLLMObserved(canceledContext, "canceled-budget-thread", "system", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call error = %v, want context canceled", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("LLM calls = %d, want the canceled call to skip the provider", calls.Load())
	}

	close(releaseFirstCall)
	if err := <-firstDone; err != nil {
		t.Fatalf("first callLLMObserved() error = %v", err)
	}
}

func TestMalformedLLMUsageDoesNotCorruptExecutionBudget(t *testing.T) {
	tests := []struct {
		name     string
		response LLMResponse
		wantErr  string
		wantUsed int
	}{
		{
			name:     "negative prompt tokens",
			response: LLMResponse{PromptTokens: -1},
			wantErr:  "prompt tokens",
		},
		{
			name:     "negative total tokens",
			response: LLMResponse{TotalTokens: -1},
			wantErr:  "total tokens",
		},
		{
			name:     "not a number cost",
			response: LLMResponse{CostUSD: math.NaN()},
			wantErr:  "cost USD",
		},
		{
			name:     "underreported total tokens",
			response: LLMResponse{PromptTokens: 4, CompletionTokens: 3, TotalTokens: 1},
			wantUsed: 7,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bus := newConnectedBus(t)
			metrics, err := NewExecutionMetricsWithBudget("malformed-usage-thread", ExecutionBudget{MaxTotalTokens: 10})
			if err != nil {
				t.Fatalf("NewExecutionMetricsWithBudget() error = %v", err)
			}
			bus.SetMetrics("malformed-usage-thread", metrics)

			agent := NewSubAgent("usage-agent", "Agent", "", "system", bus, "usage-squad")
			agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
				return test.response, nil
			}

			_, err = agent.callLLMObserved(context.Background(), "malformed-usage-thread", "system", nil)
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("callLLMObserved() error = %v, want substring %q", err, test.wantErr)
				}
			} else if err != nil {
				t.Fatalf("callLLMObserved() error = %v", err)
			}

			if snapshot := metrics.BudgetSnapshot(); snapshot.TotalTokens != test.wantUsed || snapshot.CostUSD != 0 {
				t.Fatalf("budget snapshot = %+v, want total tokens %d and zero cost", snapshot, test.wantUsed)
			}
			if _, marshalErr := json.Marshal(bus.Observability().Ledger.Timeline("malformed-usage-thread")); marshalErr != nil {
				t.Fatalf("marshal observed timeline: %v", marshalErr)
			}
		})
	}
}

// Serialization is the price of an enabled budget, so an execution without one
// must keep the framework's concurrent agent behavior unchanged.
func TestUnbudgetedLLMCallsStayConcurrent(t *testing.T) {
	bus := newConnectedBus(t)
	bus.SetMetrics("unbudgeted-thread", NewExecutionMetrics("unbudgeted-thread"))

	const concurrentCalls = 2
	var barrier sync.WaitGroup
	barrier.Add(concurrentCalls)
	overlapped := make(chan struct{})
	agent := NewSubAgent("unbudgeted-agent", "Agent", "", "system", bus, "unbudgeted-squad")
	agent.LLMCall = func(ctx context.Context, _ string, _ string, _ []map[string]any) (LLMResponse, error) {
		barrier.Done()
		select {
		case <-overlapped:
		case <-ctx.Done():
			return LLMResponse{}, ctx.Err()
		}
		return LLMResponse{Content: "response", PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5}, nil
	}

	go func() {
		barrier.Wait()
		close(overlapped)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var callers sync.WaitGroup
	for range concurrentCalls {
		callers.Add(1)
		go func() {
			defer callers.Done()
			if _, err := agent.callLLMObserved(ctx, "unbudgeted-thread", "system", nil); err != nil {
				t.Errorf("callLLMObserved() error = %v", err)
			}
		}()
	}
	callers.Wait()

	select {
	case <-overlapped:
	default:
		t.Fatal("unbudgeted LLM calls were serialized instead of running concurrently")
	}
}
