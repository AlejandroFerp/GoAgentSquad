package squads

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

// deliveryFailingBus delegates every operation to a real blackboard but rejects
// message delivery, reproducing a storage layer that refuses writes.
type deliveryFailingBus struct {
	BlackboardBus
	err error
}

func (b *deliveryFailingBus) SendMessage(context.Context, synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
	return nil, b.err
}

func newConnectedBus(t *testing.T) BlackboardBus {
	t.Helper()
	service := synapse.NewSynapseService(50, nil)
	if err := service.Connect(context.Background()); err != nil {
		t.Fatalf("connect synapse service: %v", err)
	}
	t.Cleanup(func() { service.Close() })
	return NewSynapseBlackboardBus(service)
}

func TestSquadRunReportsUndeliveredAgentContext(t *testing.T) {
	rejected := errors.New("storage rejected the message")
	bus := &deliveryFailingBus{BlackboardBus: newConnectedBus(t), err: rejected}

	squad := NewSquad("squad-1", "Squad", "", bus)
	squad.RegisterSubAgent(NewSubAgent("agent-1", "Agent", "", "", bus, "squad-1"))

	err := squad.Run(context.Background(), "thread-squad", nil)
	if err == nil {
		t.Fatal("Squad.Run returned nil while the agent context could not be delivered")
	}
	if !errors.Is(err, rejected) {
		t.Fatalf("error = %v, want it to wrap the delivery failure", err)
	}
	if !strings.Contains(err.Error(), "agent-1") {
		t.Fatalf("error = %v, want it to identify the affected agent", err)
	}
}

func TestExecuteToolWithHealingStopsWhenHealingFails(t *testing.T) {
	bus := newConnectedBus(t)
	metrics := NewExecutionMetrics("thread-1")
	bus.SetMetrics("thread-1", metrics)
	agent := NewSubAgent("agent-1", "Agent", "", "", bus, "squad-1")
	agent.ToolMaxRetry = 3

	toolCalls := 0
	agent.PythonToolsMap["broken"] = LocalTool{
		Func: func(map[string]any) (any, error) {
			toolCalls++
			return nil, errors.New("boom")
		},
	}
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		return LLMResponse{}, errors.New("healing model unavailable")
	}

	result, err := agent.executeToolWithHealing(context.Background(), "thread-1", "broken", map[string]any{"value": 1})
	if err == nil {
		t.Fatalf("expected a failure, got result %q", result)
	}
	if result != "" {
		t.Fatalf("result = %q, want the failure to be reported only through the error", result)
	}

	var toolErr *ToolExecutionError
	if !errors.As(err, &toolErr) {
		t.Fatalf("error = %v, want a *ToolExecutionError", err)
	}
	if toolErr.Tool != "broken" {
		t.Fatalf("tool = %q, want broken", toolErr.Tool)
	}
	if toolCalls != 1 {
		t.Fatalf("tool executed %d times, want 1: healing failed, so retrying replays identical arguments", toolCalls)
	}
	metricSnapshot := metrics.ToDict()
	if metricSnapshot["retry_count"] != 1 || metricSnapshot["error_count"] != 1 {
		t.Fatalf("metrics = %#v, want one retry and one tool error", metricSnapshot)
	}
}

func TestExecuteToolWithHealingRetriesWithCorrectedArguments(t *testing.T) {
	bus := newConnectedBus(t)
	metrics := NewExecutionMetrics("thread-1")
	bus.SetMetrics("thread-1", metrics)
	agent := NewSubAgent("agent-1", "Agent", "", "", bus, "squad-1")
	agent.ToolMaxRetry = 3

	var seenArgs []any
	agent.PythonToolsMap["strict"] = LocalTool{
		Func: func(args map[string]any) (any, error) {
			seenArgs = append(seenArgs, args["value"])
			if args["value"] != "corrected" {
				return nil, errors.New("unexpected value")
			}
			return "ok", nil
		},
	}
	agent.LLMCall = func(context.Context, string, string, []map[string]any) (LLMResponse, error) {
		return LLMResponse{Content: "```json\n{\"arguments\": {\"value\": \"corrected\"}}\n```"}, nil
	}

	result, err := agent.executeToolWithHealing(context.Background(), "thread-1", "strict", map[string]any{"value": "wrong"})
	if err != nil {
		t.Fatalf("execute tool with healing: %v", err)
	}
	if result != "ok" {
		t.Fatalf("result = %q, want ok", result)
	}
	if len(seenArgs) != 2 || seenArgs[0] != "wrong" || seenArgs[1] != "corrected" {
		t.Fatalf("tool arguments = %v, want the healed value on the second attempt", seenArgs)
	}
	if retryCount := metrics.ToDict()["retry_count"]; retryCount != 1 {
		t.Fatalf("retry_count = %v, want 1", retryCount)
	}
}
