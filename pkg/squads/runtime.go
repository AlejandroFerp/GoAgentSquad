package squads

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

const defaultRuntimeServiceCapacity = 200

// AgentDefinition declares an agent without exposing runtime wiring details.
type AgentDefinition struct {
	ID                   string
	Type                 string
	Description          string
	SystemPrompt         string
	Tools                map[string]LocalTool
	Model                string
	LLMCall              LLMCall
	ExcludedSquads       []string
	ExcludedTasks        []string
	ExcludedTransversals []string
}

// SquadDefinition declares a named group of agents.
type SquadDefinition struct {
	ID          string
	Name        string
	Description string
	Model       string
	LLMCall     LLMCall
	Agents      []AgentDefinition
}

// RuntimeConfig contains the application-owned declarative workflow definition.
type RuntimeConfig struct {
	ServiceCapacity   int
	MaxIterations     int
	Model             string
	LLMCall           LLMCall
	CaptureLLMContent bool
	ExecutionBudget   ExecutionBudget
	FinalSynthesizer  FinalSynthesizer
	RouteQueryFn      func(ctx context.Context, content string) ([]string, error)
	Squads            []SquadDefinition
}

// Runtime owns the infrastructure required to execute declaratively defined squads.
type Runtime struct {
	service    *synapse.SynapseService
	blackboard *SynapseBlackboardBus
	pipeline   *SquadsPipeline
}

// NewRuntime creates a connected runtime and registers every declared squad and agent.
func NewRuntime(ctx context.Context, cfg RuntimeConfig) (*Runtime, error) {
	if err := cfg.ExecutionBudget.Validate(); err != nil {
		return nil, fmt.Errorf("validate runtime execution budget: %w", err)
	}
	if len(cfg.Squads) == 0 {
		return nil, fmt.Errorf("runtime requires at least one squad")
	}

	serviceCapacity := cfg.ServiceCapacity
	if serviceCapacity <= 0 {
		serviceCapacity = defaultRuntimeServiceCapacity
	}
	service := synapse.NewSynapseService(serviceCapacity, nil)
	if err := service.Connect(ctx); err != nil {
		return nil, fmt.Errorf("connect Synapse service: %w", err)
	}

	blackboard := NewSynapseBlackboardBus(service)
	blackboard.Observability().CaptureLLMContent = cfg.CaptureLLMContent
	pipeline := NewSquadsPipeline(blackboard, cfg.FinalSynthesizer, cfg.MaxIterations)
	if err := pipeline.SetExecutionBudget(cfg.ExecutionBudget); err != nil {
		_ = service.Close()
		return nil, fmt.Errorf("configure execution budget: %w", err)
	}
	pipeline.RouteQueryFn = cfg.RouteQueryFn
	runtime := &Runtime{service: service, blackboard: blackboard, pipeline: pipeline}
	if err := runtime.registerSquads(cfg); err != nil {
		_ = service.Close()
		return nil, err
	}
	return runtime, nil
}

// Query executes a query through the configured pipeline.
func (r *Runtime) Query(ctx context.Context, threadID string, initialSquadIDs []string, content string, timeout time.Duration) (*QueryResult, error) {
	return r.pipeline.Query(ctx, threadID, initialSquadIDs, content, timeout)
}

// Observability exposes the runtime observed by the embedded dashboard.
func (r *Runtime) Observability() *ObservabilityRuntime {
	return r.blackboard.Observability()
}

// Close releases the Synapse service owned by the runtime.
func (r *Runtime) Close() error {
	return r.service.Close()
}

func (r *Runtime) registerSquads(cfg RuntimeConfig) error {
	registeredSquads := make(map[string]struct{}, len(cfg.Squads))
	registeredAgents := map[string]struct{}{}
	for _, definition := range cfg.Squads {
		if strings.TrimSpace(definition.ID) == "" {
			return fmt.Errorf("runtime squad ID must not be empty")
		}
		if _, exists := registeredSquads[definition.ID]; exists {
			return fmt.Errorf("runtime contains duplicate squad ID %q", definition.ID)
		}
		if len(definition.Agents) == 0 {
			return fmt.Errorf("runtime squad %q requires at least one agent", definition.ID)
		}

		squad := NewSquad(definition.ID, definition.Name, definition.Description, r.blackboard)
		squad.Model = firstNonEmpty(definition.Model, cfg.Model)
		squad.LLMCall = firstLLMCall(definition.LLMCall, cfg.LLMCall)
		for _, agentDefinition := range definition.Agents {
			if strings.TrimSpace(agentDefinition.ID) == "" {
				return fmt.Errorf("runtime squad %q contains an agent with an empty ID", definition.ID)
			}
			if _, exists := registeredAgents[agentDefinition.ID]; exists {
				return fmt.Errorf("runtime contains duplicate agent ID %q", agentDefinition.ID)
			}
			if strings.TrimSpace(agentDefinition.SystemPrompt) == "" {
				return fmt.Errorf("runtime agent %q requires a system prompt", agentDefinition.ID)
			}

			agent := NewSubAgent(
				agentDefinition.ID,
				agentDefinition.Type,
				agentDefinition.Description,
				agentDefinition.SystemPrompt,
				r.blackboard,
				definition.ID,
			)
			agent.Model = firstNonEmpty(agentDefinition.Model, squad.Model)
			agent.LLMCall = firstLLMCall(agentDefinition.LLMCall, squad.LLMCall)
			if agent.LLMCall == nil {
				return fmt.Errorf("runtime agent %q requires an LLM call", agentDefinition.ID)
			}
			for toolName, tool := range agentDefinition.Tools {
				agent.PythonToolsMap[toolName] = tool
			}
			agent.ExcludedSquads = append([]string(nil), agentDefinition.ExcludedSquads...)
			agent.ExcludedTasks = append([]string(nil), agentDefinition.ExcludedTasks...)
			agent.ExcludedTransversals = append([]string(nil), agentDefinition.ExcludedTransversals...)
			squad.RegisterSubAgent(agent)
			registeredAgents[agentDefinition.ID] = struct{}{}
		}
		r.pipeline.RegisterSquad(squad)
		registeredSquads[definition.ID] = struct{}{}
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func firstLLMCall(calls ...LLMCall) LLMCall {
	for _, call := range calls {
		if call != nil {
			return call
		}
	}
	return nil
}
