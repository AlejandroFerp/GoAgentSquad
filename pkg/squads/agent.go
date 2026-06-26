package squads

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

// This file contains agent-side behavior: local tool execution, delegation,
// reasoning loop orchestration, and transversal task processing.

// LLMCall is the function signature for invoking an LLM. It mirrors the Python
// llm_call contract: it receives a model name, system prompt, and a slice of
// message payloads, and returns a response map with content and token counts.
type LLMCall func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (map[string]any, error)

// CrossAgentTool represents a tool that delegates tasks to other agents/squads.
type CrossAgentTool struct {
	Name              string
	Description       string
	TaskType          string
	ReplyThreadSuffix string
	SquadID           string
}

// ToolSchema describes a local Python-style tool for the LLM.
type ToolSchema struct {
	Name        string
	Description string
	Parameters  map[string]ToolParam
}

// ToolParam describes a single parameter of a tool.
type ToolParam struct {
	Type     string
	Required bool
	Default  any
}

// LocalTool is a registered Go function callable by the LLM.
type LocalTool struct {
	Schema ToolSchema
	Func   func(args map[string]any) (any, error)
}

// PreRunHook executes logic before a subagent runs.
type PreRunHook interface {
	Execute(ctx context.Context, agent *SubAgent, threadID string, triggerMsg *synapse.SynapseMessage) error
}

// BaseAgent provides shared utilities for all agent types.
type BaseAgent struct {
	AgentID    string
	Blackboard BlackboardBus
	SquadID    string
	Clock      func() float64
}

// NewBaseAgent creates a BaseAgent with a monotonic elapsed-time clock.
func NewBaseAgent(agentID string, bb BlackboardBus, squadID string) BaseAgent {
	return BaseAgent{AgentID: agentID, Blackboard: bb, SquadID: squadID, Clock: timeMonotonic}
}

// SendContextMessage posts a ContextMessage to the blackboard.
func (b *BaseAgent) SendContextMessage(ctx context.Context, threadID, content string, role synapse.Role, citations []any, squadID string) (*synapse.SynapseMessage, error) {
	if squadID == "" {
		squadID = b.SquadID
	}
	if role == synapse.RoleAssistant {
		stepTime := time.Now()
		ctx, _ = recordObservedStep(ctx, b.Blackboard, observability.AgentStep{
			Kind:       observability.StepResponded,
			AgentID:    b.AgentID,
			SquadID:    squadID,
			ThreadID:   threadID,
			Summary:    observedSummary(content),
			StartedAt:  stepTime,
			FinishedAt: stepTime,
		})
	}
	msg := synapse.NewContextMessage(threadID, b.AgentID, role, content, squadID, citations, 3600)
	return b.Blackboard.SendMessage(ctx, msg)
}

// SendTaskMessage posts a TaskMessage to the blackboard.
func (b *BaseAgent) SendTaskMessage(ctx context.Context, threadID, taskType, replyToThread string, parameters map[string]any, squadID string, maxConsumers int) (*synapse.SynapseMessage, error) {
	msg := synapse.NewTaskMessage(threadID, b.AgentID, taskType, replyToThread, parameters, squadID, 3600, maxConsumers)
	return b.Blackboard.SendMessage(ctx, msg)
}

// Respond is a convenience wrapper for posting an assistant message.
func (b *BaseAgent) Respond(ctx context.Context, threadID, content string, citations []any) (*synapse.SynapseMessage, error) {
	return b.SendContextMessage(ctx, threadID, content, synapse.RoleAssistant, citations, "")
}

// SubAgent is a squad-scoped agent with an LLM reasoning loop, local tools,
// cross-agent delegation tools, and dynamic access policies.
type SubAgent struct {
	BaseAgent

	AgentType    string
	Description  string
	SystemPrompt string
	Model        string
	LLMCall      LLMCall
	ToolMaxRetry int
	ResumeFn     func(ctx context.Context, replyThreadID string, replyMsg *synapse.SynapseMessage, context map[string]any) error

	ExcludedTransversals []string
	ExcludedSquads       []string
	ExcludedTasks        []string

	TopologyManifesto map[string]any
	PreRunHooks       []PreRunHook

	mu                 sync.Mutex
	PendingReplies     map[string]map[string]any
	CrossAgentToolsMap map[string]CrossAgentTool
	PythonToolsMap     map[string]LocalTool
	Tools              []string
}

// NewSubAgent builds a SubAgent with default settings.
func NewSubAgent(agentID, agentType, description, systemPrompt string, bb BlackboardBus, squadID string) *SubAgent {
	return &SubAgent{
		BaseAgent:          NewBaseAgent(agentID, bb, squadID),
		AgentType:          agentType,
		Description:        description,
		SystemPrompt:       systemPrompt,
		ToolMaxRetry:       3,
		PendingReplies:     make(map[string]map[string]any),
		CrossAgentToolsMap: make(map[string]CrossAgentTool),
		PythonToolsMap:     make(map[string]LocalTool),
		TopologyManifesto:  make(map[string]any),
	}
}

// RegisterPreRunHook adds a hook to run before agent execution.
func (a *SubAgent) RegisterPreRunHook(hook PreRunHook) {
	a.PreRunHooks = append(a.PreRunHooks, hook)
}

// UpdateTopology injects awareness of other agents and transversals.
func (a *SubAgent) UpdateTopology(manifesto map[string]any) {
	a.TopologyManifesto = manifesto
}

// UpdateGlobalTopology auto-registers CrossAgentTools for other squads and
// transversals, respecting exclusion lists.
func (a *SubAgent) UpdateGlobalTopology(otherSquads []map[string]any, transversals []map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, squadInfo := range otherSquads {
		sID, _ := squadInfo["squad_id"].(string)
		if a.isExcludedSquad(sID) {
			continue
		}
		toolName := "delegate_to_" + strings.ReplaceAll(sID, "-", "_")
		if _, exists := a.CrossAgentToolsMap[toolName]; !exists {
			desc, _ := squadInfo["description"].(string)
			if desc == "" {
				desc = fmt.Sprintf("Delegates task to squad '%v'.", squadInfo["name"])
			}
			a.CrossAgentToolsMap[toolName] = CrossAgentTool{
				Name: toolName, Description: desc, TaskType: "execute_task", SquadID: sID,
			}
		}
	}

	for _, transInfo := range transversals {
		tID, _ := transInfo["agent_id"].(string)
		if a.isExcludedTransversal(tID) {
			continue
		}
		caps, _ := transInfo["capabilities"].([]any)
		for _, cap := range caps {
			capStr, _ := cap.(string)
			if a.isExcludedTask(capStr) {
				continue
			}
			toolName := "delegate_transversal_" + capStr
			if _, exists := a.CrossAgentToolsMap[toolName]; !exists {
				desc, _ := transInfo["description"].(string)
				if desc == "" {
					desc = fmt.Sprintf("Delegates transversal task '%s'.", capStr)
				}
				a.CrossAgentToolsMap[toolName] = CrossAgentTool{
					Name: toolName, Description: desc, TaskType: capStr,
				}
			}
		}
	}

	a.Tools = nil
	for name := range a.CrossAgentToolsMap {
		a.Tools = append(a.Tools, name)
	}
	for name := range a.PythonToolsMap {
		a.Tools = append(a.Tools, name)
	}
}

func (a *SubAgent) isExcludedSquad(squadID string) bool {
	for _, s := range a.ExcludedSquads {
		if s == squadID {
			return true
		}
	}
	return false
}

func (a *SubAgent) isExcludedTransversal(taskType string) bool {
	for _, t := range a.ExcludedTransversals {
		if t == taskType {
			return true
		}
	}
	return false
}

func (a *SubAgent) isExcludedTask(taskType string) bool {
	for _, t := range a.ExcludedTasks {
		if t == taskType {
			return true
		}
	}
	return false
}

// Delegate sends a task to another squad or transversal and registers a
// pending reply so the agent can resume when the response arrives.
func (a *SubAgent) Delegate(ctx context.Context, threadID, taskType, replyToThread string, parameters map[string]any, squadID, respondThreadID string) (*synapse.SynapseMessage, error) {
	if squadID != "" {
		if a.isExcludedSquad(squadID) {
			return nil, fmt.Errorf("agent '%s' is blocked from delegating to squad '%s'", a.AgentID, squadID)
		}
	} else {
		if a.isExcludedTransversal(taskType) {
			return nil, fmt.Errorf("agent '%s' is blocked from delegating transversal task '%s'", a.AgentID, taskType)
		}
	}

	a.Blackboard.ParentThreads().Set(replyToThread, threadID)

	metrics := a.Blackboard.GetMetrics(threadID)
	if metrics != nil {
		if _, ok := a.Blackboard.Metrics()[replyToThread]; !ok {
			a.Blackboard.SetMetrics(replyToThread, metrics)
		}
		metrics.RecordSubagentWaiting(a.SquadID, a.AgentID, replyToThread)
		if squadID != "" {
			metrics.RecordCrossSquadMessage(a.AgentID, squadID, taskType, parameters)
		}
		destination := squadID
		if destination == "" {
			destination = taskType
		}
		delegationType := "transversal"
		if squadID != "" {
			delegationType = "cross_squad"
		}
		metrics.RecordDelegation(a.AgentID, destination, delegationType, parameters)
	}

	stepTime := time.Now()
	summary := taskType
	if squadID != "" {
		summary = fmt.Sprintf("delegate to squad %s (%s)", squadID, taskType)
	} else {
		summary = fmt.Sprintf("delegate transversal %s", taskType)
	}
	ctx, _ = recordObservedStep(ctx, a.Blackboard, observability.AgentStep{
		Kind:       observability.StepDelegated,
		AgentID:    a.AgentID,
		AgentType:  a.AgentType,
		SquadID:    a.SquadID,
		ThreadID:   threadID,
		Summary:    summary,
		ToolName:   taskType,
		StartedAt:  stepTime,
		FinishedAt: stepTime,
	})

	return a.CallToolDelegate(ctx, threadID, taskType, replyToThread, parameters, squadID, respondThreadID)
}

// CallToolDelegate registers the pending reply and sends the TaskMessage.
func (a *SubAgent) CallToolDelegate(ctx context.Context, threadID, taskType, replyToThread string, parameters map[string]any, squadID, respondThreadID string) (*synapse.SynapseMessage, error) {
	if squadID != "" {
		if a.isExcludedSquad(squadID) {
			return nil, fmt.Errorf("agent '%s' is blocked from delegating to squad '%s'", a.AgentID, squadID)
		}
	} else {
		if a.isExcludedTransversal(taskType) {
			return nil, fmt.Errorf("agent '%s' is blocked from delegating transversal task '%s'", a.AgentID, taskType)
		}
	}

	a.mu.Lock()
	a.PendingReplies[replyToThread] = map[string]any{
		"original_thread":   threadID,
		"respond_thread_id": respondThreadID,
		"task_type":         taskType,
		"parameters":        parameters,
	}
	a.mu.Unlock()

	return a.SendTaskMessage(ctx, threadID, taskType, replyToThread, parameters, squadID, 1)
}

// ExecuteInstrumented wraps Execute with telemetry tracking.
func (a *SubAgent) ExecuteInstrumented(ctx context.Context, threadID string, triggerMsg *synapse.SynapseMessage, respondThreadID string) {
	if triggerMsg != nil && triggerMsg.MessageClass == synapse.ClassTaskMessage {
		if a.isExcludedTask(triggerMsg.TaskType()) {
			return
		}
	}
	ctx, span := startObservedSpan(ctx, a.Blackboard, "agent.execute",
		observability.Attr{Key: observability.AttrAgentID, Value: a.AgentID},
		observability.Attr{Key: observability.AttrAgentType, Value: a.AgentType},
		observability.Attr{Key: observability.AttrSquadID, Value: a.SquadID},
		observability.Attr{Key: observability.AttrThreadID, Value: threadID},
	)
	defer span.End()
	stepStartedAt := time.Now()
	ctx, _ = recordObservedStep(ctx, a.Blackboard, observability.AgentStep{
		Kind:       observability.StepAgentStarted,
		AgentID:    a.AgentID,
		AgentType:  a.AgentType,
		SquadID:    a.SquadID,
		ThreadID:   threadID,
		Summary:    "agent execution started",
		StartedAt:  stepStartedAt,
		FinishedAt: stepStartedAt,
	})
	metrics := a.Blackboard.GetMetrics(threadID)
	if metrics != nil {
		metrics.RecordSubagentStart(a.SquadID, a.AgentID)
	}
	start := a.Clock()
	defer func() {
		elapsed := a.Clock() - start
		if metrics != nil {
			metrics.RecordSubagentEnd(a.SquadID, a.AgentID, elapsed)
		}
	}()
	if err := a.Execute(ctx, threadID, triggerMsg, respondThreadID); err != nil {
		span.RecordError(err)
		stepTime := time.Now()
		_, _ = recordObservedStep(ctx, a.Blackboard, observability.AgentStep{
			Kind:       observability.StepError,
			AgentID:    a.AgentID,
			AgentType:  a.AgentType,
			SquadID:    a.SquadID,
			ThreadID:   threadID,
			Summary:    observedSummary(err.Error()),
			Error:      err.Error(),
			StartedAt:  stepTime,
			FinishedAt: stepTime,
		})
		observedLogger(ctx, a.Blackboard).Error("agent execution failed",
			"agent_id", a.AgentID,
			"agent_type", a.AgentType,
			"squad_id", a.SquadID,
			"thread_id", threadID,
			"error", err,
		)
	}
}

// Execute is the adaptor that extracts the query from the trigger message and
// runs the reasoning loop.
func (a *SubAgent) Execute(ctx context.Context, threadID string, triggerMsg *synapse.SynapseMessage, respondThreadID string) error {
	if a.LLMCall == nil {
		return nil
	}
	query := ""
	if respondThreadID == "" {
		respondThreadID = threadID
	}
	if triggerMsg != nil {
		if c := triggerMsg.Content(); c != "" {
			query = c
		} else if triggerMsg.MessageClass == synapse.ClassTaskMessage && triggerMsg.ReplyToThread() != "" {
			respondThreadID = triggerMsg.ReplyToThread()
			if p := triggerMsg.Parameters(); p != nil {
				if q, ok := p["query"].(string); ok {
					query = q
				}
			}
		}
	}
	return a.RunReasoningLoop(ctx, threadID, query, respondThreadID)
}

// RunReasoningLoop is the core LLM reasoning cycle with tool calling and
// self-healing retries.
func (a *SubAgent) RunReasoningLoop(ctx context.Context, threadID, userQuery, respondThreadID string) error {
	if respondThreadID == "" {
		respondThreadID = threadID
	}

	ctxMsgs, err := FetchSlicedContext(ctx, a.Blackboard, threadID, 100)
	if err != nil {
		return err
	}

	messagesPayload := make([]map[string]any, 0, len(ctxMsgs)+1)
	for _, msg := range ctxMsgs {
		role := string(msg.Role)
		if role != "system" && role != "user" && role != "assistant" {
			role = "user"
		}
		messagesPayload = append(messagesPayload, map[string]any{
			"role":    role,
			"content": fmt.Sprintf("[%s]: %s", msg.AgentID, msg.Content()),
		})
	}

	if userQuery != "" {
		lastContent := ""
		if len(messagesPayload) > 0 {
			if c, ok := messagesPayload[len(messagesPayload)-1]["content"].(string); ok {
				lastContent = c
			}
		}
		if !strings.Contains(lastContent, userQuery) {
			messagesPayload = append(messagesPayload, map[string]any{
				"role":    "user",
				"content": fmt.Sprintf("[user-client]: %s", userQuery),
			})
		}
	}

	systemPrompt := a.injectToolDefinitions(a.SystemPrompt)
	res, err := a.callLLMObserved(ctx, threadID, systemPrompt, messagesPayload)
	if err != nil {
		return err
	}

	content, _ := res["content"].(string)
	toolCall := parseToolCall(content)

	if toolCall != nil {
		toolName, _ := toolCall["call_tool"].(string)
		arguments, _ := toolCall["arguments"].(map[string]any)

		preliminaryText := stripJSONBlocks(content)
		if preliminaryText != "" {
			if _, err := a.Respond(ctx, respondThreadID, preliminaryText, nil); err != nil {
				return err
			}
		}

		a.mu.Lock()
		_, isLocal := a.PythonToolsMap[toolName]
		crossTool, isCross := a.CrossAgentToolsMap[toolName]
		a.mu.Unlock()

		if isLocal {
			toolStartedAt := time.Now()
			ctx, _ = recordObservedStep(ctx, a.Blackboard, observability.AgentStep{
				Kind:       observability.StepToolCall,
				AgentID:    a.AgentID,
				AgentType:  a.AgentType,
				SquadID:    a.SquadID,
				ThreadID:   threadID,
				Summary:    fmt.Sprintf("local tool %s called", toolName),
				ToolName:   toolName,
				StartedAt:  toolStartedAt,
				FinishedAt: toolStartedAt,
			})
			result, err := a.executeToolWithHealing(ctx, threadID, toolName, arguments)
			if err != nil {
				result = fmt.Sprintf("Error: %v", err)
			}
			messagesPayload = append(messagesPayload, map[string]any{
				"role":    "user",
				"content": fmt.Sprintf("[System]: Tool %s returned result: %s", toolName, result),
			})
			finalRes, err := a.callLLMObserved(ctx, threadID, a.SystemPrompt+"\nIntegrate the tool result into your final response.", messagesPayload)
			if err != nil {
				return err
			}
			finalContent, _ := finalRes["content"].(string)
			_, _ = a.Respond(ctx, respondThreadID, finalContent, nil)
		} else if isCross {
			queryVal := userQuery
			if q, ok := arguments["query"].(string); ok {
				queryVal = q
			}
			params := map[string]any{"query": queryVal}
			if m, ok := arguments["query"].(map[string]any); ok {
				params = m
			}
			_, err := a.Delegate(ctx, threadID, crossTool.TaskType, threadID+crossTool.ReplyThreadSuffix, params, crossTool.SquadID, respondThreadID)
			return err
		} else {
			_, _ = a.Respond(ctx, respondThreadID, fmt.Sprintf("Error: Tool '%s' is not registered.", toolName), nil)
		}
	} else {
		_, _ = a.Respond(ctx, respondThreadID, content, nil)
	}
	return nil
}

// HandleReply is called when a delegated task returns a response.
func (a *SubAgent) HandleReply(ctx context.Context, replyThreadID string, replyMsg *synapse.SynapseMessage) {
	a.mu.Lock()
	resumptionCtx, ok := a.PendingReplies[replyThreadID]
	if ok {
		delete(a.PendingReplies, replyThreadID)
	}
	a.mu.Unlock()
	if !ok {
		return
	}

	origThread, _ := resumptionCtx["original_thread"].(string)
	if origThread == "" && replyMsg != nil {
		origThread = replyMsg.ThreadID
	}
	metrics := a.Blackboard.GetMetrics(origThread)
	if metrics != nil {
		metrics.RecordSubagentResumed(a.SquadID, a.AgentID, replyThreadID)
	}
	stepTime := time.Now()
	ctx, _ = recordObservedStep(ctx, a.Blackboard, observability.AgentStep{
		Kind:       observability.StepReplyReceived,
		AgentID:    a.AgentID,
		AgentType:  a.AgentType,
		SquadID:    a.SquadID,
		ThreadID:   origThread,
		Summary:    "reply received from delegated task",
		StartedAt:  stepTime,
		FinishedAt: stepTime,
	})
	start := a.Clock()
	defer func() {
		elapsed := a.Clock() - start
		if metrics != nil {
			metrics.AddSubagentElapsedTime(a.SquadID, a.AgentID, elapsed)
		}
	}()
	if a.ResumeFn != nil {
		if err := a.ResumeFn(ctx, replyThreadID, replyMsg, resumptionCtx); err != nil {
			observedLogger(ctx, a.Blackboard).Error("agent resume failed",
				"agent_id", a.AgentID,
				"agent_type", a.AgentType,
				"squad_id", a.SquadID,
				"reply_thread_id", replyThreadID,
				"error", err,
			)
		}
		return
	}
	respondThreadID, _ := resumptionCtx["respond_thread_id"].(string)
	if respondThreadID == "" {
		respondThreadID = origThread
	}
	if replyMsg != nil {
		_, _ = a.Respond(ctx, respondThreadID, replyMsg.Content(), nil)
	}
}

func (a *SubAgent) injectToolDefinitions(systemPrompt string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.PythonToolsMap) == 0 && len(a.CrossAgentToolsMap) == 0 {
		return systemPrompt
	}
	var sb strings.Builder
	for name, tool := range a.PythonToolsMap {
		sb.WriteString(fmt.Sprintf("- Name: %s\n", name))
		sb.WriteString(fmt.Sprintf("  Description: %s\n", tool.Schema.Description))
		sb.WriteString("  Parameters:\n")
		for pName, pInfo := range tool.Schema.Parameters {
			req := "optional"
			if pInfo.Required {
				req = "required"
			}
			sb.WriteString(fmt.Sprintf("    * %s (%s, %s)\n", pName, pInfo.Type, req))
		}
		sb.WriteString("\n")
	}
	for name, tool := range a.CrossAgentToolsMap {
		sb.WriteString(fmt.Sprintf("- Name: %s\n", name))
		sb.WriteString(fmt.Sprintf("  Description: %s\n", tool.Description))
		sb.WriteString("  Parameters:\n    * query (str, required)\n\n")
	}
	instructions := "\n\n### Available Tools\nYou have access to the following tools:\n" + sb.String() +
		"To call a tool, you MUST respond using ONLY a JSON block like this:\n" +
		"```json\n{\n  \"call_tool\": \"tool_name\",\n  \"arguments\": {\"arg1\": \"value\"}\n}\n```\n" +
		"Do not output any text before or after the block if you call a tool."
	return systemPrompt + instructions
}

func (a *SubAgent) executeToolWithHealing(ctx context.Context, threadID, toolName string, args map[string]any) (string, error) {
	retries := 0
	currentArgs := args
	for retries < a.ToolMaxRetry {
		a.mu.Lock()
		tool, ok := a.PythonToolsMap[toolName]
		a.mu.Unlock()
		if !ok {
			return "", fmt.Errorf("tool '%s' is not registered", toolName)
		}
		result, err := tool.Func(currentArgs)
		if err == nil {
			return fmt.Sprintf("%v", result), nil
		}
		retries++
		errorMsg := fmt.Sprintf("Exception executing '%s' with arguments %v: %v", toolName, currentArgs, err)
		if retries >= a.ToolMaxRetry {
			return fmt.Sprintf("Failed execution after %d attempts. Last error: %s", a.ToolMaxRetry, errorMsg), nil
		}
		healed, healErr := a.askLLMToHeal(ctx, threadID, toolName, tool.Schema, errorMsg, currentArgs)
		if healErr != nil {
			return "", healErr
		}
		currentArgs = healed
	}
	return "", fmt.Errorf("tool '%s' failed after %d retries", toolName, a.ToolMaxRetry)
}

func (a *SubAgent) askLLMToHeal(ctx context.Context, threadID, toolName string, schema ToolSchema, errorMsg string, failedArgs map[string]any) (map[string]any, error) {
	healPrompt := fmt.Sprintf(
		"You are a tool arguments correction helper.\n"+
			"The tool '%s' failed to execute.\n"+
			"Tool Schema:\n%v\n"+
			"Arguments used:\n%v\n"+
			"Error message:\n%s\n\n"+
			"Analyze the failure. Return ONLY a corrected JSON block for the arguments:\n"+
			"```json\n{\n  \"arg1\": \"corrected_value\"\n}\n```", toolName, schema, failedArgs, errorMsg)
	start := a.Clock()
	res, err := a.LLMCall(ctx, a.Model, healPrompt, nil)
	elapsed := a.Clock() - start
	metrics := a.Blackboard.GetMetrics(threadID)
	if metrics != nil {
		metrics.RecordLLMUsage(a.SquadID, a.AgentID, getInt(res, "prompt_tokens"), getInt(res, "completion_tokens"), getInt(res, "total_tokens"), elapsed)
	}
	if err != nil {
		return failedArgs, nil
	}
	content, _ := res["content"].(string)
	corrected := parseToolCall(content)
	if m, ok := corrected["arguments"].(map[string]any); ok {
		return m, nil
	}
	return failedArgs, nil
}

func (a *SubAgent) callLLMObserved(ctx context.Context, threadID, systemPrompt string, messages []map[string]any) (map[string]any, error) {
	if a.LLMCall == nil {
		return nil, nil
	}
	llmCtx, span := startObservedSpan(ctx, a.Blackboard, "agent.llm",
		observability.Attr{Key: observability.AttrAgentID, Value: a.AgentID},
		observability.Attr{Key: observability.AttrAgentType, Value: a.AgentType},
		observability.Attr{Key: observability.AttrSquadID, Value: a.SquadID},
		observability.Attr{Key: observability.AttrThreadID, Value: threadID},
		observability.Attr{Key: observability.AttrModel, Value: a.Model},
	)
	startedAt := time.Now()
	clockStart := a.Clock()
	res, err := a.LLMCall(llmCtx, a.Model, systemPrompt, messages)
	clockElapsed := a.Clock() - clockStart
	finishedAt := time.Now()
	metrics := a.Blackboard.GetMetrics(threadID)
	if metrics != nil {
		metrics.RecordLLMUsage(a.SquadID, a.AgentID, getInt(res, "prompt_tokens"), getInt(res, "completion_tokens"), getInt(res, "total_tokens"), clockElapsed)
	}
	step := observability.AgentStep{
		Kind:       observability.StepLLMCall,
		AgentID:    a.AgentID,
		AgentType:  a.AgentType,
		SquadID:    a.SquadID,
		ThreadID:   threadID,
		Summary:    "llm call",
		Model:      a.Model,
		TokensIn:   getInt(res, "prompt_tokens"),
		TokensOut:  getInt(res, "completion_tokens"),
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
	if err != nil {
		span.RecordError(err)
		step.Error = err.Error()
	}
	_, _ = recordObservedStep(llmCtx, a.Blackboard, step)
	span.End()
	return res, err
}

// TransversalAgent is a global, shared utility agent that runs outside squads.
type TransversalAgent struct {
	BaseAgent
	AgentType    string
	Description  string
	Capabilities []string
	ExecuteTask  func(ctx context.Context, taskMsg *synapse.SynapseMessage) (string, error)
}

// NewTransversalAgent builds a TransversalAgent.
func NewTransversalAgent(agentID, agentType string, capabilities []string, bb BlackboardBus) *TransversalAgent {
	return &TransversalAgent{
		BaseAgent:    NewBaseAgent(agentID, bb, ""),
		AgentType:    agentType,
		Capabilities: capabilities,
	}
}

// ProcessTask runs the transversal task logic and replies to the reply thread.
func (t *TransversalAgent) ProcessTask(ctx context.Context, taskMsg *synapse.SynapseMessage) {
	threadID := taskMsg.ThreadID
	ctx, span := startObservedSpan(ctx, t.Blackboard, "transversal.process_task",
		observability.Attr{Key: observability.AttrAgentID, Value: t.AgentID},
		observability.Attr{Key: observability.AttrAgentType, Value: t.AgentType},
		observability.Attr{Key: observability.AttrThreadID, Value: threadID},
	)
	defer span.End()
	stepTime := time.Now()
	ctx, _ = recordObservedStep(ctx, t.Blackboard, observability.AgentStep{
		Kind:       observability.StepAgentStarted,
		AgentID:    t.AgentID,
		AgentType:  t.AgentType,
		ThreadID:   threadID,
		Summary:    "transversal task started",
		StartedAt:  stepTime,
		FinishedAt: stepTime,
	})
	metrics := t.Blackboard.GetMetrics(threadID)
	if metrics != nil {
		metrics.RecordTransversalStart(t.AgentID)
	}
	start := nowSec()
	success := false
	defer func() {
		elapsed := nowSec() - start
		if metrics != nil {
			if success {
				metrics.RecordTransversalSuccess(t.AgentID, elapsed)
			} else {
				metrics.RecordTransversalFailure(t.AgentID, elapsed)
			}
		}
	}()

	result, err := t.ExecuteTask(ctx, taskMsg)
	if err != nil {
		span.RecordError(err)
		errTime := time.Now()
		_, _ = recordObservedStep(ctx, t.Blackboard, observability.AgentStep{
			Kind:       observability.StepError,
			AgentID:    t.AgentID,
			AgentType:  t.AgentType,
			ThreadID:   threadID,
			Summary:    observedSummary(err.Error()),
			Error:      err.Error(),
			StartedAt:  errTime,
			FinishedAt: errTime,
		})
		_, _ = t.SendContextMessage(ctx, taskMsg.ReplyToThread(), fmt.Sprintf("Error executing transversal task: %v", err), synapse.RoleSystem, nil, "")
		return
	}
	success = true
	_, _ = t.SendContextMessage(ctx, taskMsg.ReplyToThread(), result, synapse.RoleAssistant, nil, "")
}

// --- Helpers ---

var jsonBlockRe = regexp.MustCompile("(?s)```json\\s*(.*?)\\s*```")

func parseToolCall(text string) map[string]any {
	if text == "" {
		return nil
	}
	if m := jsonBlockRe.FindStringSubmatch(text); len(m) > 1 {
		var out map[string]any
		if err := json.Unmarshal([]byte(m[1]), &out); err == nil {
			return out
		}
	}
	firstBrace := strings.Index(text, "{")
	if firstBrace == -1 {
		return nil
	}
	balance := 0
	for i := firstBrace; i < len(text); i++ {
		switch text[i] {
		case '{':
			balance++
		case '}':
			balance--
			if balance == 0 {
				var out map[string]any
				if err := json.Unmarshal([]byte(text[firstBrace:i+1]), &out); err == nil {
					return out
				}
			}
		}
	}
	return nil
}

var (
	jsonFencedRe = regexp.MustCompile("(?s)```json\\s*\\{.*?\\}\\s*```")
	rawJSONRe    = regexp.MustCompile("(?s)\\{.*?\\}")
)

func stripJSONBlocks(text string) string {
	out := jsonFencedRe.ReplaceAllString(text, "")
	out = rawJSONRe.ReplaceAllString(out, "")
	return strings.TrimSpace(out)
}

func getInt(m map[string]any, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		case json.Number:
			i, _ := n.Int64()
			return int(i)
		}
	}
	return 0
}

// FetchSlicedContext retrieves context messages sliced from the latest synthesis
// checkpoint onwards.
func FetchSlicedContext(ctx context.Context, bb BlackboardBus, threadID string, limit int) ([]synapse.SynapseMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	messages, err := bb.FetchContext(ctx, threadID, limit)
	if err != nil {
		return nil, err
	}
	synthesisIdx := -1
	for i := len(messages) - 1; i >= 0; i-- {
		msg := messages[i]
		if msg.Role == synapse.RoleSystem {
			if isSynth, ok := msg.Payload["is_synthesis"].(bool); ok && isSynth {
				synthesisIdx = i
				break
			}
		}
	}
	if synthesisIdx != -1 {
		return messages[synthesisIdx:], nil
	}
	return messages, nil
}

// LogSystemContext is a helper used by pre-run hooks to inject RAG context.
func (a *SubAgent) LogSystemContext(ctx context.Context, threadID, content string) error {
	_, err := a.SendContextMessage(ctx, threadID, content, synapse.RoleSystem, nil, "")
	return err
}

var monotonicClockStart = time.Now()

// timeMonotonic returns elapsed seconds using Go's monotonic time component.
func timeMonotonic() float64 {
	return time.Since(monotonicClockStart).Seconds()
}
