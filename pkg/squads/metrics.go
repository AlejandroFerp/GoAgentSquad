package squads

import (
	"context"
	"maps"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// This file provides the thread-safe metrics model used by squads, agents,
// observers, and transversals during one pipeline execution.

// AgentMetrics tracks per-agent execution stats in a thread-safe manner.
type AgentMetrics struct {
	mu               sync.Mutex
	AgentID          string
	Name             string
	Status           string // "Idle", "Running", "Waiting", "Finished"
	Invocations      int
	ElapsedTime      time.Duration
	PendingReplies   []string
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CostUSD          float64
	LLMCalls         int
	LLMElapsedTime   time.Duration
	RetryCount       int
	ErrorCount       int
}

func NewAgentMetrics(agentID, name string) *AgentMetrics {
	return &AgentMetrics{AgentID: agentID, Name: name, Status: "Idle"}
}

func (a *AgentMetrics) ToDict() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return map[string]any{
		"agent_id":          a.AgentID,
		"name":              a.Name,
		"status":            a.Status,
		"invocations":       a.Invocations,
		"elapsed_time":      a.ElapsedTime,
		"pending_replies":   append([]string{}, a.PendingReplies...),
		"prompt_tokens":     a.PromptTokens,
		"completion_tokens": a.CompletionTokens,
		"total_tokens":      a.TotalTokens,
		"cost_usd":          a.CostUSD,
		"llm_calls":         a.LLMCalls,
		"llm_elapsed_time":  a.LLMElapsedTime,
		"retry_count":       a.RetryCount,
		"error_count":       a.ErrorCount,
	}
}

// SquadMetrics tracks per-squad execution stats.
type SquadMetrics struct {
	mu               sync.Mutex
	SquadID          string
	Name             string
	Status           string
	Subagents        map[string]*AgentMetrics
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	CostUSD          float64
	LLMCalls         int
	LLMElapsedTime   time.Duration
}

func NewSquadMetrics(squadID, name string) *SquadMetrics {
	return &SquadMetrics{SquadID: squadID, Name: name, Status: "Idle", Subagents: make(map[string]*AgentMetrics)}
}

func (s *SquadMetrics) ToDict() map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	subs := make(map[string]any, len(s.Subagents))
	for aid, am := range s.Subagents {
		subs[aid] = am.ToDict()
	}
	return map[string]any{
		"squad_id":          s.SquadID,
		"name":              s.Name,
		"status":            s.Status,
		"subagents":         subs,
		"prompt_tokens":     s.PromptTokens,
		"completion_tokens": s.CompletionTokens,
		"total_tokens":      s.TotalTokens,
		"cost_usd":          s.CostUSD,
		"llm_calls":         s.LLMCalls,
		"llm_elapsed_time":  s.LLMElapsedTime,
	}
}

// ObserverMetrics tracks observer interactions.
type ObserverMetrics struct {
	mu           sync.Mutex
	Name         string
	Interactions int
}

func NewObserverMetrics(name string) *ObserverMetrics {
	return &ObserverMetrics{Name: name}
}

func (o *ObserverMetrics) ToDict() map[string]any {
	o.mu.Lock()
	defer o.mu.Unlock()
	return map[string]any{"name": o.Name, "interactions": o.Interactions}
}

// TransversalMetrics tracks transversal agent stats.
type TransversalMetrics struct {
	mu                sync.Mutex
	AgentID           string
	Name              string
	Status            string
	SuccessfulInvokes int
	ElapsedTime       time.Duration
}

func NewTransversalMetrics(agentID, name string) *TransversalMetrics {
	return &TransversalMetrics{AgentID: agentID, Name: name, Status: "Waiting"}
}

func (t *TransversalMetrics) ToDict() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	return map[string]any{
		"agent_id":           t.AgentID,
		"name":               t.Name,
		"status":             t.Status,
		"successful_invokes": t.SuccessfulInvokes,
		"elapsed_time":       t.ElapsedTime,
	}
}

// CrossSquadMessageMetrics records a single cross-squad delegation event.
type CrossSquadMessageMetrics struct {
	FromAgent  string
	ToSquad    string
	TaskType   string
	Parameters map[string]any
	Timestamp  time.Time
}

func (c CrossSquadMessageMetrics) ToDict() map[string]any {
	return map[string]any{
		"from_agent": c.FromAgent,
		"to_squad":   c.ToSquad,
		"task_type":  c.TaskType,
		"parameters": c.Parameters,
		"timestamp":  c.Timestamp,
	}
}

// TaskMetrics tracks the lifecycle of one task type across an execution.
type TaskMetrics struct {
	TaskType  string
	Started   int
	Completed int
	Failed    int
}

func (t *TaskMetrics) ToDict() map[string]any {
	return map[string]any{
		"task_type": t.TaskType,
		"started":   t.Started,
		"completed": t.Completed,
		"failed":    t.Failed,
	}
}

// ExecutionMetrics is the thread-safe PipelineMetrics implementation.
type ExecutionMetrics struct {
	mu                  sync.Mutex
	ThreadID            string
	Status              string
	StartTime           time.Time
	EndTime             time.Time
	ElapsedTime         time.Duration
	Squads              map[string]*SquadMetrics
	Observers           map[string]*ObserverMetrics
	Transversals        map[string]*TransversalMetrics
	Tasks               map[string]*TaskMetrics
	CrossSquadMessages  []CrossSquadMessageMetrics
	Delegations         []map[string]any
	RetryCount          int
	ErrorCount          int
	RetriesByOperation  map[string]int
	ErrorsByCategory    map[string]int
	PromptTokens        int
	CompletionTokens    int
	TotalTokens         int
	CostUSD             float64
	budget              ExecutionBudget
	budgetPermit        chan struct{}
	budgetBlocked       bool
	budgetUsageSequence uint64
}

var _ PipelineMetrics = (*ExecutionMetrics)(nil)
var _ budgetAwarePipelineMetrics = (*ExecutionMetrics)(nil)

// NewExecutionMetrics creates a fresh metrics tracker for a thread.
func NewExecutionMetrics(threadID string) *ExecutionMetrics {
	return newExecutionMetrics(threadID, ExecutionBudget{})
}

// NewExecutionMetricsWithBudget creates a metrics tracker with validated limits.
func NewExecutionMetricsWithBudget(threadID string, budget ExecutionBudget) (*ExecutionMetrics, error) {
	if err := budget.Validate(); err != nil {
		return nil, err
	}
	return newExecutionMetrics(threadID, budget), nil
}

func newExecutionMetrics(threadID string, budget ExecutionBudget) *ExecutionMetrics {
	metrics := &ExecutionMetrics{
		ThreadID:           threadID,
		Status:             "Running",
		StartTime:          time.Now(),
		Squads:             make(map[string]*SquadMetrics),
		Observers:          make(map[string]*ObserverMetrics),
		Transversals:       make(map[string]*TransversalMetrics),
		Tasks:              make(map[string]*TaskMetrics),
		RetriesByOperation: make(map[string]int),
		ErrorsByCategory:   make(map[string]int),
		budget:             budget,
	}
	if budget.enabled() {
		metrics.budgetPermit = make(chan struct{}, 1)
		metrics.budgetPermit <- struct{}{}
	}
	return metrics
}

func (e *ExecutionMetrics) RegisterSquad(squadID string, squad *Squad) {
	e.mu.Lock()
	defer e.mu.Unlock()
	name := squadID
	if squad.Name != "" {
		name = squad.Name
	}
	sm := NewSquadMetrics(squadID, name)
	for aid, agent := range squad.SubAgents {
		agentType := agent.AgentType
		if agentType == "" {
			agentType = aid
		}
		sm.Subagents[aid] = NewAgentMetrics(aid, agentType)
	}
	e.Squads[squadID] = sm
}

func (e *ExecutionMetrics) RegisterTransversal(agentID, agentType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Transversals[agentID] = NewTransversalMetrics(agentID, agentType)
}

func (e *ExecutionMetrics) Finalize(status string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Status = status
	e.EndTime = time.Now()
	e.ElapsedTime = e.EndTime.Sub(e.StartTime)
	for _, s := range e.Squads {
		if s.Status == "Running" {
			s.Status = "Finished"
		}
		for _, a := range s.Subagents {
			if a.Status == "Running" || a.Status == "Waiting" {
				a.Status = "Finished"
			}
		}
	}
	for _, t := range e.Transversals {
		t.Status = "Waiting"
	}
}

func (e *ExecutionMetrics) RecordSquadStart(squadID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordSquadStartLocked(squadID)
}

func (e *ExecutionMetrics) recordSquadStartLocked(squadID string) {
	s, ok := e.Squads[squadID]
	if !ok {
		s = NewSquadMetrics(squadID, squadID)
		e.Squads[squadID] = s
	}
	s.Status = "Running"
}

func (e *ExecutionMetrics) RecordSquadEnd(squadID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		s.Status = "Finished"
	}
}

func (e *ExecutionMetrics) ensureSquadLocked(squadID string) *SquadMetrics {
	s, ok := e.Squads[squadID]
	if !ok {
		s = NewSquadMetrics(squadID, squadID)
		e.Squads[squadID] = s
	}
	return s
}

func (e *ExecutionMetrics) ensureAgentLocked(squadID, agentID string) *AgentMetrics {
	s := e.ensureSquadLocked(squadID)
	a, ok := s.Subagents[agentID]
	if !ok {
		a = NewAgentMetrics(agentID, agentID)
		s.Subagents[agentID] = a
	}
	return a
}

func (e *ExecutionMetrics) ensureTaskLocked(taskType string) *TaskMetrics {
	if taskType == "" {
		taskType = "unspecified"
	}
	task, ok := e.Tasks[taskType]
	if !ok {
		task = &TaskMetrics{TaskType: taskType}
		e.Tasks[taskType] = task
	}
	return task
}

func (e *ExecutionMetrics) RecordSubagentStart(squadID, agentID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordSquadStartLocked(squadID)
	a := e.ensureAgentLocked(squadID, agentID)
	a.Status = "Running"
	a.Invocations++
}

func (e *ExecutionMetrics) RecordSubagentEnd(squadID, agentID string, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		if a, ok := s.Subagents[agentID]; ok {
			a.Status = "Finished"
			a.ElapsedTime += elapsed
		}
	}
}

func (e *ExecutionMetrics) RecordSubagentWaiting(squadID, agentID, replyToThread string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		if a, ok := s.Subagents[agentID]; ok {
			a.Status = "Waiting"
			found := false
			for _, r := range a.PendingReplies {
				if r == replyToThread {
					found = true
					break
				}
			}
			if !found {
				a.PendingReplies = append(a.PendingReplies, replyToThread)
			}
		}
	}
}

func (e *ExecutionMetrics) RecordSubagentResumed(squadID, agentID, replyThreadID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordSquadStartLocked(squadID)
	a := e.ensureAgentLocked(squadID, agentID)
	a.Status = "Running"
	out := a.PendingReplies[:0]
	for _, r := range a.PendingReplies {
		if r != replyThreadID {
			out = append(out, r)
		}
	}
	a.PendingReplies = out
}

func (e *ExecutionMetrics) AddSubagentElapsedTime(squadID, agentID string, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		if a, ok := s.Subagents[agentID]; ok {
			a.ElapsedTime += elapsed
		}
	}
}

func (e *ExecutionMetrics) RecordCrossSquadMessage(fromAgent, toSquad, taskType string, parameters map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.CrossSquadMessages = append(e.CrossSquadMessages, CrossSquadMessageMetrics{
		FromAgent:  fromAgent,
		ToSquad:    toSquad,
		TaskType:   taskType,
		Parameters: parameters,
		Timestamp:  time.Now(),
	})
}

func (e *ExecutionMetrics) RecordDelegation(source, destination, delegationType string, parameters map[string]any) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Delegations = append(e.Delegations, map[string]any{
		"source":      source,
		"destination": destination,
		"type":        delegationType,
		"parameters":  parameters,
		"timestamp":   time.Now(),
	})
}

func (e *ExecutionMetrics) RecordTaskStarted(taskType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureTaskLocked(taskType).Started++
}

func (e *ExecutionMetrics) RecordTaskCompleted(taskType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureTaskLocked(taskType).Completed++
}

func (e *ExecutionMetrics) RecordTaskFailed(taskType string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ensureTaskLocked(taskType).Failed++
}

func (e *ExecutionMetrics) RecordRetry(squadID, agentID, operation string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.RetryCount++
	if operation == "" {
		operation = "unspecified"
	}
	e.RetriesByOperation[operation]++
	if squadID != "" && agentID != "" {
		e.ensureAgentLocked(squadID, agentID).RetryCount++
	}
}

func (e *ExecutionMetrics) RecordError(squadID, agentID, category string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ErrorCount++
	if category == "" {
		category = "unspecified"
	}
	e.ErrorsByCategory[category]++
	if squadID != "" && agentID != "" {
		e.ensureAgentLocked(squadID, agentID).ErrorCount++
	}
}

func (e *ExecutionMetrics) RecordTransversalStart(agentID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.Transversals[agentID]
	if !ok {
		t = NewTransversalMetrics(agentID, agentID)
		e.Transversals[agentID] = t
	}
	t.Status = "Running"
}

func (e *ExecutionMetrics) RecordTransversalSuccess(agentID string, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.Transversals[agentID]; ok {
		t.Status = "Waiting"
		t.SuccessfulInvokes++
		t.ElapsedTime += elapsed
	}
}

func (e *ExecutionMetrics) RecordTransversalFailure(agentID string, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.Transversals[agentID]; ok {
		t.Status = "Waiting"
		t.ElapsedTime += elapsed
	}
}

func (e *ExecutionMetrics) RecordObserverInteraction(observerName string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	o, ok := e.Observers[observerName]
	if !ok {
		o = NewObserverMetrics(observerName)
		e.Observers[observerName] = o
	}
	o.Interactions++
}

func (e *ExecutionMetrics) RecordLLMUsage(squadID, agentID string, prompt, completion, total int, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordSubagentLLMUsageLocked(squadID, agentID, prompt, completion, total, 0, elapsed)
}

func (e *ExecutionMetrics) RecordCoordinatorUsage(squadID string, prompt, completion, total int, elapsed time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordCoordinatorLLMUsageLocked(squadID, prompt, completion, total, 0, elapsed)
}

func (e *ExecutionMetrics) acquireLLMBudget(ctx context.Context) (observability.ExecutionBudgetSnapshot, func(), error) {
	if e.budgetPermit != nil {
		if err := ctx.Err(); err != nil {
			return e.BudgetSnapshot(), nil, err
		}
		select {
		case <-ctx.Done():
			return e.BudgetSnapshot(), nil, ctx.Err()
		case <-e.budgetPermit:
		}
		if err := ctx.Err(); err != nil {
			e.releaseLLMBudgetPermit()
			return e.BudgetSnapshot(), nil, err
		}
	}

	e.mu.Lock()
	snapshot := e.budgetSnapshotLocked()
	if snapshot.Status == string(BudgetStatusExhausted) || snapshot.Status == string(BudgetStatusExceeded) {
		e.budgetBlocked = true
		e.mu.Unlock()
		e.releaseLLMBudgetPermit()
		return snapshot, nil, &BudgetExceededError{Snapshot: snapshot}
	}
	e.mu.Unlock()

	if e.budgetPermit == nil {
		return snapshot, nil, nil
	}
	var releaseOnce sync.Once
	return snapshot, func() {
		releaseOnce.Do(e.releaseLLMBudgetPermit)
	}, nil
}

func (e *ExecutionMetrics) releaseLLMBudgetPermit() {
	if e.budgetPermit != nil {
		e.budgetPermit <- struct{}{}
	}
}

func (e *ExecutionMetrics) recordSubagentLLMUsageWithCost(squadID, agentID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.recordSubagentLLMUsageLocked(squadID, agentID, prompt, completion, total, costUSD, elapsed)
	return e.finishBudgetedLLMUsageLocked()
}

func (e *ExecutionMetrics) recordCoordinatorLLMUsageWithCost(squadID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) (observability.ExecutionBudgetSnapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.recordCoordinatorLLMUsageLocked(squadID, prompt, completion, total, costUSD, elapsed)
	return e.finishBudgetedLLMUsageLocked()
}

func (e *ExecutionMetrics) recordSubagentLLMUsageLocked(squadID, agentID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) {
	squad := e.ensureSquadLocked(squadID)
	agent := e.ensureAgentLocked(squadID, agentID)
	agent.PromptTokens += prompt
	agent.CompletionTokens += completion
	agent.TotalTokens += total
	agent.CostUSD += costUSD
	agent.LLMCalls++
	agent.LLMElapsedTime += elapsed

	e.recordSquadLLMUsageLocked(squad, prompt, completion, total, costUSD, elapsed)
	e.recordExecutionLLMUsageLocked(prompt, completion, total, costUSD)
}

func (e *ExecutionMetrics) recordCoordinatorLLMUsageLocked(squadID string, prompt, completion, total int, costUSD float64, elapsed time.Duration) {
	squad := e.ensureSquadLocked(squadID)
	e.recordSquadLLMUsageLocked(squad, prompt, completion, total, costUSD, elapsed)
	e.recordExecutionLLMUsageLocked(prompt, completion, total, costUSD)
}

func (e *ExecutionMetrics) recordSquadLLMUsageLocked(squad *SquadMetrics, prompt, completion, total int, costUSD float64, elapsed time.Duration) {
	squad.PromptTokens += prompt
	squad.CompletionTokens += completion
	squad.TotalTokens += total
	squad.CostUSD += costUSD
	squad.LLMCalls++
	squad.LLMElapsedTime += elapsed
}

func (e *ExecutionMetrics) recordExecutionLLMUsageLocked(prompt, completion, total int, costUSD float64) {
	e.PromptTokens += prompt
	e.CompletionTokens += completion
	e.TotalTokens += total
	e.CostUSD += costUSD
	e.budgetUsageSequence++
}

func (e *ExecutionMetrics) finishBudgetedLLMUsageLocked() (observability.ExecutionBudgetSnapshot, error) {
	snapshot := e.budgetSnapshotLocked()
	if snapshot.Status == string(BudgetStatusExceeded) {
		e.budgetBlocked = true
		return snapshot, &BudgetExceededError{Snapshot: snapshot}
	}
	return snapshot, nil
}

func (e *ExecutionMetrics) budgetFailure() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	snapshot := e.budgetSnapshotLocked()
	if e.budgetBlocked || snapshot.Status == string(BudgetStatusExceeded) {
		return &BudgetExceededError{Snapshot: snapshot}
	}
	return nil
}

// BudgetSnapshot returns a consistent execution-wide usage and limit snapshot.
func (e *ExecutionMetrics) BudgetSnapshot() observability.ExecutionBudgetSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.budgetSnapshotLocked()
}

func (e *ExecutionMetrics) budgetSnapshotLocked() observability.ExecutionBudgetSnapshot {
	snapshot := observability.ExecutionBudgetSnapshot{
		UsageSequence:    e.budgetUsageSequence,
		PromptTokens:     e.PromptTokens,
		CompletionTokens: e.CompletionTokens,
		TotalTokens:      e.TotalTokens,
		CostUSD:          e.CostUSD,
		MaxTotalTokens:   e.budget.MaxTotalTokens,
		MaxCostUSD:       e.budget.MaxCostUSD,
	}
	switch {
	case !e.budget.enabled():
		snapshot.Status = string(BudgetStatusDisabled)
	case (e.budget.MaxTotalTokens > 0 && e.TotalTokens > e.budget.MaxTotalTokens) || (e.budget.MaxCostUSD > 0 && e.CostUSD > e.budget.MaxCostUSD):
		snapshot.Status = string(BudgetStatusExceeded)
	case (e.budget.MaxTotalTokens > 0 && e.TotalTokens == e.budget.MaxTotalTokens) || (e.budget.MaxCostUSD > 0 && e.CostUSD == e.budget.MaxCostUSD):
		snapshot.Status = string(BudgetStatusExhausted)
	default:
		snapshot.Status = string(BudgetStatusAvailable)
	}
	return snapshot
}

func (e *ExecutionMetrics) ToDict() map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	squads := make(map[string]any, len(e.Squads))
	for sid, sm := range e.Squads {
		squads[sid] = sm.ToDict()
	}
	observers := make(map[string]any, len(e.Observers))
	for on, om := range e.Observers {
		observers[on] = om.ToDict()
	}
	transversals := make(map[string]any, len(e.Transversals))
	for tid, tm := range e.Transversals {
		transversals[tid] = tm.ToDict()
	}
	tasks := make(map[string]any, len(e.Tasks))
	for taskType, task := range e.Tasks {
		tasks[taskType] = task.ToDict()
	}
	cross := make([]map[string]any, 0, len(e.CrossSquadMessages))
	for _, c := range e.CrossSquadMessages {
		cross = append(cross, c.ToDict())
	}
	elapsed := e.ElapsedTime
	if e.EndTime.IsZero() {
		elapsed = time.Since(e.StartTime)
	}
	budget := e.budgetSnapshotLocked()
	return map[string]any{
		"thread_id":            e.ThreadID,
		"status":               e.Status,
		"start_time":           e.StartTime,
		"end_time":             e.EndTime,
		"elapsed_time":         elapsed,
		"squads":               squads,
		"observers":            observers,
		"transversals":         transversals,
		"tasks":                tasks,
		"cross_squad_messages": cross,
		"delegations":          e.Delegations,
		"retry_count":          e.RetryCount,
		"retries_by_operation": maps.Clone(e.RetriesByOperation),
		"error_count":          e.ErrorCount,
		"errors_by_category":   maps.Clone(e.ErrorsByCategory),
		"prompt_tokens":        e.PromptTokens,
		"completion_tokens":    e.CompletionTokens,
		"total_tokens":         e.TotalTokens,
		"cost_usd":             e.CostUSD,
		"budget_status":        budget.Status,
		"budget": map[string]any{
			"usage_sequence":    budget.UsageSequence,
			"prompt_tokens":     budget.PromptTokens,
			"completion_tokens": budget.CompletionTokens,
			"total_tokens":      budget.TotalTokens,
			"cost_usd":          budget.CostUSD,
			"max_total_tokens":  budget.MaxTotalTokens,
			"max_cost_usd":      budget.MaxCostUSD,
			"status":            budget.Status,
		},
	}
}
