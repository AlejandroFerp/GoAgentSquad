package squads

import (
	"sync"
	"time"
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
	ElapsedTime      float64
	PendingReplies   []string
	PromptTokens     int
	CompletionTokens int
	TotalTokens      int
	LLMCalls         int
	LLMElapsedTime   float64
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
		"elapsed_time":      round(a.ElapsedTime, 4),
		"pending_replies":   append([]string{}, a.PendingReplies...),
		"prompt_tokens":     a.PromptTokens,
		"completion_tokens": a.CompletionTokens,
		"total_tokens":      a.TotalTokens,
		"llm_calls":         a.LLMCalls,
		"llm_elapsed_time":  round(a.LLMElapsedTime, 4),
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
	LLMCalls         int
	LLMElapsedTime   float64
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
		"llm_calls":         s.LLMCalls,
		"llm_elapsed_time":  round(s.LLMElapsedTime, 4),
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
	ElapsedTime       float64
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
		"elapsed_time":       round(t.ElapsedTime, 4),
	}
}

// CrossSquadMessageMetrics records a single cross-squad delegation event.
type CrossSquadMessageMetrics struct {
	FromAgent  string
	ToSquad    string
	TaskType   string
	Parameters map[string]any
	Timestamp  float64
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

// ExecutionMetrics is the thread-safe PipelineMetrics implementation.
type ExecutionMetrics struct {
	mu                 sync.Mutex
	ThreadID           string
	Status             string
	StartTime          float64
	EndTime            float64
	ElapsedTime        float64
	Squads             map[string]*SquadMetrics
	Observers          map[string]*ObserverMetrics
	Transversals       map[string]*TransversalMetrics
	CrossSquadMessages []CrossSquadMessageMetrics
	Delegations        []map[string]any
}

// NewExecutionMetrics creates a fresh metrics tracker for a thread.
func NewExecutionMetrics(threadID string) *ExecutionMetrics {
	return &ExecutionMetrics{
		ThreadID:     threadID,
		Status:       "Running",
		StartTime:    nowSec(),
		Squads:       make(map[string]*SquadMetrics),
		Observers:    make(map[string]*ObserverMetrics),
		Transversals: make(map[string]*TransversalMetrics),
	}
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
	e.EndTime = nowSec()
	e.ElapsedTime = e.EndTime - e.StartTime
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

func (e *ExecutionMetrics) RecordSubagentStart(squadID, agentID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recordSquadStartLocked(squadID)
	a := e.ensureAgentLocked(squadID, agentID)
	a.Status = "Running"
	a.Invocations++
}

func (e *ExecutionMetrics) RecordSubagentEnd(squadID, agentID string, elapsed float64) {
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

func (e *ExecutionMetrics) AddSubagentElapsedTime(squadID, agentID string, elapsed float64) {
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
		Timestamp:  nowSec(),
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
		"timestamp":   nowSec(),
	})
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

func (e *ExecutionMetrics) RecordTransversalSuccess(agentID string, elapsed float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if t, ok := e.Transversals[agentID]; ok {
		t.Status = "Waiting"
		t.SuccessfulInvokes++
		t.ElapsedTime += elapsed
	}
}

func (e *ExecutionMetrics) RecordTransversalFailure(agentID string, elapsed float64) {
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

func (e *ExecutionMetrics) RecordLLMUsage(squadID, agentID string, prompt, completion, total int, elapsed float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		if a, ok := s.Subagents[agentID]; ok {
			a.PromptTokens += prompt
			a.CompletionTokens += completion
			a.TotalTokens += total
			a.LLMCalls++
			a.LLMElapsedTime += elapsed
		}
	}
}

func (e *ExecutionMetrics) RecordCoordinatorUsage(squadID string, prompt, completion, total int, elapsed float64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s, ok := e.Squads[squadID]; ok {
		s.PromptTokens += prompt
		s.CompletionTokens += completion
		s.TotalTokens += total
		s.LLMCalls++
		s.LLMElapsedTime += elapsed
	}
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
	cross := make([]map[string]any, 0, len(e.CrossSquadMessages))
	for _, c := range e.CrossSquadMessages {
		cross = append(cross, c.ToDict())
	}
	elapsed := e.ElapsedTime
	if e.EndTime == 0 {
		elapsed = nowSec() - e.StartTime
	}
	return map[string]any{
		"thread_id":            e.ThreadID,
		"status":               e.Status,
		"start_time":           e.StartTime,
		"end_time":             e.EndTime,
		"elapsed_time":         round(elapsed, 4),
		"squads":               squads,
		"observers":            observers,
		"transversals":         transversals,
		"cross_squad_messages": cross,
		"delegations":          e.Delegations,
	}
}

func nowSec() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// round truncates to a fixed number of decimals for stable metric output.
func round(v float64, decimals int) float64 {
	mul := 1.0
	for i := 0; i < decimals; i++ {
		mul *= 10
	}
	return float64(int(v*mul)) / mul
}
