package observability

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type StepKind string

const (
	StepQueryReceived StepKind = "query_received"
	StepRouted        StepKind = "routed"
	StepAgentStarted  StepKind = "agent_started"
	StepLLMCall       StepKind = "llm_call"
	StepToolCall      StepKind = "tool_call"
	StepDelegated     StepKind = "delegated"
	StepReplyReceived StepKind = "reply_received"
	StepSynthesis     StepKind = "synthesis"
	StepResponded     StepKind = "responded"
	StepQuiesced      StepKind = "quiesced"
	StepError         StepKind = "error"
)

// AgentStep is a serializable, business-level record of one observable action.
type AgentStep struct {
	StepID        string    `json:"step_id"`
	ParentStepID  string    `json:"parent_step_id,omitempty"`
	CorrelationID string    `json:"correlation_id"`
	TraceID       string    `json:"trace_id"`
	SpanID        string    `json:"span_id"`
	Kind          StepKind  `json:"kind"`
	AgentID       string    `json:"agent_id,omitempty"`
	AgentType     string    `json:"agent_type,omitempty"`
	SquadID       string    `json:"squad_id,omitempty"`
	ThreadID      string    `json:"thread_id,omitempty"`
	MessageID     string    `json:"message_id,omitempty"`
	Summary       string    `json:"summary,omitempty"`
	Model         string    `json:"model,omitempty"`
	TokensIn      int       `json:"tokens_in,omitempty"`
	TokensOut     int       `json:"tokens_out,omitempty"`
	ToolName      string    `json:"tool_name,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at,omitempty"`
	DurationMS    int64     `json:"duration_ms,omitempty"`
	Error         string    `json:"error,omitempty"`
}

type QuerySummary struct {
	CorrelationID string    `json:"correlation_id"`
	ThreadID      string    `json:"thread_id"`
	Summary       string    `json:"summary,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at,omitempty"`
	Status        string    `json:"status"`
	StepCount     int       `json:"step_count"`
}

// StepLedger stores timelines by correlation id. The zero value is usable.
type StepLedger struct {
	mu   sync.RWMutex
	byID map[string][]AgentStep
	hub  *Hub
}

func NewStepLedger(hub *Hub) *StepLedger {
	return &StepLedger{byID: make(map[string][]AgentStep), hub: hub}
}

func (l *StepLedger) Record(step AgentStep) AgentStep {
	if step.StepID == "" {
		step.StepID = uuid.NewString()
	}
	if step.StartedAt.IsZero() {
		step.StartedAt = time.Now()
	}
	if !step.FinishedAt.IsZero() && step.DurationMS == 0 {
		step.DurationMS = step.FinishedAt.Sub(step.StartedAt).Milliseconds()
	}

	l.mu.Lock()
	if l.byID == nil {
		l.byID = make(map[string][]AgentStep)
	}
	l.byID[step.CorrelationID] = append(l.byID[step.CorrelationID], step)
	hub := l.hub
	l.mu.Unlock()

	if hub != nil {
		hub.Broadcast(step)
	}
	return step
}

func (l *StepLedger) Timeline(correlationID string) []AgentStep {
	l.mu.RLock()
	defer l.mu.RUnlock()
	steps := l.byID[correlationID]
	out := make([]AgentStep, len(steps))
	copy(out, steps)
	return out
}

func (l *StepLedger) Queries() []QuerySummary {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]QuerySummary, 0, len(l.byID))
	for correlationID, steps := range l.byID {
		if len(steps) == 0 {
			continue
		}
		first := steps[0]
		last := steps[len(steps)-1]
		status := "running"
		if last.Kind == StepResponded || last.Kind == StepQuiesced {
			status = "done"
		}
		if last.Kind == StepError || last.Error != "" {
			status = "error"
		}
		out = append(out, QuerySummary{
			CorrelationID: correlationID,
			ThreadID:      first.ThreadID,
			Summary:       first.Summary,
			StartedAt:     first.StartedAt,
			FinishedAt:    last.FinishedAt,
			Status:        status,
			StepCount:     len(steps),
		})
	}
	return out
}
