package observability

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

type StepKind string

// Step kinds represent normalized lifecycle events emitted by the pipeline.
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

// LLMMessage is one normalized message sent to a language model.
type LLMMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// LLMTrace contains opt-in request, completion, and provider metadata for one LLM call.
// It deliberately excludes unverified model reasoning content.
type LLMTrace struct {
	SystemPrompt    string       `json:"system_prompt"`
	Messages        []LLMMessage `json:"messages"`
	Completion      string       `json:"completion"`
	Provider        string       `json:"provider,omitempty"`
	RequestID       string       `json:"request_id,omitempty"`
	GenerationID    string       `json:"generation_id,omitempty"`
	FinishReason    string       `json:"finish_reason,omitempty"`
	CostUSD         float64      `json:"cost_usd,omitempty"`
	ReasoningTokens int          `json:"reasoning_tokens,omitempty"`
}

// Execution budget statuses that a recorded snapshot can carry. They are the
// shared vocabulary between the squads runtime and observability consumers.
const (
	BudgetStatusDisabled  = "disabled"
	BudgetStatusAvailable = "available"
	BudgetStatusExhausted = "exhausted"
	BudgetStatusExceeded  = "exceeded"
)

// ExecutionBudgetSnapshot records execution-wide LLM usage and configured limits.
// It is independent of the squads runtime so observability consumers can render it.
type ExecutionBudgetSnapshot struct {
	UsageSequence    uint64  `json:"usage_sequence"`
	PromptTokens     int     `json:"prompt_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CostUSD          float64 `json:"cost_usd"`
	MaxTotalTokens   int     `json:"max_total_tokens"`
	MaxCostUSD       float64 `json:"max_cost_usd"`
	Status           string  `json:"status"`
}

// AgentStep is a serializable, business-level record of one observable action.
type AgentStep struct {
	StepID        string                   `json:"step_id"`
	ParentStepID  string                   `json:"parent_step_id,omitempty"`
	CorrelationID string                   `json:"correlation_id"`
	TraceID       string                   `json:"trace_id"`
	SpanID        string                   `json:"span_id"`
	Kind          StepKind                 `json:"kind"`
	AgentID       string                   `json:"agent_id,omitempty"`
	AgentType     string                   `json:"agent_type,omitempty"`
	SquadID       string                   `json:"squad_id,omitempty"`
	ThreadID      string                   `json:"thread_id,omitempty"`
	MessageID     string                   `json:"message_id,omitempty"`
	Summary       string                   `json:"summary,omitempty"`
	Model         string                   `json:"model,omitempty"`
	TokensIn      int                      `json:"tokens_in,omitempty"`
	TokensOut     int                      `json:"tokens_out,omitempty"`
	CostUSD       float64                  `json:"cost_usd,omitempty"`
	Budget        *ExecutionBudgetSnapshot `json:"budget,omitempty"`
	ToolName      string                   `json:"tool_name,omitempty"`
	StartedAt     time.Time                `json:"started_at"`
	FinishedAt    time.Time                `json:"finished_at,omitempty"`
	DurationMS    int64                    `json:"duration_ms,omitempty"`
	Error         string                   `json:"error,omitempty"`
	LLMTrace      *LLMTrace                `json:"llm_trace,omitempty"`
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

const DefaultMaxRetainedCorrelations = 200

// StepLedger stores timelines by correlation id. The zero value is usable.
type StepLedger struct {
	mu               sync.RWMutex
	byID             map[string][]AgentStep
	seen             map[string]struct{}
	correlationOrder []string
	maxCorrelations  int
	hub              *Hub
}

func NewStepLedger(hub *Hub) *StepLedger {
	return NewStepLedgerWithLimit(hub, DefaultMaxRetainedCorrelations)
}

// NewStepLedgerWithLimit creates a ledger retaining at most maxCorrelations timelines.
func NewStepLedgerWithLimit(hub *Hub, maxCorrelations int) *StepLedger {
	if maxCorrelations <= 0 {
		maxCorrelations = DefaultMaxRetainedCorrelations
	}
	return &StepLedger{
		byID:            make(map[string][]AgentStep),
		seen:            make(map[string]struct{}),
		maxCorrelations: maxCorrelations,
		hub:             hub,
	}
}

// Record appends one step and broadcasts it to live subscribers.
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
	if l.seen == nil {
		l.seen = make(map[string]struct{})
	}
	// Deduplicate by step id to avoid duplicates during JSONL replay/tailing.
	if _, exists := l.seen[step.StepID]; exists {
		l.mu.Unlock()
		return step
	}
	l.seen[step.StepID] = struct{}{}
	if _, exists := l.byID[step.CorrelationID]; !exists {
		l.correlationOrder = append(l.correlationOrder, step.CorrelationID)
	}
	l.byID[step.CorrelationID] = append(l.byID[step.CorrelationID], step)
	l.evictExcessCorrelationsLocked()
	hub := l.hub
	l.mu.Unlock()

	if hub != nil {
		hub.Broadcast(step)
	}
	return step
}

func (l *StepLedger) evictExcessCorrelationsLocked() {
	maxCorrelations := l.maxCorrelations
	if maxCorrelations <= 0 {
		maxCorrelations = DefaultMaxRetainedCorrelations
	}
	for len(l.byID) > maxCorrelations {
		evictionIndex := 0
		for index, correlationID := range l.correlationOrder {
			if isTerminalTimeline(l.byID[correlationID]) {
				evictionIndex = index
				break
			}
		}
		correlationID := l.correlationOrder[evictionIndex]
		for _, step := range l.byID[correlationID] {
			delete(l.seen, step.StepID)
		}
		delete(l.byID, correlationID)
		l.correlationOrder = append(l.correlationOrder[:evictionIndex], l.correlationOrder[evictionIndex+1:]...)
	}
}

func isTerminalTimeline(steps []AgentStep) bool {
	if len(steps) == 0 {
		return false
	}
	return isRootTerminalStep(steps, steps[len(steps)-1])
}

func isRootTerminalStep(steps []AgentStep, step AgentStep) bool {
	if step.ThreadID != rootThreadID(steps) {
		return false
	}
	return step.Kind == StepResponded || step.Kind == StepQuiesced || step.Kind == StepError || step.Error != ""
}

func rootThreadID(steps []AgentStep) string {
	if len(steps) == 0 {
		return ""
	}
	return steps[0].ThreadID
}

func (l *StepLedger) Timeline(correlationID string) []AgentStep {
	l.mu.RLock()
	defer l.mu.RUnlock()
	steps := l.byID[correlationID]
	out := make([]AgentStep, len(steps))
	copy(out, steps)
	return out
}

// Queries returns one compact summary per correlation id for list views.
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
		if isRootTerminalStep(steps, last) && (last.Kind == StepResponded || last.Kind == StepQuiesced) {
			status = "done"
		}
		if isRootTerminalStep(steps, last) && (last.Kind == StepError || last.Error != "") {
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
