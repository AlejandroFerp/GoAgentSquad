// Package synapse provides the core messaging, event bus, and storage engine
// for the Synapse blackboard system designed for safe concurrent multi-threaded access.
package synapse

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// This file defines the serializable message model used by the Synapse
// blackboard and convenience constructors for each message class.

// Role enumerates the canonical message roles used across the blackboard.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// MessageClass identifies the concrete message type persisted in storage.
type MessageClass string

const (
	ClassContextMessage MessageClass = "ContextMessage"
	ClassTaskMessage    MessageClass = "TaskMessage"
	ClassCommandMessage MessageClass = "CommandMessage"
)

// TraceContext carries causal and tracing metadata across persisted messages.
type TraceContext struct {
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// SynapseMessage is the unified, serializable message structure exchanged
// through the blackboard. Payload values are copied at API boundaries so
// callers cannot mutate a stored message through an accessor result.
type SynapseMessage struct {
	ID               string        `json:"id"`
	Timestamp        time.Time     `json:"timestamp"`
	ThreadID         string        `json:"thread_id"`
	ParentThreadID   string        `json:"parent_thread_id,omitempty"`
	SquadID          string        `json:"squad_id,omitempty"`
	RecipientAgentID string        `json:"recipient_agent_id,omitempty"`
	AgentID          string        `json:"agent_id"`
	Role             Role          `json:"role"`
	TTL              time.Duration `json:"ttl"`
	MaxConsumers     int           `json:"max_consumers"`
	ConsumedCount    int           `json:"consumed_count"`
	Payload          Payload       `json:"payload"`
	MessageClass     MessageClass  `json:"message_class"`
	Trace            TraceContext  `json:"trace,omitempty"`
}

// NewSynapseMessage initializes a base message with sensible defaults.
// Panics if threadID or agentID are empty, enforcing fail-fast validation.
func NewSynapseMessage(threadID, agentID string, role Role, class MessageClass) SynapseMessage {
	if threadID == "" {
		panic("synapse: NewSynapseMessage requires non-empty threadID")
	}
	if agentID == "" {
		panic("synapse: NewSynapseMessage requires non-empty agentID")
	}
	return SynapseMessage{
		ID:            uuid.NewString(),
		Timestamp:     time.Now(),
		ThreadID:      threadID,
		AgentID:       agentID,
		Role:          role,
		TTL:           5 * time.Minute,
		MaxConsumers:  -1,
		ConsumedCount: 0,
		Payload:       Payload{values: make(map[string]any)},
		MessageClass:  class,
	}
}

// Validate checks the structural and class-specific payload contract.
func (m SynapseMessage) Validate() error {
	if m.ID == "" {
		return fmt.Errorf("synapse message ID is required")
	}
	if m.ThreadID == "" {
		return fmt.Errorf("synapse message thread ID is required")
	}
	if m.ParentThreadID == m.ThreadID && m.ParentThreadID != "" {
		return fmt.Errorf("synapse message parent thread ID cannot equal thread ID")
	}
	if m.AgentID == "" {
		return fmt.Errorf("synapse message agent ID is required")
	}
	if m.MessageClass == "" {
		return fmt.Errorf("synapse message class is required")
	}
	if m.Timestamp.IsZero() {
		return fmt.Errorf("synapse message timestamp is required")
	}
	if m.TTL <= 0 {
		return fmt.Errorf("synapse message TTL must be positive")
	}
	if m.MaxConsumers == 0 || m.MaxConsumers < -1 {
		return fmt.Errorf("synapse message max consumers must be -1 or positive")
	}
	if m.ConsumedCount < 0 {
		return fmt.Errorf("synapse message consumed count cannot be negative")
	}

	switch m.MessageClass {
	case ClassContextMessage:
		_, err := m.Payload.AsContext()
		return err
	case ClassTaskMessage:
		_, err := m.Payload.AsTask()
		return err
	case ClassCommandMessage:
		_, err := m.Payload.AsCommand()
		return err
	default:
		return fmt.Errorf("unsupported synapse message class %q", m.MessageClass)
	}
}

// IsExpired reports whether the message has outlived its TTL at the given now.
func (m SynapseMessage) IsExpired(now time.Time) bool {
	return !now.Before(m.Timestamp.Add(m.TTL))
}

// --- ContextMessage helpers ---

// NewContextMessage builds a ContextMessage ready to be sent through the bus.
func NewContextMessage(threadID, agentID string, role Role, content string, squadID string, citations []any, ttl time.Duration) SynapseMessage {
	if ttl == 0 {
		ttl = time.Hour
	}
	msg := NewSynapseMessage(threadID, agentID, role, ClassContextMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	msg.Payload.Set("content", content)
	msg.Payload.Set("citations", citations)
	return msg
}

// Content returns the textual content of a ContextMessage payload.
func (m SynapseMessage) Content() string {
	if c, ok := m.Payload.Get("content"); ok {
		if content, ok := c.(string); ok {
			return content
		}
	}
	return ""
}

// ContextPayload returns the validated typed payload of a ContextMessage.
func (m SynapseMessage) ContextPayload() (ContextPayload, error) {
	return m.Payload.AsContext()
}

// TaskPayload returns the validated typed payload of a TaskMessage.
func (m SynapseMessage) TaskPayload() (TaskPayload, error) {
	return m.Payload.AsTask()
}

// CommandPayload returns the validated typed payload of a CommandMessage.
func (m SynapseMessage) CommandPayload() (CommandPayload, error) {
	return m.Payload.AsCommand()
}

// SetPayloadValue updates one payload field through the defensive payload API.
func (m *SynapseMessage) SetPayloadValue(key string, value any) {
	m.Payload.Set(key, value)
}

// GetPayloadValue returns one payload field as a defensive copy.
func (m SynapseMessage) GetPayloadValue(key string) (any, bool) {
	return m.Payload.Get(key)
}

// Citations returns a defensive copy of the citation list attached to a ContextMessage.
func (m SynapseMessage) Citations() []any {
	if c, ok := m.Payload.Get("citations"); ok {
		if citations, ok := c.([]any); ok {
			return citations
		}
	}
	return nil
}

// --- TaskMessage helpers ---

// NewTaskMessage builds a TaskMessage used to delegate work across agents/squads.
func NewTaskMessage(threadID, agentID, taskType, replyToThread string, parameters map[string]any, squadID string, ttl time.Duration, maxConsumers int) SynapseMessage {
	if ttl == 0 {
		ttl = time.Hour
	}
	if maxConsumers == 0 {
		// Task messages are single-consumer by default to avoid duplicate work.
		maxConsumers = 1
	}
	msg := NewSynapseMessage(threadID, agentID, RoleAssistant, ClassTaskMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	msg.MaxConsumers = maxConsumers
	if parameters == nil {
		parameters = map[string]any{}
	}
	msg.Payload.Set("task_type", taskType)
	msg.Payload.Set("parameters", parameters)
	msg.Payload.Set("reply_to_thread", replyToThread)
	return msg
}

// TaskType returns the task_type field of a TaskMessage payload.
func (m SynapseMessage) TaskType() string {
	if payload, err := m.TaskPayload(); err == nil {
		return payload.TaskType
	}
	return ""
}

// Parameters returns a defensive copy of the parameters map of a TaskMessage payload.
// Callers may safely mutate the returned map without affecting the message.
func (m SynapseMessage) Parameters() map[string]any {
	switch m.MessageClass {
	case ClassTaskMessage:
		if payload, err := m.TaskPayload(); err == nil {
			return payload.Parameters
		}
	case ClassCommandMessage:
		if payload, err := m.CommandPayload(); err == nil {
			return payload.Parameters
		}
	}
	return map[string]any{}
}

// ReplyToThread returns the reply_to_thread field of a TaskMessage payload.
func (m SynapseMessage) ReplyToThread() string {
	if payload, err := m.TaskPayload(); err == nil {
		return payload.ReplyToThread
	}
	return ""
}

// --- CommandMessage helpers ---

// NewCommandMessage builds a CommandMessage used for control-plane directives.
func NewCommandMessage(threadID, agentID, command string, parameters map[string]any, squadID string, ttl time.Duration) SynapseMessage {
	if ttl == 0 {
		ttl = time.Hour
	}
	msg := NewSynapseMessage(threadID, agentID, RoleSystem, ClassCommandMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	if parameters == nil {
		parameters = map[string]any{}
	}
	msg.Payload.Set("command", command)
	msg.Payload.Set("parameters", parameters)
	return msg
}

// Command returns the command field of a CommandMessage payload.
func (m SynapseMessage) Command() string {
	if payload, err := m.CommandPayload(); err == nil {
		return payload.Command
	}
	return ""
}
