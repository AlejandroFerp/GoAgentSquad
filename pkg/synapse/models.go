// Package synapse provides the core messaging, event bus, and storage engine
// for the Synapse blackboard system. It is the Go equivalent of the Python
// agapes_synapse package, redesigned for safe concurrent multi-threaded access.
package synapse

import (
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

// Payload is the polymorphic content envelope carried by every SynapseMessage.
// Concrete payload shapes are defined as typed helpers below.
type Payload map[string]any

// TraceContext carries causal and tracing metadata across persisted messages.
type TraceContext struct {
	TraceID       string `json:"trace_id,omitempty"`
	SpanID        string `json:"span_id,omitempty"`
	CausationID   string `json:"causation_id,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
}

// SynapseMessage is the unified, serializable message structure exchanged
// through the blackboard. It replaces the Python BaseSynapseMessage hierarchy
// with a single flat struct that is safe to copy by value (the Payload map is
// shared intentionally and protected by the engine's mutex when mutated).
type SynapseMessage struct {
	ID               string       `json:"id"`
	Timestamp        float64      `json:"timestamp"`
	ThreadID         string       `json:"thread_id"`
	SquadID          string       `json:"squad_id,omitempty"`
	RecipientAgentID string       `json:"recipient_agent_id,omitempty"`
	AgentID          string       `json:"agent_id"`
	Role             Role         `json:"role"`
	TTL              int          `json:"ttl"`
	MaxConsumers     int          `json:"max_consumers"`
	ConsumedCount    int          `json:"consumed_count"`
	Payload          Payload      `json:"payload"`
	MessageClass     MessageClass `json:"message_class"`
	Trace            TraceContext `json:"trace,omitempty"`
}

// NewSynapseMessage initializes a base message with sensible defaults.
func NewSynapseMessage(threadID, agentID string, role Role, class MessageClass) SynapseMessage {
	return SynapseMessage{
		ID:            uuid.NewString(),
		Timestamp:     float64(time.Now().UnixNano()) / 1e9,
		ThreadID:      threadID,
		AgentID:       agentID,
		Role:          role,
		TTL:           300,
		MaxConsumers:  -1,
		ConsumedCount: 0,
		Payload:       Payload{},
		MessageClass:  class,
	}
}

// IsExpired reports whether the message has outlived its TTL at the given now.
func (m SynapseMessage) IsExpired(now float64) bool {
	return m.Timestamp+float64(m.TTL) <= now
}

// --- ContextMessage helpers ---

// NewContextMessage builds a ContextMessage ready to be sent through the bus.
func NewContextMessage(threadID, agentID string, role Role, content string, squadID string, citations []any, ttl int) SynapseMessage {
	if ttl == 0 {
		ttl = 3600
	}
	msg := NewSynapseMessage(threadID, agentID, role, ClassContextMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	msg.Payload = Payload{
		"content":   content,
		"citations": citations,
	}
	return msg
}

// Content returns the textual content of a ContextMessage payload.
func (m SynapseMessage) Content() string {
	if c, ok := m.Payload["content"].(string); ok {
		return c
	}
	return ""
}

// Citations returns the citation list attached to a ContextMessage.
func (m SynapseMessage) Citations() []any {
	if c, ok := m.Payload["citations"].([]any); ok {
		return c
	}
	return nil
}

// --- TaskMessage helpers ---

// NewTaskMessage builds a TaskMessage used to delegate work across agents/squads.
func NewTaskMessage(threadID, agentID, taskType, replyToThread string, parameters map[string]any, squadID string, ttl, maxConsumers int) SynapseMessage {
	if ttl == 0 {
		ttl = 3600
	}
	if maxConsumers == 0 {
		// Task messages are single-consumer by default to avoid duplicate work.
		maxConsumers = 1
	}
	msg := NewSynapseMessage(threadID, agentID, RoleAssistant, ClassTaskMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	msg.MaxConsumers = maxConsumers
	msg.Payload = Payload{
		"task_type":       taskType,
		"parameters":      parameters,
		"reply_to_thread": replyToThread,
	}
	return msg
}

// TaskType returns the task_type field of a TaskMessage payload.
func (m SynapseMessage) TaskType() string {
	if t, ok := m.Payload["task_type"].(string); ok {
		return t
	}
	return ""
}

// Parameters returns the parameters map of a TaskMessage payload.
func (m SynapseMessage) Parameters() map[string]any {
	if p, ok := m.Payload["parameters"].(map[string]any); ok {
		return p
	}
	return map[string]any{}
}

// ReplyToThread returns the reply_to_thread field of a TaskMessage payload.
func (m SynapseMessage) ReplyToThread() string {
	if r, ok := m.Payload["reply_to_thread"].(string); ok {
		return r
	}
	return ""
}

// --- CommandMessage helpers ---

// NewCommandMessage builds a CommandMessage used for control-plane directives.
func NewCommandMessage(threadID, agentID, command string, parameters map[string]any, squadID string, ttl int) SynapseMessage {
	if ttl == 0 {
		ttl = 3600
	}
	msg := NewSynapseMessage(threadID, agentID, RoleSystem, ClassCommandMessage)
	msg.SquadID = squadID
	msg.TTL = ttl
	msg.Payload = Payload{
		"command":    command,
		"parameters": parameters,
	}
	return msg
}

// Command returns the command field of a CommandMessage payload.
func (m SynapseMessage) Command() string {
	if c, ok := m.Payload["command"].(string); ok {
		return c
	}
	return ""
}
