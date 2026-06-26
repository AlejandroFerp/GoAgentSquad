package synapse

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// EventType discriminates between the two interception points of the bus.
type EventType string

const (
	PreInsert  EventType = "pre_insert"
	PostInsert EventType = "post_insert"
)

// PreInsertCallback mutates or blocks a message before persistence.
// Returning a non-nil message continues the chain; returning nil blocks insertion.
type PreInsertCallback func(ctx context.Context, msg SynapseMessage) (*SynapseMessage, error)

// PostInsertCallback is fired asynchronously after a message has been persisted.
type PostInsertCallback func(ctx context.Context, msg SynapseMessage)

type listener struct {
	patternStr string
	regex      *regexp.Regexp
	cb         any // PreInsertCallback or PostInsertCallback
	eventType  EventType
}

// EventBus is a thread-safe pub/sub registry that matches messages against
// regex patterns over thread_id, squad_id, agent_id, message_class, and content.
type EventBus struct {
	mu        sync.RWMutex
	listeners []listener
}

// NewEventBus returns an empty EventBus.
func NewEventBus() *EventBus {
	return &EventBus{}
}

// Subscribe registers a callback for the given event type and pattern.
// A pattern of "*" matches every message; otherwise the pattern is treated as
// a regex matched against the message's identifying fields.
func (b *EventBus) Subscribe(pattern string, cb any, eventType EventType) {
	regexSrc := ".*"
	if pattern != "*" {
		regexSrc = pattern
	}
	compiled := regexp.MustCompile(regexSrc)
	b.mu.Lock()
	b.listeners = append(b.listeners, listener{
		patternStr: pattern,
		regex:      compiled,
		cb:         cb,
		eventType:  eventType,
	})
	b.mu.Unlock()
}

// Unsubscribe removes every registration that points to the given callback.
// Comparison is done by pointer identity of the function value.
func (b *EventBus) Unsubscribe(cb any) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.listeners[:0]
	for _, l := range b.listeners {
		// Compare underlying pointers for function values.
		if !sameFunc(l.cb, cb) {
			out = append(out, l)
		}
	}
	b.listeners = out
}

// EmitPreInsert runs pre-insert callbacks sequentially. Each callback may mutate
// the message or abort the chain by returning (nil, nil) or an error.
func (b *EventBus) EmitPreInsert(ctx context.Context, msg SynapseMessage) (*SynapseMessage, error) {
	current := &msg
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, l := range b.listeners {
		if l.eventType != PreInsert {
			continue
		}
		if !l.matches(current) {
			continue
		}
		cb, ok := l.cb.(PreInsertCallback)
		if !ok {
			continue
		}
		next, err := cb(ctx, *current)
		if err != nil {
			observability.LoggerFromContext(ctx).Error("synapse pre-insert callback blocked insertion",
				"message_id", current.ID,
				"thread_id", current.ThreadID,
				"error", err,
			)
			return nil, err
		}
		if next == nil {
			return nil, nil
		}
		current = next
	}
	return current, nil
}

// EmitPostInsert fires all matching post-insert callbacks concurrently.
// Each callback runs in its own goroutine and panics are recovered so a faulty
// observer can never crash the engine.
func (b *EventBus) EmitPostInsert(ctx context.Context, msg SynapseMessage) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, l := range b.listeners {
		if l.eventType != PostInsert {
			continue
		}
		if !l.matches(&msg) {
			continue
		}
		cb, ok := l.cb.(PostInsertCallback)
		if !ok {
			continue
		}
		go func(cb PostInsertCallback, m SynapseMessage) {
			defer func() {
				if r := recover(); r != nil {
					observability.LoggerFromContext(ctx).Error("synapse post-insert callback panicked",
						"message_id", m.ID,
						"thread_id", m.ThreadID,
						"panic", r,
					)
				}
			}()
			cb(ctx, m)
		}(cb, msg)
	}
}

func (l listener) matches(msg *SynapseMessage) bool {
	if l.patternStr == "*" {
		return true
	}
	if l.patternStr == string(msg.MessageClass) {
		return true
	}
	if l.regex.MatchString(msg.ThreadID) {
		return true
	}
	if msg.SquadID != "" && l.regex.MatchString(msg.SquadID) {
		return true
	}
	if l.regex.MatchString(msg.AgentID) {
		return true
	}
	if l.regex.MatchString(string(msg.MessageClass)) {
		return true
	}
	if msg.MessageClass == ClassContextMessage {
		if content := msg.Content(); content != "" && l.regex.MatchString(content) {
			return true
		}
	}
	return false
}

// sameFunc reports whether two interface{} values wrapping a func are identical.
func sameFunc(a, b any) bool {
	return fmt.Sprintf("%p", a) == fmt.Sprintf("%p", b) && funcType(a) == funcType(b)
}

func funcType(v any) string {
	return strings.Replace(fmt.Sprintf("%T", v), " ", "", -1)
}
