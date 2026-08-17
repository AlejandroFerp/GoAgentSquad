package synapse

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"runtime"
	"sync"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// This file implements the blackboard event bus used to intercept messages
// before persistence and react to them after persistence.

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

// SubscriptionID uniquely identifies one EventBus listener registration.
type SubscriptionID uint64

// EventFilter selects messages using explicit fields. All configured criteria
// must match; an empty filter matches nothing unless created with
// MatchAllEventFilter.
type EventFilter struct {
	MessageClass MessageClass
	ThreadID     *regexp.Regexp
	SquadID      *regexp.Regexp
	AgentID      *regexp.Regexp
	matchAll     bool
}

// MatchAllEventFilter returns a filter that matches every message.
func MatchAllEventFilter() EventFilter {
	return EventFilter{matchAll: true}
}

// MessageClassEventFilter returns a filter that matches one message class.
func MessageClassEventFilter(messageClass MessageClass) EventFilter {
	return EventFilter{MessageClass: messageClass}
}

// ThreadEventFilter returns a filter that matches a thread ID using regex.
func ThreadEventFilter(pattern string) EventFilter {
	return EventFilter{ThreadID: regexp.MustCompile(pattern)}
}

type listener struct {
	id         SubscriptionID
	patternStr string
	filter     EventFilter
	cb         any // PreInsertCallback or PostInsertCallback
	eventType  EventType
}

// EventBus is a thread-safe pub/sub registry with explicit message filters.
type EventBus struct {
	mu        sync.RWMutex
	listeners []listener
	nextID    SubscriptionID
}

// NewEventBus returns an empty EventBus.
func NewEventBus() *EventBus {
	return &EventBus{}
}

// SubscribePreInsert registers a callback that can mutate or block a message
// before it is persisted. The pattern is matched against message fields.
func (b *EventBus) SubscribePreInsert(pattern string, cb PreInsertCallback) SubscriptionID {
	return b.subscribe(legacyEventFilter(pattern), pattern, cb, PreInsert)
}

// SubscribePostInsert registers a callback that runs asynchronously after a
// matching message has been persisted. The pattern is matched against message fields.
func (b *EventBus) SubscribePostInsert(pattern string, cb PostInsertCallback) SubscriptionID {
	return b.subscribe(legacyEventFilter(pattern), pattern, cb, PostInsert)
}

// SubscribePreInsertFilter registers a typed pre-insert callback.
func (b *EventBus) SubscribePreInsertFilter(filter EventFilter, cb PreInsertCallback) SubscriptionID {
	return b.subscribe(filter, "typed filter", cb, PreInsert)
}

// SubscribePostInsertFilter registers a typed post-insert callback.
func (b *EventBus) SubscribePostInsertFilter(filter EventFilter, cb PostInsertCallback) SubscriptionID {
	return b.subscribe(filter, "typed filter", cb, PostInsert)
}

// subscribe registers a callback for the given event type and filter.
func (b *EventBus) subscribe(filter EventFilter, pattern string, cb any, eventType EventType) SubscriptionID {
	b.mu.Lock()
	b.nextID++
	id := b.nextID
	b.listeners = append(b.listeners, listener{
		id:         id,
		patternStr: pattern,
		filter:     filter,
		cb:         cb,
		eventType:  eventType,
	})
	b.mu.Unlock()
	return id
}

// Unsubscribe removes exactly the listener registration identified by id.
func (b *EventBus) Unsubscribe(id SubscriptionID) {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.listeners[:0]
	for _, l := range b.listeners {
		if l.id != id {
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
			panic("Invariant violated: listener with eventType PreInsert has non-PreInsertCallback")
		}
		// Pre-insert hooks form a mutation chain: each hook sees the previous output.
		next, err := cb(ctx, *current)
		if err != nil {
			observability.LoggerFromContext(ctx).Error("synapse pre-insert callback blocked insertion",
				"message_id", current.ID,
				"thread_id", current.ThreadID,
				"listener_pattern", l.patternStr,
				"listener_event", l.eventType,
				"listener_type", fmt.Sprintf("%T", l.cb),
				"listener_name", functionName(l.cb),
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
			panic("Invariant violated: listener with eventType PostInsert has non-PostInsertCallback")
		}
		go func(cb PostInsertCallback, m SynapseMessage) {
			defer func() {
				if r := recover(); r != nil {
					observability.LoggerFromContext(ctx).Error("synapse post-insert callback panicked",
						"message_id", m.ID,
						"thread_id", m.ThreadID,
						"listener_pattern", l.patternStr,
						"listener_event", l.eventType,
						"listener_type", fmt.Sprintf("%T", cb),
						"listener_name", functionName(cb),
						"panic", r,
					)
				}
			}()
			cb(ctx, m)
		}(cb, msg)
	}
}

// legacyEventFilter preserves the old string API without content matching.
// Wildcards match all messages, known classes match only that class, and any
// other pattern is restricted to the thread ID for compatibility.
func legacyEventFilter(pattern string) EventFilter {
	if pattern == "*" {
		return MatchAllEventFilter()
	}
	switch MessageClass(pattern) {
	case ClassContextMessage, ClassTaskMessage, ClassCommandMessage:
		return MessageClassEventFilter(MessageClass(pattern))
	default:
		return ThreadEventFilter(pattern)
	}
}

// matches applies every configured filter criterion.
func (l listener) matches(msg *SynapseMessage) bool {
	if l.filter.matchAll {
		return true
	}
	if l.filter.MessageClass == "" && l.filter.ThreadID == nil && l.filter.SquadID == nil && l.filter.AgentID == nil {
		return false
	}
	if l.filter.MessageClass != "" && l.filter.MessageClass != msg.MessageClass {
		return false
	}
	if l.filter.ThreadID != nil && !l.filter.ThreadID.MatchString(msg.ThreadID) {
		return false
	}
	if l.filter.SquadID != nil && !l.filter.SquadID.MatchString(msg.SquadID) {
		return false
	}
	if l.filter.AgentID != nil && !l.filter.AgentID.MatchString(msg.AgentID) {
		return false
	}
	return true
}

func functionName(value any) string {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() || reflected.Kind() != reflect.Func || reflected.IsNil() {
		return ""
	}

	function := runtime.FuncForPC(reflected.Pointer())
	if function == nil {
		return ""
	}
	return function.Name()
}
