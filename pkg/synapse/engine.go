package synapse

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// This file is the Synapse runtime engine: it owns message persistence,
// in-memory indexes, context caching, task consumption, events, and expiry GC.

// SynapseService is the in-memory blackboard engine. It coordinates event
// emission, an LRU-style warm context cache, atomic task consumption, and a
// background garbage collector that purges expired messages.
type SynapseService struct {
	cacheSize int
	storage   BaseStorage
	Events    *EventBus

	// All mutable indexes below are protected by mu.
	mu           sync.RWMutex
	contextCache map[string][]SynapseMessage
	threads      map[string][]SynapseMessage
	messageIndex map[string]SynapseMessage

	// gcCancel/gcDone coordinate the background expiry loop lifecycle.
	gcCancel context.CancelFunc
	gcDone   chan struct{}
}

// NewSynapseService builds a SynapseService with the given cache size and
// optional storage. If storage is nil, a NoopStorage is used.
func NewSynapseService(cacheSize int, storage BaseStorage) *SynapseService {
	if cacheSize <= 0 {
		cacheSize = 50
	}
	if storage == nil {
		storage = NoopStorage{}
	}
	return &SynapseService{
		cacheSize:    cacheSize,
		storage:      storage,
		Events:       NewEventBus(),
		contextCache: make(map[string][]SynapseMessage),
		threads:      make(map[string][]SynapseMessage),
		messageIndex: make(map[string]SynapseMessage),
	}
}

// Connect initializes storage, loads active messages, and starts the GC loop.
func (s *SynapseService) Connect(ctx context.Context) error {
	if err := s.storage.Connect(ctx); err != nil {
		return err
	}
	active, err := s.storage.LoadAllActiveMessages(ctx)
	if err != nil {
		return err
	}
	// Rebuild in-memory state deterministically from oldest to newest message.
	sort.Slice(active, func(i, j int) bool {
		return active[i].Timestamp < active[j].Timestamp
	})
	s.mu.Lock()
	for _, msg := range active {
		s.messageIndex[msg.ID] = msg
		s.threads[msg.ThreadID] = append(s.threads[msg.ThreadID], msg)
	}
	s.mu.Unlock()

	gcCtx, cancel := context.WithCancel(context.Background())
	s.gcCancel = cancel
	s.gcDone = make(chan struct{})
	go s.gcLoop(gcCtx)
	return nil
}

// Close stops the GC loop and closes the storage backend.
func (s *SynapseService) Close() error {
	if s.gcCancel != nil {
		s.gcCancel()
		<-s.gcDone
		s.gcCancel = nil
	}
	return s.storage.Close()
}

// SendMessage emits pre-insert hooks, persists the message, updates caches,
// and finally emits post-insert hooks. Returns the (possibly mutated) message
// or nil if a pre-insert callback blocked insertion.
func (s *SynapseService) SendMessage(ctx context.Context, msg SynapseMessage) (*SynapseMessage, error) {
	current, err := s.Events.EmitPreInsert(ctx, msg)
	if err != nil {
		return nil, err
	}
	if current == nil {
		return nil, nil
	}
	// Persist trace metadata with the message so async callbacks can rebuild causality.
	if trace, ok := observability.TraceFromContext(ctx); ok {
		if current.Trace.TraceID == "" {
			current.Trace.TraceID = trace.TraceID
		}
		if current.Trace.SpanID == "" {
			current.Trace.SpanID = trace.SpanID
		}
		if current.Trace.CorrelationID == "" {
			current.Trace.CorrelationID = trace.CorrelationID
		}
	}
	if stepID, ok := observability.StepIDFromContext(ctx); ok && current.Trace.CausationID == "" {
		current.Trace.CausationID = stepID
	}

	s.mu.Lock()
	s.messageIndex[current.ID] = *current
	s.threads[current.ThreadID] = append(s.threads[current.ThreadID], *current)
	if current.MessageClass == ClassContextMessage {
		cache := s.contextCache[current.ThreadID]
		cache = append(cache, cloneMessage(*current))
		if len(cache) > s.cacheSize {
			cache = cache[len(cache)-s.cacheSize:]
		}
		s.contextCache[current.ThreadID] = cache
	}
	s.mu.Unlock()

	// Storage errors are logged but do not roll back the in-memory blackboard.
	if err := s.storage.SaveMessage(ctx, *current); err != nil {
		observability.LoggerFromContext(ctx).Error("synapse storage save failed",
			"message_id", current.ID,
			"thread_id", current.ThreadID,
			"error", err,
		)
	}

	// Post-insert callbacks dispatch squads/observers asynchronously.
	s.Events.EmitPostInsert(ctx, *current)
	return current, nil
}

// FetchContext retrieves up to limit ContextMessages for a thread, oldest first.
// It uses the warm cache when possible and falls back to the in-memory index.
func (s *SynapseService) FetchContext(ctx context.Context, threadID string, limit int) ([]SynapseMessage, error) {
	if limit <= 0 {
		limit = 100
	}
	now := nowSeconds()

	s.mu.RLock()
	if cache, ok := s.contextCache[threadID]; ok && len(cache) >= limit {
		out := make([]SynapseMessage, limit)
		copy(out, cache[len(cache)-limit:])
		s.mu.RUnlock()
		return out, nil
	}

	all := s.threads[threadID]
	active := make([]SynapseMessage, 0, len(all))
	for _, m := range all {
		if m.MessageClass != ClassContextMessage {
			continue
		}
		if m.IsExpired(now) {
			continue
		}
		active = append(active, cloneMessage(m))
	}
	s.mu.RUnlock()

	sort.Slice(active, func(i, j int) bool {
		return active[i].Timestamp < active[j].Timestamp
	})

	start := 0
	if len(active) > limit {
		start = len(active) - limit
	}
	result := active[start:]

	// Refresh warm cache after a cold read.
	s.mu.Lock()
	cached := make([]SynapseMessage, 0, s.cacheSize)
	cs := 0
	if len(active) > s.cacheSize {
		cs = len(active) - s.cacheSize
	}
	cached = append(cached, active[cs:]...)
	s.contextCache[threadID] = cached
	s.mu.Unlock()

	return result, nil
}

// ConsumeTask atomically retrieves and consumes up to limit TaskMessages
// matching the given filters. Each consumed message has its ConsumedCount
// incremented; messages that reach MaxConsumers are removed from memory and
// storage.
func (s *SynapseService) ConsumeTask(ctx context.Context, threadID, squadID, taskType, recipientAgentID string, limit int) ([]SynapseMessage, error) {
	if limit <= 0 {
		limit = 1
	}
	now := nowSeconds()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Keep candidate selection under the same lock as consumption so two workers
	// cannot consume the same task concurrently.
	var candidates []SynapseMessage
	if threadID != "" {
		candidates = s.threads[threadID]
	} else {
		candidates = make([]SynapseMessage, 0, len(s.messageIndex))
		for _, m := range s.messageIndex {
			candidates = append(candidates, m)
		}
	}

	matching := make([]SynapseMessage, 0)
	for _, m := range candidates {
		if m.MessageClass != ClassTaskMessage {
			continue
		}
		if m.IsExpired(now) {
			continue
		}
		if threadID != "" && m.ThreadID != threadID {
			continue
		}
		if squadID != "" && m.SquadID != squadID {
			continue
		}
		if taskType != "" && m.TaskType() != taskType {
			continue
		}
		if m.RecipientAgentID != "" && m.RecipientAgentID != recipientAgentID {
			continue
		}
		matching = append(matching, m)
	}

	sort.Slice(matching, func(i, j int) bool {
		return matching[i].Timestamp < matching[j].Timestamp
	})

	if len(matching) > limit {
		matching = matching[:limit]
	}

	consumed := make([]SynapseMessage, 0, len(matching))
	for _, m := range matching {
		ptr := s.messageIndex[m.ID]
		ptr.ConsumedCount++
		consumed = append(consumed, cloneMessage(ptr))

		if ptr.MaxConsumers != -1 && ptr.ConsumedCount >= ptr.MaxConsumers {
			s.deleteMessageLocked(ctx, ptr.ID)
		} else {
			s.messageIndex[m.ID] = ptr
			_ = s.storage.SaveMessage(ctx, ptr)
		}
	}
	return consumed, nil
}

// DeleteMessage removes a message by ID from memory and storage.
func (s *SynapseService) DeleteMessage(ctx context.Context, messageID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.messageIndex[messageID]; !ok {
		_ = s.storage.DeleteMessage(ctx, messageID)
		return false
	}
	s.deleteMessageLocked(ctx, messageID)
	return true
}

func (s *SynapseService) deleteMessageLocked(ctx context.Context, messageID string) {
	msg, ok := s.messageIndex[messageID]
	if !ok {
		return
	}
	delete(s.messageIndex, messageID)
	if threadMsgs, ok := s.threads[msg.ThreadID]; ok {
		filtered := threadMsgs[:0]
		for _, m := range threadMsgs {
			if m.ID != messageID {
				filtered = append(filtered, m)
			}
		}
		if len(filtered) == 0 {
			delete(s.threads, msg.ThreadID)
		} else {
			s.threads[msg.ThreadID] = filtered
		}
	}
	if msg.MessageClass == ClassContextMessage {
		// Keep the warm cache consistent with the authoritative indexes.
		if cache, ok := s.contextCache[msg.ThreadID]; ok {
			filtered := cache[:0]
			for _, m := range cache {
				if m.ID != messageID {
					filtered = append(filtered, m)
				}
			}
			if len(filtered) == 0 {
				delete(s.contextCache, msg.ThreadID)
			} else {
				s.contextCache[msg.ThreadID] = filtered
			}
		}
	}
	_ = s.storage.DeleteMessage(ctx, messageID)
}

// ClearCache invalidates the warm context cache for a thread (or all threads).
func (s *SynapseService) ClearCache(threadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if threadID != "" {
		delete(s.contextCache, threadID)
	} else {
		s.contextCache = make(map[string][]SynapseMessage)
	}
}

func (s *SynapseService) gcLoop(ctx context.Context) {
	defer close(s.gcDone)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.CollectExpired(ctx)
		}
	}
}

// CollectExpired purges all expired messages from memory and storage.
func (s *SynapseService) CollectExpired(ctx context.Context) {
	now := nowSeconds()
	s.mu.Lock()
	expiredIDs := make([]string, 0)
	for id, m := range s.messageIndex {
		if m.IsExpired(now) {
			expiredIDs = append(expiredIDs, id)
		}
	}
	for _, id := range expiredIDs {
		s.deleteMessageLocked(ctx, id)
	}
	s.mu.Unlock()
}

// nowSeconds returns wall-clock seconds for message timestamps and TTL checks.
func nowSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// cloneMessage copies the message payload map so callers cannot mutate cached state.
func cloneMessage(m SynapseMessage) SynapseMessage {
	cp := m
	if m.Payload != nil {
		cp.Payload = make(Payload, len(m.Payload))
		for k, v := range m.Payload {
			cp.Payload[k] = v
		}
	}
	return cp
}
