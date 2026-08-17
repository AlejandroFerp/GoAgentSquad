package synapse

import (
	"context"
	"sync"
	"time"
)

// This file defines the persistence boundary for Synapse. The default backend
// retains messages in memory; callers can inject a durable backend as needed.

// BaseStorage is the persistence contract used by SynapseService.
// Implementations may wrap SQLite, Postgres, or any durable store.
type BaseStorage interface {
	// Connect initializes the schema and connection pool.
	Connect(ctx context.Context) error

	// Close releases all resources.
	Close() error

	// SaveMessage persists or updates a single message. Synapse treats this as a strong-consistency boundary: callers must not publish the in-memory mutation when this returns an error.
	SaveMessage(ctx context.Context, msg SynapseMessage) error

	// DeleteMessage removes a message by its ID. Synapse keeps the message visible in memory when this returns an error.
	DeleteMessage(ctx context.Context, messageID string) error

	// LoadAllActiveMessages returns every unexpired message from storage.
	LoadAllActiveMessages(ctx context.Context) ([]SynapseMessage, error)
}

// MemoryStorage is a thread-safe, retaining in-memory BaseStorage.
// It is useful for local runtimes and deterministic tests, and can hydrate
// multiple SynapseService instances while the storage value remains alive.
type MemoryStorage struct {
	mu       sync.RWMutex
	messages map[string]SynapseMessage
}

// NewMemoryStorage returns an empty retaining in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{messages: make(map[string]SynapseMessage)}
}

func (s *MemoryStorage) Connect(context.Context) error {
	s.mu.Lock()
	if s.messages == nil {
		s.messages = make(map[string]SynapseMessage)
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStorage) Close() error { return nil }

func (s *MemoryStorage) SaveMessage(_ context.Context, msg SynapseMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.messages == nil {
		s.messages = make(map[string]SynapseMessage)
	}
	s.messages[msg.ID] = cloneMessage(msg)
	return nil
}

func (s *MemoryStorage) DeleteMessage(_ context.Context, messageID string) error {
	s.mu.Lock()
	delete(s.messages, messageID)
	s.mu.Unlock()
	return nil
}

func (s *MemoryStorage) LoadAllActiveMessages(_ context.Context) ([]SynapseMessage, error) {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	active := make([]SynapseMessage, 0, len(s.messages))
	for _, msg := range s.messages {
		if !msg.IsExpired(now) {
			active = append(active, cloneMessage(msg))
		}
	}
	return active, nil
}

// NoopStorage is an explicitly selected discard backend. It retains nothing.
type NoopStorage struct{}

func (NoopStorage) Connect(context.Context) error                     { return nil }
func (NoopStorage) Close() error                                      { return nil }
func (NoopStorage) SaveMessage(context.Context, SynapseMessage) error { return nil }
func (NoopStorage) DeleteMessage(context.Context, string) error       { return nil }
func (NoopStorage) LoadAllActiveMessages(context.Context) ([]SynapseMessage, error) {
	return nil, nil
}
