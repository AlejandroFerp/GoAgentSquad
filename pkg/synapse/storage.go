package synapse

import "context"

// This file defines the persistence boundary for Synapse. The engine can run
// fully in memory through NoopStorage or delegate durability to another backend.

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

// NoopStorage is a BaseStorage that keeps everything in memory only.
// It is the default when no concrete storage is supplied.
type NoopStorage struct{}

func (NoopStorage) Connect(context.Context) error                     { return nil }
func (NoopStorage) Close() error                                      { return nil }
func (NoopStorage) SaveMessage(context.Context, SynapseMessage) error { return nil }
func (NoopStorage) DeleteMessage(context.Context, string) error       { return nil }
func (NoopStorage) LoadAllActiveMessages(context.Context) ([]SynapseMessage, error) {
	return nil, nil
}
