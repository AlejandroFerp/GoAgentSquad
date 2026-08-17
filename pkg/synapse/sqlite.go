package synapse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// This file provides the durable SQLite backend for BaseStorage. It uses the
// pure-Go driver so the library keeps cross-compiling to a static binary.

const sqliteDriverName = "sqlite"

// sqliteSchema keeps the queryable columns indexed while the authoritative
// message state lives in the document column as JSON.
const sqliteSchema = `
CREATE TABLE IF NOT EXISTS messages (
	id            TEXT PRIMARY KEY,
	thread_id     TEXT NOT NULL,
	squad_id      TEXT,
	message_class TEXT NOT NULL,
	expires_at    INTEGER NOT NULL,
	document      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_messages_thread ON messages(thread_id);
CREATE INDEX IF NOT EXISTS idx_messages_squad ON messages(squad_id);
CREATE INDEX IF NOT EXISTS idx_messages_class ON messages(message_class);
CREATE INDEX IF NOT EXISTS idx_messages_expires ON messages(expires_at);
`

// SQLiteStorage is a durable BaseStorage backed by a SQLite database file.
// It satisfies the strong-consistency boundary documented on BaseStorage: a
// failed write returns an error before the caller publishes the mutation.
type SQLiteStorage struct {
	path string
	db   *sql.DB
}

var _ BaseStorage = (*SQLiteStorage)(nil)

// NewSQLiteStorage returns a durable storage backend for the given file path.
func NewSQLiteStorage(path string) *SQLiteStorage {
	return &SQLiteStorage{path: path}
}

// Connect opens the database, applies pragmas, and ensures the schema exists.
func (s *SQLiteStorage) Connect(ctx context.Context) error {
	if s.path == "" {
		return fmt.Errorf("synapse: SQLiteStorage requires a non-empty database path")
	}
	if s.db != nil {
		return nil
	}
	if dir := filepath.Dir(s.path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("synapse: create database directory: %w", err)
		}
	}

	db, err := sql.Open(sqliteDriverName, s.path)
	if err != nil {
		return fmt.Errorf("synapse: open sqlite database: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	pragmas := []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
		"PRAGMA foreign_keys=ON",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			db.Close()
			return fmt.Errorf("synapse: apply %q: %w", pragma, err)
		}
	}
	if _, err := db.ExecContext(ctx, sqliteSchema); err != nil {
		db.Close()
		return fmt.Errorf("synapse: initialize schema: %w", err)
	}

	s.db = db
	return nil
}

// Close releases the database handle.
func (s *SQLiteStorage) Close() error {
	if s.db == nil {
		return nil
	}
	db := s.db
	s.db = nil
	if err := db.Close(); err != nil {
		return fmt.Errorf("synapse: close sqlite database: %w", err)
	}
	return nil
}

// SaveMessage persists or replaces a message keyed by its ID.
func (s *SQLiteStorage) SaveMessage(ctx context.Context, msg SynapseMessage) error {
	if s.db == nil {
		return fmt.Errorf("synapse: SQLiteStorage is not connected")
	}
	document, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("synapse: encode message %s: %w", msg.ID, err)
	}
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO messages (id, thread_id, squad_id, message_class, expires_at, document)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			thread_id     = excluded.thread_id,
			squad_id      = excluded.squad_id,
			message_class = excluded.message_class,
			expires_at    = excluded.expires_at,
			document      = excluded.document
	`, msg.ID, msg.ThreadID, msg.SquadID, string(msg.MessageClass), msg.Timestamp.Add(msg.TTL).UnixNano(), string(document))
	if err != nil {
		return fmt.Errorf("synapse: persist message %s: %w", msg.ID, err)
	}
	return nil
}

// DeleteMessage removes a message by ID.
func (s *SQLiteStorage) DeleteMessage(ctx context.Context, messageID string) error {
	if s.db == nil {
		return fmt.Errorf("synapse: SQLiteStorage is not connected")
	}
	if _, err := s.db.ExecContext(ctx, "DELETE FROM messages WHERE id = ?", messageID); err != nil {
		return fmt.Errorf("synapse: delete message %s: %w", messageID, err)
	}
	return nil
}

// LoadAllActiveMessages returns every unexpired message. Ordering is left to
// SynapseService.Connect, which sorts by timestamp while rebuilding threads.
func (s *SQLiteStorage) LoadAllActiveMessages(ctx context.Context) ([]SynapseMessage, error) {
	if s.db == nil {
		return nil, fmt.Errorf("synapse: SQLiteStorage is not connected")
	}
	rows, err := s.db.QueryContext(ctx,
		"SELECT document FROM messages WHERE expires_at > ?", time.Now().UnixNano())
	if err != nil {
		return nil, fmt.Errorf("synapse: load active messages: %w", err)
	}
	defer rows.Close()

	messages := []SynapseMessage{}
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return nil, fmt.Errorf("synapse: scan message row: %w", err)
		}
		var msg SynapseMessage
		if err := json.Unmarshal([]byte(document), &msg); err != nil {
			return nil, fmt.Errorf("synapse: decode stored message: %w", err)
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("synapse: iterate message rows: %w", err)
	}
	return messages, nil
}
