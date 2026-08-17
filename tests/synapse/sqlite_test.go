package synapse_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

func newSQLiteServiceForTest(t *testing.T, dbPath string) *synapse.SynapseService {
	t.Helper()
	storage := synapse.NewSQLiteStorage(dbPath)
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return svc
}

// The recovery contract is the whole point of a durable backend: state written
// before a crash must be visible to a brand new service on the same file.
func TestSQLiteStorageRecoversContextMessagesAcrossRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "recovery.db")
	const threadID = "thread-recovery"

	svc := newSQLiteServiceForTest(t, dbPath)
	msg := synapse.NewContextMessage(threadID, "agent-1", synapse.RoleUser, "durable content", "squad-a", nil, time.Hour)
	msg.Trace.CorrelationID = "corr-42"
	if _, err := svc.SendMessage(ctx, msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	revived := newSQLiteServiceForTest(t, dbPath)
	defer revived.Close()

	got, err := revived.FetchContext(ctx, threadID, 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 recovered message, got %d", len(got))
	}
	recovered := got[0]
	if recovered.ID != msg.ID {
		t.Errorf("ID = %q, want %q", recovered.ID, msg.ID)
	}
	if recovered.Content() != "durable content" {
		t.Errorf("Content = %q, want %q", recovered.Content(), "durable content")
	}
	if recovered.SquadID != "squad-a" {
		t.Errorf("SquadID = %q, want %q", recovered.SquadID, "squad-a")
	}
	if recovered.Trace.CorrelationID != "corr-42" {
		t.Errorf("Trace.CorrelationID = %q, want %q", recovered.Trace.CorrelationID, "corr-42")
	}
}

// Task consumption state must survive a restart, otherwise a crash would let a
// task be claimed twice.
func TestSQLiteStorageRecoversUnconsumedTask(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "task-recovery.db")
	const threadID = "thread-task-recovery"

	svc := newSQLiteServiceForTest(t, dbPath)
	task := synapse.NewTaskMessage(threadID, "agent-1", "analyze", "reply-thread",
		map[string]any{"query": "hello"}, "squad-a", time.Hour, 1)
	if _, err := svc.SendMessage(ctx, task); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	revived := newSQLiteServiceForTest(t, dbPath)
	defer revived.Close()

	consumed, err := revived.ConsumeTask(ctx, threadID, "squad-a", "analyze", "", 1)
	if err != nil {
		t.Fatalf("ConsumeTask: %v", err)
	}
	if len(consumed) != 1 {
		t.Fatalf("expected 1 recovered task, got %d", len(consumed))
	}
	payload, err := consumed[0].TaskPayload()
	if err != nil {
		t.Fatalf("TaskPayload: %v", err)
	}
	if payload.TaskType != "analyze" {
		t.Errorf("TaskType = %q, want %q", payload.TaskType, "analyze")
	}
	if payload.ReplyToThread != "reply-thread" {
		t.Errorf("ReplyToThread = %q, want %q", payload.ReplyToThread, "reply-thread")
	}
	if got := payload.Parameters["query"]; got != "hello" {
		t.Errorf("Parameters[query] = %v, want %q", got, "hello")
	}
}

func TestSQLiteStorageDoesNotRecoverExpiredMessages(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "expiry.db")
	const threadID = "thread-expiry"

	storage := synapse.NewSQLiteStorage(dbPath)
	if err := storage.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	expired := synapse.NewContextMessage(threadID, "agent-1", synapse.RoleUser, "stale", "", nil, time.Minute)
	expired.Timestamp = time.Now().Add(-2 * time.Minute)
	if err := storage.SaveMessage(ctx, expired); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	if err := storage.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	revived := synapse.NewSQLiteStorage(dbPath)
	if err := revived.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer revived.Close()

	active, err := revived.LoadAllActiveMessages(ctx)
	if err != nil {
		t.Fatalf("LoadAllActiveMessages: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected expired message to be filtered out, got %d", len(active))
	}
}

func TestSQLiteStorageDeleteRemovesMessage(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "delete.db")

	storage := synapse.NewSQLiteStorage(dbPath)
	if err := storage.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer storage.Close()

	msg := synapse.NewContextMessage("thread-delete", "agent-1", synapse.RoleUser, "bye", "", nil, time.Hour)
	if err := storage.SaveMessage(ctx, msg); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	if err := storage.DeleteMessage(ctx, msg.ID); err != nil {
		t.Fatalf("DeleteMessage: %v", err)
	}

	active, err := storage.LoadAllActiveMessages(ctx)
	if err != nil {
		t.Fatalf("LoadAllActiveMessages: %v", err)
	}
	if len(active) != 0 {
		t.Fatalf("expected deleted message to be gone, got %d", len(active))
	}
}

// Re-saving the same ID must update in place, mirroring the consumption
// counter updates the engine performs.
func TestSQLiteStorageSaveIsIdempotentOnID(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "upsert.db")

	storage := synapse.NewSQLiteStorage(dbPath)
	if err := storage.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer storage.Close()

	msg := synapse.NewTaskMessage("thread-upsert", "agent-1", "work", "reply", nil, "squad-a", time.Hour, 2)
	if err := storage.SaveMessage(ctx, msg); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}
	msg.ConsumedCount = 1
	if err := storage.SaveMessage(ctx, msg); err != nil {
		t.Fatalf("SaveMessage (update): %v", err)
	}

	active, err := storage.LoadAllActiveMessages(ctx)
	if err != nil {
		t.Fatalf("LoadAllActiveMessages: %v", err)
	}
	if len(active) != 1 {
		t.Fatalf("expected 1 row after upsert, got %d", len(active))
	}
	if active[0].ConsumedCount != 1 {
		t.Errorf("ConsumedCount = %d, want 1", active[0].ConsumedCount)
	}
}

func TestSQLiteStorageRequiresPathAndConnection(t *testing.T) {
	ctx := context.Background()

	if err := synapse.NewSQLiteStorage("").Connect(ctx); err == nil {
		t.Error("expected Connect to fail fast on empty path")
	}

	disconnected := synapse.NewSQLiteStorage(filepath.Join(t.TempDir(), "unused.db"))
	msg := synapse.NewContextMessage("thread-x", "agent-1", synapse.RoleUser, "x", "", nil, time.Hour)
	if err := disconnected.SaveMessage(ctx, msg); err == nil {
		t.Error("expected SaveMessage to fail before Connect")
	}
	if _, err := disconnected.LoadAllActiveMessages(ctx); err == nil {
		t.Error("expected LoadAllActiveMessages to fail before Connect")
	}
}
