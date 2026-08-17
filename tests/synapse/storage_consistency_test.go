package synapse_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

type recordingStorage struct {
	mu         sync.Mutex
	messages   map[string]synapse.SynapseMessage
	failSave   bool
	failDelete bool
}

func newRecordingStorage() *recordingStorage {
	return &recordingStorage{messages: make(map[string]synapse.SynapseMessage)}
}

func (s *recordingStorage) Connect(context.Context) error { return nil }
func (s *recordingStorage) Close() error                  { return nil }

func (s *recordingStorage) SaveMessage(ctx context.Context, msg synapse.SynapseMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failSave {
		return errors.New("storage save failed")
	}
	s.messages[msg.ID] = msg
	return nil
}

func (s *recordingStorage) DeleteMessage(ctx context.Context, messageID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failDelete {
		return errors.New("storage delete failed")
	}
	delete(s.messages, messageID)
	return nil
}

func (s *recordingStorage) LoadAllActiveMessages(context.Context) ([]synapse.SynapseMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	messages := make([]synapse.SynapseMessage, 0, len(s.messages))
	for _, msg := range s.messages {
		messages = append(messages, msg)
	}
	return messages, nil
}

func (s *recordingStorage) setFailSave(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failSave = fail
}

func (s *recordingStorage) setFailDelete(fail bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failDelete = fail
}

func TestSendMessageStorageFailureDoesNotExposeMessage(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	storage.setFailSave(true)
	msg := synapse.NewContextMessage("thread-storage-save", "agent-1", synapse.RoleUser, "not durable", "", nil, time.Hour)
	sent, err := svc.SendMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected SendMessage to return storage error")
	}
	if sent != nil {
		t.Fatalf("expected no message on storage failure, got %s", sent.ID)
	}

	got, err := svc.FetchContext(ctx, "thread-storage-save", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected failed storage write to leave memory empty, got %d messages", len(got))
	}
}

func TestConsumeTaskStorageFailureDoesNotCommitConsumption(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewTaskMessage("thread-storage-consume", "agent-1", "work", "reply-thread", nil, "squad-1", time.Hour, 2)
	if _, err := svc.SendMessage(ctx, msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	storage.setFailSave(true)
	consumed, err := svc.ConsumeTask(ctx, "thread-storage-consume", "squad-1", "work", "", 1)
	if err == nil {
		t.Fatal("expected ConsumeTask to return storage error")
	}
	if len(consumed) != 0 {
		t.Fatalf("expected no consumed task on storage failure, got %d", len(consumed))
	}

	storage.setFailSave(false)
	consumed, err = svc.ConsumeTask(ctx, "thread-storage-consume", "squad-1", "work", "", 1)
	if err != nil {
		t.Fatalf("ConsumeTask after storage recovery: %v", err)
	}
	if len(consumed) != 1 {
		t.Fatalf("expected task to remain available after failed consume, got %d", len(consumed))
	}
	if consumed[0].ConsumedCount != 1 {
		t.Fatalf("consumed count = %d, want 1", consumed[0].ConsumedCount)
	}
}

func TestDeleteMessageStorageFailureKeepsMessageVisible(t *testing.T) {
	ctx := context.Background()
	storage := newRecordingStorage()
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewContextMessage("thread-storage-delete", "agent-1", synapse.RoleUser, "still visible", "", nil, time.Hour)
	if _, err := svc.SendMessage(ctx, msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	storage.setFailDelete(true)
	deleted, err := svc.DeleteMessage(ctx, msg.ID)
	if err == nil {
		t.Fatal("expected DeleteMessage to return storage error")
	}
	if !deleted {
		t.Fatal("expected DeleteMessage to report that the message exists")
	}

	got, err := svc.FetchContext(ctx, "thread-storage-delete", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected message to remain visible after failed delete, got %d", len(got))
	}
}
