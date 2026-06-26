package synapse_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

func TestNewSynapseService(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewContextMessage("thread-1", "agent-1", synapse.RoleUser, "hello", "", nil, 3600)
	sent, err := svc.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent == nil {
		t.Fatal("SendMessage returned nil")
	}
	if sent.ID != msg.ID {
		t.Errorf("expected same ID, got %s", sent.ID)
	}

	got, err := svc.FetchContext(ctx, "thread-1", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if got[0].Content() != "hello" {
		t.Errorf("expected 'hello', got %s", got[0].Content())
	}
}

func TestEventBusPreInsertMutation(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	called := atomic.Int32{}
	svc.Events.Subscribe("*", synapse.PreInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
		called.Add(1)
		msg.Payload["content"] = "mutated"
		return &msg, nil
	}), "pre_insert")

	msg := synapse.NewContextMessage("thread-2", "agent-1", synapse.RoleUser, "original", "", nil, 3600)
	sent, err := svc.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent.Content() != "mutated" {
		t.Errorf("expected 'mutated', got %s", sent.Content())
	}
	if called.Load() != 1 {
		t.Errorf("expected 1 pre-insert call, got %d", called.Load())
	}
}

func TestEventBusPreInsertBlock(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	svc.Events.Subscribe("*", synapse.PreInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
		return nil, nil
	}), "pre_insert")

	msg := synapse.NewContextMessage("thread-3", "agent-1", synapse.RoleUser, "blocked", "", nil, 3600)
	sent, err := svc.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if sent != nil {
		t.Fatal("expected nil message when blocked")
	}
}

func TestEventBusPostInsertConcurrent(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	var wg sync.WaitGroup
	counter := atomic.Int32{}
	svc.Events.Subscribe("*", synapse.PostInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) {
		defer wg.Done()
		counter.Add(1)
	}), "post_insert")

	wg.Add(1)
	msg := synapse.NewContextMessage("thread-4", "agent-1", synapse.RoleUser, "post", "", nil, 3600)
	_, _ = svc.SendMessage(ctx, msg)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("post-insert callback timed out")
	}
	if counter.Load() != 1 {
		t.Errorf("expected 1 post-insert call, got %d", counter.Load())
	}
}

func TestConsumeTaskAtomic(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	msg := synapse.NewTaskMessage("thread-5", "agent-1", "bible_study", "reply-thread-5", map[string]any{"passage": "Juan 3:16"}, "squad-1", 3600, 1)
	_, err := svc.SendMessage(ctx, msg)
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}

	// Concurrent consumers: only one should get the task.
	var wg sync.WaitGroup
	var got atomic.Int32
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumed, err := svc.ConsumeTask(ctx, "thread-5", "squad-1", "bible_study", "", 1)
			if err == nil && len(consumed) > 0 {
				got.Add(int32(len(consumed)))
			}
		}()
	}
	wg.Wait()
	if got.Load() != 1 {
		t.Errorf("expected exactly 1 consumed task, got %d", got.Load())
	}
}

func TestGarbageCollector(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	// Create a message with a very short TTL and wait for it to expire.
	msg := synapse.NewContextMessage("thread-6", "agent-1", synapse.RoleUser, "expires", "", nil, 1)
	_, _ = svc.SendMessage(ctx, msg)

	// Wait for the message to expire.
	time.Sleep(1100 * time.Millisecond)
	svc.CollectExpired(ctx)

	// The message should be gone.
	got, err := svc.FetchContext(ctx, "thread-6", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected 0 messages after GC, got %d", len(got))
	}
}

func TestFetchContextCacheReturnsClonedMessages(t *testing.T) {
	svc := synapse.NewSynapseService(2, nil)
	ctx := context.Background()
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	for _, content := range []string{"first", "second"} {
		msg := synapse.NewContextMessage("thread-cache-clone", "agent-1", synapse.RoleUser, content, "", nil, 3600)
		if _, err := svc.SendMessage(ctx, msg); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	got, err := svc.FetchContext(ctx, "thread-cache-clone", 2)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	got[0].Payload["content"] = "mutated"

	got, err = svc.FetchContext(ctx, "thread-cache-clone", 2)
	if err != nil {
		t.Fatalf("FetchContext after mutation: %v", err)
	}
	if got[0].Content() != "first" {
		t.Fatalf("cached message was externally mutated, got %q", got[0].Content())
	}
}
func TestTaskMessageHelpers(t *testing.T) {
	msg := synapse.NewTaskMessage("t1", "a1", "study", "reply-1", map[string]any{"q": "hello"}, "squad-1", 3600, 2)
	if msg.TaskType() != "study" {
		t.Errorf("expected 'study', got %s", msg.TaskType())
	}
	if msg.ReplyToThread() != "reply-1" {
		t.Errorf("expected 'reply-1', got %s", msg.ReplyToThread())
	}
	if msg.Parameters()["q"] != "hello" {
		t.Errorf("expected 'hello', got %v", msg.Parameters()["q"])
	}
	if msg.MaxConsumers != 2 {
		t.Errorf("expected max_consumers=2, got %d", msg.MaxConsumers)
	}
}

func TestCommandMessageHelpers(t *testing.T) {
	msg := synapse.NewCommandMessage("t1", "a1", "reset", map[string]any{"force": true}, "squad-1", 3600)
	if msg.Command() != "reset" {
		t.Errorf("expected 'reset', got %s", msg.Command())
	}
	if msg.Parameters()["force"] != true {
		t.Errorf("expected force=true, got %v", msg.Parameters()["force"])
	}
	if msg.Role != synapse.RoleSystem {
		t.Errorf("expected system role, got %s", msg.Role)
	}
}
