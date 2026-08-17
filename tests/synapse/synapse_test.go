package synapse_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

type postInsertListener struct {
	calls atomic.Int32
}

func (l *postInsertListener) handle(context.Context, synapse.SynapseMessage) {
	l.calls.Add(1)
}

func TestNewSynapseService(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewContextMessage("thread-1", "agent-1", synapse.RoleUser, "hello", "", nil, time.Hour)
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
	sent.SetPayloadValue("content", "mutated outside blackboard")

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

func TestSynapseMessageUsesStandardTimeTypes(t *testing.T) {
	msg := synapse.NewContextMessage("thread-time", "agent-time", synapse.RoleUser, "hello", "", nil, time.Minute)
	if msg.Timestamp.IsZero() {
		t.Fatal("Timestamp must be initialized")
	}
	if msg.TTL != time.Minute {
		t.Fatalf("TTL = %s, want %s", msg.TTL, time.Minute)
	}
	if msg.IsExpired(msg.Timestamp.Add(time.Minute - time.Nanosecond)) {
		t.Fatal("message expired before its TTL elapsed")
	}
	if !msg.IsExpired(msg.Timestamp.Add(time.Minute)) {
		t.Fatal("message did not expire at its TTL")
	}
}

func TestEventBusPreInsertMutation(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	called := atomic.Int32{}
	svc.Events.SubscribePreInsert("*", synapse.PreInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
		called.Add(1)
		msg.SetPayloadValue("content", "mutated")
		return &msg, nil
	}))

	msg := synapse.NewContextMessage("thread-2", "agent-1", synapse.RoleUser, "original", "", nil, time.Hour)
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

	svc.Events.SubscribePreInsert("*", synapse.PreInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
		return nil, nil
	}))

	msg := synapse.NewContextMessage("thread-3", "agent-1", synapse.RoleUser, "blocked", "", nil, time.Hour)
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
	svc.Events.SubscribePostInsert("*", synapse.PostInsertCallback(func(ctx context.Context, msg synapse.SynapseMessage) {
		defer wg.Done()
		counter.Add(1)
	}))

	wg.Add(1)
	msg := synapse.NewContextMessage("thread-4", "agent-1", synapse.RoleUser, "post", "", nil, time.Hour)
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

func TestEventBusUnsubscribePreservesOtherMethodListener(t *testing.T) {
	events := synapse.NewEventBus()
	first := &postInsertListener{}
	second := &postInsertListener{}

	firstSubscription := events.SubscribePostInsert("*", first.handle)
	events.SubscribePostInsert("*", second.handle)
	events.Unsubscribe(firstSubscription)

	events.EmitPostInsert(context.Background(), synapse.NewContextMessage(
		"thread-listener", "agent", synapse.RoleUser, "message", "", nil, time.Minute,
	))

	deadline := time.After(time.Second)
	for second.calls.Load() != 1 {
		select {
		case <-deadline:
			t.Fatalf("second listener was removed with the first; calls=%d", second.calls.Load())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if first.calls.Load() != 0 {
		t.Errorf("first listener received %d calls after unsubscribe", first.calls.Load())
	}
}

func TestEventFilterDoesNotMatchMessageContent(t *testing.T) {
	events := synapse.NewEventBus()
	called := make(chan struct{}, 1)
	events.SubscribePostInsertFilter(
		synapse.MessageClassEventFilter(synapse.ClassTaskMessage),
		func(context.Context, synapse.SynapseMessage) { called <- struct{}{} },
	)
	events.EmitPostInsert(context.Background(), synapse.NewContextMessage(
		"thread-filter", "agent", synapse.RoleUser, "TaskMessage", "", nil, time.Minute,
	))
	select {
	case <-called:
		t.Fatal("task listener matched a context message based on content")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestEventFilterMatchesAllConfiguredFields(t *testing.T) {
	events := synapse.NewEventBus()
	called := make(chan struct{}, 1)
	filter := synapse.EventFilter{
		MessageClass: synapse.ClassTaskMessage,
		ThreadID:     regexp.MustCompile(`^thread-filter$`),
	}
	events.SubscribePostInsertFilter(filter, func(context.Context, synapse.SynapseMessage) { called <- struct{}{} })
	events.EmitPostInsert(context.Background(), synapse.NewTaskMessage(
		"other-thread", "agent", "work", "reply", nil, "squad", time.Minute, 1,
	))
	select {
	case <-called:
		t.Fatal("filter matched a message with the wrong thread")
	case <-time.After(50 * time.Millisecond):
	}
	events.EmitPostInsert(context.Background(), synapse.NewTaskMessage(
		"thread-filter", "agent", "work", "reply", nil, "squad", time.Minute, 1,
	))
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("filter did not match all configured fields")
	}
}

func TestMemoryStorageReloadsMessagesWithoutSharingPayload(t *testing.T) {
	ctx := context.Background()
	storage := synapse.NewMemoryStorage()
	first := synapse.NewSynapseService(10, storage)
	if err := first.Connect(ctx); err != nil {
		t.Fatalf("first Connect: %v", err)
	}
	msg := synapse.NewTaskMessage("thread-memory", "agent", "work", "reply", map[string]any{
		"nested": map[string]any{"value": "original"},
	}, "squad", time.Hour, 1)
	if _, err := first.SendMessage(ctx, msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	stored, err := storage.LoadAllActiveMessages(ctx)
	if err != nil {
		t.Fatalf("LoadAllActiveMessages: %v", err)
	}
	if len(stored) != 1 {
		t.Fatalf("stored message count = %d, want 1", len(stored))
	}
	stored[0].SetPayloadValue("parameters", map[string]any{
		"nested": map[string]any{"value": "mutated outside storage"},
	})
	storedAgain, err := storage.LoadAllActiveMessages(ctx)
	if err != nil {
		t.Fatalf("LoadAllActiveMessages after mutation: %v", err)
	}
	if got := storedAgain[0].Parameters()["nested"].(map[string]any)["value"]; got != "original" {
		t.Fatalf("storage payload was externally mutated, got %v", got)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	second := synapse.NewSynapseService(10, storage)
	if err := second.Connect(ctx); err != nil {
		t.Fatalf("second Connect: %v", err)
	}
	defer second.Close()
	loaded, err := second.FetchContext(ctx, "thread-memory", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("expected no context messages for task thread, got %d", len(loaded))
	}
	consumed, err := second.ConsumeTask(ctx, "thread-memory", "squad", "work", "", 1)
	if err != nil {
		t.Fatalf("ConsumeTask: %v", err)
	}
	if len(consumed) != 1 {
		t.Fatalf("expected one reloaded task, got %d", len(consumed))
	}
	params := consumed[0].Parameters()
	params["nested"].(map[string]any)["value"] = "mutated"
	consumedAgain, err := second.ConsumeTask(ctx, "thread-memory", "squad", "work", "", 1)
	if err != nil {
		t.Fatalf("second ConsumeTask: %v", err)
	}
	if len(consumedAgain) != 0 {
		t.Fatal("task with max consumers=1 was not removed")
	}
}

func TestConnectInvalidPersistedMessageDoesNotRetainMutex(t *testing.T) {
	storage := &invalidMessageStorage{}
	svc := synapse.NewSynapseService(10, storage)
	if err := svc.Connect(context.Background()); err == nil {
		t.Fatal("expected invalid persisted message error")
	}
	done := make(chan struct{})
	go func() {
		_, _ = svc.FetchContext(context.Background(), "thread", 1)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("service mutex remained locked after Connect failure")
	}
}

type invalidMessageStorage struct{}

func (*invalidMessageStorage) Connect(context.Context) error { return nil }
func (*invalidMessageStorage) Close() error                  { return nil }
func (*invalidMessageStorage) SaveMessage(context.Context, synapse.SynapseMessage) error {
	return errors.New("not implemented")
}
func (*invalidMessageStorage) DeleteMessage(context.Context, string) error { return nil }
func (*invalidMessageStorage) LoadAllActiveMessages(context.Context) ([]synapse.SynapseMessage, error) {
	return []synapse.SynapseMessage{{ID: "invalid"}}, nil
}

func TestConsumeTaskAtomic(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	_ = svc.Connect(ctx)
	defer svc.Close()

	msg := synapse.NewTaskMessage("thread-5", "agent-1", "bible_study", "reply-thread-5", map[string]any{"passage": "Juan 3:16"}, "squad-1", time.Hour, 1)
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

	msg := synapse.NewContextMessage("thread-6", "agent-1", synapse.RoleUser, "expires", "", nil, time.Second)
	msg.Timestamp = time.Now().Add(-2 * time.Second)
	_, _ = svc.SendMessage(ctx, msg)

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
		msg := synapse.NewContextMessage("thread-cache-clone", "agent-1", synapse.RoleUser, content, "", nil, time.Hour)
		if _, err := svc.SendMessage(ctx, msg); err != nil {
			t.Fatalf("SendMessage: %v", err)
		}
	}

	got, err := svc.FetchContext(ctx, "thread-cache-clone", 2)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	got[0].SetPayloadValue("content", "mutated")

	got, err = svc.FetchContext(ctx, "thread-cache-clone", 2)
	if err != nil {
		t.Fatalf("FetchContext after mutation: %v", err)
	}
	if got[0].Content() != "first" {
		t.Fatalf("cached message was externally mutated, got %q", got[0].Content())
	}
}

func TestFetchContextCacheExcludesExpiredMessages(t *testing.T) {
	svc := synapse.NewSynapseService(10, nil)
	ctx := context.Background()
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewContextMessage("thread-cache-expiry", "agent-1", synapse.RoleUser, "short lived", "", nil, time.Second)
	msg.Timestamp = time.Now().Add(-2 * time.Second)
	if _, err := svc.SendMessage(ctx, msg); err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	got, err := svc.FetchContext(ctx, "thread-cache-expiry", 1)
	if err != nil {
		t.Fatalf("FetchContext after expiration: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("FetchContext returned %d expired cached messages", len(got))
	}
}

func TestTaskMessageHelpers(t *testing.T) {
	msg := synapse.NewTaskMessage("t1", "a1", "study", "reply-1", map[string]any{"q": "hello"}, "squad-1", time.Hour, 2)
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
	msg := synapse.NewCommandMessage("t1", "a1", "reset", map[string]any{"force": true}, "squad-1", time.Hour)
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

func TestParentThreadIDField(t *testing.T) {
	msg := synapse.NewContextMessage("thread-1", "agent-1", synapse.RoleUser, "test", "", nil, time.Hour)
	if msg.ParentThreadID != "" {
		t.Errorf("expected empty ParentThreadID, got %s", msg.ParentThreadID)
	}

	msg.ParentThreadID = "parent-thread-1"
	if msg.ParentThreadID != "parent-thread-1" {
		t.Errorf("expected 'parent-thread-1', got %s", msg.ParentThreadID)
	}

	// Verify it persists through JSON serialization
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("failed to marshal: %v", err)
	}

	var decoded synapse.SynapseMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal: %v", err)
	}

	if decoded.ParentThreadID != "parent-thread-1" {
		t.Errorf("ParentThreadID not preserved through JSON, got %s", decoded.ParentThreadID)
	}
}

func TestNewSynapseMessagePanicsOnEmptyThreadID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty threadID, but did not panic")
		}
	}()

	synapse.NewSynapseMessage("", "agent-1", synapse.RoleUser, synapse.ClassContextMessage)
}

func TestNewSynapseMessagePanicsOnEmptyAgentID(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic on empty agentID, but did not panic")
		}
	}()

	synapse.NewSynapseMessage("thread-1", "", synapse.RoleUser, synapse.ClassContextMessage)
}

func TestParametersReturnsDefensiveCopy(t *testing.T) {
	original := map[string]any{
		"key":    "value",
		"number": 42,
		"nested": map[string]any{"value": "original"},
		"items":  []any{"original"},
	}
	msg := synapse.NewTaskMessage("thread-1", "agent-1", "task-type", "reply-thread", original, "squad-1", time.Hour, 1)

	params := msg.Parameters()
	params["key"] = "mutated"
	params["new_key"] = "new_value"
	params["nested"].(map[string]any)["value"] = "mutated"
	params["items"].([]any)[0] = "mutated"

	params2 := msg.Parameters()
	if params2["key"] != "value" {
		t.Errorf("expected original value 'value', got %v", params2["key"])
	}
	if params2["number"] != 42 {
		t.Errorf("expected original number 42, got %v", params2["number"])
	}
	if _, exists := params2["new_key"]; exists {
		t.Error("mutation leaked to original message")
	}
	if got := params2["nested"].(map[string]any)["value"]; got != "original" {
		t.Errorf("nested map mutation leaked to original message, got %v", got)
	}
	if got := params2["items"].([]any)[0]; got != "original" {
		t.Errorf("nested slice mutation leaked to original message, got %v", got)
	}
}

func TestSynapseMessageValidateRejectsInvalidPayloads(t *testing.T) {
	tests := []struct {
		name        string
		message     synapse.SynapseMessage
		expectedErr string
	}{
		{
			name: "context missing content",
			message: func() synapse.SynapseMessage {
				msg := synapse.NewContextMessage("thread-1", "agent-1", synapse.RoleUser, "content", "", nil, time.Hour)
				msg.SetPayloadValue("content", 123)
				return msg
			}(),
			expectedErr: "content must be a string",
		},
		{
			name: "task missing task type",
			message: func() synapse.SynapseMessage {
				msg := synapse.NewTaskMessage("thread-1", "agent-1", "work", "reply", nil, "", time.Hour, 1)
				msg.SetPayloadValue("task_type", "")
				return msg
			}(),
			expectedErr: "task_type must be a non-empty string",
		},
		{
			name: "command parameters must be an object",
			message: func() synapse.SynapseMessage {
				msg := synapse.NewCommandMessage("thread-1", "agent-1", "reset", nil, "", time.Hour)
				msg.SetPayloadValue("parameters", []any{"invalid"})
				return msg
			}(),
			expectedErr: "parameters must be an object",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.message.Validate()
			if err == nil || err.Error() != test.expectedErr {
				t.Fatalf("Validate() error = %v, want %q", err, test.expectedErr)
			}
		})
	}
}

func TestSendMessageRejectsInvalidPayload(t *testing.T) {
	ctx := context.Background()
	svc := synapse.NewSynapseService(10, nil)
	if err := svc.Connect(ctx); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer svc.Close()

	msg := synapse.NewTaskMessage("thread-invalid", "agent-1", "work", "reply", nil, "", time.Hour, 1)
	msg.SetPayloadValue("task_type", 42)
	if _, err := svc.SendMessage(ctx, msg); err == nil {
		t.Fatal("expected SendMessage to reject invalid task payload")
	}

	got, err := svc.FetchContext(ctx, "thread-invalid", 10)
	if err != nil {
		t.Fatalf("FetchContext: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("invalid message was exposed through context, got %d messages", len(got))
	}
}
