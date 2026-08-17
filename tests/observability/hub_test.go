package observability_test

import (
	"testing"

	"github.com/embention/agent-squad-go/pkg/observability"
)

func TestHubLimitsSubscribersAndCountsDroppedEvents(t *testing.T) {
	hub := observability.NewHubWithLimit(1)
	id, _, ok := hub.TrySubscribe()
	if !ok {
		t.Fatal("first subscription was rejected")
	}
	if _, _, ok := hub.TrySubscribe(); ok {
		t.Fatal("second subscription exceeded the configured limit")
	}

	for i := 0; i < 129; i++ {
		hub.Broadcast(observability.AgentStep{StepID: "step"})
	}

	stats := hub.Stats()
	if stats.Subscribers != 1 || stats.MaxSubscribers != 1 {
		t.Fatalf("hub stats = %+v, want one subscriber with limit one", stats)
	}
	if stats.DroppedEvents != 1 {
		t.Fatalf("dropped events = %d, want 1", stats.DroppedEvents)
	}

	hub.Unsubscribe(id)
	if got := hub.Stats().Subscribers; got != 0 {
		t.Fatalf("subscribers after unsubscribe = %d, want 0", got)
	}
}
