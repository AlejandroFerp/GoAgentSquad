package squads

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

// This file implements context compaction by emitting synthesis checkpoints
// when a thread grows beyond a configured message threshold.

// Synthesizer monitors context growth and compacts history when a threshold is
// exceeded, posting a synthesis checkpoint message that slices future fetches.
type Synthesizer struct {
	Blackboard   BlackboardBus
	Threshold    int
	Summarize    func(messages []synapse.SynapseMessage) string
	mu           sync.Mutex
	subscribed   bool
	subscription synapse.SubscriptionID
}

// NewSynthesizer builds a Synthesizer with a default summarizer.
func NewSynthesizer(bb BlackboardBus, threshold int, summarize func([]synapse.SynapseMessage) string) *Synthesizer {
	if threshold <= 0 {
		threshold = 10
	}
	if summarize == nil {
		summarize = defaultSummarize
	}
	return &Synthesizer{Blackboard: bb, Threshold: threshold, Summarize: summarize}
}

// Start subscribes the synthesizer to post_insert events.
func (s *Synthesizer) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.subscribed {
		return
	}
	s.subscription = s.Blackboard.Events().SubscribePostInsert("*", s.onPostInsert)
	s.subscribed = true
}

// Stop unsubscribes the synthesizer.
func (s *Synthesizer) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.subscribed {
		return
	}
	s.Blackboard.Events().Unsubscribe(s.subscription)
	s.subscription = 0
	s.subscribed = false
}

func (s *Synthesizer) onPostInsert(ctx context.Context, msg synapse.SynapseMessage) {
	if msg.MessageClass != synapse.ClassContextMessage {
		return
	}
	if msg.Role == synapse.RoleSystem {
		if isSynth, ok := msg.GetPayloadValue("is_synthesis"); ok && isSynth == true {
			return
		}
	}
	threadID := msg.ThreadID
	active, err := FetchSlicedContext(ctx, s.Blackboard, threadID, s.Threshold*2)
	if err != nil {
		return
	}
	nonSynthCount := 0
	for _, m := range active {
		if m.Role == synapse.RoleSystem {
			if isSynth, ok := m.GetPayloadValue("is_synthesis"); ok && isSynth == true {
				continue
			}
		}
		nonSynthCount++
	}
	if nonSynthCount >= s.Threshold {
		summary := s.Summarize(active)
		synthMsg := synapse.NewContextMessage(threadID, "synthesizer", synapse.RoleSystem,
			fmt.Sprintf("[SYNTHESIS CHECKPOINT] %s", summary), msg.SquadID, nil, 24*time.Hour)
		synthMsg.SetPayloadValue("is_synthesis", true)
		_ = deliverObserved(ctx, s.Blackboard, synthMsg, "threshold", s.Threshold)
	}
}

func defaultSummarize(messages []synapse.SynapseMessage) string {
	var parts []string
	for _, m := range messages {
		snippet := truncateRunes(m.Content(), 30)
		parts = append(parts, fmt.Sprintf("%s(%s): %s", m.Role, m.AgentID, snippet))
	}
	return "Summary of active discussion: " + strings.Join(parts, " | ")
}
