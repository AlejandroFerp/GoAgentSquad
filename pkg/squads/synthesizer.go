package squads

import (
	"context"
	"fmt"
	"strings"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

// Synthesizer monitors context growth and compacts history when a threshold is
// exceeded, posting a synthesis checkpoint message that slices future fetches.
type Synthesizer struct {
	Blackboard BlackboardBus
	Threshold  int
	Summarize  func(messages []synapse.SynapseMessage) string
	Subscribed bool
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
	if s.Subscribed {
		return
	}
	s.Blackboard.Events().Subscribe("*", synapse.PostInsertCallback(s.onPostInsert), "post_insert")
	s.Subscribed = true
}

// Stop unsubscribes the synthesizer.
func (s *Synthesizer) Stop() {
	if !s.Subscribed {
		return
	}
	s.Blackboard.Events().Unsubscribe(synapse.PostInsertCallback(s.onPostInsert))
	s.Subscribed = false
}

func (s *Synthesizer) onPostInsert(ctx context.Context, msg synapse.SynapseMessage) {
	if msg.MessageClass != synapse.ClassContextMessage {
		return
	}
	if msg.Role == synapse.RoleSystem {
		if isSynth, ok := msg.Payload["is_synthesis"].(bool); ok && isSynth {
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
			if isSynth, ok := m.Payload["is_synthesis"].(bool); ok && isSynth {
				continue
			}
		}
		nonSynthCount++
	}
	if nonSynthCount >= s.Threshold {
		summary := s.Summarize(active)
		synthMsg := synapse.NewContextMessage(threadID, "synthesizer", synapse.RoleSystem,
			fmt.Sprintf("[SYNTHESIS CHECKPOINT] %s", summary), msg.SquadID, nil, 3600*24)
		synthMsg.Payload["is_synthesis"] = true
		_, _ = s.Blackboard.SendMessage(ctx, synthMsg)
	}
}

func defaultSummarize(messages []synapse.SynapseMessage) string {
	var parts []string
	for _, m := range messages {
		snippet := m.Content()
		if len(snippet) > 30 {
			snippet = snippet[:30] + "..."
		}
		if snippet == "" {
			snippet = ""
		}
		parts = append(parts, fmt.Sprintf("%s(%s): %s", m.Role, m.AgentID, snippet))
	}
	return "Summary of active discussion: " + strings.Join(parts, " | ")
}
