package squads

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

// This file contains observer middleware that can inspect/augment messages
// before they are persisted into the blackboard.

// ObserverCallback is the PreInsertCallback signature for observers.
type ObserverCallback func(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error)

// BaseObserver subscribes to pre_insert events matching a pattern and delegates
// processing to a concrete observer.
type BaseObserver struct {
	Pattern    string
	Blackboard BlackboardBus
	Subscribed bool
	process    func(ctx context.Context, msg *synapse.SynapseMessage) (*synapse.SynapseMessage, error)
}

// Start subscribes the observer to the pre_insert EventBus hook.
func (o *BaseObserver) Start() {
	if o.Subscribed {
		return
	}
	o.Blackboard.Events().Subscribe(o.Pattern, synapse.PreInsertCallback(o.onPreInsert), "pre_insert")
	o.Subscribed = true
}

// Stop unsubscribes the observer.
func (o *BaseObserver) Stop() {
	if !o.Subscribed {
		return
	}
	o.Blackboard.Events().Unsubscribe(synapse.PreInsertCallback(o.onPreInsert))
	o.Subscribed = false
}

func (o *BaseObserver) onPreInsert(ctx context.Context, msg synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
	metrics := o.Blackboard.GetMetrics(msg.ThreadID)
	if metrics != nil {
		metrics.RecordObserverInteraction(o.Name())
	}
	if o.process == nil {
		return &msg, nil
	}
	return o.process(ctx, &msg)
}

// Name returns the concrete observer type name for metrics tracking.
func (o *BaseObserver) Name() string {
	return "BaseObserver"
}

// ReferenceExpansionObserver expands bracketed scripture references in message
// content using a lookup function and appends the expanded text to the payload.
type ReferenceExpansionObserver struct {
	BaseObserver
	LookupFn func(reference string) string
	refRegex *regexp.Regexp
}

// NewReferenceExpansionObserver builds an observer that expands [Reference]
// patterns in ContextMessage content.
func NewReferenceExpansionObserver(pattern string, bb BlackboardBus, lookupFn func(string) string) *ReferenceExpansionObserver {
	obs := &ReferenceExpansionObserver{
		LookupFn: lookupFn,
		refRegex: regexp.MustCompile(`\[([^\]]+)\]`),
	}
	obs.BaseObserver = BaseObserver{
		Pattern:    pattern,
		Blackboard: bb,
		process:    obs.process,
	}
	return obs
}

// Name returns the observer name for metrics.
func (r *ReferenceExpansionObserver) Name() string {
	return "ReferenceExpansionObserver"
}

func (r *ReferenceExpansionObserver) process(ctx context.Context, msg *synapse.SynapseMessage) (*synapse.SynapseMessage, error) {
	if msg == nil || msg.MessageClass != synapse.ClassContextMessage {
		return msg, nil
	}
	content := msg.Content()
	if content == "" {
		return msg, nil
	}
	matches := r.refRegex.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return msg, nil
	}

	var expandedParts []string
	citations := msg.Citations()
	if citations == nil {
		citations = []any{}
	}

	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		refName := strings.TrimSpace(match[1])
		expandedText := r.LookupFn(refName)
		if expandedText != "" {
			expandedParts = append(expandedParts, fmt.Sprintf("[%s]: \"%s\"", refName, expandedText))
			entry := map[string]any{"reference": refName, "text": expandedText}
			citations = append(citations, entry)
		}
	}

	if len(expandedParts) > 0 {
		suffix := "\n\n---\n**Expanded References:**\n" + strings.Join(expandedParts, "\n")
		msg.Payload["content"] = content + suffix
		msg.Payload["citations"] = citations
	}
	return msg, nil
}
