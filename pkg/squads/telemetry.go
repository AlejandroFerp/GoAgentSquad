package squads

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

// This file centralizes observability helpers (span start, step record, logger
// binding, and trace-context reconstruction from persisted messages).

func observedRuntime(bb BlackboardBus) *ObservabilityRuntime {
	if bb == nil {
		return NewObservabilityRuntime()
	}
	runtime := bb.Observability()
	if runtime == nil {
		return NewObservabilityRuntime()
	}
	return runtime.ensureDefaults()
}

// startObservedSpan creates a span using the runtime tracer configured in the blackboard.
func startObservedSpan(ctx context.Context, bb BlackboardBus, name string, attrs ...observability.Attr) (context.Context, observability.Span) {
	return observedRuntime(bb).Tracer.StartSpan(ctx, name, attrs...)
}

// observedLogger binds correlation attributes from ctx to the runtime logger.
func observedLogger(ctx context.Context, bb BlackboardBus) *slog.Logger {
	runtime := observedRuntime(bb)
	if runtime.Logger == nil {
		return observability.LoggerFromContext(ctx)
	}
	return observability.BindLogger(runtime.Logger, ctx)
}

// contextWithMessageTrace rebuilds trace linkage when async callbacks receive persisted messages.
func contextWithMessageTrace(ctx context.Context, msg synapse.SynapseMessage) context.Context {
	ctx = observability.WithTraceMetadata(ctx, observability.TraceContext{
		TraceID:       msg.Trace.TraceID,
		SpanID:        msg.Trace.SpanID,
		CausationID:   msg.Trace.CausationID,
		CorrelationID: msg.Trace.CorrelationID,
	})
	if msg.Trace.CausationID != "" {
		ctx = observability.WithStepID(ctx, msg.Trace.CausationID)
	}
	return ctx
}

// recordObservedStep enriches and persists one business-level step in the ledger.
func recordObservedStep(ctx context.Context, bb BlackboardBus, step observability.AgentStep) (context.Context, observability.AgentStep) {
	trace, ok := observability.TraceFromContext(ctx)
	if ok {
		if step.CorrelationID == "" {
			step.CorrelationID = trace.CorrelationID
		}
		if step.TraceID == "" {
			step.TraceID = trace.TraceID
		}
		if step.SpanID == "" {
			step.SpanID = trace.SpanID
		}
	}
	if step.CorrelationID == "" && step.ThreadID != "" {
		step.CorrelationID = step.ThreadID
	}
	if step.ParentStepID == "" {
		if parentStepID, ok := observability.StepIDFromContext(ctx); ok {
			step.ParentStepID = parentStepID
		}
	}
	if step.StartedAt.IsZero() {
		step.StartedAt = time.Now()
	}
	step = observedRuntime(bb).Ledger.Record(step)
	return observability.WithStepID(ctx, step.StepID), step
}

// observedSummary trims long text payloads for compact timeline entries.
func observedSummary(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= 120 {
		return text
	}
	return text[:117] + "..."
}
