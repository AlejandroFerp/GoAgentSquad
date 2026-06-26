package observability

import "context"

type traceContextKey struct{}
type stepContextKey struct{}

// TraceContext carries causal metadata through context.Context without changing
// public method signatures.
type TraceContext struct {
	TraceID       string
	SpanID        string
	CausationID   string
	CorrelationID string
}

// WithTraceContext stores trace metadata in ctx.
func WithTraceContext(ctx context.Context, trace TraceContext) context.Context {
	return context.WithValue(ctx, traceContextKey{}, trace)
}

// TraceFromContext retrieves trace metadata from ctx.
func TraceFromContext(ctx context.Context) (TraceContext, bool) {
	trace, ok := ctx.Value(traceContextKey{}).(TraceContext)
	return trace, ok
}

// WithStepID stores the current step id in ctx.
func WithStepID(ctx context.Context, stepID string) context.Context {
	return context.WithValue(ctx, stepContextKey{}, stepID)
}

// StepIDFromContext retrieves the current step id from ctx.
func StepIDFromContext(ctx context.Context) (string, bool) {
	stepID, ok := ctx.Value(stepContextKey{}).(string)
	return stepID, ok
}
