package observability

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Attr is a typed key/value attribute for spans and steps.
type Attr struct {
	Key   string
	Value any
}

// Tracer opens spans and propagates their ids through context.Context.
type Tracer interface {
	StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span)
}

// Span is one node in the trace tree.
type Span interface {
	SetAttributes(attrs ...Attr)
	RecordError(err error)
	SpanID() string
	TraceID() string
	End()
}

// NoopTracer is the safe default when no exporter is configured.
type NoopTracer struct{}

func (NoopTracer) StartSpan(ctx context.Context, _ string, _ ...Attr) (context.Context, Span) {
	trace, ok := TraceFromContext(ctx)
	if !ok || trace.TraceID == "" {
		trace.TraceID = uuid.NewString()
	}
	trace.SpanID = uuid.NewString()
	if trace.CorrelationID == "" {
		trace.CorrelationID = trace.TraceID
	}
	ctx = WithTraceContext(ctx, trace)
	return ctx, &noopSpan{traceID: trace.TraceID, spanID: trace.SpanID, startedAt: time.Now()}
}

type noopSpan struct {
	traceID   string
	spanID    string
	startedAt time.Time
}

func (s *noopSpan) SetAttributes(_ ...Attr) {}
func (s *noopSpan) RecordError(_ error)      {}
func (s *noopSpan) SpanID() string           { return s.spanID }
func (s *noopSpan) TraceID() string          { return s.traceID }
func (s *noopSpan) End()                     {}
