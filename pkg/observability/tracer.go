package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	oteltrace "go.opentelemetry.io/otel/trace"
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

// OTelTracer adapts an OpenTelemetry tracer to the local observability contract.
type OTelTracer struct{ tr oteltrace.Tracer }

func NewOTelTracer(tracer oteltrace.Tracer) OTelTracer {
	return OTelTracer{tr: tracer}
}

func (t OTelTracer) StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	if t.tr == nil {
		return NoopTracer{}.StartSpan(ctx, name, attrs...)
	}
	ctx, span := t.tr.Start(ctx, name, oteltrace.WithAttributes(toOTelAttributes(attrs)...))
	return ctx, &otelSpan{span: span}
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

type otelSpan struct {
	span oteltrace.Span
}

func (s *noopSpan) SetAttributes(_ ...Attr) {}
func (s *noopSpan) RecordError(_ error)     {}
func (s *noopSpan) SpanID() string          { return s.spanID }
func (s *noopSpan) TraceID() string         { return s.traceID }
func (s *noopSpan) End()                    {}

func (s *otelSpan) SetAttributes(attrs ...Attr) {
	if s == nil || s.span == nil {
		return
	}
	s.span.SetAttributes(toOTelAttributes(attrs)...)
}

func (s *otelSpan) RecordError(err error) {
	if s == nil || s.span == nil || err == nil {
		return
	}
	s.span.RecordError(err)
	s.span.SetStatus(codes.Error, err.Error())
}

func (s *otelSpan) SpanID() string {
	if s == nil || s.span == nil {
		return ""
	}
	return s.span.SpanContext().SpanID().String()
}

func (s *otelSpan) TraceID() string {
	if s == nil || s.span == nil {
		return ""
	}
	return s.span.SpanContext().TraceID().String()
}

func (s *otelSpan) End() {
	if s == nil || s.span == nil {
		return
	}
	s.span.End()
}

// RecordedSpan captures an in-memory span snapshot for tests and debugging.
type RecordedSpan struct {
	Name      string
	TraceID   string
	SpanID    string
	ParentID  string
	Attrs     []Attr
	StartedAt time.Time
	EndedAt   time.Time
	Error     string
}

// RecorderTracer is an in-memory tracer used to verify context propagation.
type RecorderTracer struct {
	mu    sync.Mutex
	spans []RecordedSpan
}

func (t *RecorderTracer) StartSpan(ctx context.Context, name string, attrs ...Attr) (context.Context, Span) {
	trace, ok := TraceFromContext(ctx)
	if !ok || trace.TraceID == "" {
		trace.TraceID = uuid.NewString()
	}
	parentSpanID := trace.SpanID
	trace.SpanID = uuid.NewString()
	if trace.CorrelationID == "" {
		trace.CorrelationID = trace.TraceID
	}
	ctx = WithTraceContext(ctx, trace)
	return ctx, &recorderSpan{
		tracer: t,
		recorded: RecordedSpan{
			Name:      name,
			TraceID:   trace.TraceID,
			SpanID:    trace.SpanID,
			ParentID:  parentSpanID,
			Attrs:     append([]Attr(nil), attrs...),
			StartedAt: time.Now(),
		},
	}
}

func (t *RecorderTracer) Spans() []RecordedSpan {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]RecordedSpan, len(t.spans))
	copy(out, t.spans)
	return out
}

type recorderSpan struct {
	tracer   *RecorderTracer
	recorded RecordedSpan
	once     sync.Once
}

func (s *recorderSpan) SetAttributes(attrs ...Attr) {
	s.recorded.Attrs = append(s.recorded.Attrs, attrs...)
}

func (s *recorderSpan) RecordError(err error) {
	if err != nil {
		s.recorded.Error = err.Error()
	}
}

func (s *recorderSpan) SpanID() string { return s.recorded.SpanID }

func (s *recorderSpan) TraceID() string { return s.recorded.TraceID }

func (s *recorderSpan) End() {
	s.once.Do(func() {
		s.recorded.EndedAt = time.Now()
		if s.tracer == nil {
			return
		}
		s.tracer.mu.Lock()
		s.tracer.spans = append(s.tracer.spans, s.recorded)
		s.tracer.mu.Unlock()
	})
}

func toOTelAttributes(attrs []Attr) []attribute.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, len(attrs))
	for _, attr := range attrs {
		switch value := attr.Value.(type) {
		case string:
			out = append(out, attribute.String(attr.Key, value))
		case bool:
			out = append(out, attribute.Bool(attr.Key, value))
		case int:
			out = append(out, attribute.Int(attr.Key, value))
		case int64:
			out = append(out, attribute.Int64(attr.Key, value))
		case float64:
			out = append(out, attribute.Float64(attr.Key, value))
		case float32:
			out = append(out, attribute.Float64(attr.Key, float64(value)))
		default:
			out = append(out, attribute.String(attr.Key, fmt.Sprint(value)))
		}
	}
	return out
}
