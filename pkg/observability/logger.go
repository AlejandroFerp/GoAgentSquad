package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// ContextHandler injects trace/step attributes from context into each slog record.
type ContextHandler struct {
	next slog.Handler
}

func NewContextHandler(next slog.Handler) slog.Handler {
	if next == nil {
		next = slog.NewTextHandler(os.Stderr, nil)
	}
	return &ContextHandler{next: next}
}

func (h *ContextHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *ContextHandler) Handle(ctx context.Context, record slog.Record) error {
	for _, attr := range contextAttrs(ctx) {
		record.AddAttrs(attr)
	}
	return h.next.Handle(ctx, record)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{next: h.next.WithAttrs(attrs)}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{next: h.next.WithGroup(name)}
}

// NewTextLogger creates a text logger that auto-binds trace metadata from context.
func NewTextLogger(writer io.Writer, options *slog.HandlerOptions) *slog.Logger {
	if writer == nil {
		writer = os.Stderr
	}
	return slog.New(NewContextHandler(slog.NewTextHandler(writer, options)))
}

// BindLogger returns a logger preloaded with context-derived trace fields.
func BindLogger(logger *slog.Logger, ctx context.Context) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	attrs := contextAttrs(ctx)
	if len(attrs) == 0 {
		return logger
	}
	args := make([]any, 0, len(attrs))
	for _, attr := range attrs {
		args = append(args, attr)
	}
	return logger.With(args...)
}

// LoggerFromContext binds trace attributes to the default logger.
func LoggerFromContext(ctx context.Context) *slog.Logger {
	return BindLogger(slog.Default(), ctx)
}

// contextAttrs extracts stable correlation attributes for logs.
func contextAttrs(ctx context.Context) []slog.Attr {
	attrs := make([]slog.Attr, 0, 5)
	if trace, ok := TraceFromContext(ctx); ok {
		if trace.TraceID != "" {
			attrs = append(attrs, slog.String("trace_id", trace.TraceID))
		}
		if trace.SpanID != "" {
			attrs = append(attrs, slog.String("span_id", trace.SpanID))
		}
		if trace.CorrelationID != "" {
			attrs = append(attrs, slog.String("correlation_id", trace.CorrelationID))
		}
		if trace.CausationID != "" {
			attrs = append(attrs, slog.String("causation_id", trace.CausationID))
		}
	}
	if stepID, ok := StepIDFromContext(ctx); ok && stepID != "" {
		attrs = append(attrs, slog.String("step_id", stepID))
	}
	return attrs
}
