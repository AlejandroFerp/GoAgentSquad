package observability_test

import (
	"context"
	"errors"
	"testing"

	"github.com/embention/agent-squad-go/pkg/observability"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func TestOTelTracerPropagatesNestedSpanTraceIDs(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	tracer := observability.NewOTelTracer(provider.Tracer("agent-squad-go-test"))
	ctx, root := tracer.StartSpan(context.Background(), "root",
		observability.Attr{Key: observability.AttrCorrelationID, Value: "thread-1"},
	)
	defer root.End()

	if root.TraceID() == "" {
		t.Fatal("expected root trace id")
	}
	if root.SpanID() == "" {
		t.Fatal("expected root span id")
	}

	_, child := tracer.StartSpan(ctx, "child")
	defer child.End()
	child.SetAttributes(observability.Attr{Key: observability.AttrAgentID, Value: "agent-1"})
	child.RecordError(errors.New("boom"))

	if child.TraceID() != root.TraceID() {
		t.Fatalf("expected child trace id %q, got %q", root.TraceID(), child.TraceID())
	}
	if child.SpanID() == root.SpanID() {
		t.Fatal("expected child span id to differ from root span id")
	}
}
