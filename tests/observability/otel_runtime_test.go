package observability_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type stubSpanExporter struct {
	shutdownCalls int
}

func (e *stubSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	return nil
}

func (e *stubSpanExporter) Shutdown(context.Context) error {
	e.shutdownCalls++
	return nil
}

func TestNewOTelRuntimeUsesInjectedExporter(t *testing.T) {
	exporter := &stubSpanExporter{}
	runtime, err := observability.NewOTelRuntime(context.Background(), observability.OTelRuntimeConfig{
		ServiceName:    "agent-squad-go-test",
		ServiceVersion: "1.0.0",
		BatchTimeout:   10 * time.Millisecond,
		Exporter:       exporter,
	})
	if err != nil {
		t.Fatalf("expected runtime to initialize: %v", err)
	}

	ctx, root := runtime.Tracer.StartSpan(context.Background(), "root")
	_, child := runtime.Tracer.StartSpan(ctx, "child")
	defer child.End()
	defer root.End()

	if root.TraceID() == "" {
		t.Fatal("expected root trace id")
	}
	if child.TraceID() != root.TraceID() {
		t.Fatalf("expected child trace id %q, got %q", root.TraceID(), child.TraceID())
	}

	if err := runtime.Shutdown(context.Background()); err != nil {
		t.Fatalf("expected runtime shutdown to succeed: %v", err)
	}
	if exporter.shutdownCalls == 0 {
		t.Fatal("expected exporter shutdown to be invoked")
	}
}

func TestNewOTelRuntimeRequiresServiceName(t *testing.T) {
	_, err := observability.NewOTelRuntime(context.Background(), observability.OTelRuntimeConfig{
		Exporter: &stubSpanExporter{},
	})
	if err == nil {
		t.Fatal("expected missing service name to fail")
	}
	if !strings.Contains(err.Error(), "service name") {
		t.Fatalf("expected service name error, got %v", err)
	}
}

func TestParseOTLPHeaders(t *testing.T) {
	headers, err := observability.ParseOTLPHeaders("authorization=Bearer token, x-tenant = demo ")
	if err != nil {
		t.Fatalf("expected headers to parse: %v", err)
	}
	if headers["authorization"] != "Bearer token" {
		t.Fatalf("expected authorization header, got %q", headers["authorization"])
	}
	if headers["x-tenant"] != "demo" {
		t.Fatalf("expected x-tenant header, got %q", headers["x-tenant"])
	}
}

func TestParseOTLPHeadersRejectsMalformedEntries(t *testing.T) {
	_, err := observability.ParseOTLPHeaders("authorization")
	if err == nil {
		t.Fatal("expected malformed header to fail")
	}
}
