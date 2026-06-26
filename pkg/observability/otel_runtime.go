package observability

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	envSquadOTelEnabled        = "SQUAD_OTEL_ENABLED"
	envSquadOTelServiceName    = "SQUAD_OTEL_SERVICE_NAME"
	envSquadOTelServiceVersion = "SQUAD_OTEL_SERVICE_VERSION"
	envSquadOTelTracerName     = "SQUAD_OTEL_TRACER_NAME"
	envSquadOTelEndpoint       = "SQUAD_OTEL_ENDPOINT"
	envSquadOTelInsecure       = "SQUAD_OTEL_INSECURE"
	envSquadOTelHeaders        = "SQUAD_OTEL_HEADERS"
	envSquadOTelBatchTimeout   = "SQUAD_OTEL_BATCH_TIMEOUT"
)

// OTelRuntimeConfig describes how to build a real OpenTelemetry-backed tracer.
type OTelRuntimeConfig struct {
	ServiceName    string
	ServiceVersion string
	TracerName     string
	Endpoint       string
	Insecure       bool
	Headers        map[string]string
	BatchTimeout   time.Duration
	Exporter       sdktrace.SpanExporter
}

// OTelRuntime groups the provider lifecycle with the local tracer adapter.
type OTelRuntime struct {
	Provider *sdktrace.TracerProvider
	Tracer   Tracer
}

type envLookup func(string) (string, bool)

// NewOTelRuntime builds a tracer provider and adapts it to the local tracer contract.
func NewOTelRuntime(ctx context.Context, cfg OTelRuntimeConfig) (*OTelRuntime, error) {
	normalized, err := normalizeOTelRuntimeConfig(cfg)
	if err != nil {
		return nil, err
	}

	exporter := normalized.Exporter
	if exporter == nil {
		exporter, err = newOTLPTraceExporter(ctx, normalized)
		if err != nil {
			return nil, fmt.Errorf("create OTLP trace exporter: %w", err)
		}
	}

	batcherOptions := make([]sdktrace.BatchSpanProcessorOption, 0, 1)
	if normalized.BatchTimeout > 0 {
		batcherOptions = append(batcherOptions, sdktrace.WithBatchTimeout(normalized.BatchTimeout))
	}

	provider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter, batcherOptions...),
		sdktrace.WithResource(newOTelResource(normalized)),
	)

	return &OTelRuntime{
		Provider: provider,
		Tracer:   NewOTelTracer(provider.Tracer(normalized.TracerName)),
	}, nil
}

// Shutdown flushes and closes the underlying tracer provider.
func (r *OTelRuntime) Shutdown(ctx context.Context) error {
	if r == nil || r.Provider == nil {
		return nil
	}
	return r.Provider.Shutdown(ctx)
}

// OTelRuntimeConfigFromEnv loads the demo/runtime OpenTelemetry settings from environment variables.
// It returns enabled=false when no OTel-specific environment variable is present or when SQUAD_OTEL_ENABLED=false.
func OTelRuntimeConfigFromEnv(lookup func(string) (string, bool), defaults OTelRuntimeConfig) (OTelRuntimeConfig, bool, error) {
	if lookup == nil {
		lookup = func(string) (string, bool) { return "", false }
	}

	cfg := defaults
	enabled := false

	if raw, ok := lookup(envSquadOTelEnabled); ok {
		parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return cfg, false, fmt.Errorf("parse %s: %w", envSquadOTelEnabled, err)
		}
		if !parsed {
			return cfg, false, nil
		}
		enabled = true
	}

	if raw, ok := lookup(envSquadOTelServiceName); ok {
		cfg.ServiceName = strings.TrimSpace(raw)
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelServiceVersion); ok {
		cfg.ServiceVersion = strings.TrimSpace(raw)
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelTracerName); ok {
		cfg.TracerName = strings.TrimSpace(raw)
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelEndpoint); ok {
		cfg.Endpoint = strings.TrimSpace(raw)
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelInsecure); ok {
		parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
		if err != nil {
			return cfg, false, fmt.Errorf("parse %s: %w", envSquadOTelInsecure, err)
		}
		cfg.Insecure = parsed
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelHeaders); ok {
		headers, err := ParseOTLPHeaders(raw)
		if err != nil {
			return cfg, false, fmt.Errorf("parse %s: %w", envSquadOTelHeaders, err)
		}
		cfg.Headers = headers
		enabled = true
	}
	if raw, ok := lookup(envSquadOTelBatchTimeout); ok {
		parsed, err := time.ParseDuration(strings.TrimSpace(raw))
		if err != nil {
			return cfg, false, fmt.Errorf("parse %s: %w", envSquadOTelBatchTimeout, err)
		}
		cfg.BatchTimeout = parsed
		enabled = true
	}

	return cfg, enabled, nil
}

// ParseOTLPHeaders parses a comma-separated key=value list into OTLP headers.
func ParseOTLPHeaders(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}

	headers := make(map[string]string)
	for _, part := range strings.Split(raw, ",") {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}

		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, fmt.Errorf("invalid OTLP header %q: expected key=value", entry)
		}

		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			return nil, fmt.Errorf("invalid OTLP header %q: key is empty", entry)
		}
		headers[key] = value
	}

	return headers, nil
}

func normalizeOTelRuntimeConfig(cfg OTelRuntimeConfig) (OTelRuntimeConfig, error) {
	cfg.ServiceName = strings.TrimSpace(cfg.ServiceName)
	cfg.ServiceVersion = strings.TrimSpace(cfg.ServiceVersion)
	cfg.TracerName = strings.TrimSpace(cfg.TracerName)
	cfg.Endpoint = strings.TrimSpace(cfg.Endpoint)

	if cfg.ServiceName == "" {
		return cfg, fmt.Errorf("OpenTelemetry service name is required")
	}
	if cfg.TracerName == "" {
		cfg.TracerName = cfg.ServiceName
	}

	if len(cfg.Headers) > 0 {
		headers := make(map[string]string, len(cfg.Headers))
		for key, value := range cfg.Headers {
			trimmedKey := strings.TrimSpace(key)
			if trimmedKey == "" {
				return cfg, fmt.Errorf("OpenTelemetry header key cannot be empty")
			}
			headers[trimmedKey] = strings.TrimSpace(value)
		}
		cfg.Headers = headers
	}

	return cfg, nil
}

func newOTLPTraceExporter(ctx context.Context, cfg OTelRuntimeConfig) (sdktrace.SpanExporter, error) {
	options := make([]otlptracegrpc.Option, 0, 3)
	if cfg.Endpoint != "" {
		options = append(options, otlptracegrpc.WithEndpoint(cfg.Endpoint))
	}
	if cfg.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlptracegrpc.WithHeaders(cfg.Headers))
	}
	return otlptracegrpc.New(ctx, options...)
}

func newOTelResource(cfg OTelRuntimeConfig) *sdkresource.Resource {
	attrs := []attribute.KeyValue{attribute.String("service.name", cfg.ServiceName)}
	if cfg.ServiceVersion != "" {
		attrs = append(attrs, attribute.String("service.version", cfg.ServiceVersion))
	}
	return sdkresource.NewWithAttributes("", attrs...)
}
