package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDashboardDefaults(t *testing.T) {
	settings, err := LoadDashboard(nil)
	if err != nil {
		t.Fatalf("load dashboard defaults: %v", err)
	}
	if settings.Address != "127.0.0.1:8080" {
		t.Fatalf("dashboard address = %q, want default", settings.Address)
	}
	if settings.TraceFile != "" {
		t.Fatalf("trace file = %q, want empty", settings.TraceFile)
	}
}

func TestLoadDemoEnvironmentOverrides(t *testing.T) {
	t.Setenv("SQUAD_DASHBOARD_ADDR", "127.0.0.1:9090")
	t.Setenv("SQUAD_TRACE_JSONL", "traces/demo.jsonl")
	t.Setenv("SQUAD_OTEL_ENABLED", "true")
	t.Setenv("SQUAD_OTEL_ENDPOINT", "127.0.0.1:4317")
	t.Setenv("SQUAD_OTEL_INSECURE", "true")
	t.Setenv("SQUAD_OTEL_HEADERS", "authorization=Bearer token,x-tenant=demo")
	t.Setenv("SQUAD_OTEL_BATCH_TIMEOUT", "250ms")
	t.Setenv("SQUAD_OTEL_SERVICE_VERSION", "1.2.3")

	settings, err := LoadDemo(nil)
	if err != nil {
		t.Fatalf("load demo from environment: %v", err)
	}
	if settings.DashboardAddress != "127.0.0.1:9090" {
		t.Fatalf("dashboard address = %q, want environment value", settings.DashboardAddress)
	}
	if settings.TraceJSONL != "traces/demo.jsonl" {
		t.Fatalf("trace JSONL = %q, want environment value", settings.TraceJSONL)
	}
	if !settings.OTel.Enabled {
		t.Fatal("expected OpenTelemetry to be enabled")
	}
	if settings.OTel.Runtime.Endpoint != "127.0.0.1:4317" {
		t.Fatalf("endpoint = %q, want environment value", settings.OTel.Runtime.Endpoint)
	}
	if !settings.OTel.Runtime.Insecure {
		t.Fatal("expected insecure OTLP mode")
	}
	if settings.OTel.Runtime.BatchTimeout != 250*time.Millisecond {
		t.Fatalf("batch timeout = %s, want 250ms", settings.OTel.Runtime.BatchTimeout)
	}
	if settings.OTel.Runtime.Headers["authorization"] != "Bearer token" {
		t.Fatalf("authorization header = %q, want Bearer token", settings.OTel.Runtime.Headers["authorization"])
	}
	if settings.OTel.Runtime.ServiceVersion != "1.2.3" {
		t.Fatalf("service version = %q, want environment value", settings.OTel.Runtime.ServiceVersion)
	}
}

func TestLoadDemoFlagsOverrideEnvironment(t *testing.T) {
	t.Setenv("SQUAD_OTEL_ENABLED", "false")
	t.Setenv("SQUAD_OTEL_ENDPOINT", "collector.example:4317")
	t.Setenv("SQUAD_DASHBOARD_ADDR", "127.0.0.1:9090")

	settings, err := LoadDemo([]string{
		"--otel-enabled=true",
		"--otel-endpoint=127.0.0.1:4317",
		"--dashboard-addr=127.0.0.1:8081",
	})
	if err != nil {
		t.Fatalf("load demo from flags: %v", err)
	}
	if !settings.OTel.Enabled {
		t.Fatal("expected explicit flag to enable OpenTelemetry")
	}
	if settings.OTel.Runtime.Endpoint != "127.0.0.1:4317" {
		t.Fatalf("endpoint = %q, want explicit flag", settings.OTel.Runtime.Endpoint)
	}
	if settings.DashboardAddress != "127.0.0.1:8081" {
		t.Fatalf("dashboard address = %q, want explicit flag", settings.DashboardAddress)
	}
}

func TestLoadDemoRejectsMalformedOTelValues(t *testing.T) {
	tests := []struct {
		name        string
		environment string
		value       string
		want        string
	}{
		{
			name:        "boolean",
			environment: "SQUAD_OTEL_ENABLED",
			value:       "sometimes",
			want:        "SQUAD_OTEL_ENABLED",
		},
		{
			name:        "duration",
			environment: "SQUAD_OTEL_BATCH_TIMEOUT",
			value:       "later",
			want:        "SQUAD_OTEL_BATCH_TIMEOUT",
		},
		{
			name:        "headers",
			environment: "SQUAD_OTEL_HEADERS",
			value:       "authorization",
			want:        "SQUAD_OTEL_HEADERS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.name != "boolean" {
				t.Setenv("SQUAD_OTEL_ENABLED", "true")
			}
			t.Setenv(test.environment, test.value)

			_, err := LoadDemo(nil)
			if err == nil {
				t.Fatal("expected malformed OpenTelemetry value to fail")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestLoadDemoDisabledOTelSkipsOptionalOTelValidation(t *testing.T) {
	t.Setenv("SQUAD_OTEL_ENABLED", "false")
	t.Setenv("SQUAD_OTEL_HEADERS", "not-a-header")

	settings, err := LoadDemo(nil)
	if err != nil {
		t.Fatalf("disabled OpenTelemetry should not parse optional settings: %v", err)
	}
	if settings.OTel.Enabled {
		t.Fatal("expected OpenTelemetry to be disabled")
	}
}
