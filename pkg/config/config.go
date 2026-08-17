// Package config loads command configuration with Viper.
//
// Values use Viper's precedence order: explicit values, flags, environment
// variables, then defaults. Environment variable names use the SQUAD_ prefix
// with dots and hyphens converted to underscores.
package config

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	dashboardAddressDefault = "127.0.0.1:8080"
	demoServiceNameDefault  = "agent-squad-demo"
	demoServiceVersion      = "dev"
)

// Dashboard contains configuration for the standalone dashboard command.
type Dashboard struct {
	Address   string
	TraceFile string
}

// Demo contains configuration for the demonstration command.
type Demo struct {
	DashboardAddress string
	TraceJSONL       string
	OTel             OTel
}

// OTel contains validated OpenTelemetry runtime configuration.
type OTel struct {
	Enabled bool
	Runtime observability.OTelRuntimeConfig
}

type loader struct {
	viper *viper.Viper
	flags *pflag.FlagSet
}

// LoadDashboard parses standalone dashboard flags and environment variables.
func LoadDashboard(args []string) (Dashboard, error) {
	loader := newLoader("squad-dashboard")
	loader.viper.SetDefault("dashboard.addr", dashboardAddressDefault)
	loader.viper.SetDefault("dashboard.trace-file", "")
	loader.flags.String("addr", dashboardAddressDefault, "dashboard listen address")
	loader.flags.String("trace-file", "", "optional JSONL trace file to load and tail into the dashboard")

	if err := loader.bind("dashboard.addr", "addr"); err != nil {
		return Dashboard{}, err
	}
	if err := loader.bind("dashboard.trace-file", "trace-file"); err != nil {
		return Dashboard{}, err
	}
	if err := loader.parse(args); err != nil {
		return Dashboard{}, err
	}

	address := strings.TrimSpace(loader.viper.GetString("dashboard.addr"))
	if address == "" {
		return Dashboard{}, fmt.Errorf("dashboard address is required")
	}

	return Dashboard{
		Address:   address,
		TraceFile: strings.TrimSpace(loader.viper.GetString("dashboard.trace-file")),
	}, nil
}

// LoadDemo parses demo flags and environment variables.
func LoadDemo(args []string) (Demo, error) {
	loader := newLoader("squad-demo")
	setDemoDefaults(loader.viper)
	addDemoFlags(loader.flags)

	for _, binding := range []struct {
		key  string
		flag string
	}{
		{"dashboard.addr", "dashboard-addr"},
		{"trace.jsonl", "trace-jsonl"},
		{"otel.enabled", "otel-enabled"},
		{"otel.service-name", "otel-service-name"},
		{"otel.service-version", "otel-service-version"},
		{"otel.tracer-name", "otel-tracer-name"},
		{"otel.endpoint", "otel-endpoint"},
		{"otel.insecure", "otel-insecure"},
		{"otel.headers", "otel-headers"},
		{"otel.batch-timeout", "otel-batch-timeout"},
	} {
		if err := loader.bind(binding.key, binding.flag); err != nil {
			return Demo{}, err
		}
	}
	if err := loader.parse(args); err != nil {
		return Demo{}, err
	}

	otelConfig, err := loadOTel(loader.viper)
	if err != nil {
		return Demo{}, err
	}

	return Demo{
		DashboardAddress: strings.TrimSpace(loader.viper.GetString("dashboard.addr")),
		TraceJSONL:       strings.TrimSpace(loader.viper.GetString("trace.jsonl")),
		OTel:             otelConfig,
	}, nil
}

func newLoader(name string) loader {
	settings := viper.New()
	settings.SetEnvPrefix("SQUAD")
	settings.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	settings.AutomaticEnv()

	flags := pflag.NewFlagSet(name, pflag.ContinueOnError)
	flags.SetOutput(io.Discard)
	return loader{viper: settings, flags: flags}
}

func (l loader) bind(key, flagName string) error {
	if err := l.viper.BindPFlag(key, l.flags.Lookup(flagName)); err != nil {
		return fmt.Errorf("bind %s flag: %w", flagName, err)
	}
	return nil
}

func (l loader) parse(args []string) error {
	if err := l.flags.Parse(args); err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}
	return nil
}

func setDemoDefaults(settings *viper.Viper) {
	settings.SetDefault("dashboard.addr", "")
	settings.SetDefault("trace.jsonl", "")
	settings.SetDefault("otel.enabled", false)
	settings.SetDefault("otel.service-name", demoServiceNameDefault)
	settings.SetDefault("otel.service-version", demoServiceVersion)
	settings.SetDefault("otel.tracer-name", demoServiceNameDefault)
	settings.SetDefault("otel.endpoint", "")
	settings.SetDefault("otel.insecure", false)
	settings.SetDefault("otel.headers", "")
	settings.SetDefault("otel.batch-timeout", "0s")
}

func addDemoFlags(flags *pflag.FlagSet) {
	flags.String("dashboard-addr", "", "optional dashboard listen address")
	flags.String("trace-jsonl", "", "optional JSONL trace export path")
	flags.Bool("otel-enabled", false, "enable OpenTelemetry tracing")
	flags.String("otel-service-name", demoServiceNameDefault, "OpenTelemetry service name")
	flags.String("otel-service-version", demoServiceVersion, "OpenTelemetry service version")
	flags.String("otel-tracer-name", demoServiceNameDefault, "OpenTelemetry tracer name")
	flags.String("otel-endpoint", "", "OTLP gRPC endpoint")
	flags.Bool("otel-insecure", false, "disable OTLP TLS")
	flags.String("otel-headers", "", "OTLP headers as comma-separated key=value entries")
	flags.String("otel-batch-timeout", "0s", "OpenTelemetry batch timeout")
}

func loadOTel(settings *viper.Viper) (OTel, error) {
	enabled, err := parseBool(settings, "otel.enabled")
	if err != nil {
		return OTel{}, err
	}

	runtime := observability.OTelRuntimeConfig{
		ServiceName:    strings.TrimSpace(settings.GetString("otel.service-name")),
		ServiceVersion: strings.TrimSpace(settings.GetString("otel.service-version")),
		TracerName:     strings.TrimSpace(settings.GetString("otel.tracer-name")),
		Endpoint:       strings.TrimSpace(settings.GetString("otel.endpoint")),
	}
	if !enabled {
		return OTel{Runtime: runtime}, nil
	}

	insecure, err := parseBool(settings, "otel.insecure")
	if err != nil {
		return OTel{}, err
	}
	batchTimeout, err := time.ParseDuration(strings.TrimSpace(settings.GetString("otel.batch-timeout")))
	if err != nil {
		return OTel{}, fmt.Errorf("parse %s: %w", environmentName("otel.batch-timeout"), err)
	}
	headers, err := observability.ParseOTLPHeaders(settings.GetString("otel.headers"))
	if err != nil {
		return OTel{}, fmt.Errorf("parse %s: %w", environmentName("otel.headers"), err)
	}

	runtime.Insecure = insecure
	runtime.BatchTimeout = batchTimeout
	runtime.Headers = headers
	if runtime.ServiceName == "" {
		return OTel{}, fmt.Errorf("OpenTelemetry service name is required")
	}
	if runtime.TracerName == "" {
		runtime.TracerName = runtime.ServiceName
	}

	return OTel{Enabled: true, Runtime: runtime}, nil
}

func parseBool(settings *viper.Viper, key string) (bool, error) {
	value, err := strconvParseBool(strings.TrimSpace(settings.GetString(key)))
	if err != nil {
		return false, fmt.Errorf("parse %s: %w", environmentName(key), err)
	}
	return value, nil
}

func environmentName(key string) string {
	return "SQUAD_" + strings.ToUpper(strings.NewReplacer(".", "_", "-", "_").Replace(key))
}

var strconvParseBool = strconv.ParseBool
