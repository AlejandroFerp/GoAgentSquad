package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

const (
	defaultTopic         = "Audit every Embention manual category, including Apps and Discontinued, expand every latest alias into concrete product/manual versions, extract explicit compatibility evidence from versioned technical sections, and produce a version-aware evidence-gated compatibility matrix."
	maxOpenRouterModels  = 3
	defaultScope         = "1x"
	defaultVerifierModel = "openai/gpt-5.6-luna"
)

type config struct {
	APIKey            string
	Models            []string
	Providers         []string
	DashboardAddr     string
	OutputPath        string
	InventoryOutput   string
	MatrixOutput      string
	Topic             string
	Scope             string
	IncludeHistorical bool
	VerifierModel     string
	Mock              bool
	CaptureLLMContent bool
	ExitAfterRun      bool
	SearchResults     int
	RequestTimeout    time.Duration
	QueryTimeout      time.Duration
}

func loadConfig(args []string) (config, error) {
	flags := pflag.NewFlagSet("use-case", pflag.ContinueOnError)
	envFile := flags.String("env-file", ".env", "path to the experiment-local environment file")
	flags.String("dashboard-address", "127.0.0.1:8090", "dashboard listen address")
	flags.String("output", "manual-audit-report.md", "consolidated audit report output path")
	flags.String("inventory-output", "manual-vs-product.json", "manual-to-product JSON output path")
	flags.String("matrix-output", "compatibility-matrix.md", "compatibility matrix Markdown output path")
	flags.String("topic", defaultTopic, "research topic")
	flags.String("scope", defaultScope, "product ecosystem to investigate, or all")
	flags.Bool("all-versions", false, "include historical manual versions in the analysis scope")
	flags.String("verifier-model", defaultVerifierModel, "model used for candidate compatibility verification")
	mockFlag := flags.Bool("mock", false, "use deterministic responses without calling OpenRouter")
	flags.Bool("capture-llm-content", false, "persist LLM prompts and completions for dashboard inspection")
	flags.Bool("exit-after-run", false, "exit after writing the audit deliverables instead of waiting for Ctrl+C")
	flags.Int("search-results", 5, "maximum web results per research request")
	flags.Duration("request-timeout", 5*time.Minute, "timeout for one OpenRouter request")
	flags.Duration("query-timeout", 20*time.Minute, "timeout for each pipeline phase")
	if err := flags.Parse(args); err != nil {
		return config{}, fmt.Errorf("parse flags: %w", err)
	}

	if err := godotenv.Load(*envFile); err != nil && !*mockFlag {
		return config{}, fmt.Errorf("load environment file %q: %w", *envFile, err)
	}

	settings := viper.New()
	settings.SetEnvPrefix("EXPERIMENT")
	settings.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	settings.AutomaticEnv()
	if err := settings.BindPFlags(flags); err != nil {
		return config{}, fmt.Errorf("bind flags: %w", err)
	}
	for key, environment := range map[string]string{
		"openrouter-api-key":         "OPENROUTER_API_KEY",
		"openrouter-model":           "OPENROUTER_MODEL",
		"openrouter-fallback-models": "OPENROUTER_FALLBACK_MODELS",
		"openrouter-provider":        "OPENROUTER_PROVIDER",
		"verifier-model":             "OPENROUTER_VERIFIER_MODEL",
	} {
		if err := settings.BindEnv(key, environment); err != nil {
			return config{}, fmt.Errorf("bind %s: %w", environment, err)
		}
	}

	models := uniqueNonEmpty(append(
		[]string{settings.GetString("openrouter-model")},
		splitCSV(settings.GetString("openrouter-fallback-models"))...,
	))
	if len(models) > maxOpenRouterModels {
		models = models[:maxOpenRouterModels]
	}
	cfg := config{
		APIKey:            strings.TrimSpace(settings.GetString("openrouter-api-key")),
		Models:            models,
		Providers:         splitCSV(settings.GetString("openrouter-provider")),
		DashboardAddr:     strings.TrimSpace(settings.GetString("dashboard-address")),
		OutputPath:        strings.TrimSpace(settings.GetString("output")),
		InventoryOutput:   strings.TrimSpace(settings.GetString("inventory-output")),
		MatrixOutput:      strings.TrimSpace(settings.GetString("matrix-output")),
		Topic:             strings.TrimSpace(settings.GetString("topic")),
		Scope:             strings.TrimSpace(settings.GetString("scope")),
		IncludeHistorical: settings.GetBool("all-versions"),
		VerifierModel:     strings.TrimSpace(settings.GetString("verifier-model")),
		Mock:              settings.GetBool("mock"),
		CaptureLLMContent: settings.GetBool("capture-llm-content"),
		ExitAfterRun:      settings.GetBool("exit-after-run"),
		SearchResults:     settings.GetInt("search-results"),
		RequestTimeout:    settings.GetDuration("request-timeout"),
		QueryTimeout:      settings.GetDuration("query-timeout"),
	}
	if err := cfg.validate(); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func (cfg config) validate() error {
	if cfg.DashboardAddr == "" {
		return fmt.Errorf("dashboard address is required")
	}
	if cfg.OutputPath == "" {
		return fmt.Errorf("output path is required")
	}
	if cfg.InventoryOutput == "" {
		return fmt.Errorf("inventory output path is required")
	}
	if cfg.MatrixOutput == "" {
		return fmt.Errorf("matrix output path is required")
	}
	if cfg.Topic == "" {
		return fmt.Errorf("research topic is required")
	}
	if cfg.Scope == "" {
		return fmt.Errorf("analysis scope is required")
	}
	if cfg.VerifierModel == "" {
		return fmt.Errorf("verifier model is required")
	}
	if cfg.SearchResults < 1 {
		return fmt.Errorf("search results must be at least 1")
	}
	if cfg.RequestTimeout <= 0 || cfg.QueryTimeout <= 0 {
		return fmt.Errorf("request and query timeouts must be positive")
	}
	if cfg.Mock {
		return nil
	}
	if cfg.APIKey == "" {
		return fmt.Errorf("OPENROUTER_API_KEY is required")
	}
	if len(cfg.Models) == 0 {
		return fmt.Errorf("OPENROUTER_MODEL or OPENROUTER_FALLBACK_MODELS is required")
	}
	return nil
}

func splitCSV(raw string) []string {
	return uniqueNonEmpty(strings.Split(raw, ","))
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
