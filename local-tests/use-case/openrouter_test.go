package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestOpenRouterCallUsesFallbacksWebSearchAndCitations(t *testing.T) {
	var request chatRequest
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, httpRequest *http.Request) {
		if got := httpRequest.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("authorization = %q, want bearer token", got)
		}
		if err := json.NewDecoder(httpRequest.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{
			"choices": [{"message": {
				"content": "Evidence-backed finding.",
				"annotations": [{"type": "url_citation", "url_citation": {"url": "https://example.com/report", "title": "Example report"}}]
			}}],
			"usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
		}`))
	}))
	t.Cleanup(server.Close)

	client := &openRouterClient{
		endpoint: server.URL, apiKey: "test-key",
		models: []string{"primary", "fallback"}, searchResults: 4,
		httpClient: &http.Client{Timeout: time.Second},
	}
	result, err := client.call(context.Background(), "primary", webResearchMarker+" Research this.", []map[string]any{{"role": "user", "content": "topic"}})
	if err != nil {
		t.Fatalf("call OpenRouter: %v", err)
	}
	if len(request.Models) != 2 || request.Models[0] != "primary" || request.Models[1] != "fallback" {
		t.Fatalf("models = %v, want ordered primary and fallback", request.Models)
	}
	if len(request.Plugins) != 1 || request.Plugins[0].ID != "web" || request.Plugins[0].MaxResults != 4 {
		t.Fatalf("plugins = %#v, want configured web search", request.Plugins)
	}
	if strings.Contains(request.Messages[0].Content, webResearchMarker) {
		t.Fatal("internal web-research marker leaked into the model prompt")
	}
	if !strings.Contains(result.Content, "[Example report](https://example.com/report)") {
		t.Fatalf("content = %q, want normalized citation", result.Content)
	}
	if result.TotalTokens != 15 {
		t.Fatalf("total tokens = %d, want 15", result.TotalTokens)
	}
}

func TestConfigLoadsPrimaryAndFallbackModels(t *testing.T) {
	envFile := t.TempDir() + "/.env"
	content := strings.Join([]string{
		"OPENROUTER_API_KEY=test-key",
		"OPENROUTER_MODEL=primary-model",
		"OPENROUTER_FALLBACK_MODELS=fallback-a,fallback-b,primary-model",
	}, "\n")
	if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cfg, err := loadConfig([]string{"--env-file", envFile, "--exit-after-run"})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	want := []string{"primary-model", "fallback-a", "fallback-b"}
	if !slices.Equal(cfg.Models, want) {
		t.Fatalf("models = %v, want %v", cfg.Models, want)
	}
}

func TestConfigLimitsOpenRouterModelsToAPIMaximum(t *testing.T) {
	envFile := t.TempDir() + "/.env"
	content := strings.Join([]string{
		"OPENROUTER_API_KEY=test-key",
		"OPENROUTER_MODEL=primary-model",
		"OPENROUTER_FALLBACK_MODELS=fallback-a,fallback-b,fallback-c,fallback-d",
	}, "\n")
	if err := os.WriteFile(envFile, []byte(content), 0o600); err != nil {
		t.Fatalf("write env: %v", err)
	}

	cfg, err := loadConfig([]string{"--env-file", envFile, "--exit-after-run"})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	want := []string{"primary-model", "fallback-a", "fallback-b"}
	if !slices.Equal(cfg.Models, want) {
		t.Fatalf("models = %v, want API maximum %v", cfg.Models, want)
	}
}

func TestOpenRouterModelChainLimitsAgentOverrideAndFallbacks(t *testing.T) {
	want := []string{"openai/gpt-5.6-luna", "primary-model", "fallback-a"}
	got := openRouterModelChain("openai/gpt-5.6-luna", []string{"primary-model", "fallback-a", "fallback-b"})
	if !slices.Equal(got, want) {
		t.Fatalf("models = %v, want API maximum %v", got, want)
	}
}
