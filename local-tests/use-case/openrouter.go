package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/embention/agent-squad-go/pkg/squads"
)

const (
	openRouterEndpoint = "https://openrouter.ai/api/v1/chat/completions"
	webResearchMarker  = "[WEB_RESEARCH]"
	maxResponseBytes   = 8 << 20
)

type openRouterClient struct {
	endpoint      string
	apiKey        string
	models        []string
	providers     []string
	searchResults int
	httpClient    *http.Client
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type webPlugin struct {
	ID         string `json:"id"`
	MaxResults int    `json:"max_results"`
}

type providerPreferences struct {
	Order []string `json:"order,omitempty"`
}

type chatRequest struct {
	Models   []string             `json:"models"`
	Messages []chatMessage        `json:"messages"`
	Plugins  []webPlugin          `json:"plugins,omitempty"`
	Provider *providerPreferences `json:"provider,omitempty"`
}

type urlCitation struct {
	URL     string `json:"url"`
	Title   string `json:"title"`
	Content string `json:"content"`
}

type annotation struct {
	Type        string      `json:"type"`
	URLCitation urlCitation `json:"url_citation"`
}

type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content     string       `json:"content"`
			Annotations []annotation `json:"annotations"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage struct {
		PromptTokens            int     `json:"prompt_tokens"`
		CompletionTokens        int     `json:"completion_tokens"`
		TotalTokens             int     `json:"total_tokens"`
		Cost                    float64 `json:"cost"`
		CompletionTokensDetails struct {
			ReasoningTokens int `json:"reasoning_tokens"`
		} `json:"completion_tokens_details"`
	} `json:"usage"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func newOpenRouterClient(cfg config) *openRouterClient {
	return &openRouterClient{
		endpoint:      openRouterEndpoint,
		apiKey:        cfg.APIKey,
		models:        append([]string(nil), cfg.Models...),
		providers:     append([]string(nil), cfg.Providers...),
		searchResults: cfg.SearchResults,
		httpClient:    &http.Client{Timeout: cfg.RequestTimeout},
	}
}

func (client *openRouterClient) call(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
	webResearch := strings.Contains(systemPrompt, webResearchMarker)
	systemPrompt = strings.ReplaceAll(systemPrompt, webResearchMarker, "")

	request := chatRequest{
		Models:   openRouterModelChain(model, client.models),
		Messages: []chatMessage{{Role: "system", Content: strings.TrimSpace(systemPrompt)}},
	}
	for _, message := range messages {
		role, _ := message["role"].(string)
		content, _ := message["content"].(string)
		if role == "" || content == "" {
			continue
		}
		request.Messages = append(request.Messages, chatMessage{Role: role, Content: content})
	}
	if webResearch {
		request.Plugins = []webPlugin{{ID: "web", MaxResults: client.searchResults}}
	}
	if len(client.providers) > 0 {
		request.Provider = &providerPreferences{Order: client.providers}
	}

	body, err := json.Marshal(request)
	if err != nil {
		return squads.LLMResponse{}, fmt.Errorf("encode OpenRouter request: %w", err)
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, client.endpoint, bytes.NewReader(body))
	if err != nil {
		return squads.LLMResponse{}, fmt.Errorf("create OpenRouter request: %w", err)
	}
	httpRequest.Header.Set("Authorization", "Bearer "+client.apiKey)
	httpRequest.Header.Set("Content-Type", "application/json")
	httpRequest.Header.Set("X-OpenRouter-Title", "GoAgentSquad manuals compatibility use-case experiment")

	response, err := client.httpClient.Do(httpRequest)
	if err != nil {
		return squads.LLMResponse{}, fmt.Errorf("call OpenRouter: %w", err)
	}
	defer response.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes))
	if err != nil {
		return squads.LLMResponse{}, fmt.Errorf("read OpenRouter response: %w", err)
	}
	var completion chatResponse
	if err := json.Unmarshal(responseBody, &completion); err != nil {
		return squads.LLMResponse{}, fmt.Errorf("decode OpenRouter response (status %d): %w", response.StatusCode, err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		message := strings.TrimSpace(response.Status)
		if completion.Error != nil && completion.Error.Message != "" {
			message = completion.Error.Message
		}
		return squads.LLMResponse{}, fmt.Errorf("OpenRouter request failed with status %d: %s", response.StatusCode, message)
	}
	if completion.Error != nil {
		return squads.LLMResponse{}, fmt.Errorf("OpenRouter error %d: %s", completion.Error.Code, completion.Error.Message)
	}
	if len(completion.Choices) == 0 {
		return squads.LLMResponse{}, fmt.Errorf("OpenRouter returned no choices")
	}

	choice := completion.Choices[0].Message
	content := appendCitations(strings.TrimSpace(choice.Content), choice.Annotations)
	return squads.LLMResponse{
		Content:          content,
		PromptTokens:     completion.Usage.PromptTokens,
		CompletionTokens: completion.Usage.CompletionTokens,
		TotalTokens:      completion.Usage.TotalTokens,
		Provider:         "OpenRouter",
		RequestID:        response.Header.Get("X-Request-ID"),
		GenerationID:     completion.ID,
		FinishReason:     completion.Choices[0].FinishReason,
		CostUSD:          completion.Usage.Cost,
		ReasoningTokens:  completion.Usage.CompletionTokensDetails.ReasoningTokens,
	}, nil
}

func openRouterModelChain(primary string, fallbacks []string) []string {
	models := uniqueNonEmpty(append([]string{primary}, fallbacks...))
	if len(models) > maxOpenRouterModels {
		models = models[:maxOpenRouterModels]
	}
	return models
}

func appendCitations(content string, annotations []annotation) string {
	seen := make(map[string]struct{}, len(annotations))
	var sources []string
	for _, item := range annotations {
		if item.Type != "url_citation" {
			continue
		}
		url := strings.TrimSpace(item.URLCitation.URL)
		if url == "" {
			continue
		}
		if _, exists := seen[url]; exists {
			continue
		}
		seen[url] = struct{}{}
		title := strings.TrimSpace(item.URLCitation.Title)
		if title == "" {
			title = url
		}
		sources = append(sources, fmt.Sprintf("- [%s](%s)", title, url))
	}
	if len(sources) == 0 {
		return content
	}
	return strings.TrimSpace(content) + "\n\n### Sources\n" + strings.Join(sources, "\n")
}

func mockLLMCall(_ context.Context, _ string, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
	switch {
	case strings.Contains(systemPrompt, "squad 'Manual Ingestion'"):
		return squads.LLMResponse{Content: mockIngestionCoordination(systemPrompt), PromptTokens: 360, CompletionTokens: 420, TotalTokens: 780}, nil
	case strings.Contains(systemPrompt, "MANUAL_NAVIGATOR"):
		if toolResult, ok := mockToolResult(messages, "scrape_manual_index"); ok {
			return squads.LLMResponse{Content: toolResult, PromptTokens: 140, CompletionTokens: 240, TotalTokens: 380}, nil
		}
		return squads.LLMResponse{Content: mockToolCall("scrape_manual_index", map[string]any{"url": manualsIndexURL}), PromptTokens: 80, CompletionTokens: 30, TotalTokens: 110}, nil
	case strings.Contains(systemPrompt, "MANUAL_READER"):
		if toolResult, ok := mockToolResult(messages, "read_assigned_manuals"); ok {
			return squads.LLMResponse{Content: mockEvidenceBatch(toolResult), PromptTokens: 220, CompletionTokens: 260, TotalTokens: 480}, nil
		}
		return squads.LLMResponse{Content: mockToolCall("read_assigned_manuals", map[string]any{}), PromptTokens: 90, CompletionTokens: 30, TotalTokens: 120}, nil
	case strings.Contains(systemPrompt, "MANUAL_VERIFIER"):
		if toolResult, ok := mockToolResult(messages, "verify_candidate_edges"); ok {
			return squads.LLMResponse{Content: mockVerificationBatch(toolResult), PromptTokens: 180, CompletionTokens: 260, TotalTokens: 440}, nil
		}
		return squads.LLMResponse{Content: mockToolCall("verify_candidate_edges", map[string]any{}), PromptTokens: 90, CompletionTokens: 30, TotalTokens: 120}, nil
	case strings.Contains(systemPrompt, "MANUAL_SYNTHESIZER"):
		if _, ok := mockToolResult(messages, "lookup_shared_manual"); ok {
			return squads.LLMResponse{Content: mockCompatibilityDraft(promptText(systemPrompt, messages)), PromptTokens: 260, CompletionTokens: 220, TotalTokens: 480}, nil
		}
		return squads.LLMResponse{Content: mockToolCall("lookup_shared_manual", map[string]any{"reference": firstManualURL(promptText(systemPrompt, messages))}), PromptTokens: 100, CompletionTokens: 30, TotalTokens: 130}, nil
	case strings.Contains(systemPrompt, "MANUAL_REPORTER"):
		return squads.LLMResponse{Content: "{}", PromptTokens: 180, CompletionTokens: 40, TotalTokens: 220}, nil
	default:
		return squads.LLMResponse{Content: "{}", PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30}, nil
	}
}

func promptText(systemPrompt string, messages []map[string]any) string {
	var builder strings.Builder
	builder.WriteString(systemPrompt)
	for _, message := range messages {
		if content, ok := message["content"].(string); ok {
			builder.WriteByte('\n')
			builder.WriteString(content)
		}
	}
	return builder.String()
}

func firstManualURL(text string) string {
	for _, object := range jsonObjects(text) {
		var inventory manualInventory
		if err := json.Unmarshal(object, &inventory); err == nil && len(inventory.Manuals) > 0 && inventory.Manuals[0].URL != "" {
			return inventory.Manuals[0].URL
		}
	}
	return manualsIndexURL
}

func mockIngestionCoordination(systemPrompt string) string {
	batch := evidenceBatch{}
	seen := make(map[string]struct{})
	for _, object := range jsonObjects(systemPrompt) {
		var candidate evidenceBatch
		if err := json.Unmarshal(object, &candidate); err != nil {
			continue
		}
		for _, fact := range candidate.Facts {
			key := strings.ToLower(fact.Product + "|" + fact.ManualURL)
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}
			batch.Facts = append(batch.Facts, fact)
		}
	}
	return mustJSON(batch)
}

func mockToolCall(toolName string, arguments map[string]any) string {
	return mustJSON(map[string]any{"call_tool": toolName, "arguments": arguments})
}

func mockToolResult(messages []map[string]any, toolName string) (string, bool) {
	marker := "Tool " + toolName + " returned result: "
	for index := len(messages) - 1; index >= 0; index-- {
		content, _ := messages[index]["content"].(string)
		if position := strings.Index(content, marker); position >= 0 {
			return strings.TrimSpace(content[position+len(marker):]), true
		}
	}
	return "", false
}

func mockEvidenceBatch(toolResult string) string {
	var pages []manualPageExtract
	if err := json.Unmarshal([]byte(toolResult), &pages); err != nil {
		return mustJSON(evidenceBatch{})
	}
	batch := evidenceBatch{Facts: make([]manualEvidence, 0, len(pages))}
	for _, page := range pages {
		fact := manualEvidence{
			Product:        page.Product,
			ManualURL:      page.ManualURL,
			ManualType:     page.ManualType,
			ProductVersion: page.ProductVersion,
			ManualVersion:  page.ManualVersion,
			Status:         page.Status,
			KeywordsFound:  append([]string(nil), page.KeywordsFound...),
			Warnings:       append([]string(nil), page.Warnings...),
			Notes:          page.Error,
		}
		quote := "The manual contains an explicit compatibility or integration statement."
		if len(page.EvidenceWindows) > 0 {
			quote = page.EvidenceWindows[0]
		}
		citation := evidenceQuote{SourceURL: page.ManualURL, Quote: quote}
		if len(page.Evidence) > 0 {
			fact.Evidence = append(fact.Evidence, page.Evidence...)
			for _, evidence := range page.Evidence {
				if evidence.Section != "" && evidence.Section != "Manual" {
					citation = evidence
					break
				}
			}
		} else {
			fact.Evidence = append(fact.Evidence, citation)
		}
		switch page.Product {
		case "1x":
			fact.CompatibilityClaims = append(fact.CompatibilityClaims, compatibilityClaim{
				FromProduct: "1x", FromManualType: page.ManualType, ToProduct: "DRx", FromProductVersion: page.ProductVersion, FromManualVersion: page.ManualVersion, Status: "compatible", Relationship: "documented compatible device", Connection: "CAN bus", Evidence: []evidenceQuote{citation},
			})
		case "DRx":
			fact.CompatibilityClaims = append(fact.CompatibilityClaims, compatibilityClaim{
				FromProduct: "DRx", FromManualType: page.ManualType, ToProduct: "1x", FromProductVersion: page.ProductVersion, FromManualVersion: page.ManualVersion, Status: "compatible", Relationship: "documented integration", Connection: "CAN connection", Evidence: []evidenceQuote{citation},
			})
		case "HIL Simulator":
			fact.CompatibilityClaims = append(fact.CompatibilityClaims, compatibilityClaim{
				FromProduct: "HIL Simulator", FromManualType: page.ManualType, ToProduct: "4x", FromProductVersion: page.ProductVersion, FromManualVersion: page.ManualVersion, Status: "compatible", Relationship: "documented software integration", Connection: "Veronte Pipe", Evidence: []evidenceQuote{citation},
			})
		}
		batch.Facts = append(batch.Facts, fact)
	}
	return mustJSON(batch)
}

func mockVerificationBatch(toolResult string) string {
	var candidates []compatibilityCandidate
	if err := json.Unmarshal([]byte(toolResult), &candidates); err != nil {
		return mustJSON(compatibilityDraft{})
	}
	draft := compatibilityDraft{Relationships: make([]compatibilityClaim, 0, len(candidates))}
	for _, candidate := range candidates {
		claim := compatibilityClaim{
			FromProduct:        candidate.FromProduct,
			FromManualType:     candidate.FromManualType,
			ToProduct:          candidate.ToProduct,
			ToManualType:       candidate.ToManualType,
			FromProductVersion: candidate.FromProductVersion,
			FromManualVersion:  candidate.FromManualVersion,
			ToProductVersion:   candidate.ToProductVersion,
			ToManualVersion:    candidate.ToManualVersion,
			Status:             "Not specified",
			Relationship:       "Candidate requires direct manual verification",
			Evidence:           append([]evidenceQuote(nil), candidate.Evidence...),
			Notes:              candidate.Reason,
		}
		if (candidate.FromProduct == "1x" && candidate.ToProduct == "DRx") || (candidate.FromProduct == "DRx" && candidate.ToProduct == "1x") {
			claim.Status = "Compatible"
			claim.Relationship = "documented compatible device"
			claim.Connection = "CAN bus"
		}
		if candidate.FromProduct == "HIL Simulator" && candidate.ToProduct == "4x" {
			claim.Status = "Compatible"
			claim.Relationship = "documented software integration"
			claim.Connection = "Veronte Pipe"
		}
		draft.Relationships = append(draft.Relationships, claim)
	}
	return mustJSON(draft)
}

func mockCompatibilityDraft(systemPrompt string) string {
	oneXURL := manualURLForProduct(systemPrompt, "1x")
	hilURL := manualURLForProduct(systemPrompt, "HIL Simulator")
	return mustJSON(compatibilityDraft{Relationships: []compatibilityClaim{
		{FromProduct: "1x", ToProduct: "DRx", Status: "compatible", Relationship: "documented compatible device", Connection: "CAN bus", Evidence: []evidenceQuote{{SourceURL: manualSectionURL(oneXURL, "compatible devices"), Section: "Compatible Devices", Quote: "1x is compatible with DRx over CAN bus."}}},
		{FromProduct: "HIL Simulator", ToProduct: "4x", Status: "compatible", Relationship: "documented software integration", Connection: "Veronte Pipe", Evidence: []evidenceQuote{{SourceURL: manualSectionURL(hilURL, "integration examples"), Section: "Integration examples", Quote: "HIL Simulator integrates with Veronte Pipe."}}},
	}})
}

func manualURLForProduct(text, product string) string {
	var fallback string
	for _, object := range jsonObjects(text) {
		var inventory manualInventory
		if err := json.Unmarshal(object, &inventory); err != nil {
			continue
		}
		for _, manual := range inventory.Manuals {
			if !strings.EqualFold(manual.Product, product) || manual.URL == "" {
				continue
			}
			if fallback == "" {
				fallback = manual.URL
			}
			if manual.IsLatest {
				return manual.URL
			}
		}
	}
	return fallback
}

func manualSectionURL(manualURL, section string) string {
	if strings.TrimSpace(manualURL) == "" {
		return manualsIndexURL
	}
	return strings.TrimRight(manualURL, "/") + "/" + strings.ReplaceAll(section, " ", "%20") + "/index.md"
}
