package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

type compatibilityEvidenceIndex struct {
	Query             string                   `json:"query"`
	Inventory         manualInventory          `json:"inventory"`
	AnalysisInventory manualInventory          `json:"analysis_inventory"`
	Pages             []manualPageExtract      `json:"pages"`
	Candidates        []compatibilityCandidate `json:"candidates"`
}

func manualEvidenceIndexTask(memory *manualMemory) func(context.Context, *synapse.SynapseMessage) (string, error) {
	return func(ctx context.Context, taskMsg *synapse.SynapseMessage) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		query := "all"
		if taskMsg != nil {
			if value, ok := taskMsg.Parameters()["query"].(string); ok && strings.TrimSpace(value) != "" {
				query = strings.TrimSpace(value)
			}
		}
		index := buildCompatibilityEvidenceIndex(memory, query)
		encoded, err := json.Marshal(index)
		if err != nil {
			return "", fmt.Errorf("encode shared compatibility evidence: %w", err)
		}
		return string(encoded), nil
	}
}

func buildCompatibilityEvidenceIndex(memory *manualMemory, query string) compatibilityEvidenceIndex {
	inventory, _ := memory.getInventory()
	analysisInventory, _ := memory.getAnalysisInventory()
	pages := memory.getPages()
	candidates := memory.getCandidates()
	query = strings.TrimSpace(query)

	index := compatibilityEvidenceIndex{
		Query:             query,
		Inventory:         deterministicInventory(inventory, inventory.Manuals),
		AnalysisInventory: deterministicInventory(analysisInventory, analysisInventory.Manuals),
		Pages:             make([]manualPageExtract, 0, len(pages)),
		Candidates:        append([]compatibilityCandidate{}, candidates...),
	}
	for _, page := range pages {
		index.Pages = append(index.Pages, pageExtract(page))
	}
	sortCompatibilityEvidenceIndex(&index)

	if strings.EqualFold(query, "all") || query == "" {
		return index
	}

	relevantURLs := make(map[string]struct{})
	addRelevantURL := func(pageURL string) {
		if normalized := normaliseURL(pageURL); normalized != "" {
			relevantURLs[normalized] = struct{}{}
		}
	}
	for _, manual := range inventory.Manuals {
		if compatibilityManualMatches(manual, query) {
			addRelevantURL(manual.URL)
			addRelevantURL(manual.LatestURL)
		}
	}
	for _, page := range pages {
		if compatibilityPageMatches(page, query) {
			addRelevantURL(page.Link.URL)
		}
	}
	for _, candidate := range candidates {
		if compatibilityCandidateMatches(candidate, query) {
			addRelevantURL(candidate.FromManualURL)
			addRelevantURL(candidate.ToManualURL)
		}
	}

	index.Inventory.Manuals = filterManualLinks(index.Inventory.Manuals, query, relevantURLs)
	index.AnalysisInventory.Manuals = filterManualLinks(index.AnalysisInventory.Manuals, query, relevantURLs)
	filteredPages := make([]manualPageExtract, 0, len(index.Pages))
	for _, page := range index.Pages {
		if compatibilityPageMatchesExtract(page, query, relevantURLs) {
			filteredPages = append(filteredPages, page)
		}
	}
	index.Pages = filteredPages
	filteredCandidates := make([]compatibilityCandidate, 0, len(index.Candidates))
	for _, candidate := range index.Candidates {
		if compatibilityCandidateMatches(candidate, query) || compatibilityURLIsRelevant(candidate.FromManualURL, relevantURLs) || compatibilityURLIsRelevant(candidate.ToManualURL, relevantURLs) {
			filteredCandidates = append(filteredCandidates, candidate)
		}
	}
	index.Candidates = filteredCandidates
	return index
}

func deterministicInventory(inventory manualInventory, manuals []manualLink) manualInventory {
	clone := cloneInventory(inventory)
	clone.Manuals = append([]manualLink{}, manuals...)
	if clone.Manuals == nil {
		clone.Manuals = []manualLink{}
	}
	if clone.Issues == nil {
		clone.Issues = []manualIssue{}
	}
	sort.SliceStable(clone.Manuals, func(left, right int) bool {
		leftManual := clone.Manuals[left]
		rightManual := clone.Manuals[right]
		if leftManual.IsLatest != rightManual.IsLatest {
			return leftManual.IsLatest
		}
		leftKey := strings.Join([]string{
			normaliseProduct(leftManual.Product),
			strings.ToLower(leftManual.ManualType),
			leftManual.ProductVersion,
			leftManual.ManualVersion,
			leftManual.URL,
		}, "\x00")
		rightKey := strings.Join([]string{
			normaliseProduct(rightManual.Product),
			strings.ToLower(rightManual.ManualType),
			rightManual.ProductVersion,
			rightManual.ManualVersion,
			rightManual.URL,
		}, "\x00")
		return leftKey < rightKey
	})
	sort.SliceStable(clone.Issues, func(left, right int) bool {
		leftKey := clone.Issues[left].Stage + "\x00" + clone.Issues[left].URL + "\x00" + clone.Issues[left].Error
		rightKey := clone.Issues[right].Stage + "\x00" + clone.Issues[right].URL + "\x00" + clone.Issues[right].Error
		return leftKey < rightKey
	})
	return clone
}

func sortCompatibilityEvidenceIndex(index *compatibilityEvidenceIndex) {
	sort.SliceStable(index.Pages, func(left, right int) bool {
		return index.Pages[left].ManualURL < index.Pages[right].ManualURL
	})
	sort.SliceStable(index.Candidates, func(left, right int) bool {
		leftKey := index.Candidates[left].ID
		if leftKey == "" {
			leftKey = index.Candidates[left].FromManualURL + "\x00" + index.Candidates[left].ToManualURL
		}
		rightKey := index.Candidates[right].ID
		if rightKey == "" {
			rightKey = index.Candidates[right].FromManualURL + "\x00" + index.Candidates[right].ToManualURL
		}
		return leftKey < rightKey
	})
}

func filterManualLinks(manuals []manualLink, query string, relevantURLs map[string]struct{}) []manualLink {
	filtered := make([]manualLink, 0, len(manuals))
	for _, manual := range manuals {
		if compatibilityManualMatches(manual, query) || compatibilityURLIsRelevant(manual.URL, relevantURLs) || compatibilityURLIsRelevant(manual.LatestURL, relevantURLs) {
			filtered = append(filtered, manual)
		}
	}
	return filtered
}

func compatibilityManualMatches(manual manualLink, query string) bool {
	if compatibilityReferenceMatches(manual.URL, query) || compatibilityReferenceMatches(manual.LatestURL, query) {
		return true
	}
	return compatibilityTextContains(query,
		manual.Category,
		manual.Family,
		manual.Product,
		manual.ManualType,
		manual.Title,
		manual.ProductVersion,
		manual.ManualVersion,
	)
}

func compatibilityPageMatches(page manualPage, query string) bool {
	if compatibilityManualMatches(page.Link, query) {
		return true
	}
	values := []string{page.Text, page.Error}
	for _, warning := range page.Warnings {
		values = append(values, warning)
	}
	for _, evidence := range page.Evidence {
		values = append(values, evidence.SourceURL, evidence.Section, evidence.Quote)
	}
	return compatibilityTextContains(query, values...)
}

func compatibilityPageMatchesExtract(page manualPageExtract, query string, relevantURLs map[string]struct{}) bool {
	if compatibilityURLIsRelevant(page.ManualURL, relevantURLs) || compatibilityReferenceMatches(page.ManualURL, query) {
		return true
	}
	return compatibilityTextContains(query,
		page.Product,
		page.ManualURL,
		page.ManualType,
		page.ProductVersion,
		page.ManualVersion,
		page.Title,
		page.Text,
		page.Error,
	)
}

func compatibilityCandidateMatches(candidate compatibilityCandidate, query string) bool {
	values := []string{
		candidate.ID,
		candidate.FromProduct,
		candidate.FromManualType,
		candidate.FromManualURL,
		candidate.FromProductVersion,
		candidate.FromManualVersion,
		candidate.ToProduct,
		candidate.ToManualType,
		candidate.ToManualURL,
		candidate.ToProductVersion,
		candidate.ToManualVersion,
		candidate.FromExcerpt,
		candidate.ToExcerpt,
		candidate.Reason,
	}
	for _, evidence := range candidate.Evidence {
		values = append(values, evidence.SourceURL, evidence.Section, evidence.Quote)
	}
	return compatibilityTextContains(query, values...)
}

func compatibilityTextContains(query string, values ...string) bool {
	query = strings.ToLower(strings.TrimSpace(query))
	if query == "" {
		return false
	}
	for _, value := range values {
		if strings.Contains(strings.ToLower(value), query) {
			return true
		}
	}
	return false
}

func compatibilityReferenceMatches(reference, query string) bool {
	queryURL := normaliseURL(query)
	referenceURL := normaliseURL(reference)
	if queryURL == "" || referenceURL == "" {
		return false
	}
	return queryURL == referenceURL || strings.HasPrefix(queryURL, referenceURL+"/") || strings.HasPrefix(referenceURL, queryURL+"/")
}

func compatibilityURLIsRelevant(pageURL string, relevantURLs map[string]struct{}) bool {
	_, ok := relevantURLs[normaliseURL(pageURL)]
	return ok
}

func (memory *manualMemory) getPages() []manualPage {
	memory.mu.RLock()
	pages := make([]manualPage, 0, len(memory.pages))
	for _, page := range memory.pages {
		pages = append(pages, cloneManualPage(page))
	}
	memory.mu.RUnlock()
	sort.SliceStable(pages, func(left, right int) bool {
		return pages[left].Link.URL < pages[right].Link.URL
	})
	return pages
}

func cloneManualPage(page manualPage) manualPage {
	page.Sections = append([]manualSection{}, page.Sections...)
	page.Keywords = append([]string{}, page.Keywords...)
	page.Windows = append([]string{}, page.Windows...)
	page.Evidence = append([]evidenceQuote{}, page.Evidence...)
	page.Warnings = append([]string{}, page.Warnings...)
	return page
}
