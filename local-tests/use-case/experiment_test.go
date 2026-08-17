package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/embention/agent-squad-go/pkg/dashboard"
	"github.com/embention/agent-squad-go/pkg/observability"
	"github.com/embention/agent-squad-go/pkg/squads"
)

func TestManualAuditRunsAllSquadsAndExposesEvidenceInDashboard(t *testing.T) {
	cfg := config{
		Models:          []string{"mock-model"},
		Mock:            true,
		Scope:           "1x",
		Topic:           defaultTopic,
		DashboardAddr:   "127.0.0.1:0",
		OutputPath:      "manual-audit-report.md",
		InventoryOutput: "manual-vs-product.json",
		MatrixOutput:    "compatibility-matrix.md",
		SearchResults:   3,
		RequestTimeout:  time.Second,
		QueryTimeout:    30 * time.Second,
	}
	var calls atomic.Int32
	delayedLLM := func(ctx context.Context, model, systemPrompt string, messages []map[string]any) (squads.LLMResponse, error) {
		calls.Add(1)
		time.Sleep(10 * time.Millisecond)
		return mockLLMCall(ctx, model, systemPrompt, messages)
	}
	experiment, err := newExperiment(context.Background(), cfg, delayedLLM)
	if err != nil {
		t.Fatalf("create experiment: %v", err)
	}
	t.Cleanup(func() { _ = experiment.Close() })
	result, err := experiment.Run(context.Background())
	if err != nil {
		t.Fatalf("run experiment: %v", err)
	}
	if len(result.Inventory.Manuals) != 12 {
		t.Fatalf("manual count = %d, want two versions for each of six mock manuals", len(result.Inventory.Manuals))
	}
	if len(result.Evidence) != len(result.AnalysisInventory.Manuals) {
		t.Fatalf("evidence count = %d, want one fact per scoped mock manual", len(result.Evidence))
	}
	if !strings.Contains(result.Report, "4.12 / 1.6") || !strings.Contains(result.Report, "compatible%20devices") {
		t.Fatalf("report omitted versioned compatibility evidence: %s", result.Report)
	}
	if len(result.Matrix) == 0 || len(result.Matrix) >= 6 {
		t.Fatalf("compatibility rows = %d, want a non-empty sparse candidate matrix", len(result.Matrix))
	}
	for _, section := range []string{"Manual vs. Product Map", "Compatibility Matrix", "Points Blind"} {
		if !strings.Contains(result.Report, section) {
			t.Fatalf("report does not contain %q: %q", section, result.Report)
		}
	}

	server := httptest.NewServer(dashboard.NewServer(experiment.Observability()))
	t.Cleanup(server.Close)
	response, err := http.Get(server.URL + "/api/queries")
	if err != nil {
		t.Fatalf("query dashboard API: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard status = %d, want 200", response.StatusCode)
	}

	var queries []dashboard.QuerySnapshot
	if err := json.NewDecoder(response.Body).Decode(&queries); err != nil {
		t.Fatalf("decode dashboard queries: %v", err)
	}
	if len(queries) != 4 {
		t.Fatalf("dashboard query count = %d, want four audit squad phases", len(queries))
	}

	seen := map[string]bool{}
	for _, query := range queries {
		seen[query.Summary.CorrelationID] = true
		if query.Summary.Status != "done" {
			t.Fatalf("dashboard query %s status = %q, want done", query.Summary.CorrelationID, query.Summary.Status)
		}
	}
	for _, phaseQuery := range []string{result.DiscoveryQuery, result.IngestionQuery, result.SynthesisQuery, result.ReportingQuery} {
		if !seen[phaseQuery] {
			t.Fatalf("dashboard queries = %v, missing audit phase %s", seen, phaseQuery)
		}
	}

	toolCalls := 0
	delegations := 0
	readerStarts := 0
	for _, queryID := range []string{result.DiscoveryQuery, result.IngestionQuery, result.SynthesisQuery, result.ReportingQuery} {
		for _, step := range experiment.Observability().Ledger.Timeline(queryID) {
			if step.Kind == observability.StepToolCall {
				toolCalls++
			}
			if step.Kind == observability.StepDelegated {
				delegations++
			}
			if step.Kind == observability.StepAgentStarted && step.SquadID == ingestionSquadID {
				readerStarts++
			}
		}
	}
	if toolCalls < 9 {
		t.Fatalf("tool calls = %d, want navigator, four readers, and four verifiers", toolCalls)
	}
	if delegations != 4 {
		t.Fatalf("delegations = %d, want one shared evidence transversal delegation per verifier", delegations)
	}
	transversals, ok := result.SynthesisMetrics["transversals"].(map[string]any)
	if !ok {
		t.Fatalf("synthesis transversals = %T, want map[string]any", result.SynthesisMetrics["transversals"])
	}
	evidenceMetrics, ok := transversals[manualEvidenceIndexID].(map[string]any)
	if !ok {
		t.Fatalf("shared evidence transversal metrics = %T, want map[string]any", transversals[manualEvidenceIndexID])
	}
	if evidenceMetrics["successful_invokes"] != 4 {
		t.Fatalf("shared evidence transversal metrics = %#v, want four successful invocations", evidenceMetrics)
	}
	if readerStarts != 4 {
		t.Fatalf("reader starts = %d, want four concurrent reader agents", readerStarts)
	}
	if got := calls.Load(); got < 10 {
		t.Fatalf("LLM calls = %d, want reader, verifier, and coordination calls across all phases", got)
	}
}

func TestManualCrawlerParsesHierarchyAndSharedCache(t *testing.T) {
	memory := newManualMemory()
	crawler := newManualCrawler(true, memory)
	inventory, err := crawler.scrapeIndex(manualsIndexURL)
	if err != nil {
		t.Fatalf("scrape mock index: %v", err)
	}
	if len(inventory.Manuals) != 12 {
		t.Fatalf("manual count = %d, want two versions for six mock manuals", len(inventory.Manuals))
	}
	if inventory.Manuals[0].Category != "Embention Manuals" && inventory.Manuals[0].Category != "Autopilot platforms" {
		t.Fatalf("first category = %q, want parsed heading", inventory.Manuals[0].Category)
	}
	pages, err := crawler.readAssignedManuals(0, 6)
	if err != nil {
		t.Fatalf("read shard: %v", err)
	}
	if len(pages) != 2 {
		t.Fatalf("shard pages = %#v, want two analyzed pages from the versioned shard", pages)
	}
	for _, page := range pages {
		if page.Status != "analyzed" || len(page.EvidenceWindows) == 0 || len(page.Evidence) == 0 {
			t.Fatalf("reader page = %#v, want analyzed page with direct evidence", page)
		}
	}
	if _, ok := memory.getPage(pages[0].ManualURL); !ok {
		t.Fatalf("manual %q was not retained in shared memory", pages[0].ManualURL)
	}
}

func TestManualEvidenceIndexTransversalPreservesVersionedEvidence(t *testing.T) {
	memory := newManualMemory()
	crawler := newManualCrawler(true, memory)
	inventory, err := crawler.scrapeIndex(manualsIndexURL)
	if err != nil {
		t.Fatalf("scrape mock index: %v", err)
	}
	memory.setInventory(inventory)
	analysisInventory := selectAnalysisInventory(inventory, "1x", false)
	memory.setAnalysisInventory(analysisInventory)
	pages, err := crawler.readAssignedManuals(0, 1)
	if err != nil {
		t.Fatalf("read mock manuals: %v", err)
	}
	if len(pages) != len(analysisInventory.Manuals) {
		t.Fatalf("shared pages = %d, want %d", len(pages), len(analysisInventory.Manuals))
	}
	memory.setCandidates([]compatibilityCandidate{{
		ID:                 "1x-to-drx",
		FromProduct:        "1x",
		FromManualType:     "hardware",
		FromManualURL:      inventory.Manuals[0].URL,
		FromProductVersion: inventory.Manuals[0].ProductVersion,
		FromManualVersion:  inventory.Manuals[0].ManualVersion,
		ToProduct:          "DRx",
		ToManualType:       "user",
		ToManualURL:        inventory.Manuals[1].URL,
	}})

	allIndex := buildCompatibilityEvidenceIndex(memory, "all")
	if len(allIndex.Pages) == 0 || len(allIndex.Candidates) != 1 {
		t.Fatalf("all evidence index = %#v, want pages and one candidate", allIndex)
	}
	if len(allIndex.Inventory.Manuals) != len(inventory.Manuals) {
		t.Fatalf("all inventory manuals = %d, want %d", len(allIndex.Inventory.Manuals), len(inventory.Manuals))
	}
	firstJSON, err := json.Marshal(allIndex)
	if err != nil {
		t.Fatalf("marshal first evidence index: %v", err)
	}
	repeatedJSON, err := json.Marshal(buildCompatibilityEvidenceIndex(memory, "all"))
	if err != nil {
		t.Fatalf("marshal repeated evidence index: %v", err)
	}
	if string(firstJSON) != string(repeatedJSON) {
		t.Fatal("repeated evidence index serialization is not deterministic")
	}
	hasLatest := false
	for _, manual := range allIndex.Inventory.Manuals {
		if manual.IsLatest {
			hasLatest = true
			break
		}
	}
	if !hasLatest {
		t.Fatal("all inventory lost latest-version flags")
	}
	hasLatestPage := false
	hasExactEvidence := false
	for _, page := range allIndex.Pages {
		if page.IsLatest && page.LatestURL != "" {
			hasLatestPage = true
		}
		for _, evidence := range page.Evidence {
			if evidence.SourceURL == "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.6/compatible%20devices/index.md" && evidence.Quote == "Compatible Devices 1x is compatible with DRx over CAN bus." {
				hasExactEvidence = true
			}
		}
	}
	if !hasLatestPage {
		t.Fatal("shared pages lost latest URL metadata")
	}
	if !hasExactEvidence {
		t.Fatal("shared pages lost exact compatibility evidence URL or quote")
	}

	filtered := buildCompatibilityEvidenceIndex(memory, "DRx")
	if len(filtered.Inventory.Manuals) == 0 || len(filtered.Pages) == 0 || len(filtered.Candidates) != 1 {
		t.Fatalf("filtered evidence index = %#v, want matching manuals, pages, and candidate", filtered)
	}
	hasDRx := false
	hasSourceManual := false
	for _, manual := range filtered.Inventory.Manuals {
		if strings.Contains(strings.ToLower(manual.Product), "drx") || strings.Contains(strings.ToLower(manual.URL), "drx") {
			hasDRx = true
		}
		if strings.EqualFold(manual.Product, "1x") {
			hasSourceManual = true
		}
	}
	if !hasDRx || !hasSourceManual {
		t.Fatalf("filtered manuals = %#v, want DRx and linked 1x manuals", filtered.Inventory.Manuals)
	}
}

func TestManualCrawlerPreservesBlockedIndexAsBlindSpot(t *testing.T) {
	memory := newManualMemory()
	crawler := &manualCrawler{
		memory: memory,
		fetch: func(context.Context, string) (int, string, []byte, error) {
			return 0, "", nil, context.DeadlineExceeded
		},
	}
	inventory, err := crawler.scrapeIndex(manualsIndexURL)
	if err == nil {
		t.Fatal("scrapeIndex succeeded for a blocked index")
	}
	if inventory.Manuals == nil {
		t.Fatal("blocked inventory serialized a null manuals list")
	}
	if len(inventory.Issues) != 1 || inventory.Issues[0].Stage != "scrape_index" {
		t.Fatalf("blocked inventory issues = %#v, want one scrape_index issue", inventory.Issues)
	}
}

func TestManualCrawlerExpandsVersionsAndReadsCompatibilitySections(t *testing.T) {
	memory := newManualMemory()
	latestURL := "https://manuals.embention.com/1x/en/latest"
	version16URL := "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.6/"
	version15URL := "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.5/"
	section16URL := "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.6/compatible%20devices/index.md"
	section15URL := "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.5/compatible%20devices/index.md"
	indexHTML := `<html><body><nav><span role="button" href="#autopilots">Autopilots</span><div id="autopilots"><span role="button" href="#family-1x">1x</span><div id="family-1x"><a href="/1x/en/latest">1x Hardware Manual</a></div></div><span role="button" href="#apps">Apps</span><div id="apps"><a href="/1x-pdi-builder/en/latest">1x PDI Builder</a></div><span role="button" href="#discontinued">Discontinued</span><div id="discontinued"><a href="/legacy/en/latest">Legacy Hardware Manual</a></div></nav></body></html>`
	versionHTML := `<html><body><a href="/1x/en/4.12%E2%A7%B81.6/">4.12/1.6</a><a href="/1x/en/4.12%E2%A7%B81.5/">4.12/1.5</a></body></html>`
	manualHTML := `<html><head><title>1x Hardware Manual</title></head><body><h1>1x Hardware Manual</h1><a href="compatible devices/index.md">Compatible Devices</a></body></html>`
	sectionHTML := `<html><body><h1>Compatible Devices</h1><p>1x is compatible with DRx over CAN bus.</p></body></html>`
	crawler := &manualCrawler{memory: memory, fetch: func(_ context.Context, pageURL string) (int, string, []byte, error) {
		switch pageURL {
		case manualsIndexURL:
			return http.StatusOK, "text/html", []byte(indexHTML), nil
		case latestURL:
			return http.StatusOK, "text/html", []byte(versionHTML), nil
		case version16URL, version15URL:
			return http.StatusOK, "text/html", []byte(manualHTML), nil
		case section16URL, section15URL:
			return http.StatusOK, "text/html", []byte(sectionHTML), nil
		case "https://manuals.embention.com/legacy/en/latest":
			return http.StatusOK, "text/html", []byte(`<html><head><title>Legacy Hardware Manual</title></head><body><h1>Legacy Hardware Manual</h1></body></html>`), nil
		default:
			return http.StatusNotFound, "text/html", nil, nil
		}
	}}

	inventory, err := crawler.scrapeIndex(manualsIndexURL)
	if err != nil {
		t.Fatalf("scrape versioned index: %v", err)
	}
	if len(inventory.Manuals) != 4 {
		t.Fatalf("manual count = %d, want two 1x versions, one app, and one discontinued manual", len(inventory.Manuals))
	}
	if inventory.Manuals[0].Category != "Autopilots" || inventory.Manuals[0].Family != "1x" || inventory.Manuals[0].ManualType != "hardware" {
		t.Fatalf("hardware context = %#v, want Autopilots/1x/hardware", inventory.Manuals[0])
	}
	if inventory.Manuals[1].ProductVersion != "4.12" || inventory.Manuals[1].ManualVersion != "1.5" {
		t.Fatalf("second version = %#v, want product 4.12/manual 1.5", inventory.Manuals[1])
	}
	if inventory.Manuals[2].Category != "Apps" || inventory.Manuals[2].ManualType != "application" {
		t.Fatalf("app context = %#v, want Apps/application", inventory.Manuals[2])
	}
	if inventory.Manuals[3].Category != "Discontinued" || inventory.Manuals[3].ManualType != "hardware" {
		t.Fatalf("discontinued context = %#v, want Discontinued/hardware", inventory.Manuals[3])
	}

	pages, err := crawler.readAssignedManuals(0, 1)
	if err != nil {
		t.Fatalf("read versioned manuals: %v", err)
	}
	if len(pages) != 4 {
		t.Fatalf("page count = %d, want four assigned manuals", len(pages))
	}
	for _, page := range pages[:2] {
		if page.ProductVersion != "4.12" || page.ManualVersion == "" {
			t.Fatalf("page version = %#v, want product 4.12 and manual version", page)
		}
		hasSectionEvidence := false
		for _, evidence := range page.Evidence {
			if strings.Contains(evidence.SourceURL, "compatible%20devices") {
				hasSectionEvidence = true
				break
			}
		}
		if !hasSectionEvidence {
			t.Fatalf("page evidence = %#v, want direct compatibility section URL", page.Evidence)
		}
	}
}

func TestCompatibilityMatrixDowngradesUnsupportedClaims(t *testing.T) {
	inventory := manualInventory{Manuals: []manualLink{
		{Product: "1x", ManualType: "hardware", URL: "https://manuals.embention.com/1x/en/latest"},
		{Product: "DRx", ManualType: "user", URL: "https://manuals.embention.com/drx/en/latest"},
	}}
	matrix, blindSpots := buildCompatibilityMatrix(inventory, nil, compatibilityDraft{Relationships: []compatibilityClaim{{
		FromProduct: "1x", ToProduct: "DRx", Status: "compatible", Relationship: "model assertion without evidence",
	}}})
	if len(matrix) != 1 || matrix[0].Status != "Not specified" {
		t.Fatalf("matrix = %#v, want unsupported claim downgraded to Not specified", matrix)
	}
	if len(blindSpots) != 0 {
		t.Fatalf("blind spots = %#v, want no blocking issue for a known pair", blindSpots)
	}
}

func TestCompatibilityMatrixKeepsVersionedSectionEvidence(t *testing.T) {
	inventory := manualInventory{Manuals: []manualLink{
		{
			Product:        "1x",
			ManualType:     "hardware",
			URL:            "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.6/",
			LatestURL:      "https://manuals.embention.com/1x/en/latest",
			ProductVersion: "4.12",
			ManualVersion:  "1.6",
		},
		{Product: "DRx", ManualType: "user", URL: "https://manuals.embention.com/drx/en/3.2%E2%A7%B81.1/", ProductVersion: "3.2", ManualVersion: "1.1"},
	}}
	draft := compatibilityDraft{Relationships: []compatibilityClaim{{
		FromProduct: "1x", ToProduct: "DRx", Status: "compatible", Relationship: "compatible device", Connection: "CAN bus",
		Evidence: []evidenceQuote{{SourceURL: "https://manuals.embention.com/1x/en/4.12%E2%A7%B81.6/compatible%20devices/index.md", Section: "Compatible Devices", Quote: "1x is compatible with DRx over CAN bus."}},
	}}}
	matrix, blindSpots := buildCompatibilityMatrix(inventory, nil, draft)
	if len(blindSpots) != 0 {
		t.Fatalf("blind spots = %#v, want no blind spots for versioned section evidence", blindSpots)
	}
	if len(matrix) != 1 || matrix[0].Status != "Compatible" {
		t.Fatalf("matrix = %#v, want one compatible versioned pair", matrix)
	}
	if matrix[0].FromProductVersion != "4.12" || matrix[0].FromManualVersion != "1.6" {
		t.Fatalf("claim versions = %#v, want 4.12/1.6", matrix[0])
	}
	if matrix[0].FromManualType != "hardware" {
		t.Fatalf("claim source manual type = %q, want hardware", matrix[0].FromManualType)
	}
	if matrix[0].ToManualType != "user" {
		t.Fatalf("claim target manual type = %q, want user", matrix[0].ToManualType)
	}
	if !strings.Contains(formatCompatibilityMatrix(matrix), "1x is compatible with DRx over CAN bus.") {
		t.Fatal("formatted matrix omitted the verbatim compatibility quote")
	}
}
