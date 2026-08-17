package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/embention/agent-squad-go/pkg/squads"
	"golang.org/x/net/html"
)

const (
	manualsIndexURL          = "https://manuals.embention.com/"
	manualPageByteLimit      = 16 << 20
	manualPromptTextLimit    = 5000
	manualVersionSeparator   = "⧸"
	manualVersionPatternText = `\d+(?:\.\d+)*⧸\d+(?:\.\d+)*`
)

var manualVersionPattern = regexp.MustCompile(manualVersionPatternText)

var manualAuditKeywords = []string{
	"compatible with",
	"compatible devices",
	"connects to",
	"interfaces",
	"requires",
	"pinout",
	"integration",
	"supported software",
	"software installation",
}

type manualLink struct {
	Category       string `json:"category"`
	Family         string `json:"family,omitempty"`
	Product        string `json:"product"`
	ManualType     string `json:"manual_type"`
	Title          string `json:"title"`
	URL            string `json:"url"`
	LatestURL      string `json:"latest_url,omitempty"`
	ProductVersion string `json:"product_version,omitempty"`
	ManualVersion  string `json:"manual_version,omitempty"`
	IsLatest       bool   `json:"is_latest"`
}

type manualSection struct {
	Title string `json:"title"`
	URL   string `json:"url"`
}

type manualIssue struct {
	Stage string `json:"stage"`
	URL   string `json:"url,omitempty"`
	Error string `json:"error"`
}

type manualInventory struct {
	SourceURL   string        `json:"source_url"`
	RetrievedAt time.Time     `json:"retrieved_at"`
	Manuals     []manualLink  `json:"manuals"`
	Issues      []manualIssue `json:"issues,omitempty"`
}

type evidenceQuote struct {
	SourceURL string `json:"source_url"`
	Section   string `json:"section,omitempty"`
	Quote     string `json:"quote"`
}

type compatibilityClaim struct {
	FromProduct        string          `json:"from_product"`
	FromManualType     string          `json:"from_manual_type,omitempty"`
	ToProduct          string          `json:"to_product"`
	ToManualType       string          `json:"to_manual_type,omitempty"`
	FromProductVersion string          `json:"from_product_version,omitempty"`
	FromManualVersion  string          `json:"from_manual_version,omitempty"`
	ToProductVersion   string          `json:"to_product_version,omitempty"`
	ToManualVersion    string          `json:"to_manual_version,omitempty"`
	Status             string          `json:"status"`
	Relationship       string          `json:"relationship,omitempty"`
	Connection         string          `json:"connection,omitempty"`
	Evidence           []evidenceQuote `json:"evidence,omitempty"`
	Notes              string          `json:"notes,omitempty"`
}

type dependencyClaim struct {
	Product    string          `json:"product"`
	Dependency string          `json:"dependency"`
	Kind       string          `json:"kind,omitempty"`
	Evidence   []evidenceQuote `json:"evidence,omitempty"`
}

type manualEvidence struct {
	Product             string               `json:"product"`
	ManualURL           string               `json:"manual_url"`
	ManualType          string               `json:"manual_type"`
	ProductVersion      string               `json:"product_version,omitempty"`
	ManualVersion       string               `json:"manual_version,omitempty"`
	Status              string               `json:"status"`
	KeywordsFound       []string             `json:"keywords_found,omitempty"`
	CompatibilityClaims []compatibilityClaim `json:"compatibility_claims,omitempty"`
	Dependencies        []dependencyClaim    `json:"dependencies,omitempty"`
	Evidence            []evidenceQuote      `json:"evidence,omitempty"`
	Warnings            []string             `json:"warnings,omitempty"`
	Notes               string               `json:"notes,omitempty"`
}

type compatibilityDraft struct {
	Relationships []compatibilityClaim `json:"relationships"`
	Unresolved    []string             `json:"unresolved,omitempty"`
}

type compatibilityCandidate struct {
	ID                 string          `json:"id"`
	FromProduct        string          `json:"from_product"`
	FromManualType     string          `json:"from_manual_type"`
	FromManualURL      string          `json:"from_manual_url"`
	FromProductVersion string          `json:"from_product_version,omitempty"`
	FromManualVersion  string          `json:"from_manual_version,omitempty"`
	ToProduct          string          `json:"to_product"`
	ToManualType       string          `json:"to_manual_type"`
	ToManualURL        string          `json:"to_manual_url"`
	ToProductVersion   string          `json:"to_product_version,omitempty"`
	ToManualVersion    string          `json:"to_manual_version,omitempty"`
	Evidence           []evidenceQuote `json:"evidence,omitempty"`
	FromExcerpt        string          `json:"from_excerpt,omitempty"`
	ToExcerpt          string          `json:"to_excerpt,omitempty"`
	Reason             string          `json:"reason,omitempty"`
}

type manualPage struct {
	Link        manualLink
	Sections    []manualSection
	StatusCode  int
	ContentType string
	Text        string
	Keywords    []string
	Windows     []string
	Evidence    []evidenceQuote
	Warnings    []string
	FetchedAt   time.Time
	Error       string
}

type manualPageExtract struct {
	Product         string          `json:"product"`
	ManualURL       string          `json:"manual_url"`
	ManualType      string          `json:"manual_type"`
	ProductVersion  string          `json:"product_version,omitempty"`
	ManualVersion   string          `json:"manual_version,omitempty"`
	Title           string          `json:"title"`
	Status          string          `json:"status"`
	LatestURL       string          `json:"latest_url,omitempty"`
	IsLatest        bool            `json:"is_latest"`
	Sections        []manualSection `json:"sections,omitempty"`
	KeywordsFound   []string        `json:"keywords_found,omitempty"`
	EvidenceWindows []string        `json:"evidence_windows,omitempty"`
	Evidence        []evidenceQuote `json:"evidence,omitempty"`
	Text            string          `json:"text,omitempty"`
	Warnings        []string        `json:"warnings,omitempty"`
	Error           string          `json:"error,omitempty"`
}

type manualMemory struct {
	mu                sync.RWMutex
	inventory         manualInventory
	hasIndex          bool
	analysisInventory manualInventory
	hasAnalysis       bool
	candidates        []compatibilityCandidate
	pages             map[string]manualPage
}

func newManualMemory() *manualMemory {
	return &manualMemory{pages: make(map[string]manualPage)}
}

func newManualInventory(sourceURL string) manualInventory {
	return manualInventory{
		SourceURL:   sourceURL,
		RetrievedAt: time.Now().UTC(),
		Manuals:     make([]manualLink, 0),
		Issues:      make([]manualIssue, 0),
	}
}

func (memory *manualMemory) setInventory(inventory manualInventory) {
	memory.mu.Lock()
	memory.inventory = inventory
	memory.hasIndex = true
	memory.mu.Unlock()
}

func (memory *manualMemory) getInventory() (manualInventory, bool) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	if !memory.hasIndex {
		return manualInventory{}, false
	}
	return cloneInventory(memory.inventory), true
}

func (memory *manualMemory) setAnalysisInventory(inventory manualInventory) {
	memory.mu.Lock()
	memory.analysisInventory = inventory
	memory.hasAnalysis = true
	memory.mu.Unlock()
}

func (memory *manualMemory) getAnalysisInventory() (manualInventory, bool) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	if !memory.hasAnalysis {
		return manualInventory{}, false
	}
	return cloneInventory(memory.analysisInventory), true
}

func (memory *manualMemory) setCandidates(candidates []compatibilityCandidate) {
	memory.mu.Lock()
	memory.candidates = append([]compatibilityCandidate(nil), candidates...)
	memory.mu.Unlock()
}

func (memory *manualMemory) getCandidates() []compatibilityCandidate {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	return append([]compatibilityCandidate(nil), memory.candidates...)
}

func (memory *manualMemory) setPage(page manualPage) {
	memory.mu.Lock()
	memory.pages[page.Link.URL] = page
	memory.mu.Unlock()
}

func (memory *manualMemory) getPage(reference string) (manualPage, bool) {
	memory.mu.RLock()
	defer memory.mu.RUnlock()
	reference = normaliseURL(reference)
	for pageURL, page := range memory.pages {
		if normaliseURL(pageURL) == reference {
			return page, true
		}
	}
	candidates := make([]manualPage, 0)
	for _, page := range memory.pages {
		if strings.EqualFold(page.Link.Product, reference) || normaliseURL(page.Link.LatestURL) == reference {
			candidates = append(candidates, page)
		}
	}
	if len(candidates) == 0 {
		return manualPage{}, false
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].Link.IsLatest != candidates[right].Link.IsLatest {
			return candidates[left].Link.IsLatest
		}
		return candidates[left].Link.URL < candidates[right].Link.URL
	})
	return candidates[0], true
}

type manualPageFetcher func(context.Context, string) (int, string, []byte, error)

type manualCrawler struct {
	memory *manualMemory
	fetch  manualPageFetcher
}

func newManualCrawler(mock bool, memory *manualMemory) *manualCrawler {
	crawler := &manualCrawler{memory: memory}
	if mock {
		crawler.fetch = mockManualPage
		return crawler
	}
	client := &http.Client{Timeout: 45 * time.Second}
	crawler.fetch = func(ctx context.Context, pageURL string) (int, string, []byte, error) {
		return fetchManualPage(ctx, client, pageURL)
	}
	return crawler
}

func fetchManualPage(ctx context.Context, client *http.Client, pageURL string) (int, string, []byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return 0, "", nil, fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Accept", "text/html,application/xhtml+xml,application/pdf;q=0.9,*/*;q=0.8")
	request.Header.Set("User-Agent", "GoAgentSquad manual compatibility experiment")
	response, err := client.Do(request)
	if err != nil {
		return 0, "", nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, manualPageByteLimit+1))
	if err != nil {
		return response.StatusCode, response.Header.Get("Content-Type"), nil, err
	}
	if len(body) > manualPageByteLimit {
		return response.StatusCode, response.Header.Get("Content-Type"), nil, fmt.Errorf("response exceeds %d bytes", manualPageByteLimit)
	}
	return response.StatusCode, response.Header.Get("Content-Type"), body, nil
}

func (crawler *manualCrawler) scrapeIndex(pageURL string) (manualInventory, error) {
	if inventory, ok := crawler.memory.getInventory(); ok && inventory.SourceURL == pageURL && len(inventory.Manuals) > 0 {
		return inventory, nil
	}
	statusCode, _, body, err := crawler.fetch(context.Background(), pageURL)
	if err != nil {
		inventory := newManualInventory(pageURL)
		inventory.Issues = append(inventory.Issues, manualIssue{Stage: "scrape_index", URL: pageURL, Error: err.Error()})
		crawler.memory.setInventory(inventory)
		return inventory, fmt.Errorf("scrape manuals index: %w", err)
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		err = fmt.Errorf("manuals index returned HTTP status %d", statusCode)
		inventory := newManualInventory(pageURL)
		inventory.Issues = append(inventory.Issues, manualIssue{Stage: "scrape_index", URL: pageURL, Error: err.Error()})
		crawler.memory.setInventory(inventory)
		return inventory, err
	}
	manuals, err := parseManualLinks(pageURL, body)
	if err != nil {
		inventory := newManualInventory(pageURL)
		inventory.Issues = append(inventory.Issues, manualIssue{Stage: "parse_index", URL: pageURL, Error: err.Error()})
		crawler.memory.setInventory(inventory)
		return inventory, err
	}
	inventory := newManualInventory(pageURL)
	inventory.Manuals, inventory.Issues = crawler.expandManualVersions(manuals)
	crawler.memory.setInventory(inventory)
	if len(inventory.Manuals) == 0 {
		issue := manualIssue{Stage: "parse_index", URL: pageURL, Error: "no direct manual links found"}
		inventory.Issues = append(inventory.Issues, issue)
		crawler.memory.setInventory(inventory)
		return inventory, errors.New(issue.Error)
	}
	return inventory, nil
}

func (crawler *manualCrawler) expandManualVersions(manuals []manualLink) ([]manualLink, []manualIssue) {
	expanded := make([]manualLink, 0, len(manuals))
	issues := make([]manualIssue, 0)
	for _, manual := range manuals {
		manual.LatestURL = manual.URL
		manual.IsLatest = true
		statusCode, _, body, err := crawler.fetch(context.Background(), manual.URL)
		if err != nil {
			expanded = append(expanded, manual)
			issues = append(issues, manualIssue{Stage: "expand_versions", URL: manual.URL, Error: err.Error()})
			continue
		}
		if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
			expanded = append(expanded, manual)
			issues = append(issues, manualIssue{Stage: "expand_versions", URL: manual.URL, Error: fmt.Sprintf("manual returned HTTP status %d while discovering versions", statusCode)})
			continue
		}
		versions, parseErr := parseManualVersionLinks(manual, body)
		if parseErr != nil {
			expanded = append(expanded, manual)
			issues = append(issues, manualIssue{Stage: "expand_versions", URL: manual.URL, Error: parseErr.Error()})
			continue
		}
		if len(versions) == 0 {
			if productVersion, manualVersion, ok := parseManualVersionLabel(string(body)); ok {
				versioned := manual
				versioned.URL = replaceManualVersion(manual.URL, productVersion+manualVersionSeparator+manualVersion)
				versioned.ProductVersion = productVersion
				versioned.ManualVersion = manualVersion
				versioned.IsLatest = true
				expanded = append(expanded, versioned)
				continue
			}
			expanded = append(expanded, manual)
			continue
		}
		latestProductVersion, latestManualVersion, hasLatestVersion := matchManualVersion(versions, string(body))
		for versionIndex := range versions {
			versions[versionIndex].IsLatest = hasLatestVersion && versions[versionIndex].ProductVersion == latestProductVersion && versions[versionIndex].ManualVersion == latestManualVersion
		}
		expanded = append(expanded, versions...)
	}
	return expanded, uniqueIssues(issues)
}

func parseManualVersionLinks(base manualLink, body []byte) ([]manualLink, error) {
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse manual version selector: %w", err)
	}
	baseURL, err := url.Parse(base.URL)
	if err != nil {
		return nil, fmt.Errorf("parse manual URL for version selector: %w", err)
	}
	versions := make([]manualLink, 0)
	seen := make(map[string]struct{})
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "a" {
			resolved, resolveErr := resolveManualURL(baseURL, attribute(node, "href"))
			if resolveErr == nil && isManualVersionRootURL(resolved) && sameManualRoot(base.URL, resolved) {
				if _, exists := seen[resolved]; !exists {
					productVersion, manualVersion, _ := parseManualVersionURL(resolved)
					version := base
					version.URL = resolved
					version.LatestURL = base.URL
					version.ProductVersion = productVersion
					version.ManualVersion = manualVersion
					version.IsLatest = false
					versions = append(versions, version)
					seen[resolved] = struct{}{}
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return versions, nil
}

func parseManualVersionURL(pageURL string) (string, string, bool) {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return "", "", false
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 3 {
		return "", "", false
	}
	versionSegment, err := url.PathUnescape(parts[2])
	if err != nil {
		return "", "", false
	}
	productVersion, manualVersion, ok := strings.Cut(versionSegment, manualVersionSeparator)
	if !ok || strings.TrimSpace(productVersion) == "" || strings.TrimSpace(manualVersion) == "" {
		return "", "", false
	}
	return productVersion, manualVersion, true
}

func parseManualVersionLabel(text string) (string, string, bool) {
	match := manualVersionPattern.FindString(text)
	if match == "" {
		return "", "", false
	}
	productVersion, manualVersion, ok := strings.Cut(match, manualVersionSeparator)
	return productVersion, manualVersion, ok
}

func matchManualVersion(versions []manualLink, text string) (string, string, bool) {
	productVersion, manualVersion, ok := parseManualVersionLabel(text)
	if !ok {
		return "", "", false
	}
	for _, version := range versions {
		if version.ProductVersion == productVersion && version.ManualVersion == manualVersion {
			return productVersion, manualVersion, true
		}
	}
	parts := strings.Split(productVersion, ".")
	for start := 1; start < len(parts); start++ {
		candidateProductVersion := strings.Join(parts[start:], ".")
		for _, version := range versions {
			if version.ProductVersion == candidateProductVersion && version.ManualVersion == manualVersion {
				return candidateProductVersion, manualVersion, true
			}
		}
	}
	return "", "", false
}

func isManualVersionRootURL(pageURL string) bool {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return false
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) != 3 {
		return false
	}
	_, _, ok := parseManualVersionURL(pageURL)
	return ok
}

func sameManualRoot(leftURL, rightURL string) bool {
	left, leftErr := url.Parse(leftURL)
	right, rightErr := url.Parse(rightURL)
	if leftErr != nil || rightErr != nil || left.Host != right.Host || left.Scheme != right.Scheme {
		return false
	}
	leftParts := strings.Split(strings.Trim(left.Path, "/"), "/")
	rightParts := strings.Split(strings.Trim(right.Path, "/"), "/")
	return len(leftParts) >= 2 && len(rightParts) >= 2 && strings.EqualFold(leftParts[0], rightParts[0]) && strings.EqualFold(leftParts[1], rightParts[1])
}

func sameManualVersionRoot(leftURL, rightURL string) bool {
	left, leftErr := url.Parse(leftURL)
	right, rightErr := url.Parse(rightURL)
	if leftErr != nil || rightErr != nil || left.Host != right.Host || left.Scheme != right.Scheme {
		return false
	}
	leftParts := strings.Split(strings.Trim(left.Path, "/"), "/")
	rightParts := strings.Split(strings.Trim(right.Path, "/"), "/")
	return len(leftParts) >= 3 && len(rightParts) >= 3 && strings.EqualFold(leftParts[0], rightParts[0]) && strings.EqualFold(leftParts[1], rightParts[1]) && strings.EqualFold(leftParts[2], rightParts[2])
}

func replaceManualVersion(pageURL, versionSegment string) string {
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return pageURL
	}
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) < 3 {
		return pageURL
	}
	parts[2] = versionSegment
	parsed.Path = "/" + strings.Join(parts, "/")
	parsed.RawPath = ""
	return parsed.String()
}

func (crawler *manualCrawler) readAssignedManuals(shardIndex, shardCount int) ([]manualPageExtract, error) {
	inventory, ok := crawler.memory.getAnalysisInventory()
	if !ok {
		inventory, ok = crawler.memory.getInventory()
	}
	if !ok || len(inventory.Manuals) == 0 {
		return nil, fmt.Errorf("shared manual inventory is empty")
	}
	if shardCount <= 0 {
		shardCount = 1
	}
	if shardIndex < 0 || shardIndex >= shardCount {
		return nil, fmt.Errorf("manual reader shard %d is outside 0..%d", shardIndex, shardCount-1)
	}
	results := make([]manualPageExtract, 0)
	for index, link := range inventory.Manuals {
		if index%shardCount != shardIndex {
			continue
		}
		results = append(results, crawler.readManual(link))
	}
	return results, nil
}

func (crawler *manualCrawler) readManual(link manualLink) manualPageExtract {
	if page, ok := crawler.memory.getPage(link.URL); ok {
		return pageExtract(page)
	}
	statusCode, contentType, body, err := crawler.fetch(context.Background(), link.URL)
	if err != nil {
		page := manualPage{Link: link, StatusCode: statusCode, ContentType: contentType, FetchedAt: time.Now(), Error: err.Error()}
		crawler.memory.setPage(page)
		return pageExtract(page)
	}
	if statusCode < http.StatusOK || statusCode >= http.StatusMultipleChoices {
		err = fmt.Errorf("manual returned HTTP status %d", statusCode)
		page := manualPage{Link: link, StatusCode: statusCode, ContentType: contentType, FetchedAt: time.Now(), Error: err.Error()}
		crawler.memory.setPage(page)
		return pageExtract(page)
	}
	if strings.Contains(strings.ToLower(contentType), "pdf") {
		err = fmt.Errorf("PDF content requires a document reader: %s", contentType)
		page := manualPage{Link: link, StatusCode: statusCode, ContentType: contentType, FetchedAt: time.Now(), Error: err.Error()}
		crawler.memory.setPage(page)
		return pageExtract(page)
	}
	text, title, err := extractHTMLDocument(body)
	if err != nil {
		page := manualPage{Link: link, StatusCode: statusCode, ContentType: contentType, FetchedAt: time.Now(), Error: err.Error()}
		crawler.memory.setPage(page)
		return pageExtract(page)
	}
	page := manualPage{
		Link:        link,
		StatusCode:  statusCode,
		ContentType: contentType,
		Text:        text,
		FetchedAt:   time.Now(),
	}
	if page.Link.Title == "" {
		page.Link.Title = title
	}
	if page.Link.ProductVersion == "" || page.Link.ManualVersion == "" {
		if productVersion, manualVersion, ok := parseManualVersionLabel(text); ok {
			page.Link.ProductVersion = productVersion
			page.Link.ManualVersion = manualVersion
		}
	}
	page.Keywords, page.Windows = findManualEvidence(text)
	page.Evidence = evidenceQuotes(link.URL, "Manual", page.Windows)
	sections, sectionErr := parseManualSectionLinks(link.URL, body)
	if sectionErr != nil {
		page.Warnings = append(page.Warnings, sectionErr.Error())
	}
	page.Sections = sections
	for _, section := range sections {
		sectionStatus, sectionContentType, sectionBody, fetchErr := crawler.fetch(context.Background(), section.URL)
		if fetchErr != nil {
			page.Warnings = append(page.Warnings, fmt.Sprintf("section %q: %s", section.Title, fetchErr))
			continue
		}
		if sectionStatus < http.StatusOK || sectionStatus >= http.StatusMultipleChoices {
			page.Warnings = append(page.Warnings, fmt.Sprintf("section %q returned HTTP status %d", section.Title, sectionStatus))
			continue
		}
		if strings.Contains(strings.ToLower(sectionContentType), "pdf") {
			page.Warnings = append(page.Warnings, fmt.Sprintf("section %q is PDF content", section.Title))
			continue
		}
		sectionText, sectionTitle, extractErr := extractHTMLDocument(sectionBody)
		if extractErr != nil {
			page.Warnings = append(page.Warnings, fmt.Sprintf("section %q: %s", section.Title, extractErr))
			continue
		}
		if sectionTitle == "" {
			sectionTitle = section.Title
		}
		sectionKeywords, sectionWindows := findManualEvidence(sectionText)
		page.Keywords = appendUniqueStrings(page.Keywords, sectionKeywords...)
		if len(sectionWindows) == 0 && strings.TrimSpace(sectionText) != "" {
			sectionWindows = []string{truncateManualText(sectionText, 1800)}
		}
		for _, window := range sectionWindows {
			page.Windows = append(page.Windows, section.Title+": "+window)
			page.Evidence = append(page.Evidence, evidenceQuote{SourceURL: section.URL, Section: sectionTitle, Quote: strings.TrimSpace(window)})
		}
		if strings.TrimSpace(sectionText) != "" {
			page.Text += "\n\n## " + sectionTitle + "\n" + sectionText
		}
	}
	page.Keywords = uniqueStrings(page.Keywords)
	page.Evidence = uniqueEvidenceQuotes(page.Evidence)
	crawler.memory.setPage(page)
	return pageExtract(page)
}

func pageExtract(page manualPage) manualPageExtract {
	status := "analyzed"
	if page.Error != "" {
		status = "blocked"
	} else if len(page.Warnings) > 0 {
		status = "partial"
	}
	return manualPageExtract{
		Product:         page.Link.Product,
		ManualURL:       page.Link.URL,
		ManualType:      page.Link.ManualType,
		ProductVersion:  page.Link.ProductVersion,
		ManualVersion:   page.Link.ManualVersion,
		Title:           page.Link.Title,
		Status:          status,
		LatestURL:       page.Link.LatestURL,
		IsLatest:        page.Link.IsLatest,
		Sections:        append([]manualSection(nil), page.Sections...),
		KeywordsFound:   append([]string(nil), page.Keywords...),
		EvidenceWindows: append([]string(nil), page.Windows...),
		Evidence:        append([]evidenceQuote(nil), page.Evidence...),
		Text:            truncateManualText(page.Text, manualPromptTextLimit),
		Warnings:        append([]string(nil), page.Warnings...),
		Error:           page.Error,
	}
}

func evidenceQuotes(sourceURL, section string, windows []string) []evidenceQuote {
	quotes := make([]evidenceQuote, 0, len(windows))
	for _, window := range windows {
		if strings.TrimSpace(window) == "" {
			continue
		}
		quotes = append(quotes, evidenceQuote{SourceURL: sourceURL, Section: section, Quote: strings.TrimSpace(window)})
	}
	return quotes
}

func appendUniqueStrings(values []string, additions ...string) []string {
	seen := make(map[string]struct{}, len(values)+len(additions))
	for _, value := range values {
		seen[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
	}
	for _, value := range additions {
		key := strings.ToLower(strings.TrimSpace(value))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		values = append(values, value)
	}
	return values
}

func uniqueStrings(values []string) []string {
	return appendUniqueStrings(nil, values...)
}

func uniqueEvidenceQuotes(values []evidenceQuote) []evidenceQuote {
	result := make([]evidenceQuote, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value.SourceURL = strings.TrimSpace(value.SourceURL)
		value.Section = strings.TrimSpace(value.Section)
		value.Quote = strings.TrimSpace(value.Quote)
		if value.SourceURL == "" || value.Quote == "" {
			continue
		}
		key := value.SourceURL + "|" + value.Section + "|" + value.Quote
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, value)
	}
	return result
}

func parseManualLinks(baseURL string, body []byte) ([]manualLink, error) {
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse HTML: %w", err)
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parse base URL: %w", err)
	}
	links := make([]manualLink, 0)
	seen := make(map[string]struct{})
	headingByLevel := make(map[int]string)
	menuLabels := navigationMenuLabels(root)
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode {
			if level := headingLevel(node.Data); level > 0 {
				headingByLevel[level] = cleanHTMLText(node)
				for deeper := level + 1; deeper <= 6; deeper++ {
					delete(headingByLevel, deeper)
				}
			}
			if node.Data == "a" {
				href := attribute(node, "href")
				resolved, resolveErr := resolveManualURL(base, href)
				if resolveErr == nil && isManualURL(resolved, cleanHTMLText(node)) {
					if _, exists := seen[resolved]; !exists {
						seen[resolved] = struct{}{}
						title := cleanHTMLText(node)
						headings := headingPath(headingByLevel)
						category, family := manualNavigationContext(node, menuLabels)
						if category == "" {
							category = "Embention manuals"
						}
						if len(headings) > 0 {
							if category == "Embention manuals" {
								category = headings[0]
							}
						}
						if len(headings) > 1 {
							if family == "" {
								family = headings[len(headings)-1]
							}
						}
						links = append(links, manualLink{
							Category:   category,
							Family:     family,
							Product:    productFromTitle(title, resolved),
							ManualType: manualTypeFromContext(title, resolved, category),
							Title:      title,
							URL:        resolved,
						})
					}
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return links, nil
}

func navigationMenuLabels(root *html.Node) map[string]string {
	labels := make(map[string]string)
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "span" && attribute(node, "role") == "button" {
			href := strings.TrimSpace(attribute(node, "href"))
			if strings.HasPrefix(href, "#") {
				label := strings.TrimSpace(cleanHTMLText(node))
				if label != "" {
					labels[strings.TrimPrefix(href, "#")] = label
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return labels
}

func manualNavigationContext(node *html.Node, labels map[string]string) (string, string) {
	path := make([]string, 0, 3)
	for current := node.Parent; current != nil; current = current.Parent {
		if current.Type != html.ElementNode || current.Data != "div" {
			continue
		}
		if label, ok := labels[attribute(current, "id")]; ok {
			path = append(path, label)
		}
	}
	if len(path) == 0 {
		return "", ""
	}
	category := path[len(path)-1]
	family := ""
	if len(path) > 1 {
		family = path[0]
	}
	return category, family
}

func parseManualSectionLinks(pageURL string, body []byte) ([]manualSection, error) {
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("parse manual section links: %w", err)
	}
	base, err := url.Parse(pageURL)
	if err != nil {
		return nil, fmt.Errorf("parse manual section base URL: %w", err)
	}
	sections := make([]manualSection, 0)
	seen := make(map[string]struct{})
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "a" {
			title := cleanHTMLText(node)
			if isEvidenceSectionTitle(title) {
				resolved, resolveErr := resolveManualURL(base, attribute(node, "href"))
				if resolveErr == nil && sameManualVersionRoot(pageURL, resolved) {
					if _, exists := seen[resolved]; !exists {
						sections = append(sections, manualSection{Title: strings.TrimSpace(title), URL: resolved})
						seen[resolved] = struct{}{}
					}
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return sections, nil
}

func isEvidenceSectionTitle(title string) bool {
	normalised := strings.ToLower(strings.Join(strings.Fields(title), " "))
	for _, candidate := range []string{
		"compatible devices",
		"integration examples",
		"software installation",
		"software applications",
		"applications",
		"supported software",
		"technical",
		"hardware installation",
		"quick start",
		"configuration",
	} {
		if normalised == candidate || strings.Contains(normalised, candidate) {
			return true
		}
	}
	return false
}

func resolveManualURL(base *url.URL, href string) (string, error) {
	if strings.TrimSpace(href) == "" {
		return "", fmt.Errorf("empty href")
	}
	parsed, err := url.Parse(strings.TrimSpace(href))
	if err != nil {
		return "", err
	}
	resolved := base.ResolveReference(parsed)
	if resolved.Host != base.Host || resolved.Scheme != base.Scheme {
		return "", fmt.Errorf("external manual URL")
	}
	resolved.Fragment = ""
	resolved.RawQuery = ""
	return resolved.String(), nil
}

func isManualURL(pageURL, title string) bool {
	parsed, err := url.Parse(pageURL)
	if err != nil || strings.Contains(strings.ToLower(parsed.Path), "/resources/") {
		return false
	}
	path := strings.ToLower(strings.Trim(parsed.Path, "/"))
	title = strings.ToLower(title)
	return strings.Contains(path, "/latest") || strings.Contains(path, "/en/latest") || strings.Contains(title, "manual") || strings.Contains(title, "simulator")
}

func productFromTitle(title, pageURL string) string {
	cleanTitle := strings.TrimSpace(title)
	lowerTitle := strings.ToLower(cleanTitle)
	for _, suffix := range []string{" hardware manual", " software manual", " application manual", " user manual", " manual"} {
		if strings.HasSuffix(lowerTitle, suffix) {
			return strings.TrimSpace(cleanTitle[:len(cleanTitle)-len(suffix)])
		}
	}
	if cleanTitle != "" {
		return cleanTitle
	}
	parsed, _ := url.Parse(pageURL)
	parts := strings.Split(strings.Trim(parsed.Path, "/"), "/")
	if len(parts) > 0 {
		return strings.ReplaceAll(parts[0], "-", " ")
	}
	return "Unknown product"
}

func manualTypeFromTitle(title, pageURL string) string {
	value := strings.ToLower(title + " " + pageURL)
	switch {
	case strings.Contains(value, "software"):
		return "software"
	case strings.Contains(value, "application") || strings.Contains(value, "pdi builder") || strings.Contains(value, "pdi calibration") || strings.Contains(value, "pdi tuning") || strings.Contains(value, "/apps/"):
		return "application"
	case strings.Contains(value, "hardware"):
		return "hardware"
	default:
		return "user"
	}
}

func manualTypeFromContext(title, pageURL, category string) string {
	value := strings.ToLower(title + " " + category)
	switch {
	case strings.Contains(value, "framework"):
		return "framework"
	case strings.Contains(strings.ToLower(category), "apps"):
		return "application"
	default:
		return manualTypeFromTitle(title, pageURL)
	}
}

func headingLevel(tag string) int {
	if len(tag) != 2 || tag[0] != 'h' || tag[1] < '1' || tag[1] > '6' {
		return 0
	}
	return int(tag[1] - '0')
}

func headingPath(headings map[int]string) []string {
	path := make([]string, 0, len(headings))
	for level := 1; level <= 6; level++ {
		if heading := strings.TrimSpace(headings[level]); heading != "" {
			path = append(path, heading)
		}
	}
	return path
}

func attribute(node *html.Node, name string) string {
	for _, attr := range node.Attr {
		if attr.Key == name {
			return attr.Val
		}
	}
	return ""
}

func cleanHTMLText(node *html.Node) string {
	var builder strings.Builder
	var walk func(*html.Node)
	walk = func(current *html.Node) {
		if current.Type == html.TextNode {
			builder.WriteString(current.Data)
			builder.WriteByte(' ')
			return
		}
		if current.Type == html.ElementNode && (current.Data == "script" || current.Data == "style") {
			return
		}
		for child := current.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(node)
	return strings.Join(strings.Fields(builder.String()), " ")
}

func extractHTMLDocument(body []byte) (string, string, error) {
	root, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return "", "", fmt.Errorf("parse manual HTML: %w", err)
	}
	contentRoot := findDocumentContentRoot(root)
	text := cleanHTMLText(contentRoot)
	title := ""
	var findTitle func(*html.Node)
	findTitle = func(node *html.Node) {
		if title != "" {
			return
		}
		if node.Type == html.ElementNode && node.Data == "title" {
			title = cleanHTMLText(node)
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			findTitle(child)
		}
	}
	findTitle(root)
	return text, title, nil
}

func findDocumentContentRoot(root *html.Node) *html.Node {
	var contentRoot *html.Node
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if contentRoot != nil {
			return
		}
		if node.Type == html.ElementNode && (node.Data == "main" || node.Data == "article") {
			contentRoot = node
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	if contentRoot != nil {
		return contentRoot
	}
	return root
}

func findManualEvidence(text string) ([]string, []string) {
	textRunes := []rune(text)
	lowerRunes := []rune(strings.ToLower(text))
	keywords := make([]string, 0)
	windows := make([]string, 0)
	for _, keyword := range manualAuditKeywords {
		keywordRunes := []rune(strings.ToLower(keyword))
		for index := 0; index+len(keywordRunes) <= len(lowerRunes); index++ {
			if string(lowerRunes[index:index+len(keywordRunes)]) != string(keywordRunes) {
				continue
			}
			keywords = append(keywords, keyword)
			start := index - 280
			if start < 0 {
				start = 0
			}
			end := index + len(keywordRunes) + 420
			if end > len(textRunes) {
				end = len(textRunes)
			}
			windows = append(windows, strings.TrimSpace(string(textRunes[start:end])))
			break
		}
	}
	return keywords, windows
}

func truncateManualText(text string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(text)
	if len(runes) <= limit {
		return text
	}
	return string(runes[:limit]) + "..."
}

func cloneInventory(inventory manualInventory) manualInventory {
	clone := inventory
	clone.Manuals = append(make([]manualLink, 0, len(inventory.Manuals)), inventory.Manuals...)
	clone.Issues = append(make([]manualIssue, 0, len(inventory.Issues)), inventory.Issues...)
	return clone
}

func selectAnalysisInventory(inventory manualInventory, scope string, includeHistorical bool) manualInventory {
	selected := newManualInventory(inventory.SourceURL)
	selected.RetrievedAt = inventory.RetrievedAt
	selected.Issues = append(selected.Issues, inventory.Issues...)
	byManual := make(map[string]manualLink)
	for _, manual := range inventory.Manuals {
		if !manualMatchesScope(manual, scope) {
			continue
		}
		if includeHistorical {
			selected.Manuals = append(selected.Manuals, manual)
			continue
		}
		key := normaliseProduct(manual.Product) + "|" + strings.ToLower(strings.TrimSpace(manual.ManualType))
		current, exists := byManual[key]
		if !exists || preferredManualVersion(manual, current) {
			byManual[key] = manual
		}
	}
	if !includeHistorical {
		for _, manual := range byManual {
			selected.Manuals = append(selected.Manuals, manual)
		}
	}
	sort.SliceStable(selected.Manuals, func(left, right int) bool {
		leftKey := normaliseProduct(selected.Manuals[left].Product) + "|" + selected.Manuals[left].ManualType + "|" + selected.Manuals[left].URL
		rightKey := normaliseProduct(selected.Manuals[right].Product) + "|" + selected.Manuals[right].ManualType + "|" + selected.Manuals[right].URL
		return leftKey < rightKey
	})
	return selected
}

func preferredManualVersion(candidate, current manualLink) bool {
	if candidate.IsLatest != current.IsLatest {
		return candidate.IsLatest
	}
	if candidate.IsLatest {
		return false
	}
	if comparison := compareVersion(candidate.ProductVersion, current.ProductVersion); comparison != 0 {
		return comparison > 0
	}
	if comparison := compareVersion(candidate.ManualVersion, current.ManualVersion); comparison != 0 {
		return comparison > 0
	}
	return candidate.URL > current.URL
}

func compareVersion(left, right string) int {
	leftParts := strings.Split(strings.TrimSpace(left), ".")
	rightParts := strings.Split(strings.TrimSpace(right), ".")
	length := len(leftParts)
	if len(rightParts) > length {
		length = len(rightParts)
	}
	for index := 0; index < length; index++ {
		leftValue, _ := strconv.Atoi(versionPart(leftParts, index))
		rightValue, _ := strconv.Atoi(versionPart(rightParts, index))
		if leftValue != rightValue {
			if leftValue > rightValue {
				return 1
			}
			return -1
		}
	}
	return 0
}

func versionPart(parts []string, index int) string {
	if index >= len(parts) {
		return "0"
	}
	return parts[index]
}

func manualMatchesScope(manual manualLink, scope string) bool {
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" || scope == "all" {
		return true
	}
	if scope == "1x" || scope == "1x-ecosystem" {
		for _, product := range []string{"1x", "drx", "hil simulator", "veronte link", "veronte ops", "veronte cloud", "veronte fdr", "veronte toolbox", "veronte updater", "veronte vsa"} {
			if normaliseProduct(manual.Product) == product || strings.HasPrefix(normaliseProduct(manual.Product), product+" ") {
				return true
			}
		}
		return false
	}
	return normaliseProduct(manual.Product) == normaliseProduct(scope)
}

func (crawler *manualCrawler) buildCompatibilityCandidates(inventory manualInventory, evidence []manualEvidence) []compatibilityCandidate {
	manualsByProduct := make(map[string][]manualLink)
	for _, manual := range inventory.Manuals {
		key := normaliseProduct(manual.Product)
		manualsByProduct[key] = append(manualsByProduct[key], manual)
	}
	products := make([]string, 0, len(manualsByProduct))
	for product := range manualsByProduct {
		products = append(products, product)
	}
	sort.Strings(products)
	candidates := make([]compatibilityCandidate, 0)
	seen := make(map[string]struct{})
	appendCandidate := func(candidate compatibilityCandidate) {
		candidate.ID = compatibilityCandidateKey(candidate)
		if candidate.ID == "" {
			return
		}
		if _, exists := seen[candidate.ID]; exists {
			return
		}
		seen[candidate.ID] = struct{}{}
		candidates = append(candidates, candidate)
	}

	for _, source := range inventory.Manuals {
		page, ok := crawler.memory.getPage(source.URL)
		if !ok {
			continue
		}
		pageText := strings.ToLower(page.Text)
		for _, targetProduct := range products {
			if targetProduct == normaliseProduct(source.Product) || !productReferenceMatches(pageText, targetProduct) {
				continue
			}
			for _, target := range manualsByProduct[targetProduct] {
				appendCandidate(candidateFromPages(source, target, page, crawler.memory, "deterministic product reference"))
			}
		}
	}

	for _, fact := range evidence {
		source, ok := findManualByURL(inventory.Manuals, fact.ManualURL)
		if !ok {
			continue
		}
		for _, claim := range fact.CompatibilityClaims {
			for _, target := range manualsByProduct[normaliseProduct(claim.ToProduct)] {
				candidate := candidateFromPages(source, target, manualPage{}, crawler.memory, "reader claim")
				candidate.Evidence = append([]evidenceQuote(nil), claim.Evidence...)
				appendCandidate(candidate)
			}
		}
	}

	sort.SliceStable(candidates, func(left, right int) bool { return candidates[left].ID < candidates[right].ID })
	return candidates
}

func candidateFromPages(source, target manualLink, sourcePage manualPage, memory *manualMemory, reason string) compatibilityCandidate {
	candidate := compatibilityCandidate{
		FromProduct:        source.Product,
		FromManualType:     source.ManualType,
		FromManualURL:      source.URL,
		FromProductVersion: source.ProductVersion,
		FromManualVersion:  source.ManualVersion,
		ToProduct:          target.Product,
		ToManualType:       target.ManualType,
		ToManualURL:        target.URL,
		ToProductVersion:   target.ProductVersion,
		ToManualVersion:    target.ManualVersion,
		Reason:             reason,
	}
	if sourcePage.Link.URL != "" {
		candidate.Evidence = relevantCandidateEvidence(sourcePage, target.Product)
		candidate.FromExcerpt = candidateExcerpt(sourcePage.Text, target.Product)
	}
	if memory != nil {
		if targetPage, ok := memory.getPage(target.URL); ok {
			candidate.ToExcerpt = manualExcerpt(targetPage)
		}
	}
	return candidate
}

func compatibilityCandidateKey(candidate compatibilityCandidate) string {
	if strings.TrimSpace(candidate.FromManualURL) == "" || strings.TrimSpace(candidate.ToManualURL) == "" {
		return ""
	}
	return normaliseURL(candidate.FromManualURL) + "|" + normaliseURL(candidate.ToManualURL)
}

func findManualByURL(manuals []manualLink, pageURL string) (manualLink, bool) {
	pageURL = normaliseURL(pageURL)
	for _, manual := range manuals {
		manualURL := normaliseURL(manual.URL)
		if pageURL == manualURL || (manualURL != "" && strings.HasPrefix(pageURL, manualURL+"/")) {
			return manual, true
		}
	}
	return manualLink{}, false
}

func productReferenceMatches(text, product string) bool {
	product = strings.ToLower(strings.TrimSpace(product))
	if product == "" {
		return false
	}
	if len(product) > 3 {
		return strings.Contains(text, product)
	}
	pattern := `(^|[^a-z0-9])` + regexp.QuoteMeta(product) + `([^a-z0-9]|$)`
	return regexp.MustCompile(pattern).MatchString(text)
}

func relevantCandidateEvidence(page manualPage, targetProduct string) []evidenceQuote {
	evidence := make([]evidenceQuote, 0, 4)
	for _, quote := range page.Evidence {
		if quote.Section != "Manual" || productReferenceMatches(strings.ToLower(quote.Quote), targetProduct) {
			evidence = append(evidence, quote)
		}
		if len(evidence) == 4 {
			break
		}
	}
	if len(evidence) == 0 {
		if quote := candidateExcerpt(page.Text, targetProduct); quote != "" {
			evidence = append(evidence, evidenceQuote{SourceURL: page.Link.URL, Section: "Manual", Quote: quote})
		}
	}
	return evidence
}

func candidateExcerpt(text, targetProduct string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	index := strings.Index(strings.ToLower(text), strings.ToLower(strings.TrimSpace(targetProduct)))
	if index < 0 {
		return truncateManualText(text, 1200)
	}
	start := index - 320
	if start < 0 {
		start = 0
	}
	end := index + len(targetProduct) + 900
	if end > len(text) {
		end = len(text)
	}
	return strings.TrimSpace(text[start:end])
}

func manualExcerpt(page manualPage) string {
	if len(page.Evidence) > 0 {
		quotes := make([]string, 0, 3)
		for _, evidence := range page.Evidence {
			quotes = append(quotes, evidence.Section+": "+evidence.Quote)
			if len(quotes) == 3 {
				break
			}
		}
		return strings.Join(quotes, "\n")
	}
	return truncateManualText(page.Text, 1200)
}

func manualTools(crawler *manualCrawler, memory *manualMemory, shardIndex, shardCount int) map[string]squads.LocalTool {
	return map[string]squads.LocalTool{
		"scrape_manual_index": {
			Schema: squads.ToolSchema{
				Name:        "scrape_manual_index",
				Description: "Fetch the full Embention manuals index, preserve category and family context, and expand every latest link into concrete product/manual versions. Preserve errors instead of inventing links.",
				Parameters:  map[string]squads.ToolParam{"url": {Type: "string", Required: true}},
			},
			Func: func(arguments map[string]any) (any, error) {
				pageURL, _ := arguments["url"].(string)
				if strings.TrimSpace(pageURL) == "" {
					pageURL = manualsIndexURL
				}
				inventory, err := crawler.scrapeIndex(pageURL)
				encoded, encodeErr := json.Marshal(inventory)
				if encodeErr != nil {
					return nil, encodeErr
				}
				if err != nil {
					return string(encoded), err
				}
				return string(encoded), nil
			},
		},
		"read_assigned_manuals": {
			Schema: squads.ToolSchema{
				Name:        "read_assigned_manuals",
				Description: "Read every versioned manual assigned to this concurrent reader shard, inspect compatibility sections, extract direct quotes, and reuse the shared page cache.",
				Parameters:  map[string]squads.ToolParam{},
			},
			Func: func(_ map[string]any) (any, error) {
				pages, err := crawler.readAssignedManuals(shardIndex, shardCount)
				encoded, encodeErr := json.Marshal(pages)
				if encodeErr != nil {
					return nil, encodeErr
				}
				if err != nil {
					return string(encoded), err
				}
				return string(encoded), nil
			},
		},
		"verify_candidate_edges": {
			Schema: squads.ToolSchema{
				Name:        "verify_candidate_edges",
				Description: "Return the bounded versioned compatibility candidates assigned to this verifier shard, with source and target excerpts from Embention manuals only.",
				Parameters:  map[string]squads.ToolParam{},
			},
			Func: func(_ map[string]any) (any, error) {
				if shardCount <= 0 {
					shardCount = 1
				}
				candidates := memory.getCandidates()
				assigned := make([]compatibilityCandidate, 0)
				for index, candidate := range candidates {
					if index%shardCount == shardIndex {
						assigned = append(assigned, candidate)
					}
				}
				encoded, encodeErr := json.Marshal(assigned)
				if encodeErr != nil {
					return nil, encodeErr
				}
				return string(encoded), nil
			},
		},
		"lookup_shared_manual": {
			Schema: squads.ToolSchema{
				Name:        "lookup_shared_manual",
				Description: "Look up a previously fetched versioned manual from shared memory by exact direct URL, latest alias, or product name; product lookups prefer the current published version.",
				Parameters:  map[string]squads.ToolParam{"reference": {Type: "string", Required: true}},
			},
			Func: func(arguments map[string]any) (any, error) {
				reference, _ := arguments["reference"].(string)
				page, ok := memory.getPage(strings.TrimSpace(reference))
				if !ok {
					return nil, fmt.Errorf("shared manual %q was not fetched", reference)
				}
				encoded, err := json.Marshal(pageExtract(page))
				if err != nil {
					return nil, err
				}
				return string(encoded), nil
			},
		},
	}
}

func mockManualPage(_ context.Context, pageURL string) (int, string, []byte, error) {
	if strings.TrimRight(pageURL, "/") == strings.TrimRight(manualsIndexURL, "/") {
		return http.StatusOK, "text/html; charset=utf-8", []byte(mockManualIndexHTML), nil
	}
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return 0, "", nil, err
	}
	content, ok := mockManualPages[strings.TrimRight(parsed.Path, "/")]
	if !ok {
		return http.StatusNotFound, "text/html; charset=utf-8", nil, nil
	}
	return http.StatusOK, "text/html; charset=utf-8", []byte(content), nil
}

const mockManualIndexHTML = `<!doctype html>
<html><body>
<h1>Embention Manuals</h1>
<h2>Autopilot platforms</h2>
<h3>Veronte</h3>
<a href="/1x/en/latest">1x Hardware Manual</a>
<a href="/1x-software-manual/en/latest">1x Software Manual</a>
<a href="/4x/en/latest">4x Hardware Manual</a>
<a href="/4x-software-manual/en/latest">4x Software Manual</a>
<h2>Tools and peripherals</h2>
<a href="/drx/en/latest">DRx User Manual</a>
<a href="/hil-simulator/en/latest">HIL Simulator</a>
</body></html>`

var mockManualPages = map[string]string{
	"/1x/en/latest":   `<html><head><title>1x Hardware Manual</title></head><body><h1>1x Hardware Manual</h1><p>Version: UM.305.4.12⧸1.6</p><a href="/1x/en/4.12%E2%A7%B81.6/">4.12/1.6</a><a href="/1x/en/4.8%E2%A7%B81.4/">4.8/1.4</a></body></html>`,
	"/1x/en/4.12⧸1.6": `<html><head><title>1x Hardware Manual</title></head><body><h1>1x Hardware Manual</h1><p>Version: UM.305.4.12⧸1.6</p><a href="compatible devices/index.md">Compatible Devices</a><a href="technical/index.md">Technical</a></body></html>`,
	"/1x/en/4.8⧸1.4":  `<html><head><title>1x Hardware Manual</title></head><body><h1>1x Hardware Manual</h1><p>Version: UM.305.4.8⧸1.4</p><a href="compatible devices/index.md">Compatible Devices</a></body></html>`,
	"/1x/en/4.12⧸1.6/compatible devices/index.md":          `<html><body><h1>Compatible Devices</h1><p>1x is compatible with DRx over CAN bus.</p></body></html>`,
	"/1x/en/4.8⧸1.4/compatible devices/index.md":           `<html><body><h1>Compatible Devices</h1><p>1x is compatible with DRx over CAN bus.</p></body></html>`,
	"/1x/en/4.12⧸1.6/technical/index.md":                   `<html><body><h1>Technical</h1><p>Interfaces include CAN and serial interfaces. See the pinout section.</p></body></html>`,
	"/1x-software-manual/en/latest":                        `<html><head><title>1x Software Manual</title></head><body><h1>1x Software Manual</h1><p>Version: UM.309.8.2⧸1.1</p><a href="/1x-software-manual/en/8.2%E2%A7%B81.1/">8.2/1.1</a><a href="/1x-software-manual/en/8.0%E2%A7%B81.0/">8.0/1.0</a></body></html>`,
	"/1x-software-manual/en/8.2⧸1.1":                       `<html><head><title>1x Software Manual</title></head><body><h1>1x Software Manual</h1><p>Version: UM.309.8.2⧸1.1</p><a href="applications/index.md">Software applications</a></body></html>`,
	"/1x-software-manual/en/8.0⧸1.0":                       `<html><head><title>1x Software Manual</title></head><body><h1>1x Software Manual</h1><p>Version: UM.309.8.0⧸1.0</p><a href="applications/index.md">Software applications</a></body></html>`,
	"/1x-software-manual/en/8.2⧸1.1/applications/index.md": `<html><body><h1>Software applications</h1><p>The supported software is Veronte Pipe. The software connects to 1x through the documented interface.</p></body></html>`,
	"/1x-software-manual/en/8.0⧸1.0/applications/index.md": `<html><body><h1>Software applications</h1><p>The supported software is Veronte Pipe. The software connects to 1x through the documented interface.</p></body></html>`,
	"/4x/en/latest":  `<html><head><title>4x Hardware Manual</title></head><body><h1>4x Hardware Manual</h1><p>Version: UM.306.4.2⧸1.2</p><a href="/4x/en/4.2%E2%A7%B81.2/">4.2/1.2</a><a href="/4x/en/4.0%E2%A7%B81.0/">4.0/1.0</a></body></html>`,
	"/4x/en/4.2⧸1.2": `<html><head><title>4x Hardware Manual</title></head><body><h1>4x Hardware Manual</h1><p>Version: UM.306.4.2⧸1.2</p><a href="compatible devices/index.md">Compatible Devices</a></body></html>`,
	"/4x/en/4.0⧸1.0": `<html><head><title>4x Hardware Manual</title></head><body><h1>4x Hardware Manual</h1><p>Version: UM.306.4.0⧸1.0</p><a href="compatible devices/index.md">Compatible Devices</a></body></html>`,
	"/4x/en/4.2⧸1.2/compatible devices/index.md":           `<html><body><h1>Compatible Devices</h1><p>4x connects to Veronte Pipe through its supported interface.</p></body></html>`,
	"/4x/en/4.0⧸1.0/compatible devices/index.md":           `<html><body><h1>Compatible Devices</h1><p>4x connects to Veronte Pipe through its supported interface.</p></body></html>`,
	"/4x-software-manual/en/latest":                        `<html><head><title>4x Software Manual</title></head><body><h1>4x Software Manual</h1><p>Version: UM.310.7.1⧸1.1</p><a href="/4x-software-manual/en/7.1%E2%A7%B81.1/">7.1/1.1</a><a href="/4x-software-manual/en/7.0%E2%A7%B81.0/">7.0/1.0</a></body></html>`,
	"/4x-software-manual/en/7.1⧸1.1":                       `<html><head><title>4x Software Manual</title></head><body><h1>4x Software Manual</h1><p>Version: UM.310.7.1⧸1.1</p><a href="applications/index.md">Software applications</a></body></html>`,
	"/4x-software-manual/en/7.0⧸1.0":                       `<html><head><title>4x Software Manual</title></head><body><h1>4x Software Manual</h1><p>Version: UM.310.7.0⧸1.0</p><a href="applications/index.md">Software applications</a></body></html>`,
	"/4x-software-manual/en/7.1⧸1.1/applications/index.md": `<html><body><h1>Software applications</h1><p>Supported software: Veronte Pipe. Requires the 4x hardware manual for installation.</p></body></html>`,
	"/4x-software-manual/en/7.0⧸1.0/applications/index.md": `<html><body><h1>Software applications</h1><p>Supported software: Veronte Pipe. Requires the 4x hardware manual for installation.</p></body></html>`,
	"/drx/en/latest":  `<html><head><title>DRx User Manual</title></head><body><h1>DRx User Manual</h1><p>Version: UM.401.2.0⧸1.0</p><a href="/drx/en/2.0%E2%A7%B81.0/">2.0/1.0</a><a href="/drx/en/1.9%E2%A7%B81.0/">1.9/1.0</a></body></html>`,
	"/drx/en/2.0⧸1.0": `<html><head><title>DRx User Manual</title></head><body><h1>DRx User Manual</h1><p>Version: UM.401.2.0⧸1.0</p><a href="integration examples/index.md">Integration examples</a></body></html>`,
	"/drx/en/1.9⧸1.0": `<html><head><title>DRx User Manual</title></head><body><h1>DRx User Manual</h1><p>Version: UM.401.1.9⧸1.0</p><a href="integration examples/index.md">Integration examples</a></body></html>`,
	"/drx/en/2.0⧸1.0/integration examples/index.md":           `<html><body><h1>Integration examples</h1><p>DRx is compatible with 1x. It requires a CAN connection and the 1x hardware interface.</p></body></html>`,
	"/drx/en/1.9⧸1.0/integration examples/index.md":           `<html><body><h1>Integration examples</h1><p>DRx is compatible with 1x. It requires a CAN connection and the 1x hardware interface.</p></body></html>`,
	"/hil-simulator/en/latest":                                `<html><head><title>HIL Simulator</title></head><body><h1>HIL Simulator</h1><p>Version: UM.501.3.0⧸1.0</p><a href="/hil-simulator/en/3.0%E2%A7%B81.0/">3.0/1.0</a><a href="/hil-simulator/en/2.9%E2%A7%B81.0/">2.9/1.0</a></body></html>`,
	"/hil-simulator/en/3.0⧸1.0":                               `<html><head><title>HIL Simulator</title></head><body><h1>HIL Simulator</h1><p>Version: UM.501.3.0⧸1.0</p><a href="integration examples/index.md">Integration examples</a></body></html>`,
	"/hil-simulator/en/2.9⧸1.0":                               `<html><head><title>HIL Simulator</title></head><body><h1>HIL Simulator</h1><p>Version: UM.501.2.9⧸1.0</p><a href="integration examples/index.md">Integration examples</a></body></html>`,
	"/hil-simulator/en/3.0⧸1.0/integration examples/index.md": `<html><body><h1>Integration examples</h1><p>HIL Simulator integrates with Veronte Pipe. No hardware compatibility is specified in this manual.</p></body></html>`,
	"/hil-simulator/en/2.9⧸1.0/integration examples/index.md": `<html><body><h1>Integration examples</h1><p>HIL Simulator integrates with Veronte Pipe. No hardware compatibility is specified in this manual.</p></body></html>`,
}
