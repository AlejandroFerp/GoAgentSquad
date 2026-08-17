package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/synapse"
)

type evidenceBatch struct {
	Facts []manualEvidence `json:"facts"`
}

func mustJSON(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "{}"
	}
	return string(encoded)
}

func appendManualIssue(inventory *manualInventory, stage, pageURL string, err error) {
	if err == nil {
		return
	}
	inventory.Issues = append(inventory.Issues, manualIssue{Stage: stage, URL: pageURL, Error: err.Error()})
}

func mergeInventory(primary, secondary manualInventory) manualInventory {
	merged := primary
	if merged.SourceURL == "" {
		merged.SourceURL = secondary.SourceURL
	}
	if merged.RetrievedAt.IsZero() || secondary.RetrievedAt.After(merged.RetrievedAt) {
		merged.RetrievedAt = secondary.RetrievedAt
	}
	seen := make(map[string]struct{}, len(primary.Manuals)+len(secondary.Manuals))
	merged.Manuals = make([]manualLink, 0, len(primary.Manuals)+len(secondary.Manuals))
	for _, manual := range append(append([]manualLink(nil), primary.Manuals...), secondary.Manuals...) {
		if _, exists := seen[manual.URL]; exists {
			continue
		}
		seen[manual.URL] = struct{}{}
		merged.Manuals = append(merged.Manuals, manual)
	}
	seenIssues := make(map[string]struct{})
	for _, issue := range append(append([]manualIssue(nil), primary.Issues...), secondary.Issues...) {
		key := issue.Stage + "|" + issue.URL + "|" + issue.Error
		if _, exists := seenIssues[key]; exists {
			continue
		}
		seenIssues[key] = struct{}{}
		merged.Issues = append(merged.Issues, issue)
	}
	return merged
}

func assistantContents(history []synapse.SynapseMessage) []string {
	contents := make([]string, 0)
	for _, message := range history {
		if message.Role != synapse.RoleAssistant {
			continue
		}
		content := strings.TrimSpace(message.Content())
		if content != "" {
			contents = append(contents, content)
		}
	}
	return contents
}

func jsonObjects(text string) [][]byte {
	objects := make([][]byte, 0)
	for index := 0; index < len(text); index++ {
		if text[index] != '{' {
			continue
		}
		decoder := json.NewDecoder(strings.NewReader(text[index:]))
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil || len(raw) == 0 || raw[0] != '{' {
			continue
		}
		objects = append(objects, append([]byte(nil), raw...))
		index += len(raw) - 1
	}
	return objects
}

func inventoryFromHistory(history []synapse.SynapseMessage) manualInventory {
	for _, content := range assistantContents(history) {
		for _, object := range jsonObjects(content) {
			var inventory manualInventory
			if err := json.Unmarshal(object, &inventory); err != nil {
				continue
			}
			if inventory.SourceURL != "" || len(inventory.Manuals) > 0 || len(inventory.Issues) > 0 {
				return inventory
			}
		}
	}
	return newManualInventory("")
}

func evidenceFromHistory(history []synapse.SynapseMessage) []manualEvidence {
	evidence := make([]manualEvidence, 0)
	seen := make(map[string]struct{})
	for _, content := range assistantContents(history) {
		for _, object := range jsonObjects(content) {
			var batch evidenceBatch
			if err := json.Unmarshal(object, &batch); err == nil && len(batch.Facts) > 0 {
				for _, fact := range batch.Facts {
					appendEvidence(&evidence, seen, fact)
				}
				continue
			}
			var fact manualEvidence
			if err := json.Unmarshal(object, &fact); err == nil && (fact.Product != "" || fact.ManualURL != "") {
				appendEvidence(&evidence, seen, fact)
			}
		}
	}
	return evidence
}

func appendEvidence(evidence *[]manualEvidence, seen map[string]struct{}, fact manualEvidence) {
	key := strings.ToLower(strings.TrimSpace(fact.Product) + "|" + strings.TrimSpace(fact.ManualURL))
	if key == "|" {
		return
	}
	if _, exists := seen[key]; exists {
		return
	}
	seen[key] = struct{}{}
	*evidence = append(*evidence, fact)
}

func draftFromHistory(history []synapse.SynapseMessage) compatibilityDraft {
	draft := compatibilityDraft{}
	seenRelationships := make(map[string]struct{})
	for _, content := range assistantContents(history) {
		for _, object := range jsonObjects(content) {
			var candidateDraft compatibilityDraft
			if err := json.Unmarshal(object, &candidateDraft); err != nil {
				continue
			}
			for _, relationship := range candidateDraft.Relationships {
				key := relationshipKey(relationship)
				if _, exists := seenRelationships[key]; exists {
					continue
				}
				seenRelationships[key] = struct{}{}
				draft.Relationships = append(draft.Relationships, relationship)
			}
			for _, unresolved := range candidateDraft.Unresolved {
				if strings.TrimSpace(unresolved) != "" {
					draft.Unresolved = append(draft.Unresolved, unresolved)
				}
			}
		}
	}
	return draft
}

func draftFromEvidence(evidence []manualEvidence) compatibilityDraft {
	draft := compatibilityDraft{}
	for _, fact := range evidence {
		draft.Relationships = append(draft.Relationships, fact.CompatibilityClaims...)
	}
	return draft
}

func relationshipKey(claim compatibilityClaim) string {
	return strings.Join([]string{
		normaliseProduct(claim.FromProduct),
		strings.ToLower(strings.TrimSpace(claim.FromManualType)),
		strings.TrimSpace(claim.FromProductVersion),
		strings.TrimSpace(claim.FromManualVersion),
		normaliseProduct(claim.ToProduct),
		strings.ToLower(strings.TrimSpace(claim.ToManualType)),
		strings.TrimSpace(claim.ToProductVersion),
		strings.TrimSpace(claim.ToManualVersion),
	}, "|")
}

func enrichDraftFromCandidates(draft compatibilityDraft, candidates []compatibilityCandidate) compatibilityDraft {
	for relationshipIndex := range draft.Relationships {
		claim := &draft.Relationships[relationshipIndex]
		for _, candidate := range candidates {
			if normaliseProduct(claim.FromProduct) != normaliseProduct(candidate.FromProduct) || normaliseProduct(claim.ToProduct) != normaliseProduct(candidate.ToProduct) {
				continue
			}
			if claim.FromManualType == "" {
				claim.FromManualType = candidate.FromManualType
			}
			if claim.ToManualType == "" {
				claim.ToManualType = candidate.ToManualType
			}
			if claim.FromProductVersion == "" {
				claim.FromProductVersion = candidate.FromProductVersion
			}
			if claim.FromManualVersion == "" {
				claim.FromManualVersion = candidate.FromManualVersion
			}
			if claim.ToProductVersion == "" {
				claim.ToProductVersion = candidate.ToProductVersion
			}
			if claim.ToManualVersion == "" {
				claim.ToManualVersion = candidate.ToManualVersion
			}
			break
		}
	}
	return draft
}

func ensureCandidateCoverage(draft compatibilityDraft, candidates []compatibilityCandidate) compatibilityDraft {
	covered := make(map[string]struct{}, len(draft.Relationships))
	for _, claim := range draft.Relationships {
		for _, candidate := range candidates {
			if candidateMatchesClaim(candidate, claim) {
				covered[candidate.ID] = struct{}{}
			}
		}
	}
	for _, candidate := range candidates {
		if _, exists := covered[candidate.ID]; exists {
			continue
		}
		draft.Relationships = append(draft.Relationships, compatibilityClaim{
			FromProduct:        candidate.FromProduct,
			FromManualType:     candidate.FromManualType,
			ToProduct:          candidate.ToProduct,
			ToManualType:       candidate.ToManualType,
			FromProductVersion: candidate.FromProductVersion,
			FromManualVersion:  candidate.FromManualVersion,
			ToProductVersion:   candidate.ToProductVersion,
			ToManualVersion:    candidate.ToManualVersion,
			Status:             "Not specified",
			Relationship:       "Candidate returned without a verified conclusion",
			Evidence:           append([]evidenceQuote(nil), candidate.Evidence...),
			Notes:              "The verifier did not return a conclusion for this candidate; compatibility remains not specified.",
		})
	}
	return draft
}

func buildCompatibilityMatrix(inventory manualInventory, evidence []manualEvidence, draft compatibilityDraft, candidateSets ...[]compatibilityCandidate) ([]compatibilityClaim, []manualIssue) {
	knownProducts := make(map[string]string)
	manualURLs := make(map[string]struct{})
	for _, manual := range inventory.Manuals {
		knownProducts[normaliseProduct(manual.Product)] = manual.Product
		manualURLs[normaliseURL(manual.URL)] = struct{}{}
		if manual.LatestURL != "" {
			manualURLs[normaliseURL(manual.LatestURL)] = struct{}{}
		}
	}
	if len(draft.Relationships) == 0 {
		draft = draftFromEvidence(evidence)
	}
	blindSpots := append([]manualIssue(nil), inventory.Issues...)
	claimsByKey := make(map[string]compatibilityClaim)
	for _, unresolved := range draft.Unresolved {
		if strings.TrimSpace(unresolved) == "" {
			continue
		}
		blindSpots = append(blindSpots, manualIssue{Stage: "synthesis", Error: unresolved})
	}
	for _, fact := range evidence {
		if fact.Status == "blocked" || fact.Status == "partial" {
			detail := strings.TrimSpace(fact.Notes)
			if detail == "" {
				detail = "manual was not fully readable"
			}
			blindSpots = append(blindSpots, manualIssue{Stage: "ingestion", URL: fact.ManualURL, Error: detail})
		}
		for _, warning := range fact.Warnings {
			if strings.TrimSpace(warning) != "" {
				blindSpots = append(blindSpots, manualIssue{Stage: "ingestion", URL: fact.ManualURL, Error: warning})
			}
		}
		for _, dependency := range fact.Dependencies {
			dependencyName := normaliseProduct(dependency.Dependency)
			if dependencyName == "" {
				continue
			}
			if _, exists := knownProducts[dependencyName]; !exists {
				blindSpots = append(blindSpots, manualIssue{Stage: "ingestion", URL: fact.ManualURL, Error: "dependency mentioned without a mapped manual: " + dependency.Dependency})
			}
		}
	}
	for _, claim := range draft.Relationships {
		from := normaliseProduct(claim.FromProduct)
		to := normaliseProduct(claim.ToProduct)
		if from == "" || to == "" || from == to {
			blindSpots = append(blindSpots, manualIssue{Stage: "matrix", Error: "relationship has missing or identical products: " + claim.FromProduct + " -> " + claim.ToProduct})
			continue
		}
		if _, exists := knownProducts[from]; !exists {
			blindSpots = append(blindSpots, manualIssue{Stage: "matrix", Error: "product mentioned without a mapped manual: " + claim.FromProduct})
			continue
		}
		if _, exists := knownProducts[to]; !exists {
			blindSpots = append(blindSpots, manualIssue{Stage: "matrix", Error: "product mentioned without a mapped manual: " + claim.ToProduct})
			continue
		}
		claim.FromProduct = knownProducts[from]
		claim.ToProduct = knownProducts[to]
		inferClaimVersions(&claim, inventory.Manuals)
		claim.Status = normaliseCompatibilityStatus(claim.Status)
		if claim.Status != "Not specified" && !claimHasManualEvidence(claim, manualURLs) {
			claim.Status = "Not specified"
			claim.Notes = strings.TrimSpace(claim.Notes + " No direct evidence URL from the audited manuals was supplied.")
		}
		key := relationshipKey(claim)
		if existing, exists := claimsByKey[key]; !exists || compatibilityRank(claim.Status) > compatibilityRank(existing.Status) {
			claimsByKey[key] = claim
		}
	}

	keys := make([]string, 0, len(claimsByKey))
	for key := range claimsByKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	matrix := make([]compatibilityClaim, 0, len(keys))
	for _, key := range keys {
		matrix = append(matrix, claimsByKey[key])
	}
	if len(candidateSets) > 0 {
		missingCandidates := 0
		for _, candidate := range candidateSets[0] {
			verified := false
			for _, claim := range matrix {
				if candidateMatchesClaim(candidate, claim) {
					verified = true
					break
				}
			}
			if !verified {
				missingCandidates++
			}
		}
		if missingCandidates > 0 {
			blindSpots = append(blindSpots, manualIssue{Stage: "verification", Error: fmt.Sprintf("%d of %d candidate relationships were not returned by a verifier", missingCandidates, len(candidateSets[0]))})
		}
	}
	return matrix, uniqueIssues(blindSpots)
}

func candidateMatchesClaim(candidate compatibilityCandidate, claim compatibilityClaim) bool {
	if normaliseProduct(candidate.FromProduct) != normaliseProduct(claim.FromProduct) || normaliseProduct(candidate.ToProduct) != normaliseProduct(claim.ToProduct) {
		return false
	}
	if claim.FromProductVersion != "" && claim.FromProductVersion != candidate.FromProductVersion {
		return false
	}
	if claim.ToProductVersion != "" && claim.ToProductVersion != candidate.ToProductVersion {
		return false
	}
	for _, evidence := range claim.Evidence {
		if normaliseURL(evidence.SourceURL) == normaliseURL(candidate.FromManualURL) || strings.HasPrefix(normaliseURL(evidence.SourceURL), normaliseURL(candidate.FromManualURL)+"/") {
			return true
		}
	}
	return false
}

func normaliseProduct(product string) string {
	return strings.ToLower(strings.Join(strings.Fields(strings.TrimSpace(product)), " "))
}

func normaliseURL(pageURL string) string {
	return strings.TrimRight(strings.TrimSpace(pageURL), "/")
}

func productPairKey(left, right string) string {
	products := []string{normaliseProduct(left), normaliseProduct(right)}
	sort.Strings(products)
	return products[0] + "|" + products[1]
}

func normaliseCompatibilityStatus(status string) string {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "compatible":
		return "Compatible"
	case "incompatible":
		return "Incompatible"
	default:
		return "Not specified"
	}
}

func compatibilityRank(status string) int {
	switch status {
	case "Compatible", "Incompatible":
		return 2
	default:
		return 1
	}
}

func claimHasManualEvidence(claim compatibilityClaim, manualURLs map[string]struct{}) bool {
	for _, citation := range claim.Evidence {
		sourceURL := normaliseURL(citation.SourceURL)
		if strings.TrimSpace(citation.Quote) == "" {
			continue
		}
		if _, exists := manualURLs[sourceURL]; exists {
			return true
		}
		for manualURL := range manualURLs {
			if strings.HasPrefix(sourceURL, manualURL+"/") {
				return true
			}
		}
	}
	return false
}

func inferClaimVersions(claim *compatibilityClaim, manuals []manualLink) {
	for _, citation := range claim.Evidence {
		for _, manual := range manuals {
			if !manualSourceMatches(citation.SourceURL, manual) {
				continue
			}
			if normaliseProduct(claim.FromProduct) == normaliseProduct(manual.Product) {
				if claim.FromManualType == "" {
					claim.FromManualType = manual.ManualType
				}
				if claim.FromProductVersion == "" {
					claim.FromProductVersion = manual.ProductVersion
				}
				if claim.FromManualVersion == "" {
					claim.FromManualVersion = manual.ManualVersion
				}
			}
			if normaliseProduct(claim.ToProduct) == normaliseProduct(manual.Product) {
				if claim.ToManualType == "" {
					claim.ToManualType = manual.ManualType
				}
				if claim.ToProductVersion == "" {
					claim.ToProductVersion = manual.ProductVersion
				}
				if claim.ToManualVersion == "" {
					claim.ToManualVersion = manual.ManualVersion
				}
			}
		}
	}
	if claim.FromManualType == "" {
		claim.FromManualType = uniqueManualType(claim.FromProduct, manuals)
	}
	if claim.ToManualType == "" {
		claim.ToManualType = uniqueManualType(claim.ToProduct, manuals)
	}
}

func uniqueManualType(product string, manuals []manualLink) string {
	typeName := ""
	for _, manual := range manuals {
		if normaliseProduct(manual.Product) != normaliseProduct(product) || strings.TrimSpace(manual.ManualType) == "" {
			continue
		}
		if typeName == "" {
			typeName = manual.ManualType
			continue
		}
		if !strings.EqualFold(typeName, manual.ManualType) {
			return ""
		}
	}
	return typeName
}

func manualSourceMatches(sourceURL string, manual manualLink) bool {
	source := normaliseURL(sourceURL)
	for _, root := range []string{manual.URL, manual.LatestURL} {
		root = normaliseURL(root)
		if root != "" && (source == root || strings.HasPrefix(source, root+"/")) {
			return true
		}
	}
	return false
}

func uniqueIssues(issues []manualIssue) []manualIssue {
	result := make([]manualIssue, 0, len(issues))
	seen := make(map[string]struct{})
	for _, issue := range issues {
		key := issue.Stage + "|" + issue.URL + "|" + issue.Error
		if strings.Trim(issue.Error, " ") == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		result = append(result, issue)
	}
	return result
}

func formatAuditReport(inventory manualInventory, matrix []compatibilityClaim, blindSpots []manualIssue, scope string, candidateCount int) string {
	var report strings.Builder
	fmt.Fprintf(&report, "# Embention Manuals Audit\n\nGenerated at: %s\n\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintf(&report, "Source index: %s\n\n", inventory.SourceURL)
	fmt.Fprintf(&report, "Analysis scope: %s\n\n", markdownCell(scope))
	fmt.Fprintf(&report, "Verification candidates: %d\nVerified relationships: %d\n\n", candidateCount, len(matrix))
	report.WriteString("## Manual vs. Product Map\n\n")
	report.WriteString("| Category | Family | Product | Manual type | Product version | Manual version | Latest URL | Direct manual URL |\n| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, manual := range inventory.Manuals {
		latestURL := markdownCell(manual.LatestURL)
		if latestURL == "" {
			latestURL = "Not specified"
		}
		directURL := markdownCell(manual.URL)
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %s | %s | %s | [%s](%s) |\n", markdownCell(manual.Category), markdownCell(manual.Family), markdownCell(manual.Product), markdownCell(manual.ManualType), markdownCell(manual.ProductVersion), markdownCell(manual.ManualVersion), latestURL, directURL, manual.URL)
	}
	if len(inventory.Manuals) == 0 {
		report.WriteString("| Not available | Not available | Not available | Not available | Not available | Not available | Not available | Not available |\n")
	}

	report.WriteString("\n## Compatibility Matrix\n\n")
	report.WriteString(formatCompatibilityMatrix(matrix))

	report.WriteString("\n## Points Blind\n\n")
	report.WriteString("| Stage | URL | Detail |\n| --- | --- | --- |\n")
	for _, issue := range blindSpots {
		fmt.Fprintf(&report, "| %s | %s | %s |\n", markdownCell(issue.Stage), markdownCell(issue.URL), markdownCell(issue.Error))
	}
	if len(blindSpots) == 0 {
		report.WriteString("| None | None | No blocking or missing-documentation issue was recorded. |\n")
	}
	return strings.TrimSpace(report.String()) + "\n"
}

func formatCompatibilityMatrix(matrix []compatibilityClaim) string {
	var report strings.Builder
	report.WriteString("| Product A | Product A manual type | Product A versions | Product B | Product B manual type | Product B versions | Status | Relationship | Connection or dependency | Evidence | Notes |\n| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, claim := range matrix {
		evidence := "None"
		if len(claim.Evidence) > 0 {
			citations := make([]string, 0, len(claim.Evidence))
			for _, citation := range claim.Evidence {
				label := citation.Section
				if label == "" {
					label = citation.SourceURL
				}
				citations = append(citations, fmt.Sprintf("[%s](%s): %s", markdownCell(label), citation.SourceURL, markdownCell(citation.Quote)))
			}
			evidence = strings.Join(citations, "; ")
		}
		fmt.Fprintf(&report, "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n", markdownCell(claim.FromProduct), markdownCell(claim.FromManualType), markdownCell(formatClaimVersions(claim.FromProductVersion, claim.FromManualVersion)), markdownCell(claim.ToProduct), markdownCell(claim.ToManualType), markdownCell(formatClaimVersions(claim.ToProductVersion, claim.ToManualVersion)), markdownCell(claim.Status), markdownCell(claim.Relationship), markdownCell(claim.Connection), evidence, markdownCell(claim.Notes))
	}
	if len(matrix) == 0 {
		report.WriteString("| Not available | Not specified | Not specified | Not available | Not specified | Not specified | Not specified | Not specified | Not specified | None | No mapped product pair was available. |\n")
	}
	return report.String()
}

func formatClaimVersions(productVersion, manualVersion string) string {
	productVersion = strings.TrimSpace(productVersion)
	manualVersion = strings.TrimSpace(manualVersion)
	if productVersion == "" && manualVersion == "" {
		return "Not specified"
	}
	return productVersion + " / " + manualVersion
}

func markdownCell(value string) string {
	return strings.ReplaceAll(strings.TrimSpace(value), "|", "\\|")
}
