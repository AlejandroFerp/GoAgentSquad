package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/squads"
	"github.com/embention/agent-squad-go/pkg/synapse"
)

const (
	discoverySquadID         = "manual-discovery"
	ingestionSquadID         = "manual-ingestion"
	synthesisSquadID         = "manual-synthesis"
	reportingSquadID         = "manual-reporting"
	manualEvidenceIndexID    = "manual-evidence-index"
	manualEvidenceCapability = "get_shared_compatibility_evidence"
)

type experiment struct {
	cfg     config
	runtime *squads.Runtime
	memory  *manualMemory
	crawler *manualCrawler
}

type experimentResult struct {
	Inventory         manualInventory
	AnalysisInventory manualInventory
	Evidence          []manualEvidence
	Candidates        []compatibilityCandidate
	Draft             compatibilityDraft
	Matrix            []compatibilityClaim
	BlindSpots        []manualIssue
	Report            string
	DiscoveryQuery    string
	IngestionQuery    string
	SynthesisMetrics  map[string]any
	SynthesisQuery    string
	ReportingQuery    string
}

func newExperiment(ctx context.Context, cfg config, llm squads.LLMCall) (*experiment, error) {
	memory := newManualMemory()
	crawler := newManualCrawler(cfg.Mock, memory)
	definitions := manualExperimentDefinitions(crawler, memory, cfg.VerifierModel)
	configureManualDelegationPolicy(definitions)

	runtime, err := squads.NewRuntime(ctx, squads.RuntimeConfig{
		ServiceCapacity:   300,
		MaxIterations:     40,
		Model:             primaryModel(cfg),
		LLMCall:           llm,
		CaptureLLMContent: cfg.CaptureLLMContent,
		Squads:            definitions,
		Transversals: []squads.TransversalDefinition{
			{
				ID:           manualEvidenceIndexID,
				Type:         "MANUAL_EVIDENCE_INDEX",
				Description:  "Returns shared versioned manual evidence and compatibility candidates.",
				Capabilities: []string{manualEvidenceCapability},
				ExecuteTask:  manualEvidenceIndexTask(memory),
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("create declarative runtime: %w", err)
	}
	return &experiment{cfg: cfg, runtime: runtime, memory: memory, crawler: crawler}, nil
}

func configureManualDelegationPolicy(definitions []squads.SquadDefinition) {
	for squadIndex := range definitions {
		for agentIndex := range definitions[squadIndex].Agents {
			allowedSquad := ""
			if definitions[squadIndex].ID == reportingSquadID && definitions[squadIndex].Agents[agentIndex].ID == "data-formatter" {
				allowedSquad = synthesisSquadID
			}
			excludedSquads := make([]string, 0, 4)
			for _, squadID := range []string{discoverySquadID, ingestionSquadID, synthesisSquadID, reportingSquadID} {
				if squadID != allowedSquad {
					excludedSquads = append(excludedSquads, squadID)
				}
			}
			definitions[squadIndex].Agents[agentIndex].ExcludedSquads = excludedSquads
		}
	}
}

func (experiment *experiment) Close() error {
	return experiment.runtime.Close()
}

func (experiment *experiment) Observability() *squads.ObservabilityRuntime {
	return experiment.runtime.Observability()
}

func (experiment *experiment) Run(ctx context.Context) (experimentResult, error) {
	result := experimentResult{}
	inventory := newManualInventory(manualsIndexURL)

	result.DiscoveryQuery = fmt.Sprintf("manual-discovery-%d", time.Now().UnixNano())
	discoveryPrompt := fmt.Sprintf(`You are the Web Navigator in Squad 1. Audit the public Embention manuals index at %s.
Call scrape_manual_index exactly once. The tool must include links from every navigation section, including collapsed Apps, frameworks, and Discontinued sections. Treat each latest link as an alias: preserve one inventory record for every concrete manual version exposed by its version selector. Keep the category, family, manual type, product version, manual version, latest URL, and direct version URL. The tool is authoritative for links; if it reports a block or timeout, preserve that issue and do not invent links.
Return ONLY JSON matching this shape:
{"source_url":"...","retrieved_at":"RFC3339","manuals":[{"category":"...","family":"...","product":"...","manual_type":"hardware|software|application|framework|user","title":"...","url":"...","latest_url":"...","product_version":"...","manual_version":"...","is_latest":false}],"issues":[{"stage":"...","url":"...","error":"..."}]}`,
		manualsIndexURL,
	)
	discovery, err := experiment.runtime.Query(ctx, result.DiscoveryQuery, []string{discoverySquadID}, discoveryPrompt, experiment.cfg.QueryTimeout)
	if err != nil {
		appendManualIssue(&inventory, "discovery", manualsIndexURL, err)
	} else {
		inventory = inventoryFromHistory(discovery.History)
	}
	if cached, ok := experiment.memory.getInventory(); ok {
		inventory = mergeInventory(inventory, cached)
	}
	if inventory.SourceURL == "" {
		inventory.SourceURL = manualsIndexURL
	}
	analysisInventory := selectAnalysisInventory(inventory, experiment.cfg.Scope, experiment.cfg.IncludeHistorical)
	experiment.memory.setAnalysisInventory(analysisInventory)
	experiment.memory.setInventory(inventory)
	result.Inventory = inventory
	result.AnalysisInventory = analysisInventory

	inventoryJSON := mustJSON(analysisInventory)
	result.IngestionQuery = fmt.Sprintf("manual-ingestion-%d", time.Now().UnixNano())
	ingestionPrompt := fmt.Sprintf(`You are a Technical Reader in Squad 2. Multiple readers run concurrently; this reader owns a deterministic shard from the scoped inventory.
Use read_assigned_manuals exactly once. Read every assigned versioned manual, including historical and discontinued records. Do not use web search or external sources. The tool reads the manual landing page and the relevant technical sections such as Compatible Devices, Integration examples, Software Installation, Software applications, Technical, Quick Start, and Configuration. Extract only explicit compatibility, interface, dependency, pinout, integration, and supported-software evidence. Preserve exact Embention section URLs and verbatim quotes; do not treat a navigation label or a product name as evidence. Return one fact per assigned manual and never summarize or drop assigned manuals. Preserve blocked pages as facts with status "blocked" and partial section failures with status "partial".
Return ONLY JSON matching this shape:
{"facts":[{"product":"...","manual_url":"...","manual_type":"hardware|software|application|framework|user","product_version":"...","manual_version":"...","status":"analyzed|partial|blocked|not_specified","keywords_found":["..."],"compatibility_claims":[{"from_product":"...","from_manual_type":"hardware|software|application|framework|user","to_product":"...","to_manual_type":"hardware|software|application|framework|user","from_product_version":"...","from_manual_version":"...","to_product_version":"...","to_manual_version":"...","status":"compatible|incompatible|not_specified","relationship":"...","connection":"...","evidence":[{"source_url":"...","section":"...","quote":"verbatim quote"}],"notes":"..."}],"dependencies":[{"product":"...","dependency":"...","kind":"...","evidence":[{"source_url":"...","quote":"verbatim quote"}]}],"evidence":[{"source_url":"...","section":"...","quote":"verbatim quote"}],"warnings":["..."],"notes":"..."}]}
	Shared inventory:
%s`, inventoryJSON)
	ingestion, err := experiment.runtime.Query(ctx, result.IngestionQuery, []string{ingestionSquadID}, ingestionPrompt, experiment.cfg.QueryTimeout)
	if err != nil {
		appendManualIssue(&inventory, "ingestion", "", err)
	} else {
		result.Evidence = evidenceFromHistory(ingestion.History)
	}
	result.Inventory = inventory
	result.AnalysisInventory = analysisInventory
	candidates := experiment.crawler.buildCompatibilityCandidates(analysisInventory, result.Evidence)
	experiment.memory.setCandidates(candidates)
	result.Candidates = candidates

	result.SynthesisQuery = fmt.Sprintf("manual-synthesis-%d", time.Now().UnixNano())
	synthesisPrompt := fmt.Sprintf(`You are a Compatibility Verifier in Squad 3. Each verifier owns a deterministic shard of bounded candidate relationships.
Use verify_candidate_edges exactly once. Do not use web search or external sources. For every candidate, compare the source and target product/manual versions and their supplied Embention excerpts. Emit every assigned candidate exactly once. Mark a relationship Compatible or Incompatible only when a direct Embention quote supports that exact relationship; otherwise emit Not specified and explain why in unresolved. Never drop a candidate because the manuals are ambiguous, blocked, or silent. Preserve all exact URLs, manual types, product versions, manual versions, and verbatim quotes.
Return ONLY JSON matching this shape:
{"relationships":[{"from_product":"...","from_manual_type":"hardware|software|application|framework|user","to_product":"...","to_manual_type":"hardware|software|application|framework|user","from_product_version":"...","from_manual_version":"...","to_product_version":"...","to_manual_version":"...","status":"compatible|incompatible|not_specified","relationship":"...","connection":"...","evidence":[{"source_url":"...","section":"...","quote":"verbatim quote"}],"notes":"..."}],"unresolved":["..."]}
Candidate count: %d
Scoped inventory:
%s`, len(candidates), inventoryJSON)
	synthesis, err := experiment.runtime.Query(ctx, result.SynthesisQuery, []string{synthesisSquadID}, synthesisPrompt, experiment.cfg.QueryTimeout)
	if err != nil {
		appendManualIssue(&inventory, "synthesis", "", err)
	} else {
		result.Draft = draftFromHistory(synthesis.History)
		result.SynthesisMetrics = synthesis.Metrics
	}
	if len(result.Draft.Relationships) == 0 {
		result.Draft = draftFromEvidence(result.Evidence)
	}
	result.Draft = enrichDraftFromCandidates(result.Draft, candidates)
	result.Draft = ensureCandidateCoverage(result.Draft, candidates)
	draftJSON := mustJSON(result.Draft)

	result.ReportingQuery = fmt.Sprintf("manual-reporting-%d", time.Now().UnixNano())
	reportingPrompt := fmt.Sprintf(`You are the Data Formatter in Squad 4. Audit the verified candidate relationships below without dropping or collapsing any relationship.
Preserve exact product and manual versions, direct Embention section URLs, and verbatim evidence quotes. Do not use web search, invent compatibility, or replace versioned URLs with latest aliases. Return ONLY JSON matching this shape:
{"relationships":[{"from_product":"...","to_product":"...","from_product_version":"...","from_manual_version":"...","to_product_version":"...","to_manual_version":"...","status":"compatible|incompatible|not_specified","relationship":"...","connection":"...","evidence":[{"source_url":"...","section":"...","quote":"verbatim quote"}],"notes":"..."}],"unresolved":["..."]}
Scoped inventory:
%s
Verified candidate relationships:
%s`, inventoryJSON, draftJSON)
	_, err = experiment.runtime.Query(ctx, result.ReportingQuery, []string{reportingSquadID}, reportingPrompt, experiment.cfg.QueryTimeout)
	if err != nil {
		appendManualIssue(&inventory, "reporting", "", err)
	}
	result.Inventory = inventory
	result.AnalysisInventory = analysisInventory
	result.Matrix, result.BlindSpots = buildCompatibilityMatrix(analysisInventory, result.Evidence, result.Draft, candidates)
	result.Report = formatAuditReport(inventory, result.Matrix, result.BlindSpots, experiment.cfg.Scope, len(candidates))
	return result, nil
}

func primaryModel(cfg config) string {
	if len(cfg.Models) == 0 {
		return "mock-model"
	}
	return cfg.Models[0]
}

func manualExperimentDefinitions(crawler *manualCrawler, memory *manualMemory, verifierModel string) []squads.SquadDefinition {
	const readerCount = 4
	const verifierCount = 4
	navigatorTools := manualTools(crawler, memory, 0, 1)
	definitions := []squads.SquadDefinition{
		{
			ID: discoverySquadID, Name: "Manual Discovery", Description: "Builds the exact manual-to-product inventory from the public index.",
			Agents: []squads.AgentDefinition{
				{ID: "web-navigator", Type: "WEB_NAVIGATOR", Description: "Scrapes the full manuals navigation tree and expands version selectors.", SystemPrompt: "MANUAL_NAVIGATOR\nYou are the Web Navigator. Use only the registered scraper tool, preserve collapsed and discontinued sections, expand concrete versions, and preserve blocked links as issues.", Tools: toolSubset(navigatorTools, "scrape_manual_index")},
			},
		},
		{
			ID: ingestionSquadID, Name: "Manual Ingestion", Description: "Reads manual shards concurrently and extracts cited technical evidence.",
			Agents: makeManualReaderDefinitions(crawler, memory, readerCount),
		},
		{
			ID: synthesisSquadID, Name: "Compatibility Verification", Description: "Verifies bounded product/version candidate relationships against Embention manual evidence.",
			Agents: makeManualVerifierDefinitions(crawler, memory, verifierModel, verifierCount),
		},
		{
			ID: reportingSquadID, Name: "Audit Reporting", Description: "Formats the validated mapping, matrix, and blind spots for the user.",
			Agents: []squads.AgentDefinition{
				{ID: "data-formatter", Type: "DATA_FORMATTER", Description: "Audits the verified candidate mapping without dropping relationships.", SystemPrompt: "MANUAL_REPORTER\nYou are the data formatter. Do not delegate. Preserve categories, manual types, exact versions, direct section URLs, every verified relationship, and return only the requested JSON."},
			},
		},
	}
	return definitions
}

func makeManualReaderDefinitions(crawler *manualCrawler, memory *manualMemory, readerCount int) []squads.AgentDefinition {
	definitions := make([]squads.AgentDefinition, 0, readerCount)
	for readerIndex := 0; readerIndex < readerCount; readerIndex++ {
		readerTools := manualTools(crawler, memory, readerIndex, readerCount)
		definitions = append(definitions, squads.AgentDefinition{
			ID:           fmt.Sprintf("technical-reader-%02d", readerIndex+1),
			Type:         "TECHNICAL_READER",
			Description:  fmt.Sprintf("Reads manual shard %d of %d and extracts direct evidence.", readerIndex+1, readerCount),
			SystemPrompt: fmt.Sprintf("MANUAL_READER\nYou are technical reader shard %d of %d. Use only the assigned-manuals tool once, inspect the versioned evidence sections it returns, and return one cited fact per assigned manual. Never use web search or external sources.", readerIndex+1, readerCount),
			Tools:        toolSubset(readerTools, "read_assigned_manuals"),
		})
	}
	return definitions
}

func makeManualVerifierDefinitions(crawler *manualCrawler, memory *manualMemory, model string, verifierCount int) []squads.AgentDefinition {
	definitions := make([]squads.AgentDefinition, 0, verifierCount)
	for verifierIndex := 0; verifierIndex < verifierCount; verifierIndex++ {
		verifierTools := manualTools(crawler, memory, verifierIndex, verifierCount)
		definitions = append(definitions, squads.AgentDefinition{
			ID:            fmt.Sprintf("compatibility-verifier-%02d", verifierIndex+1),
			Type:          "COMPATIBILITY_VERIFIER",
			Description:   fmt.Sprintf("Verifies candidate relationship shard %d of %d against direct Embention evidence.", verifierIndex+1, verifierCount),
			SystemPrompt:  fmt.Sprintf("MANUAL_VERIFIER\nYou are compatibility verifier shard %d of %d. First call delegate_transversal_%s with {\"query\":\"all\"}. After the shared evidence reply, call verify_candidate_edges exactly once. Verify every assigned candidate independently, preserve all candidates in the JSON response, and use only direct Embention manual evidence.", verifierIndex+1, verifierCount, manualEvidenceCapability),
			Model:         model,
			ResumeWithLLM: true,
			Tools:         toolSubset(verifierTools, "verify_candidate_edges"),
		})
	}
	return definitions
}

func toolSubset(tools map[string]squads.LocalTool, names ...string) map[string]squads.LocalTool {
	selected := make(map[string]squads.LocalTool, len(names))
	for _, name := range names {
		if tool, ok := tools[name]; ok {
			selected[name] = tool
		}
	}
	return selected
}

func legacyExperimentDefinitions() []squads.SquadDefinition {
	return []squads.SquadDefinition{
		{
			ID: "governance-compliance", Name: "Governance and Compliance", Description: "Researches governance frameworks, accountability, regulation, and legal risk.",
			Agents: []squads.AgentDefinition{
				{ID: "governance-researcher", Type: "GOVERNANCE", Description: "Finds AI governance and risk-management frameworks.", SystemPrompt: webPrompt("GOVERNANCE", "Research AI governance, accountability, portfolio controls, NIST AI RMF, ISO/IEC 42001, OECD guidance, and board-level oversight.")},
				{ID: "compliance-researcher", Type: "COMPLIANCE", Description: "Finds regulatory and legal implementation guidance.", SystemPrompt: webPrompt("COMPLIANCE", "Research applicable AI regulation, privacy, intellectual property, procurement, documentation, auditability, and human-oversight obligations for technology companies.")},
				{ID: "assurance-researcher", Type: "ASSURANCE", Description: "Finds audit, accountability, and evidence requirements.", SystemPrompt: webPrompt("ASSURANCE", "Research AI assurance cases, audit evidence, model inventories, incident registers, documentation controls, and accountability practices for technology companies.")},
			},
		},
		{
			ID: "adoption-workforce", Name: "Adoption and Workforce", Description: "Researches organizational change, operating models, skills, and employee impact.",
			Agents: []squads.AgentDefinition{
				{ID: "change-researcher", Type: "CHANGE", Description: "Studies organizational adoption and change-management failures.", SystemPrompt: webPrompt("CHANGE", "Research organizational adoption, operating-model design, stakeholder engagement, pilot selection, process redesign, and common AI transformation failure modes.")},
				{ID: "workforce-researcher", Type: "WORKFORCE", Description: "Studies skills, training, job impact, and responsible workforce transition.", SystemPrompt: webPrompt("WORKFORCE", "Research AI literacy, role redesign, employee participation, training, psychological safety, productivity effects, deskilling, and workforce-transition controls.")},
				{ID: "portfolio-researcher", Type: "PORTFOLIO", Description: "Studies adoption portfolio choices and operating-model sequencing.", SystemPrompt: webPrompt("PORTFOLIO", "Research AI use-case selection, portfolio management, benefit realization, stakeholder alignment, and governance gates for enterprise technology teams.")},
			},
		},
		{
			ID: "security-measurement", Name: "Security and Measurement", Description: "Researches technical controls, data governance, monitoring, ROI, and impact measurement.",
			Agents: []squads.AgentDefinition{
				{ID: "security-researcher", Type: "SECURITY", Description: "Studies AI security, privacy, data quality, and operational controls.", SystemPrompt: webPrompt("SECURITY", "Research secure AI lifecycle practices, data governance, model and prompt attacks, supply-chain risk, privacy, incident response, monitoring, and rollback controls.")},
				{ID: "measurement-researcher", Type: "MEASUREMENT", Description: "Studies business value, risk indicators, and impact measurement.", SystemPrompt: webPrompt("MEASUREMENT", "Research how organizations measure AI value, cost, quality, safety, fairness, adoption, productivity, environmental impact, and unintended consequences before and after deployment.")},
				{ID: "data-governance-researcher", Type: "DATA_GOVERNANCE", Description: "Studies data lineage, quality, retention, and access controls.", SystemPrompt: webPrompt("DATA_GOVERNANCE", "Research data lineage, consent, quality gates, retention, access control, data contracts, and dataset monitoring for AI systems.")},
			},
		},
		{
			ID: "delivery-operations", Name: "Delivery and Operations", Description: "Researches implementation delivery, platform operations, and product controls.",
			Agents: []squads.AgentDefinition{
				{ID: "platform-researcher", Type: "PLATFORM", Description: "Studies platform architecture and operational reliability.", SystemPrompt: webPrompt("PLATFORM", "Research enterprise AI platform architecture, model routing, reliability, observability, cost controls, and service ownership.")},
				{ID: "product-researcher", Type: "PRODUCT", Description: "Studies product discovery, user experience, and release controls.", SystemPrompt: webPrompt("PRODUCT", "Research AI product discovery, user testing, human-in-the-loop design, release criteria, feedback loops, and safe product operations.")},
				{ID: "incident-researcher", Type: "INCIDENT", Description: "Studies operational incidents, monitoring, and rollback practices.", SystemPrompt: webPrompt("INCIDENT", "Research AI incident management, red teaming, monitoring thresholds, kill switches, rollback planning, and post-incident learning.")},
			},
		},
		{
			ID: "evidence-review", Name: "Evidence Review", Description: "Reviews evidence quality and implementation readiness before drafting.",
			Agents: []squads.AgentDefinition{
				{ID: "evidence-auditor", Type: "EVIDENCE_AUDITOR", Description: "Finds unsupported claims and weak evidence.", SystemPrompt: "You are the evidence auditor. Review the supplied dossier, identify unsupported claims or weak sources, and preserve only defensible recommendations."},
				{ID: "implementation-reviewer", Type: "IMPLEMENTATION_REVIEWER", Description: "Builds a practical implementation sequence from reviewed evidence.", SystemPrompt: "You are the implementation reviewer. Review the supplied dossier, identify missing controls, and produce a phased implementation sequence grounded in the available evidence."},
			},
		},
		{
			ID: "article-production", Name: "Article Production", Description: "Produces the final evidence-based article.",
			Agents: []squads.AgentDefinition{
				{ID: "article-writer", Type: "ARTICLE_WRITER", Description: "Writes a practical, cited article from the reviewed evidence.", SystemPrompt: articleWriterPrompt},
			},
		},
	}
}

func researchDossier(history []synapse.SynapseMessage, heading string) string {
	responses := make([]string, 0, len(history))
	for _, message := range history {
		if message.Role != synapse.RoleAssistant {
			continue
		}
		content := strings.TrimSpace(message.Content())
		if content != "" {
			responses = append(responses, content)
		}
	}
	if len(responses) == 0 {
		return ""
	}
	if len(responses) == 1 && heading == "" {
		return responses[0]
	}

	var dossier strings.Builder
	if heading != "" {
		fmt.Fprintf(&dossier, "# %s\n", heading)
	}
	for index, response := range responses {
		fmt.Fprintf(&dossier, "\n## Evidence stream %d\n%s\n", index+1, response)
	}
	return strings.TrimSpace(dossier.String())
}

func webPrompt(marker, specialty string) string {
	return webResearchMarker + "\nYou are the " + marker + " specialist in a multi-agent research team. " + specialty + " Use current web research. Return a concise Spanish evidence brief with inline source URLs and a final bibliography. Never invent a source."
}

const articleWriterPrompt = `You are the ARTICLE_WRITER in a multi-squad research and review workflow. Write a polished Spanish article for leaders of a technology company.

Required structure:
1. Title and executive summary.
2. Why AI implementations fail.
3. The most common strategic, organizational, data, security, legal, and measurement mistakes.
4. A phased implementation playbook: discovery, governance, pilot, validation, deployment, monitoring, and retirement.
5. Roles and decision rights.
6. A practical control and KPI checklist.
7. Conclusions.
8. Bibliography grouped by standards, academic research, regulators, and industry evidence.

Preserve direct source URLs from the dossier. Do not invent citations or factual claims. Explicitly label recommendations that are not directly supported by a cited source.`
