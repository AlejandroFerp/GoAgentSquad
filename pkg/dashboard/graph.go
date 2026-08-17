package dashboard

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/embention/agent-squad-go/pkg/observability"
)

// BuildGraph projects the step timeline into a user/squad/agent/tool graph.
// This function is read-model code for the dashboard only: it does not execute
// agents, control flow, or enforce depth limits between agents.
func BuildGraph(correlationID string, steps []observability.AgentStep) GraphModel {
	nodes := map[string]*GraphNode{}
	edges := map[string]*GraphEdge{}
	ensureNode := func(id, label, nodeType string) *GraphNode {
		if node, exists := nodes[id]; exists {
			return node
		}
		node := &GraphNode{ID: id, Label: label, Type: nodeType, Status: "idle"}
		nodes[id] = node
		return node
	}
	ensureEdge := func(source, target, kind, label string) {
		// Ignore invalid/self links in the visualization model.
		if source == "" || target == "" || source == target {
			return
		}
		id := fmt.Sprintf("%s|%s|%s|%s", source, target, kind, label)
		if edge, exists := edges[id]; exists {
			edge.Count++
			return
		}
		edges[id] = &GraphEdge{ID: id, Source: source, Target: target, Kind: kind, Label: label, Count: 1}
	}
	setStatus := func(node *GraphNode, step observability.AgentStep) {
		// Status here is purely visual and derived from observed steps.
		if node == nil {
			return
		}
		node.Calls++
		node.TokensIn += step.TokensIn
		node.TokensOut += step.TokensOut
		switch {
		case step.Kind == observability.StepError || step.Error != "":
			node.Status = "error"
		case node.Status == "error":
			return
		case step.Kind == observability.StepResponded || step.Kind == observability.StepSynthesis || step.Kind == observability.StepQuiesced:
			node.Status = "done"
		case node.Status != "done":
			node.Status = "running"
		}
	}
	setSquadStatus := func(node *GraphNode, step observability.AgentStep) {
		if node == nil || node.Status == "error" {
			return
		}
		switch {
		case step.Kind == observability.StepError || step.Error != "":
			node.Status = "error"
		case step.Kind == observability.StepSynthesis && step.AgentID == step.SquadID+"-coordinator":
			node.Status = "done"
		case node.Status != "done":
			node.Status = "running"
		}
	}

	userNodeID := "user:" + correlationID
	ensureNode(userNodeID, "User Query", "user")

	for _, step := range steps {
		// Each step mutates the graph projection only; it does not feed back into
		// orchestration, so there is no loop prevention/depth enforcement here.
		var squadNodeID string
		if step.SquadID != "" {
			squadNodeID = "squad:" + step.SquadID
			setSquadStatus(ensureNode(squadNodeID, step.SquadID, "squad"), step)
		}

		var agentNodeID string
		if step.AgentID != "" {
			nodeType := graphAgentNodeType(step)
			agentNodeID = "agent:" + step.AgentID
			agentNode := ensureNode(agentNodeID, step.AgentID, nodeType)
			setStatus(agentNode, step)
			if squadNodeID != "" && step.Kind == observability.StepAgentStarted {
				ensureEdge(squadNodeID, agentNodeID, "message", "runs")
			}
		}

		if step.Kind == observability.StepRouted {
			// Routed squads are inferred from the step summary text emitted by the pipeline.
			prefix := "routed to squads: "
			if strings.HasPrefix(step.Summary, prefix) {
				for _, squadID := range strings.Split(strings.TrimPrefix(step.Summary, prefix), ",") {
					squadID = strings.TrimSpace(squadID)
					if squadID == "" {
						continue
					}
					target := "squad:" + squadID
					ensureNode(target, squadID, "squad")
					ensureEdge(userNodeID, target, "message", "route")
				}
			}
		}

		if step.ToolName != "" {
			toolNodeID := "tool:" + step.ToolName
			toolNode := ensureNode(toolNodeID, step.ToolName, "tool")
			setStatus(toolNode, step)
			if agentNodeID != "" {
				kind := "tool_call"
				if step.Kind == observability.StepDelegated {
					kind = "delegate"
				}
				ensureEdge(agentNodeID, toolNodeID, kind, step.ToolName)
			}
		}

		if step.Kind == observability.StepResponded && agentNodeID != "" {
			if squadNodeID != "" {
				ensureEdge(agentNodeID, squadNodeID, "message", "result")
			} else {
				ensureEdge(agentNodeID, userNodeID, "reply", "response")
			}
		}

		if step.Kind == observability.StepSynthesis && squadNodeID != "" && step.AgentID == step.SquadID+"-coordinator" {
			if agentNodeID != "" {
				ensureEdge(squadNodeID, agentNodeID, "coordination", "synthesize")
				ensureEdge(agentNodeID, userNodeID, "summary", "summary")
			} else {
				ensureEdge(squadNodeID, userNodeID, "summary", "summary")
			}
		}
	}
	if terminal, ok := graphRootTerminalStep(correlationID, steps); ok {
		userNode := ensureNode(userNodeID, "User Query", "user")
		terminalStatus := graphTerminalStatus(terminal)
		userNode.Status = terminalStatus
		if terminalStatus == "done" {
			// A root terminal event proves the pipeline quiesced. Single-agent
			// squads can reach this state without a separate coordinator synthesis.
			for _, node := range nodes {
				if node.Type == "squad" && node.Status != "error" {
					node.Status = "done"
				}
			}
		}
	}

	nodeList := make([]GraphNode, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, *node)
	}
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].ID < nodeList[j].ID })

	edgeList := make([]GraphEdge, 0, len(edges))
	for _, edge := range edges {
		edgeList = append(edgeList, *edge)
	}
	sort.Slice(edgeList, func(i, j int) bool { return edgeList[i].ID < edgeList[j].ID })

	return GraphModel{CorrelationID: correlationID, Nodes: nodeList, Edges: edgeList}
}

func graphRootTerminalStep(correlationID string, steps []observability.AgentStep) (observability.AgentStep, bool) {
	if len(steps) == 0 {
		return observability.AgentStep{}, false
	}
	last := steps[len(steps)-1]
	if last.ThreadID != correlationID {
		return observability.AgentStep{}, false
	}
	switch last.Kind {
	case observability.StepResponded, observability.StepQuiesced, observability.StepError:
		return last, true
	default:
		return observability.AgentStep{}, last.Error != ""
	}
}

func graphTerminalStatus(step observability.AgentStep) string {
	if step.Kind == observability.StepError || step.Error != "" {
		return "error"
	}
	return "done"
}

func graphAgentNodeType(step observability.AgentStep) string {
	if strings.EqualFold(step.AgentType, "COORDINATOR") || strings.HasSuffix(step.AgentID, "-coordinator") {
		return "coordinator"
	}
	if strings.Contains(strings.ToLower(step.AgentType), "transversal") {
		return "transversal"
	}
	return "agent"
}

// BuildWorkflowGraph combines query graphs into an aggregate execution view.
func BuildWorkflowGraph(stages []WorkflowStage) GraphModel {
	orderedStages := append([]WorkflowStage(nil), stages...)
	sort.SliceStable(orderedStages, func(i, j int) bool {
		if orderedStages[i].Summary.StartedAt.Equal(orderedStages[j].Summary.StartedAt) {
			return orderedStages[i].Summary.CorrelationID < orderedStages[j].Summary.CorrelationID
		}
		return orderedStages[i].Summary.StartedAt.Before(orderedStages[j].Summary.StartedAt)
	})
	nodes := map[string]GraphNode{
		"workflow:root": {
			ID:     "workflow:root",
			Label:  "Whole workflow",
			Type:   "workflow",
			Status: workflowStatus(orderedStages),
		},
	}
	edges := map[string]GraphEdge{}
	for index, stage := range orderedStages {
		phaseID := "phase:" + stage.Summary.CorrelationID
		nodes[phaseID] = GraphNode{
			ID:     phaseID,
			Label:  workflowStageLabel(stage.Summary.Summary, index+1),
			Type:   "phase",
			Status: queryStatus(stage.Summary.Status),
		}
		mergeGraphEdge(edges, GraphEdge{
			Source: "workflow:root",
			Target: phaseID,
			Kind:   "workflow",
			Label:  "phase",
			Count:  1,
		})

		queryGraph := BuildGraph(stage.Summary.CorrelationID, stage.Timeline)
		queryUserID := "user:" + stage.Summary.CorrelationID
		for _, node := range queryGraph.Nodes {
			if node.ID == queryUserID {
				continue
			}
			mergeGraphNode(nodes, node)
		}
		for _, edge := range queryGraph.Edges {
			if edge.Source == queryUserID {
				edge.Source = phaseID
			}
			if edge.Target == queryUserID {
				edge.Target = phaseID
			}
			mergeGraphEdge(edges, edge)
		}
	}
	if terminalStatus, terminal := workflowTerminalState(orderedStages); terminal {
		lastStage := orderedStages[len(orderedStages)-1]
		terminalID := "terminal:workflow"
		terminalLabel := "Workflow complete"
		if terminalStatus == "error" {
			terminalLabel = "Workflow stopped"
		}
		nodes[terminalID] = GraphNode{
			ID:     terminalID,
			Label:  terminalLabel,
			Type:   "terminal",
			Status: terminalStatus,
		}
		mergeGraphEdge(edges, GraphEdge{
			Source: "phase:" + lastStage.Summary.CorrelationID,
			Target: terminalID,
			Kind:   "completion",
			Label:  "complete",
			Count:  1,
		})
	}

	nodeList := make([]GraphNode, 0, len(nodes))
	for _, node := range nodes {
		nodeList = append(nodeList, node)
	}
	sort.Slice(nodeList, func(i, j int) bool { return nodeList[i].ID < nodeList[j].ID })
	edgeList := make([]GraphEdge, 0, len(edges))
	for _, edge := range edges {
		edgeList = append(edgeList, edge)
	}
	sort.Slice(edgeList, func(i, j int) bool { return edgeList[i].ID < edgeList[j].ID })
	return GraphModel{CorrelationID: WorkflowCorrelationID, Nodes: nodeList, Edges: edgeList}
}

func mergeGraphNode(nodes map[string]GraphNode, node GraphNode) {
	existing, found := nodes[node.ID]
	if !found {
		nodes[node.ID] = node
		return
	}
	existing.Calls += node.Calls
	existing.TokensIn += node.TokensIn
	existing.TokensOut += node.TokensOut
	if graphStatusRank(node.Status) > graphStatusRank(existing.Status) {
		existing.Status = node.Status
	}
	nodes[node.ID] = existing
}

func mergeGraphEdge(edges map[string]GraphEdge, edge GraphEdge) {
	edge.ID = fmt.Sprintf("%s|%s|%s|%s", edge.Source, edge.Target, edge.Kind, edge.Label)
	existing, found := edges[edge.ID]
	if found {
		existing.Count += edge.Count
		edges[edge.ID] = existing
		return
	}
	edges[edge.ID] = edge
}

func workflowStatus(stages []WorkflowStage) string {
	if status, terminal := workflowTerminalState(stages); terminal {
		return status
	}
	for _, stage := range stages {
		if queryStatus(stage.Summary.Status) == "running" {
			return "running"
		}
	}
	return "idle"
}

func workflowTerminalState(stages []WorkflowStage) (string, bool) {
	if len(stages) == 0 {
		return "idle", false
	}
	hasRunning := false
	hasIdle := false
	hasError := false
	for _, stage := range stages {
		switch queryStatus(stage.Summary.Status) {
		case "error":
			hasError = true
		case "running":
			hasRunning = true
		case "idle":
			hasIdle = true
		}
	}
	if hasError {
		return "error", !hasRunning && !hasIdle
	}
	if hasRunning || hasIdle {
		return "running", false
	}
	return "done", true
}

func queryStatus(status string) string {
	switch status {
	case "error":
		return "error"
	case "running":
		return "running"
	case "done":
		return "done"
	default:
		return "idle"
	}
}

func graphStatusRank(status string) int {
	switch status {
	case "error":
		return 3
	case "running":
		return 2
	case "done":
		return 1
	default:
		return 0
	}
}

func workflowStageLabel(summary string, phaseNumber int) string {
	const maxRunes = 32
	label := fmt.Sprintf("Phase %d", phaseNumber)
	if summary == "" {
		return label
	}
	runes := []rune(summary)
	if len(runes) > maxRunes {
		summary = string(runes[:maxRunes]) + "..."
	}
	return label + ": " + summary
}

// BuildMetricsSummary derives dashboard summary cards from a query timeline.
// It summarizes observed execution data; it is not used as a control signal.
func BuildMetricsSummary(correlationID string, steps []observability.AgentStep) MetricsSummary {
	summary := MetricsSummary{CorrelationID: correlationID, TotalSteps: len(steps), BudgetStatus: budgetStatusUnavailable}
	uniqueAgents := map[string]struct{}{}
	var earliest, latest time.Time
	usageByCorrelation := map[string]observedUsage{}
	budgetByCorrelation := map[string]observability.ExecutionBudgetSnapshot{}
	for _, step := range steps {
		stepCorrelationID := step.CorrelationID
		if stepCorrelationID == "" {
			stepCorrelationID = correlationID
		}
		usage := usageByCorrelation[stepCorrelationID]
		if step.AgentID != "" {
			uniqueAgents[step.AgentID] = struct{}{}
		}
		summary.TotalTokensIn += step.TokensIn
		summary.TotalTokensOut += step.TokensOut
		usage.totalTokens += step.TokensIn + step.TokensOut
		if step.Kind == observability.StepLLMCall {
			costUSD := step.CostUSD
			if costUSD == 0 && step.LLMTrace != nil {
				costUSD = step.LLMTrace.CostUSD
			}
			summary.TotalCostUSD += costUSD
			usage.costUSD += costUSD
		}
		usageByCorrelation[stepCorrelationID] = usage
		if step.Budget != nil {
			current, exists := budgetByCorrelation[stepCorrelationID]
			if !exists || step.Budget.UsageSequence >= current.UsageSequence {
				budgetByCorrelation[stepCorrelationID] = *step.Budget
			}
		}
		if step.Kind == observability.StepLLMCall {
			summary.LLMCalls++
		}
		if step.Kind == observability.StepToolCall || step.Kind == observability.StepDelegated {
			summary.ToolCalls++
		}
		if step.Kind == observability.StepError || step.Error != "" {
			summary.Errors++
		}
		if earliest.IsZero() || (!step.StartedAt.IsZero() && step.StartedAt.Before(earliest)) {
			earliest = step.StartedAt
		}
		finish := step.FinishedAt
		if finish.IsZero() {
			finish = step.StartedAt
		}
		if finish.After(latest) {
			latest = finish
		}
	}
	summary.TotalTokens = summary.TotalTokensIn + summary.TotalTokensOut
	if correlationID == WorkflowCorrelationID {
		applyWorkflowBudgetSummary(&summary, usageByCorrelation, budgetByCorrelation)
	} else if budget, exists := budgetByCorrelation[correlationID]; exists {
		applyExecutionBudgetSummary(&summary, budget)
	}
	summary.UniqueAgents = len(uniqueAgents)
	if !earliest.IsZero() && !latest.IsZero() && latest.After(earliest) {
		summary.DurationMS = latest.Sub(earliest).Milliseconds()
	}
	return summary
}

type observedUsage struct {
	totalTokens int
	costUSD     float64
}

// Projection-only budget statuses. They never appear in a recorded snapshot.
const (
	budgetStatusUnavailable = "unavailable"
	budgetStatusMixed       = "mixed"
)

func applyExecutionBudgetSummary(summary *MetricsSummary, budget observability.ExecutionBudgetSnapshot) {
	summary.TotalTokens = budget.TotalTokens
	summary.TotalCostUSD = budget.CostUSD
	summary.MaxTotalTokens = budget.MaxTotalTokens
	summary.MaxCostUSD = budget.MaxCostUSD
	summary.BudgetStatus = budget.Status
}

func applyWorkflowBudgetSummary(summary *MetricsSummary, usageByCorrelation map[string]observedUsage, budgetByCorrelation map[string]observability.ExecutionBudgetSnapshot) {
	if len(budgetByCorrelation) == 0 {
		return
	}

	totalTokens := 0
	totalCostUSD := 0.0
	for correlationID, usage := range usageByCorrelation {
		if budget, exists := budgetByCorrelation[correlationID]; exists {
			totalTokens += budget.TotalTokens
			totalCostUSD += budget.CostUSD
			continue
		}
		totalTokens += usage.totalTokens
		totalCostUSD += usage.costUSD
	}
	for correlationID, budget := range budgetByCorrelation {
		if _, exists := usageByCorrelation[correlationID]; exists {
			continue
		}
		totalTokens += budget.TotalTokens
		totalCostUSD += budget.CostUSD
	}

	summary.TotalTokens = totalTokens
	summary.TotalCostUSD = totalCostUSD
	summary.BudgetStatus = workflowBudgetStatus(budgetByCorrelation)
	if len(budgetByCorrelation) != len(usageByCorrelation) {
		if summary.BudgetStatus != observability.BudgetStatusExceeded && summary.BudgetStatus != observability.BudgetStatusExhausted {
			summary.BudgetStatus = budgetStatusMixed
		}
		return
	}

	totalTokenLimit := 0
	totalCostLimit := 0.0
	for _, budget := range budgetByCorrelation {
		if budget.MaxTotalTokens == 0 {
			totalTokenLimit = 0
			break
		}
		totalTokenLimit += budget.MaxTotalTokens
	}
	for _, budget := range budgetByCorrelation {
		if budget.MaxCostUSD == 0 {
			totalCostLimit = 0
			break
		}
		totalCostLimit += budget.MaxCostUSD
	}
	summary.MaxTotalTokens = totalTokenLimit
	summary.MaxCostUSD = totalCostLimit
}

func workflowBudgetStatus(budgetByCorrelation map[string]observability.ExecutionBudgetSnapshot) string {
	statuses := map[string]struct{}{}
	for _, budget := range budgetByCorrelation {
		statuses[budget.Status] = struct{}{}
	}
	if len(statuses) == 1 {
		for status := range statuses {
			return status
		}
	}
	if _, exists := statuses[observability.BudgetStatusExceeded]; exists {
		return observability.BudgetStatusExceeded
	}
	if _, exists := statuses[observability.BudgetStatusExhausted]; exists {
		return observability.BudgetStatusExhausted
	}
	return budgetStatusMixed
}

func addHubMetrics(summary MetricsSummary, hub *observability.Hub) MetricsSummary {
	if hub == nil {
		return summary
	}
	stats := hub.Stats()
	summary.SSEDropped = stats.DroppedEvents
	summary.SSESubscribers = stats.Subscribers
	summary.SSEMaxClients = stats.MaxSubscribers
	return summary
}
