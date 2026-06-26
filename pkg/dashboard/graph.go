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

	userNodeID := "user:" + correlationID
	ensureNode(userNodeID, "User Query", "user")

	for _, step := range steps {
		// Each step mutates the graph projection only; it does not feed back into
		// orchestration, so there is no loop prevention/depth enforcement here.
		var squadNodeID string
		if step.SquadID != "" {
			squadNodeID = "squad:" + step.SquadID
			ensureNode(squadNodeID, step.SquadID, "squad")
		}

		var agentNodeID string
		if step.AgentID != "" {
			nodeType := "agent"
			if strings.Contains(strings.ToLower(step.AgentType), "transversal") {
				nodeType = "transversal"
			}
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
			ensureEdge(agentNodeID, userNodeID, "reply", "response")
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

// BuildMetricsSummary derives dashboard summary cards from a query timeline.
// It summarizes observed execution data; it is not used as a control signal.
func BuildMetricsSummary(correlationID string, steps []observability.AgentStep) MetricsSummary {
	summary := MetricsSummary{CorrelationID: correlationID, TotalSteps: len(steps)}
	uniqueAgents := map[string]struct{}{}
	var earliest, latest time.Time
	for _, step := range steps {
		if step.AgentID != "" {
			uniqueAgents[step.AgentID] = struct{}{}
		}
		summary.TotalTokensIn += step.TokensIn
		summary.TotalTokensOut += step.TokensOut
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
	summary.UniqueAgents = len(uniqueAgents)
	if !earliest.IsZero() && !latest.IsZero() && latest.After(earliest) {
		summary.DurationMS = latest.Sub(earliest).Milliseconds()
	}
	return summary
}
