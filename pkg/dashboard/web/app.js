const stepEvents = [
  "query_received",
  "routed",
  "agent_started",
  "llm_call",
  "tool_call",
  "delegated",
  "reply_received",
  "synthesis",
  "responded",
  "quiesced",
  "error",
];

let selectedQuery = null;
let graphScale = 1;
let selectionVersion = 0;
let refreshTimer = null;
const workflowSelection = "__workflow__";
const maxLogEntries = 300;
const logEntries = [];
const loggedStepIDs = new Set();
let currentTimeline = [];

function byId(id) {
  return document.getElementById(id);
}

function pretty(obj) {
  return JSON.stringify(obj, null, 2);
}

function formatEventName(kind) {
  return (kind || "activity").replaceAll("_", " ");
}

function setStreamState(state, label) {
  const status = byId("stream-status");
  status.className = `stream-status is-${state}`;
  byId("stream-state").textContent = label;
}

function markUpdated(element) {
  element.classList.remove("has-update");
  requestAnimationFrame(() => element.classList.add("has-update"));
}

function inspectValue(value) {
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", "Event payload");
  appendTextElement(inspector, "pre", "inspection-payload", pretty(value));
  openDrawer("inspector");
}

function resetInspector() {
  const inspector = byId("inspector");
  inspector.replaceChildren();
  return inspector;
}

function appendInspectionSection(parent, title) {
  const section = document.createElement("section");
  section.className = "inspection-section";
  appendTextElement(section, "h3", "inspection-title", title);
  parent.appendChild(section);
  return section;
}

function appendInspectionFields(parent, fields) {
  const list = document.createElement("dl");
  list.className = "inspection-fields";
  for (const [label, value] of fields) {
    if (value === undefined || value === null || value === "") {
      continue;
    }
    appendTextElement(list, "dt", "", label);
    appendTextElement(list, "dd", "", String(value));
  }
  parent.appendChild(list);
}

function appendInspectionDetails(parent, title, content, open = false) {
  const details = document.createElement("details");
  details.className = "inspection-details";
  details.open = open;
  appendTextElement(details, "summary", "", title);
  appendTextElement(details, "pre", "inspection-payload", content || "No content recorded.");
  parent.appendChild(details);
}

function formatCost(cost) {
  return Number.isFinite(cost) && cost > 0 ? `$${cost.toFixed(6)}` : "--";
}

function appendLLMCallInspection(parent, step, callNumber) {
  const call = document.createElement("article");
  call.className = "inspection-call";
  const heading = document.createElement("header");
  appendTextElement(heading, "strong", "", `LLM call ${callNumber}`);
  appendTextElement(heading, "span", "", `${step.model || "model unavailable"} · ${step.duration_ms || 0} ms`);
  call.appendChild(heading);

  const trace = step.llm_trace;
  if (!trace) {
    appendTextElement(call, "p", "inspection-warning", "Request and completion content was not captured for this call. Enable capture for a new execution to inspect it.");
    parent.appendChild(call);
    return;
  }

  appendInspectionFields(call, [
    ["Provider", trace.provider],
    ["Request ID", trace.request_id],
    ["Generation ID", trace.generation_id],
    ["Finish reason", trace.finish_reason],
    ["Latency", `${step.duration_ms || 0} ms`],
    ["Tokens", `${step.tokens_in || 0} in / ${step.tokens_out || 0} out`],
    ["Reasoning tokens", trace.reasoning_tokens || 0],
    ["Cost", formatCost(trace.cost_usd)],
  ]);
  appendInspectionDetails(call, "System prompt", trace.system_prompt, true);
  const messages = (trace.messages || [])
    .map((message, index) => `${index + 1}. ${message.role || "message"}\n${message.content || ""}`)
    .join("\n\n");
  appendInspectionDetails(call, "Prompt messages", messages, true);
  appendInspectionDetails(call, "Completion", trace.completion, true);
  parent.appendChild(call);
}

function inspectAgentNode(node) {
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", node.label);
  const steps = currentTimeline.filter((step) => step.agent_id === node.label);
  const overview = appendInspectionSection(inspector, "Agent overview");
  appendInspectionFields(overview, [
    ["Agent ID", node.label],
    ["Type", node.type],
    ["Status", node.status],
    ["Observed calls", node.calls],
    ["Input tokens", node.tokens_in || 0],
    ["Output tokens", node.tokens_out || 0],
  ]);

  const communication = appendInspectionSection(inspector, "Observed communication");
  const squadID = steps.find((step) => step.squad_id)?.squad_id;
  const publishedResults = steps.filter((step) => step.kind === "responded").length;
  const delegations = steps.filter((step) => step.kind === "delegated").length;
  appendInspectionFields(communication, [
    ["Squad", squadID],
    ["Published results", publishedResults],
    ["Delegations", delegations],
  ]);
  appendTextElement(
    communication,
    "p",
    "inspection-note",
    delegations
      ? "This agent delegated work through the recorded tool events below."
      : "No direct agent-to-agent delegation was observed. This agent worked independently and published its result to the squad.",
  );

  const events = appendInspectionSection(inspector, `Observed execution (${steps.length} events)`);
  if (!steps.length) {
    appendTextElement(events, "p", "inspection-warning", "No timeline events are available for this node in the selected scope.");
  } else {
    const eventList = document.createElement("div");
    eventList.className = "inspection-events";
    for (const step of steps) {
      const event = document.createElement("article");
      event.className = `inspection-event${step.error ? " is-error" : ""}`;
      appendTextElement(event, "strong", "", formatEventName(step.kind));
      appendTextElement(event, "span", "", `${formatLogTime(step.started_at)} · ${step.duration_ms || 0} ms`);
      appendTextElement(event, "p", "", step.error || step.summary || "Event recorded");
      eventList.appendChild(event);
    }
    events.appendChild(eventList);
  }

  const llmCalls = steps.filter((step) => step.kind === "llm_call");
  const calls = appendInspectionSection(inspector, `LLM trace (${llmCalls.length} calls)`);
  if (!llmCalls.length) {
    appendTextElement(calls, "p", "inspection-warning", "This agent did not make an LLM call in the selected scope.");
  } else {
    llmCalls.forEach((step, index) => appendLLMCallInspection(calls, step, index + 1));
  }
  openDrawer("inspector");
}

function inspectSquadNode(node) {
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", node.label);
  const steps = currentTimeline.filter((step) => step.squad_id === node.label);
  const agents = [...new Set(steps.map((step) => step.agent_id).filter(Boolean))];
  const publishedResults = steps.filter((step) => step.kind === "responded").length;
  const summaries = steps.filter((step) => step.kind === "synthesis").length;
  const delegations = steps.filter((step) => step.kind === "delegated").length;

  const overview = appendInspectionSection(inspector, "Squad coordination");
  appendInspectionFields(overview, [
    ["Status", node.status],
    ["Participating agents", agents.length],
    ["Published results", publishedResults],
    ["Coordinated summaries", summaries],
    ["Delegations", delegations],
  ]);
  appendTextElement(
    overview,
    "p",
    "inspection-note",
    delegations
      ? "This squad contains recorded delegation events. Inspect the timeline to follow each delegated task."
      : "Agents in this squad worked in parallel, published results to the squad, and the coordinator synthesized them. No direct agent-to-agent delegation was observed.",
  );
  appendInspectionDetails(overview, "Participating agents", agents.join("\n"), true);

  const events = appendInspectionSection(inspector, `Observed execution (${steps.length} events)`);
  if (!steps.length) {
    appendTextElement(events, "p", "inspection-warning", "No timeline events are available for this squad in the selected scope.");
  } else {
    const eventList = document.createElement("div");
    eventList.className = "inspection-events";
    for (const step of steps) {
      const event = document.createElement("article");
      event.className = `inspection-event${step.error ? " is-error" : ""}`;
      appendTextElement(event, "strong", "", formatEventName(step.kind));
      appendTextElement(event, "span", "", `${step.agent_id || "squad"} · ${formatLogTime(step.started_at)}`);
      appendTextElement(event, "p", "", step.error || step.summary || "Event recorded");
      eventList.appendChild(event);
    }
    events.appendChild(eventList);
  }
  openDrawer("inspector");
}

function inspectGraphNode(node) {
  if (node.type === "agent" || node.type === "transversal" || node.type === "coordinator") {
    inspectAgentNode(node);
    return;
  }
	if (node.type === "squad") {
		inspectSquadNode(node);
		return;
	}
  inspectValue(node);
}

function formatLogTime(timestamp) {
  const parsed = timestamp ? new Date(timestamp) : new Date();
  return Number.isNaN(parsed.getTime()) ? "--:--:--" : parsed.toLocaleTimeString();
}

function renderLogs() {
  const container = byId("logs");
  container.innerHTML = "";
  byId("logs-toggle-count").textContent = logEntries.length.toString();
  if (!logEntries.length) {
    appendTextElement(container, "p", "empty-state", "Waiting for real-time events...");
    return;
  }

  const fragment = document.createDocumentFragment();
  for (const step of logEntries) {
    const row = document.createElement("article");
    row.className = `log-row${step.kind === "error" || step.error ? " is-error" : ""}`;
    const heading = document.createElement("div");
    heading.className = "log-heading";
    appendTextElement(heading, "time", "", formatLogTime(step.started_at));
    appendTextElement(heading, "strong", "", formatEventName(step.kind));
    row.appendChild(heading);
    appendTextElement(row, "div", "log-source", [step.agent_id, step.squad_id, step.tool_name, step.model].filter(Boolean).join(" · ") || "pipeline");
    appendTextElement(row, "p", "", step.error || step.summary || "Event received");
    appendTextElement(row, "code", "", step.correlation_id || "-");
    row.addEventListener("click", () => {
      if (step.kind === "llm_call") {
        const inspector = resetInspector();
        appendTextElement(inspector, "h3", "inspection-title", step.agent_id || "LLM call");
        appendLLMCallInspection(inspector, step, 1);
        openDrawer("inspector");
        return;
      }
      inspectValue(step);
    });
    fragment.appendChild(row);
  }
  container.appendChild(fragment);
  if (byId("follow-logs").checked) {
    container.scrollTop = container.scrollHeight;
  }
}

function recordLogSteps(steps) {
  let changed = false;
  for (const step of steps) {
    if (!step.step_id || loggedStepIDs.has(step.step_id)) {
      continue;
    }
    loggedStepIDs.add(step.step_id);
    logEntries.push(step);
    changed = true;
  }
  if (!changed) {
    return;
  }
  logEntries.sort((left, right) => new Date(left.started_at) - new Date(right.started_at));
  while (logEntries.length > maxLogEntries) {
    loggedStepIDs.delete(logEntries.shift().step_id);
  }
  renderLogs();
}

function openDrawer(tabName) {
  const drawer = byId("workspace-drawer");
  const titles = { queries: "Queries", logs: "Live logs", inspector: "Inspector" };
  drawer.classList.add("is-open");
  drawer.setAttribute("aria-hidden", "false");
  drawer.inert = false;
  byId("drawer-backdrop").hidden = false;
  document.body.classList.add("drawer-open");
  byId("drawer-title").textContent = titles[tabName];

  for (const button of document.querySelectorAll("[data-drawer-tab]")) {
    const selected = button.dataset.drawerTab === tabName;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-selected", selected.toString());
    button.setAttribute("aria-expanded", selected.toString());
  }
  for (const view of document.querySelectorAll(".drawer-view")) {
    view.hidden = view.id !== `drawer-${tabName}`;
  }
  if (tabName === "logs" && byId("follow-logs").checked) {
    byId("logs").scrollTop = byId("logs").scrollHeight;
  }
}

function closeDrawer() {
  const drawer = byId("workspace-drawer");
  drawer.classList.remove("is-open");
  drawer.setAttribute("aria-hidden", "true");
  drawer.inert = true;
  byId("drawer-backdrop").hidden = true;
  document.body.classList.remove("drawer-open");
  for (const button of document.querySelectorAll("[data-drawer-tab]")) {
    button.classList.remove("active");
    button.setAttribute("aria-selected", "false");
    button.setAttribute("aria-expanded", "false");
  }
}

function toggleDrawer(tabName) {
  const drawer = byId("workspace-drawer");
  const selectedTab = document.querySelector(`[data-drawer-tab="${tabName}"].active`);
  if (drawer.classList.contains("is-open") && selectedTab) {
    closeDrawer();
    return;
  }
  openDrawer(tabName);
}

function bindWorkspaceControls() {
  for (const button of document.querySelectorAll("[data-drawer-tab]")) {
    button.addEventListener("click", () => {
      if (button.closest(".drawer-tabs")) {
        openDrawer(button.dataset.drawerTab);
        return;
      }
      toggleDrawer(button.dataset.drawerTab);
    });
  }
  byId("drawer-close").addEventListener("click", closeDrawer);
  byId("drawer-backdrop").addEventListener("click", closeDrawer);
  byId("clear-logs").addEventListener("click", () => {
    logEntries.length = 0;
    loggedStepIDs.clear();
    renderLogs();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeDrawer();
    }
  });
}

function updateLiveActivity(step) {
  byId("latest-event").textContent = formatEventName(step.kind);
  byId("latest-context").textContent = [step.agent_id, step.squad_id, step.summary]
    .filter(Boolean)
    .join(" · ") || step.correlation_id;
  byId("last-event-time").textContent = new Date().toLocaleTimeString();
  byId("timeline-state").textContent = "Updating";
  markUpdated(byId("activity-bar"));
  markUpdated(byId("graph-viewport"));
}

function appendTextElement(parent, tagName, className, text) {
  const element = document.createElement(tagName);
  if (className) {
    element.className = className;
  }
  element.textContent = text;
  parent.appendChild(element);
}

async function fetchJSON(url) {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status} ${response.statusText}`);
  }
  return response.json();
}

function renderQueries(queries) {
  const container = byId("queries");
  container.innerHTML = "";
  byId("query-count").textContent = queries.length.toString();
  byId("queries-toggle-count").textContent = queries.length.toString();
  if (!queries.length) {
    appendTextElement(container, "p", "empty-state", "Waiting for the first query...");
    return;
  }

  const workflowItem = document.createElement("button");
  workflowItem.className = `query-item workflow-item${selectedQuery === workflowSelection ? " active" : ""}`;
  workflowItem.type = "button";
  workflowItem.setAttribute("aria-pressed", selectedQuery === workflowSelection ? "true" : "false");
  workflowItem.dataset.tooltip = "Open the graph and metrics for every retained query";
  const workflowHeading = document.createElement("span");
  workflowHeading.className = "query-heading";
  appendTextElement(workflowHeading, "i", "query-status-dot", "");
  appendTextElement(workflowHeading, "strong", "", "Whole workflow");
  workflowItem.appendChild(workflowHeading);
  const workflowSteps = queries.reduce((total, query) => total + (query.metrics.total_steps || 0), 0);
  appendTextElement(workflowItem, "div", "meta", `${queries.length} phases · ${workflowSteps} steps`);
  workflowItem.onclick = async () => {
    await selectQuery(workflowSelection);
    closeDrawer();
  };
  container.appendChild(workflowItem);

  for (const query of queries) {
    const item = document.createElement("button");
    const status = query.summary.status || "idle";
    item.className = `query-item status-${status}${query.summary.correlation_id === selectedQuery ? " active" : ""}`;
    item.type = "button";
    item.setAttribute("aria-pressed", query.summary.correlation_id === selectedQuery ? "true" : "false");
    item.dataset.tooltip = `Open ${status} query ${query.summary.correlation_id}`;
    const heading = document.createElement("span");
    heading.className = "query-heading";
    appendTextElement(heading, "i", "query-status-dot", "");
    appendTextElement(heading, "strong", "", query.summary.summary || query.summary.correlation_id);
    item.appendChild(heading);
    appendTextElement(item, "div", "meta", `${query.summary.status} · ${query.metrics.total_steps} steps · ${query.metrics.duration_ms} ms`);
    item.onclick = async () => {
      await selectQuery(query.summary.correlation_id);
      closeDrawer();
    };
    container.appendChild(item);
  }
}

function renderMetrics(metrics) {
  const isWorkflow = metrics.correlation_id === "workflow";
  const cards = [
    ["Duration", `${metrics.duration_ms || 0} ms`, "Elapsed execution time"],
    ["Tokens", `${metrics.total_tokens_in || 0} / ${metrics.total_tokens_out || 0}`, "Input tokens / output tokens"],
    ["LLM Calls", `${metrics.llm_calls || 0}`, "Calls made to language models"],
    ["Agents", `${metrics.unique_agents || 0}`, "Agents participating in this query"],
    ["Tool Calls", `${metrics.tool_calls || 0}`, "Tool executions observed"],
    ["Steps", `${metrics.total_steps || 0}`, "Events recorded in the timeline"],
    ["Errors", `${metrics.errors || 0}`, "Errors recorded for this query"],
    ["Scope", isWorkflow ? "Whole workflow" : (metrics.correlation_id || "-"), "Displayed execution scope"],
  ];
  const container = byId("metrics");
  container.innerHTML = "";
  for (const [label, value, hint] of cards) {
    const card = document.createElement("article");
    card.className = "card";
    card.dataset.tooltip = hint;
    appendTextElement(card, "h3", "", label);
    appendTextElement(card, "strong", "", value);
    container.appendChild(card);
  }
}

function renderTimeline(steps) {
  const container = byId("timeline");
  container.innerHTML = "";
  recordLogSteps(steps);
  if (!steps.length) {
    appendTextElement(container, "p", "empty-state", "Waiting for execution events...");
    byId("timeline-state").textContent = "Waiting";
    return;
  }
  steps.forEach((step, index) => {
    const row = document.createElement("button");
    row.type = "button";
    row.className = `timeline-row${index === steps.length - 1 ? " is-latest" : ""}`;
    row.dataset.tooltip = `Inspect ${formatEventName(step.kind)} event`;
    appendTextElement(row, "div", "kind", step.kind);
    appendTextElement(row, "div", "", step.summary || "(no summary)");
    appendTextElement(row, "div", "meta", `${step.agent_id || "-"} · ${step.squad_id || "-"} · ${step.duration_ms || 0} ms`);
    row.onclick = () => {
      inspectValue(step);
    };
    container.appendChild(row);
  });
  byId("timeline-state").textContent = `${steps.length} events`;
}

function renderGraph(graph) {
  const svg = byId("graph");
  svg.innerHTML = "";
  const isWorkflow = graph.correlation_id === "workflow";
  byId("selected-query").textContent = isWorkflow ? "Whole workflow" : (graph.correlation_id || "");
  byId("graph-empty").hidden = graph.nodes.length > 0;
  svg.hidden = graph.nodes.length === 0;
  if (!graph.nodes.length) {
    return;
  }

  const layout = isWorkflow ? layoutWorkflowGraph(graph) : layoutQueryGraph(graph);
  svg.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);
  svg.style.aspectRatio = `${layout.width} / ${layout.height}`;
  renderArrowDefinitions(svg);
  const nodesByID = new Map(graph.nodes.map((node) => [node.id, node]));
  const edgeTooltip = byId("graph-edge-tooltip");
  const hideEdgeTooltip = () => {
    edgeTooltip.hidden = true;
  };
  const showEdgeTooltip = (message) => {
    edgeTooltip.textContent = message;
    edgeTooltip.hidden = false;
  };

  for (const edge of graph.edges) {
    const from = layout.positions.get(edge.source);
    const to = layout.positions.get(edge.target);
    if (!from || !to) continue;

    const kind = edge.kind || "message";
    const line = document.createElementNS("http://www.w3.org/2000/svg", "path");
    line.setAttribute("class", `edge edge-${kind}`);
    line.setAttribute("d", graphEdgePath(edge, from, to));
    line.setAttribute("marker-end", `url(#arrow-${kind})`);
    const edgeTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
    const source = nodesByID.get(edge.source)?.label || edge.source;
    const destination = nodesByID.get(edge.target)?.label || edge.target;
    const tooltipMessage = `${source} -> ${destination}: ${describeGraphEdge(edge)} ${edge.count} observed event${edge.count === 1 ? "" : "s"}`;
    edgeTitle.textContent = tooltipMessage;
    line.appendChild(edgeTitle);
    svg.appendChild(line);

    const hoverTarget = document.createElementNS("http://www.w3.org/2000/svg", "path");
    hoverTarget.setAttribute("class", "edge-hover-target");
    hoverTarget.setAttribute("d", graphEdgePath(edge, from, to));
    const hoverTitle = document.createElementNS("http://www.w3.org/2000/svg", "title");
    hoverTitle.textContent = edgeTitle.textContent;
    hoverTarget.appendChild(hoverTitle);
    hoverTarget.addEventListener("pointerenter", () => showEdgeTooltip(tooltipMessage));
    hoverTarget.addEventListener("pointerleave", hideEdgeTooltip);
    svg.appendChild(hoverTarget);
  }

  for (const node of graph.nodes) {
    const position = layout.positions.get(node.id);
    if (!position) continue;

    const group = document.createElementNS("http://www.w3.org/2000/svg", "g");
    group.setAttribute("class", "graph-node");
    group.setAttribute("role", "button");
    group.setAttribute("tabindex", "0");
    group.setAttribute("aria-label", `${node.label}, ${node.type}, ${node.status || "idle"}, ${node.calls} calls`);

    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = `${node.label}\nType: ${node.type}\nStatus: ${node.status || "idle"}\nCalls: ${node.calls}`;
    group.appendChild(title);

    const shape = createNodeShape(node, position);
    const inspectNode = () => inspectGraphNode(node);
    group.addEventListener("click", inspectNode);
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        inspectNode();
      }
    });
    group.appendChild(shape);

    const labelText = graphNodeLabel(node);
    const callsText = `${node.calls} calls`;
    const labelWidth = Math.max(76, labelText.length * 7 + 14, callsText.length * 6.5 + 14);
    const labelBackground = document.createElementNS("http://www.w3.org/2000/svg", "rect");
    labelBackground.setAttribute("class", "node-label-background");
    labelBackground.setAttribute("x", (position.x - labelWidth / 2).toString());
    labelBackground.setAttribute("y", (position.y - 29).toString());
    labelBackground.setAttribute("width", labelWidth.toString());
    labelBackground.setAttribute("height", "45");
    labelBackground.setAttribute("rx", "3");
    group.appendChild(labelBackground);

    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("class", "node-label");
    text.setAttribute("x", position.x.toString());
    text.setAttribute("y", (position.y - 11).toString());
    text.textContent = labelText;
    group.appendChild(text);

    const calls = document.createElementNS("http://www.w3.org/2000/svg", "text");
    calls.setAttribute("class", "node-call-count");
    calls.setAttribute("x", position.x.toString());
    calls.setAttribute("y", (position.y + 10).toString());
    calls.textContent = callsText;
    group.appendChild(calls);
    svg.appendChild(group);
  }
}

function layoutQueryGraph(graph) {
  const nodeByID = new Map(graph.nodes.map((node) => [node.id, node]));
  const positions = new Map();
  const squads = sortGraphNodes(graph.nodes.filter((node) => node.type === "squad"));
  const groups = squads.map((squad) => graphSquadGroup(graph, nodeByID, squad));
  let nextY = 100;
  for (const group of groups) {
    group.height = Math.max(150, 96 + group.agents.length * 76);
    group.centerY = nextY + group.height / 2;
    placeSquadGroup(positions, group, { squad: 330, coordinator: 570, agent: 810 });
    nextY += group.height + 44;
  }
  const height = Math.max(560, nextY + 70);
  const user = graph.nodes.find((node) => node.type === "user");
  if (user) positions.set(user.id, { x: 100, y: height / 2 });
  placeRemainingNodes(graph, positions, 1080, 100);
  return { positions, width: 1260, height };
}

function layoutWorkflowGraph(graph) {
  const nodeByID = new Map(graph.nodes.map((node) => [node.id, node]));
  const positions = new Map();
  const phases = sortGraphNodes(graph.nodes.filter((node) => node.type === "phase"), phaseOrder);
  const phaseLayouts = phases.map((phase) => {
    const squads = sortGraphNodes(
      graph.edges
        .filter((edge) => edge.source === phase.id && nodeByID.get(edge.target)?.type === "squad")
        .map((edge) => nodeByID.get(edge.target)),
    );
    return { phase, groups: squads.map((squad) => graphSquadGroup(graph, nodeByID, squad)) };
  });

  let nextY = 100;
  for (const phaseLayout of phaseLayouts) {
    const groupHeight = phaseLayout.groups.reduce((total, group) => total + Math.max(150, 96 + group.agents.length * 76), 0);
    const laneHeight = Math.max(220, groupHeight + Math.max(0, phaseLayout.groups.length - 1) * 40);
    phaseLayout.centerY = nextY + laneHeight / 2;
    positions.set(phaseLayout.phase.id, { x: 300, y: phaseLayout.centerY });
    let groupY = nextY;
    for (const group of phaseLayout.groups) {
      group.height = Math.max(150, 96 + group.agents.length * 76);
      group.centerY = groupY + group.height / 2;
      placeSquadGroup(positions, group, { squad: 550, coordinator: 790, agent: 1040 });
      groupY += group.height + 40;
    }
    nextY += laneHeight + 76;
  }

  const height = Math.max(600, nextY + 70);
  const root = graph.nodes.find((node) => node.type === "workflow");
  if (root) positions.set(root.id, { x: 85, y: height / 2 });
  const terminal = graph.nodes.find((node) => node.type === "terminal");
  if (terminal) {
    const lastPhase = phaseLayouts.at(-1);
    positions.set(terminal.id, { x: 1370, y: lastPhase?.centerY ?? height / 2 });
  }
  placeRemainingNodes(graph, positions, 1270, 100);
  return { positions, width: 1500, height };
}

function graphSquadGroup(graph, nodeByID, squad) {
  const members = graph.edges
    .filter((edge) => edge.source === squad.id)
    .map((edge) => nodeByID.get(edge.target))
    .filter(Boolean);
  const coordinator = members.find((node) => node.type === "coordinator")
    || [...nodeByID.values()].find((node) => node.type === "coordinator" && node.label === `${squad.label}-coordinator`);
  const agents = sortGraphNodes(members.filter((node) => node.type === "agent" || node.type === "transversal"));
  return { squad, coordinator, agents, height: 0, centerY: 0 };
}

function placeSquadGroup(positions, group, columns) {
  positions.set(group.squad.id, { x: columns.squad, y: group.centerY });
  if (group.coordinator) positions.set(group.coordinator.id, { x: columns.coordinator, y: group.centerY });
  const firstAgentY = group.centerY - ((group.agents.length - 1) * 76) / 2;
  group.agents.forEach((agent, index) => {
    positions.set(agent.id, { x: columns.agent, y: firstAgentY + index * 76 });
  });
}

function placeRemainingNodes(graph, positions, fallbackX, firstY) {
  let fallbackIndex = 0;
  for (const node of sortGraphNodes(graph.nodes)) {
    if (positions.has(node.id)) continue;
    const incoming = graph.edges.find((edge) => edge.target === node.id && positions.has(edge.source));
    if (incoming) {
      const source = positions.get(incoming.source);
      positions.set(node.id, { x: Math.max(fallbackX, source.x + 180), y: source.y + (fallbackIndex++ % 3) * 46 });
      continue;
    }
    positions.set(node.id, { x: fallbackX, y: firstY + fallbackIndex++ * 76 });
  }
}

function sortGraphNodes(nodes, comparator = (left, right) => left.label.localeCompare(right.label)) {
  return [...nodes].sort(comparator);
}

function phaseOrder(left, right) {
  const leftNumber = Number.parseInt(left.label.match(/^Phase (\d+)/)?.[1] || "0", 10);
  const rightNumber = Number.parseInt(right.label.match(/^Phase (\d+)/)?.[1] || "0", 10);
  return leftNumber - rightNumber || left.label.localeCompare(right.label);
}

function graphEdgePath(edge, from, to) {
  const direction = Math.sign(to.x - from.x) || 1;
  const offset = edge.label === "result" ? 16 : (edge.kind === "summary" ? -16 : 0);
  const startX = from.x + direction * 54;
  const endX = to.x - direction * 54;
  const startY = from.y + offset;
  const endY = to.y + offset;
  const middleX = (startX + endX) / 2;
  return `M ${startX} ${startY} H ${middleX} V ${endY} H ${endX}`;
}

function renderArrowDefinitions(svg) {
  const namespace = "http://www.w3.org/2000/svg";
  const colors = {
    workflow: "#0f766e",
    message: "#0f766e",
    coordination: "#1d4ed8",
    summary: "#166534",
    completion: "#166534",
    delegate: "#7c3aed",
    tool_call: "#a16207",
    reply: "#0f766e",
  };
  const defs = document.createElementNS(namespace, "defs");
  for (const [kind, color] of Object.entries(colors)) {
    const marker = document.createElementNS(namespace, "marker");
    marker.setAttribute("id", `arrow-${kind}`);
    marker.setAttribute("viewBox", "0 0 10 10");
    marker.setAttribute("refX", "9");
    marker.setAttribute("refY", "5");
    marker.setAttribute("markerWidth", "7");
    marker.setAttribute("markerHeight", "7");
    marker.setAttribute("orient", "auto-start-reverse");
    const tip = document.createElementNS(namespace, "path");
    tip.setAttribute("d", "M 0 0 L 10 5 L 0 10 z");
    tip.setAttribute("fill", color);
    marker.appendChild(tip);
    defs.appendChild(marker);
  }
  svg.appendChild(defs);
}

function graphNodeLabel(node) {
  const label = node.type === "phase" ? node.label.split(":")[0] : node.label;
  const limit = node.type === "agent" || node.type === "transversal" ? 18 : 22;
  return label.length > limit ? `${label.slice(0, limit - 1)}…` : label;
}

function createNodeShape(node, position) {
  const namespace = "http://www.w3.org/2000/svg";
  const status = node.status || "idle";
  const type = node.type || "agent";
  let shape;
  if (type === "squad") {
    shape = document.createElementNS(namespace, "polygon");
    shape.setAttribute("points", hexagonPoints(position.x, position.y, 44, 31));
  } else if (type === "coordinator") {
    shape = document.createElementNS(namespace, "polygon");
    shape.setAttribute("points", `${position.x},${position.y - 37} ${position.x + 48},${position.y} ${position.x},${position.y + 37} ${position.x - 48},${position.y}`);
  } else if (type === "agent" || type === "transversal") {
    shape = document.createElementNS(namespace, "circle");
    shape.setAttribute("cx", position.x.toString());
    shape.setAttribute("cy", position.y.toString());
    shape.setAttribute("r", (27 + Math.min(node.calls, 6) * 2).toString());
  } else {
    shape = document.createElementNS(namespace, "rect");
    const width = type === "tool" ? 84 : 112;
    shape.setAttribute("x", (position.x - width / 2).toString());
    shape.setAttribute("y", (position.y - 29).toString());
    shape.setAttribute("width", width.toString());
    shape.setAttribute("height", "58");
    shape.setAttribute("rx", type === "workflow" || type === "terminal" ? "18" : "7");
  }
  shape.setAttribute("class", `node-shape node-${status} node-${type}`);
  return shape;
}

function hexagonPoints(centerX, centerY, radiusX, radiusY) {
  return [
    [centerX - radiusX / 2, centerY - radiusY],
    [centerX + radiusX / 2, centerY - radiusY],
    [centerX + radiusX, centerY],
    [centerX + radiusX / 2, centerY + radiusY],
    [centerX - radiusX / 2, centerY + radiusY],
    [centerX - radiusX, centerY],
  ].map(([x, y]) => `${x},${y}`).join(" ");
}

function describeGraphEdge(edge) {
  const descriptions = {
    workflow: "Workflow starts phase.",
    message: edge.label === "runs" ? "Squad assigns an agent." : "Agent publishes a result to its squad.",
    coordination: "Squad sends accumulated results to its coordinator.",
    summary: "Squad coordinator publishes a summary to the phase.",
    completion: "Every retained phase reached a terminal root event.",
    delegate: "Agent delegates a task through a tool.",
    tool_call: "Agent invokes a local tool.",
    reply: "Agent returns a response to the phase.",
  };
  return descriptions[edge.kind] || edge.label || "Observed relationship.";
}

async function refreshQueries(selectLatest = false) {
  const queries = await fetchJSON("/api/queries");
  renderQueries(queries);
  if (!selectedQuery && queries.length) {
    selectedQuery = workflowSelection;
  }
  if (selectLatest && queries.length) {
    selectedQuery = workflowSelection;
  }
}

async function selectQuery(correlationID) {
  selectedQuery = correlationID;
  const currentSelectionVersion = ++selectionVersion;
  await refreshQueries(false);
  const isWorkflow = correlationID === workflowSelection;
  const resourceBase = isWorkflow
    ? "/api/workflow"
    : `/api/queries/${encodeURIComponent(correlationID)}`;
  const metricsURL = isWorkflow
    ? "/api/workflow/metrics"
    : `/api/metrics/summary?query=${encodeURIComponent(correlationID)}`;
  const [timeline, graph, metrics] = await Promise.all([
    fetchJSON(`${resourceBase}/timeline`),
    fetchJSON(`${resourceBase}/graph`),
    fetchJSON(metricsURL),
  ]);
  if (currentSelectionVersion !== selectionVersion || correlationID !== selectedQuery) {
    return;
  }
  currentTimeline = timeline;
  renderTimeline(timeline);
  renderGraph(graph);
  renderMetrics(metrics);
}

function scheduleSelectedQueryRefresh() {
  window.clearTimeout(refreshTimer);
  refreshTimer = window.setTimeout(() => {
    if (selectedQuery) {
      selectQuery(selectedQuery).catch((error) => {
        byId("inspector").textContent = error.stack || String(error);
      });
    }
  }, 80);
}

function setGraphScale(nextScale) {
  graphScale = Math.min(1.8, Math.max(0.6, nextScale));
  byId("graph").style.setProperty("--graph-scale", graphScale.toString());
  byId("zoom-level").textContent = `${Math.round(graphScale * 100)}%`;
}

function fitGraph() {
  const viewport = byId("graph-viewport");
  setGraphScale(Math.min(1, viewport.clientWidth / 900));
  viewport.scrollTo({ top: 0, left: 0, behavior: "smooth" });
}

function bindGraphControls() {
  byId("zoom-out").addEventListener("click", () => setGraphScale(graphScale - 0.2));
  byId("zoom-in").addEventListener("click", () => setGraphScale(graphScale + 0.2));
  byId("zoom-fit").addEventListener("click", fitGraph);
}

function connectStream() {
  const source = new EventSource("/api/stream");
  source.onopen = () => {
    setStreamState("live", "Live");
  };
  source.onerror = () => {
    setStreamState("reconnecting", "Reconnecting");
  };
  const onStep = (event) => {
    if (!(event instanceof MessageEvent) || typeof event.data !== "string") {
      return;
    }
    const step = JSON.parse(event.data);
    recordLogSteps([step]);
    updateLiveActivity(step);
    if (!selectedQuery) {
      selectedQuery = workflowSelection;
    }
    scheduleSelectedQueryRefresh();
  };
  stepEvents.forEach((eventName) => source.addEventListener(eventName, onStep));
}

async function bootstrap() {
  bindGraphControls();
  bindWorkspaceControls();
  renderLogs();
  await refreshQueries(true);
  if (selectedQuery) {
    await selectQuery(selectedQuery);
    fitGraph();
  } else {
    renderMetrics({});
  }
  connectStream();
}

bootstrap().catch((error) => {
  byId("inspector").textContent = error.stack || String(error);
});
