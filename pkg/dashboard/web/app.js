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
  "agent_error",
];

let selectedQuery = null;
let graphScale = 1;
let graphBaseSize = null;
let sequenceRenderID = 0;
let graphViewMode = "sequence";
let currentGraph = null;
const graphZoomMin = 0.25;
const graphZoomMax = 3;
const graphZoomStep = 0.05;
const graphInteraction = {
  pointerID: null,
  startX: 0,
  startY: 0,
  startScrollLeft: 0,
  startScrollTop: 0,
  didPan: false,
  suppressClick: false,
};
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
  if (value && value.step_id && value.kind) {
    inspectEvent(value);
    return;
  }
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", "Event payload");
  appendTextElement(inspector, "pre", "inspection-payload", pretty(value));
  openDrawer("inspector");
}

function inspectEvent(step, relation = null) {
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", `${formatEventName(step.kind)} event`);
  const eventDetails = appendInspectionSection(inspector, "Event audit");
  const event = currentGraph ? sequenceEvent(step, currentGraph) : null;
  appendInspectionFields(eventDetails, [
    ["Event kind", step.kind],
    ["Timestamp", step.started_at],
    ["Duration", `${step.duration_ms || 0} ms`],
    ["Agent", step.agent_id],
    ["Squad", step.squad_id],
    ["Thread", step.thread_id],
    ["Correlation", step.correlation_id],
    ["Source", relation?.source || event?.source],
    ["Destination", relation?.target || event?.target],
    ["Tool", step.tool_name],
    ["Model", step.model],
    ["Tokens", step.tokens_in || step.tokens_out ? `${step.tokens_in || 0} in / ${step.tokens_out || 0} out` : ""],
    ["Error", step.error],
  ]);
  appendTextElement(eventDetails, "p", "inspection-note", step.summary || "No summary recorded.");

  if (step.llm_trace) {
    const trace = step.llm_trace;
    const transfer = {
      system_prompt: trace.system_prompt,
      messages: trace.messages,
      completion: trace.completion,
      provider: trace.provider,
      request_id: trace.request_id,
      generation_id: trace.generation_id,
      finish_reason: trace.finish_reason,
    };
    appendInspectionDetails(inspector, "Visible data passed to and from the model", pretty(transfer), true);
  } else {
    appendInspectionDetails(inspector, "Visible event data", pretty({
      summary: step.summary,
      tool_name: step.tool_name,
      message_id: step.message_id,
      trace_id: step.trace_id,
      span_id: step.span_id,
      parent_step_id: step.parent_step_id,
    }), true);
  }
  appendInspectionDetails(inspector, "Raw AgentStep", pretty(step));
  openDrawer("inspector");
}

function inspectGraphRelationship(relation) {
  const inspector = resetInspector();
  appendTextElement(inspector, "h3", "inspection-title", "Observed relationship");
  const details = appendInspectionSection(inspector, "Connection audit");
  appendInspectionFields(details, [
    ["Source", relation.source],
    ["Destination", relation.target],
    ["Kind", relation.edge?.kind || relation.kind],
    ["Label", relation.edge?.label || relation.label],
    ["Observed count", relation.edge?.count || relation.count],
  ]);
  const relatedEvents = currentTimeline.filter((step) => {
    const source = step.agent_id || step.squad_id;
    return source === relation.source || source === relation.target || step.tool_name === relation.target;
  });
  appendInspectionDetails(inspector, "Events associated with this relationship", pretty(relatedEvents), true);
  appendInspectionDetails(inspector, "Raw relationship", pretty(relation.edge || relation), false);
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

function formatUSD(value) {
  const amount = Number(value);
  return Number.isFinite(amount) ? `$${amount.toFixed(6)}` : "--";
}

function formatTokenBudget(current, maximum) {
  const max = Number(maximum);
  return Number.isFinite(max) && max > 0 ? `${current || 0} / ${max}` : `${current || 0} / unlimited`;
}

function formatUSDBudget(current, maximum) {
  const max = Number(maximum);
  const maximumLabel = Number.isFinite(max) && max > 0 ? formatUSD(max) : "unlimited";
  return `${formatUSD(current)} / ${maximumLabel}`;
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
    ["Token Budget", formatTokenBudget(metrics.total_tokens, metrics.max_total_tokens), "Current total tokens / configured maximum"],
    ["USD Budget", formatUSDBudget(metrics.total_cost_usd, metrics.max_cost_usd), "Current USD cost / configured maximum"],
    ["Budget Status", metrics.budget_status || "unavailable", "Current execution budget state"],
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

function sequenceText(value, fallback = "event") {
  const normalized = String(value || fallback)
    .replace(/\s+/g, " ")
    .replace(/[:;]/g, " - ")
    .replace(/[<>{}]/g, "")
    .replace(/"/g, "'")
    .trim();
  return (normalized || fallback).slice(0, 180);
}

function mermaidText(value, fallback = "event") {
  return sequenceText(value, fallback)
    .replace(/[()[\]{}#]/g, "")
    .replace(/\|/g, " - ")
    .trim() || fallback;
}

function initializeMermaid() {
  if (!window.mermaid || typeof window.mermaid.render !== "function") {
    return false;
  }
  if (window.mermaidInitialized) {
    return true;
  }
  window.mermaid.initialize({
    startOnLoad: false,
    securityLevel: "strict",
    theme: "base",
    themeVariables: {
      primaryColor: "#ccfbf1",
      primaryTextColor: "#1e293b",
      primaryBorderColor: "#0f766e",
      lineColor: "#64748b",
      secondaryColor: "#dbeafe",
      tertiaryColor: "#f8fafc",
    },
  });
  window.mermaidInitialized = true;
  return true;
}

async function renderMermaidSource(container, source, ariaLabel, renderPrefix) {
  const rendered = await window.mermaid.render(`${renderPrefix}-${sequenceRenderID++}`, source);
  const parsed = new DOMParser().parseFromString(rendered.svg, "image/svg+xml");
  const svg = document.importNode(parsed.documentElement, true);
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", ariaLabel);
  svg.style.maxWidth = "none";
  container.replaceChildren(svg);
  rendered.bindFunctions?.(container);
  setSequenceBaseSize(svg);
  applySequenceScale();
  return svg;
}

function graphLabelForID(id, graph) {
  if (!id) {
    return "";
  }
  const node = graph.nodes.find((candidate) => candidate.id === id || candidate.label === id);
  return node?.label || id;
}

function sequenceNodeForLabel(label, graph) {
  const node = graph.nodes.find((candidate) => candidate.label === label || candidate.id === label);
  if (node) {
    return node;
  }
  const type = label === "LLM service" ? "tool" : label === "Pipeline" || label === "Timeline" ? "phase" : "agent";
  return { id: `sequence-${label}`, label, type, status: "idle", calls: 0 };
}

function delegationTarget(step, graph) {
  const match = (step.summary || "").match(/delegate to squad ([^ (]+)/i);
  if (match) {
    return graphLabelForID(match[1], graph);
  }
  return step.tool_name ? sequenceText(step.tool_name, "delegated task") : "delegated task";
}

function sequenceEvent(step, graph) {
  const agent = graphLabelForID(step.agent_id, graph);
  const squad = graphLabelForID(step.squad_id, graph);
  const sourceAgent = agent || squad || "Pipeline";
  switch (step.kind) {
    case "query_received":
      return { source: "Timeline", target: "Pipeline", arrow: "->>", label: step.summary || "Query received" };
    case "routed": {
      const routedSquad = (step.summary || "").split(":").slice(1).join(":").split(",")[0].trim();
      return { source: "Pipeline", target: graphLabelForID(routedSquad || step.squad_id, graph) || "Pipeline", arrow: "->>", label: step.summary || "Query routed" };
    }
    case "agent_started":
      return { source: squad || "Pipeline", target: agent || "Agent", arrow: "->>", label: "Agent started" };
    case "llm_call":
      return { source: sourceAgent, target: "LLM service", arrow: "->>", label: `${step.summary || "LLM call"}${step.model ? ` (${step.model})` : ""}` };
    case "tool_call":
      return { source: sourceAgent, target: sequenceText(step.tool_name, "local tool"), arrow: "->>", label: step.summary || "Tool called" };
    case "delegated":
      return { source: sourceAgent, target: delegationTarget(step, graph), arrow: "->>", label: step.summary || "Task delegated" };
    case "reply_received":
      return { source: squad || "Delegated task", target: agent || "Agent", arrow: "-->>", label: step.summary || "Reply received" };
    case "synthesis":
      return { source: agent || "Coordinator", target: squad || "Pipeline", arrow: "-->>", label: step.summary || "Summary synthesized" };
    case "responded":
      return { source: sourceAgent, target: squad || "Pipeline", arrow: "-->>", label: step.summary || "Response published" };
    case "quiesced":
      return { source: "Pipeline", target: "Timeline", arrow: "-->>", label: step.summary || "Execution reached quiescence" };
    case "error":
      return { source: sourceAgent, target: "Timeline", arrow: "--x", label: step.error || step.summary || "Execution error" };
    default:
      return { source: sourceAgent, target: "Timeline", arrow: "->>", label: step.summary || step.kind || "Event" };
  }
}

function buildSequenceDefinition(graph) {
  const participants = new Map();
  const events = currentTimeline.map((step) => sequenceEvent(step, graph));
  const addParticipant = (label) => {
    const cleanLabel = sequenceText(label, "Actor");
    if (!participants.has(cleanLabel)) {
      participants.set(cleanLabel, {
        id: `participant${participants.size}`,
        label: cleanLabel,
        node: sequenceNodeForLabel(cleanLabel, graph),
      });
    }
    return participants.get(cleanLabel);
  };

  for (const node of graph.nodes) {
    addParticipant(node.label);
  }
  for (const event of events) {
    addParticipant(event.source);
    addParticipant(event.target);
  }

  const lines = ["sequenceDiagram", "  autonumber"];
  for (const participant of participants.values()) {
    lines.push(`  participant ${participant.id} as ${sequenceText(participant.label, "Actor")}`);
  }
  for (const event of events) {
    const source = participants.get(sequenceText(event.source, "Actor"));
    const target = participants.get(sequenceText(event.target, "Actor"));
    lines.push(`  ${source.id}${event.arrow}${target.id}: ${sequenceText(event.label)}`);
  }
  return { source: lines.join("\n"), participants: [...participants.values()] };
}

function buildStateDefinition() {
  const entries = currentTimeline.map((step, index) => ({
    id: `state${index}`,
    label: `${index + 1} ${formatEventName(step.kind)} - ${step.summary || step.error || "event"}`,
    step,
  }));
  const lines = ["stateDiagram-v2", "  direction TB"];
  if (!entries.length) {
    lines.push("  [*] --> empty", '  state "No events recorded" as empty');
    return { source: lines.join("\n"), entries, transitions: [] };
  }
  lines.push(`  [*] --> ${entries[0].id}`);
  for (const [index, entry] of entries.entries()) {
    lines.push(`  state "${mermaidText(entry.label)}" as ${entry.id}`);
    if (index > 0) {
      const previous = entries[index - 1];
      lines.push(`  ${previous.id} --> ${entry.id} : ${mermaidText(formatEventName(entry.step.kind))}`);
    }
  }
  lines.push(`  ${entries.at(-1).id} --> [*]`);
  return { source: lines.join("\n"), entries, transitions: entries.slice(1) };
}

function buildMindmapDefinition(graph) {
  const nodeByID = new Map(graph.nodes.map((node) => [node.id, node]));
  const children = new Map();
  for (const edge of graph.edges) {
    if (edge.source === edge.target || !nodeByID.has(edge.source) || !nodeByID.has(edge.target)) {
      continue;
    }
    const list = children.get(edge.source) || [];
    if (!list.some((node) => node.id === edge.target)) {
      list.push(nodeByID.get(edge.target));
    }
    children.set(edge.source, list);
  }
  const root = graph.nodes.find((node) => node.type === "workflow")
    || graph.nodes.find((node) => node.type === "user")
    || graph.nodes[0]
    || { id: "mindmap-root", label: "Execution", type: "workflow", status: "idle", calls: 0 };
  const used = new Set();
  const labels = new Set();
  const entries = [];
  const lines = ["mindmap"];
  const uniqueLabel = (node, fallback = "Observed node") => {
    const base = mermaidText(node.label || fallback, fallback);
    let label = base;
    let suffix = 2;
    while (labels.has(label)) {
      label = `${base} ${suffix++}`;
    }
    labels.add(label);
    return label;
  };
  const addNode = (node, depth, isRoot = false, parent = null) => {
    if (used.has(node.id)) {
      return;
    }
    used.add(node.id);
    const label = uniqueLabel(node);
    entries.push({ label, node, parent });
    const indentation = "  ".repeat(depth + 1);
    lines.push(isRoot ? `${indentation}root((${label}))` : `${indentation}${label}`);
    const descendants = [...(children.get(node.id) || [])].sort((left, right) => left.label.localeCompare(right.label));
    for (const child of descendants) {
      addNode(child, depth + 1, false, node);
    }
  };
  addNode(root, 0, true);
  for (const node of [...graph.nodes].sort((left, right) => left.label.localeCompare(right.label))) {
    if (!used.has(node.id)) {
      addNode(node, 1);
    }
  }
  const relationships = entries.filter((entry) => entry.parent).map((entry) => ({
    source: entry.parent.label,
    target: entry.node.label,
    kind: "hierarchy",
    label: "hierarchy",
  }));
  return { source: lines.join("\n"), entries, relationships };
}

function decorateSequenceActors(container, participants) {
  const groups = [...container.querySelectorAll("g")].filter((group) => (
    group.matches('g[data-et="participant"]') || group.querySelector("rect.actor-bottom")
  ));
  for (const group of groups) {
    const renderedLabel = group.textContent.replace(/\s+/g, " ").trim();
    const participant = participants.find((candidate) => renderedLabel === candidate.label || renderedLabel.includes(candidate.label));
    if (!participant?.node) {
      continue;
    }
    const node = participant.node;
    const isBottomActor = !group.matches('g[data-et="participant"]') && Boolean(group.querySelector("rect.actor-bottom"));
    const inspect = () => inspectGraphNode(node);
    if (!isBottomActor) {
      group.setAttribute("role", "button");
      group.setAttribute("tabindex", "0");
      group.setAttribute("aria-label", `${node.label}, ${node.type}, ${node.status || "idle"}, ${node.calls || 0} calls`);
    }
    group.dataset.tooltip = `${node.label} · ${node.type} · ${node.status || "idle"} · ${node.calls || 0} calls`;
    group.classList.add("sequence-actor");
    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = group.dataset.tooltip;
    group.prepend(title);
    group.addEventListener("click", inspect);
    if (!isBottomActor) {
      group.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          inspect();
        }
      });
    }
  }
}

function decorateSequenceMessages(container) {
  for (const target of container.querySelectorAll('[data-et="message"]')) {
    const messageIndex = Number.parseInt((target.getAttribute("data-id") || "").replace(/^i/, ""), 10) - 1;
    const step = currentTimeline[messageIndex];
    if (!step) {
      continue;
    }
    const event = sequenceEvent(step, currentGraph);
    const tooltip = `${formatEventName(step.kind)} · ${event.source} -> ${event.target}: ${event.label}`;
    target.classList.add("sequence-message-target");
    target.setAttribute("role", "button");
    target.setAttribute("tabindex", "0");
    target.setAttribute("aria-label", tooltip);
    target.dataset.tooltip = tooltip;
    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = tooltip;
    target.appendChild(title);
    target.addEventListener("click", () => inspectValue(step));
    target.addEventListener("keydown", (keyboardEvent) => {
      if (keyboardEvent.key === "Enter" || keyboardEvent.key === " ") {
        keyboardEvent.preventDefault();
        inspectValue(step);
      }
    });
  }
}

function normalizedDiagramText(value) {
  return String(value || "").replace(/\s+/g, " ").trim();
}

function findMermaidLabelGroup(container, label) {
  const expected = normalizedDiagramText(label);
  const candidates = [...container.querySelectorAll("g")]
    .filter((group) => {
      const actual = normalizedDiagramText(group.textContent);
      return actual === expected || actual.includes(expected);
    })
    .sort((left, right) => {
      const rightIsNode = right.classList.contains("node") || right.classList.contains("statediagram-state");
      const leftIsNode = left.classList.contains("node") || left.classList.contains("statediagram-state");
      return Number(rightIsNode) - Number(leftIsNode) || left.textContent.length - right.textContent.length;
    });
  return candidates[0] || null;
}

function decorateMermaidNodeEntries(container, entries, clickHandler) {
  for (const entry of entries) {
    const group = findMermaidLabelGroup(container, entry.label);
    if (!group || group.dataset.decorated === "true") {
      continue;
    }
    const tooltip = entry.tooltip || `${entry.label} · click to inspect observed data`;
    group.dataset.decorated = "true";
    group.dataset.tooltip = tooltip;
    group.classList.add("mermaid-node-target");
    group.setAttribute("role", "button");
    group.setAttribute("tabindex", "0");
    group.setAttribute("aria-label", tooltip);
    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = tooltip;
    group.prepend(title);
    group.addEventListener("click", () => clickHandler(entry));
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        clickHandler(entry);
      }
    });
  }
}

function decorateMermaidEdges(container, descriptors, fallbackLabel) {
  const svg = container.querySelector("svg");
  const targets = [...container.querySelectorAll('[data-et="edge"], [data-et="transition"], [data-et="message"]')]
    .filter((element) => !element.classList.contains("actor-line"));
  targets.forEach((target, index) => {
    const descriptor = descriptors[index % Math.max(1, descriptors.length)];
    const tooltip = descriptor?.tooltip || fallbackLabel;
    target.classList.add("mermaid-edge-target");
    target.dataset.tooltip = tooltip;
    const title = document.createElementNS("http://www.w3.org/2000/svg", "title");
    title.textContent = tooltip;
    target.appendChild(title);
    if (descriptor?.onClick) {
      target.setAttribute("role", "button");
      target.setAttribute("tabindex", "0");
      target.setAttribute("aria-label", tooltip);
      target.addEventListener("click", descriptor.onClick);
      target.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          descriptor.onClick();
        }
      });

      const hitTarget = target.cloneNode(false);
      hitTarget.removeAttribute("id");
      hitTarget.removeAttribute("role");
      hitTarget.removeAttribute("tabindex");
      hitTarget.classList.remove("mermaid-edge-target");
      hitTarget.classList.add("mermaid-edge-hit-target");
      hitTarget.style.stroke = "transparent";
      hitTarget.style.strokeWidth = "28px";
      hitTarget.style.pointerEvents = "bounding-box";
      hitTarget.setAttribute("aria-hidden", "true");
      hitTarget.addEventListener("click", descriptor.onClick);
      svg?.appendChild(hitTarget);
    }
  });
}

async function renderStateGraph(graph) {
  const container = byId("graph");
  container.replaceChildren();
  byId("selected-query").textContent = graph.correlation_id === "workflow" ? "Whole workflow" : (graph.correlation_id || "");
  byId("graph-empty").hidden = currentTimeline.length > 0;
  container.hidden = currentTimeline.length === 0;
  graphBaseSize = null;
  const definition = buildStateDefinition();
  if (!currentTimeline.length) {
    return;
  }
  if (!initializeMermaid()) {
    renderSequenceFallback(container, graph);
    return;
  }
  try {
    const svg = await renderMermaidSource(container, definition.source, "Selected query state diagram", "execution-state");
    const stateEntries = definition.entries.map((entry) => ({
      label: entry.label,
      tooltip: `${formatEventName(entry.step.kind)} · ${entry.step.summary || entry.step.error || "event"}`,
      onClick: () => inspectValue(entry.step),
    }));
    decorateMermaidNodeEntries(container, stateEntries, (entry) => entry.onClick());
    decorateMermaidEdges(container, definition.transitions.map((entry) => ({
      tooltip: `${formatEventName(entry.step.kind)} · ${entry.step.summary || entry.step.error || "state transition"}`,
      onClick: () => inspectValue(entry.step),
    })), "State transition · click to inspect");
    svg.setAttribute("aria-roledescription", "stateDiagram");
  } catch (error) {
    renderSequenceFallback(container, graph);
    byId("latest-context").textContent = `Mermaid state rendering failed: ${error.message || error}`;
  }
}

async function renderMindmapGraph(graph) {
  const container = byId("graph");
  container.replaceChildren();
  byId("selected-query").textContent = graph.correlation_id === "workflow" ? "Whole workflow" : (graph.correlation_id || "");
  byId("graph-empty").hidden = graph.nodes.length > 0;
  container.hidden = graph.nodes.length === 0;
  graphBaseSize = null;
  if (!graph.nodes.length) {
    return;
  }
  const definition = buildMindmapDefinition(graph);
  if (!initializeMermaid()) {
    renderSequenceFallback(container, graph);
    return;
  }
  try {
    const svg = await renderMermaidSource(container, definition.source, "Selected query mindmap", "execution-mindmap");
    decorateMermaidNodeEntries(container, definition.entries.map((entry) => ({
      label: entry.label,
      tooltip: `${entry.node.label} · ${entry.node.type} · ${entry.node.status || "idle"} · click to inspect`,
      onClick: () => inspectGraphNode(entry.node),
    })), (entry) => entry.onClick());
    decorateMermaidEdges(container, definition.relationships.map((relationship) => ({
      tooltip: `${relationship.source} -> ${relationship.target} · hierarchy relationship · click to inspect`,
      onClick: () => inspectGraphRelationship(relationship),
    })), "Mindmap hierarchy relationship · click to inspect");
    svg.setAttribute("aria-roledescription", "mindmap");
  } catch (error) {
    renderSequenceFallback(container, graph);
    byId("latest-context").textContent = `Mermaid mindmap rendering failed: ${error.message || error}`;
  }
}

function buildFlowchartDefinition(graph) {
  const nodes = graph.nodes.map((node, index) => ({
    node,
    id: `flow${index}`,
    label: mermaidText(node.label || node.id, "Observed node"),
  }));
  const nodeIDs = new Map(nodes.map((entry) => [entry.node.id, entry]));
  const lines = ["flowchart TD"];
  for (const entry of nodes) {
    const label = entry.label;
    switch (entry.node.type) {
      case "workflow":
      case "phase":
      case "terminal":
        lines.push(`  ${entry.id}([${label}])`);
        break;
      case "squad":
        lines.push(`  ${entry.id}{{${label}}}`);
        break;
      case "coordinator":
        lines.push(`  ${entry.id}{${label}}`);
        break;
      case "agent":
      case "transversal":
        lines.push(`  ${entry.id}((${label}))`);
        break;
      default:
        lines.push(`  ${entry.id}[${label}]`);
    }
  }
  const relationships = [];
  for (const edge of graph.edges) {
    const source = nodeIDs.get(edge.source);
    const target = nodeIDs.get(edge.target);
    if (!source || !target) {
      continue;
    }
    const sourceLabel = source.node.label;
    const targetLabel = target.node.label;
    const tooltip = `${sourceLabel} -> ${targetLabel}: ${describeGraphEdge(edge)} (${edge.count} observed event${edge.count === 1 ? "" : "s"})`;
    relationships.push({ source: sourceLabel, target: targetLabel, edge, tooltip });
    const label = mermaidText(edge.label || edge.kind || "observed");
    lines.push(`  ${source.id} -->|${label}| ${target.id}`);
  }
  return { source: lines.join("\n"), nodes, relationships };
}

async function renderFlowchartGraph(graph) {
  const container = byId("graph");
  container.replaceChildren();
  byId("selected-query").textContent = graph.correlation_id === "workflow" ? "Whole workflow" : (graph.correlation_id || "");
  byId("graph-empty").hidden = graph.nodes.length > 0;
  container.hidden = graph.nodes.length === 0;
  graphBaseSize = null;
  if (!graph.nodes.length) {
    return;
  }
  const definition = buildFlowchartDefinition(graph);
  if (!initializeMermaid()) {
    renderSequenceFallback(container, graph);
    return;
  }
  try {
    const svg = await renderMermaidSource(container, definition.source, "Selected query flowchart", "execution-flow");
    decorateMermaidNodeEntries(container, definition.nodes.map((entry) => ({
      label: entry.label,
      tooltip: `${entry.node.label} · ${entry.node.type} · ${entry.node.status || "idle"} · click to inspect`,
      onClick: () => inspectGraphNode(entry.node),
    })), (entry) => entry.onClick());
    decorateMermaidEdges(container, definition.relationships.map((relationship) => ({
      tooltip: relationship.tooltip,
      onClick: () => inspectGraphRelationship(relationship),
    })), "Flow relationship · click to inspect");
    svg.setAttribute("aria-roledescription", "flowchart");
  } catch (error) {
    renderSequenceFallback(container, graph);
    byId("latest-context").textContent = `Mermaid flowchart rendering failed: ${error.message || error}`;
  }
}

function renderSequenceFallback(container, graph) {
  container.replaceChildren();
  const note = document.createElement("p");
  note.className = "sequence-fallback-note";
  note.textContent = "Mermaid is unavailable; showing the ordered execution timeline.";
  container.appendChild(note);
  const list = document.createElement("ol");
  list.className = "sequence-fallback";
  for (const step of currentTimeline) {
    const event = sequenceEvent(step, graph);
    const row = document.createElement("li");
    appendTextElement(row, "strong", "sequence-fallback-message", `${event.source} ${event.arrow} ${event.target}`);
    appendTextElement(row, "span", "sequence-fallback-label", event.label);
    list.appendChild(row);
  }
  container.appendChild(list);
  graphBaseSize = null;
}

function setSequenceBaseSize(svg) {
  const viewBox = svg.viewBox?.baseVal;
  const width = viewBox?.width || Number.parseFloat(svg.getAttribute("width")) || svg.getBoundingClientRect().width;
  const height = viewBox?.height || Number.parseFloat(svg.getAttribute("height")) || svg.getBoundingClientRect().height;
  graphBaseSize = { width: Math.max(1, width), height: Math.max(1, height) };
}

function applySequenceScale() {
  const container = byId("graph");
  const svg = container.querySelector("svg");
  if (!svg || !graphBaseSize) {
    return;
  }
  const width = graphBaseSize.width * graphScale;
  const height = graphBaseSize.height * graphScale;
  const viewport = byId("graph-viewport");
  container.style.width = `${Math.max(viewport.clientWidth, width)}px`;
  container.style.height = `${height}px`;
  svg.style.width = `${width}px`;
  svg.style.height = `${height}px`;
}

async function renderSequenceGraph(graph) {
  const container = byId("graph");
  container.replaceChildren();
  const isWorkflow = graph.correlation_id === "workflow";
  byId("selected-query").textContent = isWorkflow ? "Whole workflow" : (graph.correlation_id || "");
  byId("graph-empty").hidden = graph.nodes.length > 0;
  container.hidden = graph.nodes.length === 0;
  graphBaseSize = null;
  if (!graph.nodes.length) {
    return;
  }

  const definition = buildSequenceDefinition(graph);
  if (!window.mermaid || typeof window.mermaid.render !== "function") {
    renderSequenceFallback(container, graph);
    return;
  }

  try {
    await renderMermaidSource(container, definition.source, "Selected query execution sequence diagram", "execution-sequence");
    decorateSequenceActors(container, definition.participants);
    decorateSequenceMessages(container);
  } catch (error) {
    renderSequenceFallback(container, graph);
    byId("latest-context").textContent = `Mermaid rendering failed: ${error.message || error}`;
  }
}

function renderFlowGraph(graph) {
  const container = byId("graph");
  container.replaceChildren();
  byId("graph-empty").hidden = graph.nodes.length > 0;
  container.hidden = graph.nodes.length === 0;
  graphBaseSize = null;
  if (!graph.nodes.length) {
    return;
  }

  const isWorkflow = graph.correlation_id === "workflow";
  const layout = isWorkflow ? layoutWorkflowGraph(graph) : layoutQueryGraph(graph);
  const namespace = "http://www.w3.org/2000/svg";
  const svg = document.createElementNS(namespace, "svg");
  svg.id = "flow-graph";
  svg.setAttribute("viewBox", `0 0 ${layout.width} ${layout.height}`);
  svg.setAttribute("preserveAspectRatio", "xMidYMid meet");
  svg.setAttribute("role", "img");
  svg.setAttribute("aria-label", "Selected query flow diagram");
  svg.style.maxWidth = "none";
  renderArrowDefinitions(svg);

  const nodesByID = new Map(graph.nodes.map((node) => [node.id, node]));
  const edgeTooltip = byId("graph-edge-tooltip");
  const hideEdgeTooltip = () => { edgeTooltip.hidden = true; };
  const showEdgeTooltip = (message) => {
    edgeTooltip.textContent = message;
    edgeTooltip.hidden = false;
  };

  for (const edge of graph.edges) {
    const from = layout.positions.get(edge.source);
    const to = layout.positions.get(edge.target);
    if (!from || !to) continue;

    const kind = edge.kind || "message";
    const source = nodesByID.get(edge.source)?.label || edge.source;
    const destination = nodesByID.get(edge.target)?.label || edge.target;
    const tooltipMessage = `${source} -> ${destination}: ${describeGraphEdge(edge)} ${edge.count} observed event${edge.count === 1 ? "" : "s"}`;
    const line = document.createElementNS(namespace, "path");
    line.setAttribute("class", `edge edge-${kind}`);
    line.setAttribute("d", graphEdgePath(edge, from, to));
    line.setAttribute("marker-end", `url(#arrow-${kind})`);
    const lineTitle = document.createElementNS(namespace, "title");
    lineTitle.textContent = tooltipMessage;
    line.appendChild(lineTitle);
    svg.appendChild(line);

    const hoverTarget = document.createElementNS(namespace, "path");
    hoverTarget.setAttribute("class", "edge-hover-target");
    hoverTarget.setAttribute("d", graphEdgePath(edge, from, to));
    const hoverTitle = document.createElementNS(namespace, "title");
    hoverTitle.textContent = tooltipMessage;
    hoverTarget.appendChild(hoverTitle);
    hoverTarget.addEventListener("pointerenter", () => showEdgeTooltip(tooltipMessage));
    hoverTarget.addEventListener("pointerleave", hideEdgeTooltip);
    svg.appendChild(hoverTarget);
  }

  for (const node of graph.nodes) {
    const position = layout.positions.get(node.id);
    if (!position) continue;

    const group = document.createElementNS(namespace, "g");
    group.setAttribute("class", "graph-node");
    group.setAttribute("role", "button");
    group.setAttribute("tabindex", "0");
    group.setAttribute("aria-label", `${node.label}, ${node.type}, ${node.status || "idle"}, ${node.calls} calls`);
    group.setAttribute("data-tooltip", `${node.label} · ${node.type} · ${node.status || "idle"} · ${node.calls} calls`);

    const title = document.createElementNS(namespace, "title");
    title.textContent = `${node.label}\nType: ${node.type}\nStatus: ${node.status || "idle"}\nCalls: ${node.calls}`;
    group.appendChild(title);
    const inspectNode = () => inspectGraphNode(node);
    group.addEventListener("click", (event) => {
      if (consumeSuppressedGraphClick()) {
        event.preventDefault();
        return;
      }
      inspectNode();
    });
    group.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        inspectNode();
      }
    });
    group.appendChild(createNodeShape(node, position));

    const labelText = graphNodeLabel(node);
    const callsText = `${node.calls} calls`;
    const labelWidth = Math.max(76, labelText.length * 7 + 14, callsText.length * 6.5 + 14);
    const labelBackground = document.createElementNS(namespace, "rect");
    labelBackground.setAttribute("class", "node-label-background");
    labelBackground.setAttribute("x", (position.x - labelWidth / 2).toString());
    labelBackground.setAttribute("y", (position.y - 29).toString());
    labelBackground.setAttribute("width", labelWidth.toString());
    labelBackground.setAttribute("height", "45");
    labelBackground.setAttribute("rx", "3");
    group.appendChild(labelBackground);

    const text = document.createElementNS(namespace, "text");
    text.setAttribute("class", "node-label");
    text.setAttribute("x", position.x.toString());
    text.setAttribute("y", (position.y - 11).toString());
    text.textContent = labelText;
    group.appendChild(text);

    const calls = document.createElementNS(namespace, "text");
    calls.setAttribute("class", "node-call-count");
    calls.setAttribute("x", position.x.toString());
    calls.setAttribute("y", (position.y + 10).toString());
    calls.textContent = callsText;
    group.appendChild(calls);
    svg.appendChild(group);
  }

  container.appendChild(svg);
  setSequenceBaseSize(svg);
  applySequenceScale();
}

async function renderGraph(graph) {
  currentGraph = graph;
  const panel = byId("graph-panel");
  panel.classList.toggle("is-sequence-view", graphViewMode === "sequence");
  panel.classList.toggle("is-state-view", graphViewMode === "state");
  panel.classList.toggle("is-mindmap-view", graphViewMode === "mindmap");
  panel.classList.toggle("is-flow-view", graphViewMode === "flow");
  switch (graphViewMode) {
    case "state":
      await renderStateGraph(graph);
      return;
    case "mindmap":
      await renderMindmapGraph(graph);
      return;
    case "flow":
      await renderFlowchartGraph(graph);
      return;
    default:
      await renderSequenceGraph(graph);
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
  await renderGraph(graph);
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
  graphScale = Math.min(graphZoomMax, Math.max(graphZoomMin, nextScale));
  applySequenceScale();
  byId("zoom-level").textContent = `${Math.round(graphScale * 100)}%`;
  byId("zoom-slider").value = String(Math.round(graphScale * 100));
}

function zoomGraphAt(nextScale, clientX, clientY) {
  const viewport = byId("graph-viewport");
  const bounds = viewport.getBoundingClientRect();
  const offsetX = clientX - bounds.left;
  const offsetY = clientY - bounds.top;
  const contentWidth = Math.max(viewport.scrollWidth, viewport.clientWidth);
  const contentHeight = Math.max(viewport.scrollHeight, viewport.clientHeight);
  const anchorX = (viewport.scrollLeft + offsetX) / contentWidth;
  const anchorY = (viewport.scrollTop + offsetY) / contentHeight;

  setGraphScale(nextScale);
  window.requestAnimationFrame(() => {
    viewport.scrollLeft = anchorX * viewport.scrollWidth - offsetX;
    viewport.scrollTop = anchorY * viewport.scrollHeight - offsetY;
  });
}

function consumeSuppressedGraphClick() {
  const suppressed = graphInteraction.suppressClick;
  graphInteraction.suppressClick = false;
  return suppressed;
}

function handleGraphPointerDown(event) {
  if (event.button !== 0) {
    return;
  }
  if (event.target.closest?.(".graph-node, .sequence-actor, .mermaid-node-target, .mermaid-edge-target, .mermaid-edge-hit-target, g[data-et=\"participant\"]")) {
    return;
  }
  const viewport = event.currentTarget;
  graphInteraction.pointerID = event.pointerId;
  graphInteraction.startX = event.clientX;
  graphInteraction.startY = event.clientY;
  graphInteraction.startScrollLeft = viewport.scrollLeft;
  graphInteraction.startScrollTop = viewport.scrollTop;
  graphInteraction.didPan = false;
  viewport.classList.add("is-panning");
  viewport.setPointerCapture(event.pointerId);
}

function handleGraphPointerMove(event) {
  if (event.pointerId !== graphInteraction.pointerID) {
    return;
  }
  const viewport = event.currentTarget;
  const deltaX = event.clientX - graphInteraction.startX;
  const deltaY = event.clientY - graphInteraction.startY;
  if (Math.abs(deltaX) > 4 || Math.abs(deltaY) > 4) {
    graphInteraction.didPan = true;
  }
  if (graphInteraction.didPan) {
    event.preventDefault();
  }
  viewport.scrollLeft = graphInteraction.startScrollLeft - deltaX;
  viewport.scrollTop = graphInteraction.startScrollTop - deltaY;
}

function handleGraphPointerUp(event) {
  if (event.pointerId !== graphInteraction.pointerID) {
    return;
  }
  const viewport = event.currentTarget;
  if (graphInteraction.didPan) {
    graphInteraction.suppressClick = true;
    window.setTimeout(() => { graphInteraction.suppressClick = false; }, 0);
  }
  viewport.classList.remove("is-panning");
  if (viewport.hasPointerCapture(event.pointerId)) {
    viewport.releasePointerCapture(event.pointerId);
  }
  graphInteraction.pointerID = null;
}

function handleGraphWheel(event) {
  if (event.deltaY === 0) {
    return;
  }
  event.preventDefault();
  const direction = event.deltaY < 0 ? graphZoomStep : -graphZoomStep;
  zoomGraphAt(graphScale + direction, event.clientX, event.clientY);
}

function fitGraph() {
  const viewport = byId("graph-viewport");
  const baseWidth = graphBaseSize?.width || 900;
  setGraphScale(Math.max(0.65, Math.min(1, viewport.clientWidth / baseWidth)));
  viewport.scrollTo({ top: 0, left: 0, behavior: "smooth" });
}

async function setGraphViewMode(mode) {
  if (!currentGraph || !["sequence", "state", "mindmap", "flow"].includes(mode)) {
    return;
  }
  graphViewMode = mode;
  for (const button of document.querySelectorAll(".graph-view-tab")) {
    const selected = button.id === `${mode}-view`;
    button.classList.toggle("active", selected);
    button.setAttribute("aria-selected", selected.toString());
  }
  await renderGraph(currentGraph);
  fitGraph();
}

async function downloadGraphPNG() {
  const svg = byId("graph").querySelector("svg");
  if (!svg) {
    return;
  }
  const viewBox = svg.viewBox?.baseVal;
  const sourceWidth = Math.max(1, viewBox?.width || svg.getBoundingClientRect().width);
  const sourceHeight = Math.max(1, viewBox?.height || svg.getBoundingClientRect().height);
  const scale = Math.min(2, 4096 / Math.max(sourceWidth, sourceHeight));
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(sourceWidth * scale));
  canvas.height = Math.max(1, Math.round(sourceHeight * scale));
  const context = canvas.getContext("2d");
  context.fillStyle = "#ffffff";
  context.fillRect(0, 0, canvas.width, canvas.height);
  const serialized = new XMLSerializer().serializeToString(svg);
  const blob = new Blob([serialized], { type: "image/svg+xml;charset=utf-8" });
  const sourceURL = URL.createObjectURL(blob);
  try {
    const image = new Image();
    await new Promise((resolve, reject) => {
      image.onload = resolve;
      image.onerror = reject;
      image.src = sourceURL;
    });
    context.drawImage(image, 0, 0, canvas.width, canvas.height);
    const link = document.createElement("a");
    link.download = `agent-squad-${graphViewMode}-diagram.png`;
    link.href = canvas.toDataURL("image/png");
    link.click();
  } finally {
    URL.revokeObjectURL(sourceURL);
  }
}

async function toggleGraphFullscreen() {
  const panel = byId("graph-panel");
  if (document.fullscreenElement === panel) {
    await document.exitFullscreen();
    return;
  }
  if (panel.requestFullscreen) {
    await panel.requestFullscreen();
    return;
  }
  panel.classList.toggle("is-fullscreen-fallback");
  updateGraphFullscreenControl();
}

function updateGraphFullscreenControl() {
  const panel = byId("graph-panel");
  const button = byId("graph-fullscreen");
  const isFullscreen = document.fullscreenElement === panel || panel.classList.contains("is-fullscreen-fallback");
  button.setAttribute("aria-label", isFullscreen ? "Close graph fullscreen" : "Open graph fullscreen");
  button.dataset.tooltip = isFullscreen ? "Close graph fullscreen" : "Open graph fullscreen";
}

function bindGraphControls() {
  for (const button of document.querySelectorAll(".graph-view-tab")) {
    button.addEventListener("click", () => {
      setGraphViewMode(button.id.replace(/-view$/, "")).catch((error) => {
        byId("inspector").textContent = error.stack || String(error);
      });
    });
  }
  byId("zoom-out").addEventListener("click", () => setGraphScale(graphScale - graphZoomStep));
  byId("zoom-in").addEventListener("click", () => setGraphScale(graphScale + graphZoomStep));
  byId("zoom-slider").addEventListener("input", (event) => setGraphScale(Number(event.target.value) / 100));
  byId("zoom-fit").addEventListener("click", fitGraph);
  byId("graph-download").addEventListener("click", () => {
    downloadGraphPNG().catch((error) => {
      byId("inspector").textContent = error.stack || String(error);
    });
  });
  byId("graph-fullscreen").addEventListener("click", () => {
    toggleGraphFullscreen().catch((error) => {
      byId("inspector").textContent = error.stack || String(error);
    });
  });
  document.addEventListener("fullscreenchange", () => {
    updateGraphFullscreenControl();
    window.requestAnimationFrame(fitGraph);
  });

  const viewport = byId("graph-viewport");
  viewport.addEventListener("pointerdown", handleGraphPointerDown);
  viewport.addEventListener("pointermove", handleGraphPointerMove);
  viewport.addEventListener("pointerup", handleGraphPointerUp);
  viewport.addEventListener("pointercancel", handleGraphPointerUp);
  viewport.addEventListener("wheel", handleGraphWheel, { passive: false });
  updateGraphFullscreenControl();
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
