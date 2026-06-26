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

function byId(id) {
  return document.getElementById(id);
}

function pretty(obj) {
  return JSON.stringify(obj, null, 2);
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
  if (!queries.length) {
    container.innerHTML = "<p>No queries yet.</p>";
    return;
  }
  for (const query of queries) {
    const item = document.createElement("button");
    item.className = `query-item${query.summary.correlation_id === selectedQuery ? " active" : ""}`;
    item.innerHTML = `
      <strong>${query.summary.summary || query.summary.correlation_id}</strong>
      <div class="meta">${query.summary.status} · ${query.metrics.total_steps} steps · ${query.metrics.duration_ms} ms</div>
    `;
    item.onclick = () => selectQuery(query.summary.correlation_id);
    container.appendChild(item);
  }
}

function renderMetrics(metrics) {
  const cards = [
    ["Duration", `${metrics.duration_ms || 0} ms`],
    ["Tokens", `${metrics.total_tokens_in || 0} / ${metrics.total_tokens_out || 0}`],
    ["LLM Calls", `${metrics.llm_calls || 0}`],
    ["Agents", `${metrics.unique_agents || 0}`],
    ["Tool Calls", `${metrics.tool_calls || 0}`],
    ["Steps", `${metrics.total_steps || 0}`],
    ["Errors", `${metrics.errors || 0}`],
    ["Query", metrics.correlation_id || "-"],
  ];
  byId("metrics").innerHTML = cards.map(([label, value]) => `
    <article class="card">
      <h3>${label}</h3>
      <strong>${value}</strong>
    </article>
  `).join("");
}

function renderTimeline(steps) {
  const container = byId("timeline");
  container.innerHTML = "";
  if (!steps.length) {
    container.innerHTML = "<p>No timeline yet.</p>";
    return;
  }
  for (const step of steps) {
    const row = document.createElement("div");
    row.className = "timeline-row";
    row.innerHTML = `
      <div class="kind">${step.kind}</div>
      <div>${step.summary || "(no summary)"}</div>
      <div class="meta">${step.agent_id || "-"} · ${step.squad_id || "-"} · ${step.duration_ms || 0} ms</div>
    `;
    row.onclick = () => {
      byId("inspector").textContent = pretty(step);
    };
    container.appendChild(row);
  }
}

function renderGraph(graph) {
  const svg = byId("graph");
  svg.innerHTML = "";
  byId("selected-query").textContent = graph.correlation_id || "";
  if (!graph.nodes.length) {
    return;
  }

  const columns = { user: 0, squad: 1, agent: 2, transversal: 3, tool: 4 };
  const grouped = {};
  for (const node of graph.nodes) {
    const type = node.type || "agent";
    if (!grouped[type]) grouped[type] = [];
    grouped[type].push(node);
  }

  const positions = new Map();
  const width = 1200;
  const height = 560;
  const colWidth = width / 5;
  for (const [type, nodes] of Object.entries(grouped)) {
    nodes.forEach((node, index) => {
      const x = 100 + (columns[type] ?? 2) * colWidth;
      const y = 90 + index * 110;
      positions.set(node.id, { x, y });
    });
  }

  for (const edge of graph.edges) {
    const from = positions.get(edge.source);
    const to = positions.get(edge.target);
    if (!from || !to) continue;

    const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
    line.setAttribute("class", "edge");
    line.setAttribute("x1", from.x);
    line.setAttribute("y1", from.y);
    line.setAttribute("x2", to.x);
    line.setAttribute("y2", to.y);
    svg.appendChild(line);

    const label = document.createElementNS("http://www.w3.org/2000/svg", "text");
    label.setAttribute("class", "edge-label");
    label.setAttribute("x", ((from.x + to.x) / 2).toString());
    label.setAttribute("y", (((from.y + to.y) / 2) - 6).toString());
    label.textContent = `${edge.label} (${edge.count})`;
    svg.appendChild(label);
  }

  for (const node of graph.nodes) {
    const position = positions.get(node.id);
    if (!position) continue;

    const circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    circle.setAttribute("cx", position.x.toString());
    circle.setAttribute("cy", position.y.toString());
    circle.setAttribute("r", (26 + Math.min(node.calls, 6) * 2).toString());
    circle.setAttribute("class", `node-${node.status || "idle"}`);
    circle.style.cursor = "pointer";
    circle.addEventListener("click", () => {
      byId("inspector").textContent = pretty(node);
    });
    svg.appendChild(circle);

    const text = document.createElementNS("http://www.w3.org/2000/svg", "text");
    text.setAttribute("class", "node-label");
    text.setAttribute("x", position.x.toString());
    text.setAttribute("y", (position.y - 4).toString());
    text.textContent = node.label;
    svg.appendChild(text);

    const caption = document.createElementNS("http://www.w3.org/2000/svg", "text");
    caption.setAttribute("class", "node-caption");
    caption.setAttribute("x", position.x.toString());
    caption.setAttribute("y", (position.y + 14).toString());
    caption.textContent = `${node.type} · ${node.calls} calls`;
    svg.appendChild(caption);
  }
}

async function refreshQueries(selectLatest = false) {
  const queries = await fetchJSON("/api/queries");
  renderQueries(queries);
  if (!selectedQuery && queries.length) {
    selectedQuery = queries[0].summary.correlation_id;
  }
  if (selectLatest && queries.length) {
    selectedQuery = queries[0].summary.correlation_id;
  }
}

async function selectQuery(correlationID) {
  selectedQuery = correlationID;
  await refreshQueries(false);
  const [timeline, graph, metrics] = await Promise.all([
    fetchJSON(`/api/queries/${correlationID}/timeline`),
    fetchJSON(`/api/queries/${correlationID}/graph`),
    fetchJSON(`/api/metrics/summary?query=${encodeURIComponent(correlationID)}`),
  ]);
  renderTimeline(timeline);
  renderGraph(graph);
  renderMetrics(metrics);
}

function connectStream() {
  const streamStatus = byId("stream-status");
  const source = new EventSource("/api/stream");
  source.onopen = () => {
    streamStatus.textContent = "live";
  };
  source.onerror = () => {
    streamStatus.textContent = "reconnecting…";
  };
  const onStep = async (event) => {
    const step = JSON.parse(event.data);
    if (!selectedQuery) {
      selectedQuery = step.correlation_id;
    }
    await refreshQueries(false);
    if (step.correlation_id === selectedQuery) {
      await selectQuery(selectedQuery);
    }
  };
  stepEvents.forEach((eventName) => source.addEventListener(eventName, onStep));
}

async function bootstrap() {
  await refreshQueries(true);
  if (selectedQuery) {
    await selectQuery(selectedQuery);
  } else {
    renderMetrics({});
  }
  connectStream();
}

bootstrap().catch((error) => {
  byId("inspector").textContent = error.stack || String(error);
});
