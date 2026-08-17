const { test, expect } = require("@playwright/test");

test("inspects a retained query through graph, inspector, and live logs", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle("Agent Squad Dashboard");

  await page.getByRole("button", { name: "Queries 1" }).click();
  const query = page.getByRole("button", { name: /Open done query browser-query/ });
  await expect(query).toBeVisible();
  await query.click();

  await expect(page.getByRole("heading", { name: "Execution graph" })).toBeVisible();
  await expect(page.locator('#graph svg[aria-roledescription="sequence"]')).toBeVisible();
  await expect(page.getByRole("heading", { name: "Token Budget" }).locator(".."))
    .toContainText("20 / 50");
  await expect(page.getByRole("heading", { name: "USD Budget" }).locator(".."))
    .toContainText("$0.004200 / $0.010000");
  await expect(page.getByRole("heading", { name: "Budget Status" }).locator(".."))
    .toContainText("available");
  await page.getByRole("button", { name: /agent-1, agent, done/ }).click();
  await expect(page.getByRole("heading", { name: "agent-1" })).toBeVisible();
  await expect(page.getByText("LLM trace (1 calls)")).toBeVisible();

  await page.getByRole("tab", { name: "Logs" }).click();
  await expect(page.getByRole("log")).toContainText("Browser validation agent response.");

  await page.getByRole("button", { name: "Close panel" }).click();
  await expect(page.locator("#workspace-drawer")).toHaveAttribute("aria-hidden", "true");
});

test("keeps the dashboard usable on a narrow viewport", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto("/");

  await expect(page.getByRole("heading", { name: "Agent Squad Dashboard" })).toBeVisible();
  await expect(page.getByRole("button", { name: "Queries 1" })).toBeVisible();
  await page.getByRole("button", { name: "Queries 1" }).click();
  await expect(page.getByRole("tab", { name: "Queries" })).toBeVisible();
  await expect(page.getByRole("button", { name: /Open done query browser-query/ })).toBeVisible();
});

test("supports graph wheel zoom, mouse panning, and fullscreen mode", async ({ page }) => {
  await page.goto("/");

  const viewport = page.locator("#graph-viewport");
  await expect(viewport).toBeVisible();
  await viewport.scrollIntoViewIfNeeded();
  const viewportBox = await viewport.boundingBox();
  if (!viewportBox) {
    throw new Error("graph viewport has no bounding box");
  }

  const zoomLevel = page.locator("#zoom-level");
  const initialZoom = await zoomLevel.textContent();
  await page.mouse.move(viewportBox.x + viewportBox.width / 2, viewportBox.y + viewportBox.height / 2);
  await page.mouse.wheel(0, -600);
  await expect(zoomLevel).not.toHaveText(initialZoom || "100%");

  const initialScroll = await viewport.evaluate((element) => ({
    left: element.scrollLeft,
    top: element.scrollTop,
  }));
  await page.mouse.move(viewportBox.x + 420, viewportBox.y + 200);
  await page.mouse.down();
  await page.mouse.move(viewportBox.x + 300, viewportBox.y + 140, { steps: 4 });
  await page.mouse.up();
  const pannedScroll = await viewport.evaluate((element) => ({
    left: element.scrollLeft,
    top: element.scrollTop,
  }));
  expect(pannedScroll.left !== initialScroll.left || pannedScroll.top !== initialScroll.top).toBe(true);

  await page.getByRole("button", { name: "Open graph fullscreen" }).click();
  await expect.poll(() => page.locator("#graph-panel").evaluate((element) => document.fullscreenElement === element)).toBe(true);
  await expect(page.getByRole("button", { name: "Close graph fullscreen" })).toBeVisible();
  await page.getByRole("button", { name: "Close graph fullscreen" }).click();
  await expect.poll(() => page.locator("#graph-panel").evaluate((element) => document.fullscreenElement === element)).toBe(false);
});

test("switches Mermaid views, inspects transitions, and downloads PNG", async ({ page }) => {
  await page.goto("/");

  await page.getByRole("tab", { name: "State" }).click();
  await expect(page.locator('#graph svg[aria-roledescription="stateDiagram"]')).toBeVisible();
  const stateEdge = page.locator('#graph [data-et="edge"]').first();
  await expect(stateEdge).toHaveAttribute("data-tooltip", /click to inspect|routed|event/i);
  await stateEdge.click({ force: true });
  await expect(page.getByRole("heading", { name: /event$/i })).toBeVisible();
  await expect(page.getByRole("heading", { name: "Event audit" })).toBeVisible();
  await expect(page.locator("dt").filter({ hasText: "Thread" })).toBeVisible();
  await expect(page.getByText("Visible event data")).toBeVisible();
  await page.getByRole("button", { name: "Close panel" }).click();

  await page.getByRole("tab", { name: "Mindmap" }).click();
  await expect(page.locator('#graph svg[aria-roledescription="mindmap"]')).toBeVisible();
  const mindmapEdge = page.locator('#graph .mermaid-edge-hit-target').first();
  await mindmapEdge.click();
  await expect(page.getByRole("heading", { name: "Observed relationship" })).toBeVisible();
  await expect(page.getByText("Events associated with this relationship")).toBeVisible();
  await page.getByRole("button", { name: "Close panel" }).click();
  const mindmapAgent = page.locator('#graph g.mermaid-node-target[role="button"]').filter({ hasText: "agent-1" });
  await expect(mindmapAgent).toBeVisible();
  await expect(mindmapAgent).toHaveAttribute("data-tooltip", /agent-1.*click to inspect/);
  await mindmapAgent.click();
  await expect(page.getByRole("heading", { name: "agent-1" })).toBeVisible();
  await page.getByRole("button", { name: "Close panel" }).click();

  await page.getByRole("tab", { name: "Flow" }).click();
  await expect(page.locator('#graph svg[aria-roledescription="flowchart"]')).toBeVisible();
  const flowEdge = page.locator('#graph .mermaid-edge-hit-target').first();
  await expect(flowEdge).toHaveAttribute("data-tooltip", /->/);
  await flowEdge.click();
  await expect(page.getByRole("heading", { name: "Observed relationship" })).toBeVisible();
  await expect(page.getByText("Raw relationship")).toBeVisible();
  await page.getByRole("button", { name: "Close panel" }).click();

  await page.getByRole("tab", { name: "Sequence" }).click();
  await expect(page.locator('#graph svg[aria-roledescription="sequence"]')).toBeVisible();
  const downloadPromise = page.waitForEvent("download");
  await page.getByRole("button", { name: "Download current diagram as PNG" }).click();
  const download = await downloadPromise;
  expect(download.suggestedFilename()).toBe("agent-squad-sequence-diagram.png");
});