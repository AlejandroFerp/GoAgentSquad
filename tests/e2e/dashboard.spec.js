const { test, expect } = require("@playwright/test");

test("inspects a retained query through graph, inspector, and live logs", async ({ page }) => {
  await page.goto("/");
  await expect(page).toHaveTitle("Agent Squad Dashboard");

  await page.getByRole("button", { name: "Queries 1" }).click();
  const query = page.getByRole("button", { name: /Open done query browser-query/ });
  await expect(query).toBeVisible();
  await query.click();

  await expect(page.getByRole("heading", { name: "Execution graph" })).toBeVisible();
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