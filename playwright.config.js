const { defineConfig, devices } = require("@playwright/test");

const port = process.env.PLAYWRIGHT_PORT || "18080";
const baseURL = process.env.PLAYWRIGHT_BASE_URL || `http://127.0.0.1:${port}`;
const useExternalServer = Boolean(process.env.PLAYWRIGHT_BASE_URL);

module.exports = defineConfig({
  testDir: "./tests/e2e",
  timeout: 15000,
  reporter: process.env.CI ? "line" : "list",
  use: {
    baseURL,
    trace: "on-first-retry",
    screenshot: "only-on-failure",
    video: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: useExternalServer
    ? undefined
    : {
        command: `go run ./cmd/squad-dashboard --addr 127.0.0.1:${port} --trace-file tests/e2e/fixtures/dashboard.jsonl`,
        url: `${baseURL}/`,
        reuseExistingServer: false,
        timeout: 120000,
      },
});