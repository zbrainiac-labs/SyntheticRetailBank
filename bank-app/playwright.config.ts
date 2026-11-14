import { defineConfig } from "@playwright/test";

// Runs against an already running app: `make dev`, `make run-local` (Docker) or,
// later, the SPCS endpoint via BASE_URL.
export default defineConfig({
  testDir: "./e2e",
  timeout: 60_000,
  use: { baseURL: process.env.BASE_URL ?? "http://localhost:8080" },
  reporter: [["list"]],
});
