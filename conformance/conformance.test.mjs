// The pinned Durable Streams server conformance suite, run UNMODIFIED
// against a live server.
//
// Why this file exists: the package ships an npx CLI, but its own
// include glob does not match the runner it invokes, so the CLI reports
// zero tests and exits 0 — a green result that ran nothing. Importing
// the entry point from a normal test file that vitest discovers avoids
// the CLI entirely.
import { runConformanceTests } from "@durable-streams/server-conformance-tests";

runConformanceTests({
  baseUrl: process.env.CONFORMANCE_TEST_URL ?? "http://127.0.0.1:8090",
});
