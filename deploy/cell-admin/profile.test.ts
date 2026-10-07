// The shared-cell profile, and every constant cell-admin shares with the
// server, pinned against the tree. Run with `bun test ./deploy/cell-admin`.
import { describe, expect, test } from "bun:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { CUSTOMER_SCOPES, DEFAULT_PROFILES, SYSTEM_PROJECT } from "./commands";
import { schema } from "./feeds";
import { cellSizing, QUOTA_FIELDS, readProfiles, STREAM_MAP_ENTRIES } from "./profile";
import { MAX_TOKEN_LIFETIME_SECS } from "./tokens";

const ROOT = join(import.meta.dir, "..", "..");
const src = (path: string) => readFileSync(join(ROOT, path), "utf8");
const SHARED = join(ROOT, "deploy", "profiles", "shared-cell.env");

/// The names the PENDING WIRING paragraph of the profile lists: the
/// binary does not read them until the owner's bootstrap::run rows land.
const AWAITING_WIRING = [
  "CELL_ENVELOPE_APPEND_BYTES_PER_SEC",
  "CELL_ENVELOPE_READ_BYTES_PER_SEC",
  "CELL_ENVELOPE_REQUESTS_PER_SEC",
  "PROJECT_SHARE_K",
];

/// Every environment name the server binary or its wrapper reads, found
/// the way bench/k2cost/field/deploy.py's known_names finds them.
function namesTheServerReads(): Set<string> {
  const names = new Set<string>();
  const grep = (text: string, patterns: RegExp[]) => {
    for (const p of patterns) for (const m of text.matchAll(p)) names.add(m[1]);
  };
  const config = join(ROOT, "src", "config");
  for (const f of readdirSync(config).filter((f) => f.endsWith(".rs") && !f.includes("test"))) {
    grep(readFileSync(join(config, f), "utf8"), [
      /env\s*=\s*"([A-Z][A-Z0-9_]+)"/g,
      /\.get\(\s*"([A-Z][A-Z0-9_]+)"\s*\)/g,
      /env_parse(?:::<[^>]+>)?\(\s*env,\s*"([A-Z][A-Z0-9_]+)"/g,
      /envf\(\s*"([A-Z][A-Z0-9_]+)"/g,
    ]);
  }
  const wrapper = join(ROOT, "deploy", "app-server");
  for (const f of readdirSync(wrapper).filter((f) => f.endsWith(".ts"))) {
    grep(readFileSync(join(wrapper, f), "utf8"), [/process\.env\.([A-Z][A-Z0-9_]+)/g]);
  }
  return names;
}

describe("the shared-cell profile", () => {
  test("layered on compute-1g.env it is a single-server shared cell shared 8 ways", () => {
    expect(cellSizing(readProfiles(DEFAULT_PROFILES))).toEqual({
      share_k: 8,
      bounds: {
        requests_per_sec: 1411,
        append_bytes_per_sec: 2060000,
        read_bytes_per_sec: 30000000,
        max_inflight_requests: 512,
        max_live_subscriptions: 1200,
        max_streams: 65536,
      },
      ceilings: {
        requests_per_sec: 176,
        append_bytes_per_sec: 257500,
        read_bytes_per_sec: 3750000,
        max_inflight_requests: 64,
        max_live_subscriptions: 150,
        max_streams: 8192,
      },
    });
    const env = readProfiles(DEFAULT_PROFILES);
    expect([env.get("SSE_FEED_PROJECT_BYTES"), env.get("PROJECT_MEMORY_PRESSURE_BYTES")]).toEqual([
      "8388608",
      "16384000",
    ]);
  });

  test("every name it sets is one the server reads, except the four awaiting the owner's wiring", () => {
    const reads = namesTheServerReads();
    const set = [...readProfiles([SHARED]).keys()];
    expect(set.filter((n) => !reads.has(n)).sort()).toEqual(AWAITING_WIRING);
    // When the binary starts reading one of them, the profile's PENDING
    // WIRING paragraph and this list must change with it.
    expect(AWAITING_WIRING.filter((n) => reads.has(n))).toEqual([]);
  });

  const breaks: [string, string | undefined, string][] = [
    ["STREAMS_AUTH_MODE", "shadow", "a shared cell needs STREAMS_AUTH_MODE=enforce"],
    ["BILLING_MODE", "off", "a shared cell needs BILLING_MODE=required"],
    ["ROLLUP", "0", "a shared cell needs ROLLUP=1"],
    ["INITIAL_SHARDS", "4", "a shared cell needs INITIAL_SHARDS=1"],
    ["FLEET_PREFIX", "fleet/", "profile sets FLEET_PREFIX: a single-server shared cell runs without it"],
    ["FLEET_INTERNAL_TOKEN", "x".repeat(32), "profile sets FLEET_INTERNAL_TOKEN"],
    ["WORKLOAD_TOKEN_FILE", "/tmp/w", "profile sets WORKLOAD_TOKEN_FILE"],
    ["PROJECT_SHARE_K", "1", "a shared cell needs k >= 2"],
    ["PROJECT_SHARE_K", undefined, "profile does not set PROJECT_SHARE_K"],
    ["SSE_FEED_PROJECT_BYTES", "33554432", "needs SSE_FEED_TOTAL_BYTES / k = 8388608"],
    ["PROJECT_MEMORY_PRESSURE_BYTES", "16384001", "exceeds the instance read memory / k = 16384000"],
    ["CELL_ENVELOPE_REQUESTS_PER_SEC", undefined, "profile does not set CELL_ENVELOPE_REQUESTS_PER_SEC"],
    ["ADMIT_MAX_INFLIGHT", "5e2", "ADMIT_MAX_INFLIGHT=5e2 is not a positive integer"],
  ];
  test.each(breaks)("refuses %s=%s", (name, value, message) => {
    const env = readProfiles(DEFAULT_PROFILES);
    if (value === undefined) env.delete(name);
    else env.set(name, value);
    expect(() => cellSizing(env)).toThrow(message);
  });

  test("a ceiling is max(1, floor(bound / k)), as the server's", () => {
    const env = readProfiles(DEFAULT_PROFILES);
    env.set("SSE_MAX_CONNECTIONS", "5");
    env.set("ADMIT_MAX_INFLIGHT", "17");
    const c = cellSizing(env).ceilings;
    expect([c.max_live_subscriptions, c.max_inflight_requests]).toEqual([1, 2]);
  });

  test("a line that is not a space-free KEY=VALUE is refused", () => {
    const dir = mkdtempSync(join(tmpdir(), "profile-"));
    const bad = join(dir, "bad.env");
    writeFileSync(bad, "# ok\nSSE_MAX_CONNECTIONS = 1200\n");
    expect(() => readProfiles([bad])).toThrow(`${bad}:2: not a space-free KEY=VALUE line`);
    rmSync(dir, { recursive: true });
  });
});

describe("constants cell-admin shares with the server", () => {
  test("the stream-map size is the descriptor cache's", () => {
    expect(src("src/registry/cache.rs")).toContain(
      `const REGISTRY_CACHE_MAX: usize = ${STREAM_MAP_ENTRIES.toLocaleString("en-US").replaceAll(",", "_")};`,
    );
  });

  test("the scopes are exactly the server's, in its order", () => {
    const arms = [...src("src/tenant.rs").matchAll(/Scope::[A-Za-z]+ => "(streams\.[a-z.]+)"/g)].map((m) => m[1]);
    expect(arms).toEqual([...CUSTOMER_SCOPES]);
  });

  test("the system project, the token lifetime and the read-memory share are the server's", () => {
    expect(src("src/tenant.rs")).toContain(`pub(crate) const SYSTEM_PROJECT: &str = "${SYSTEM_PROJECT}";`);
    expect(MAX_TOKEN_LIFETIME_SECS).toBe(24 * 3600);
    expect(src("src/auth.rs")).toContain("pub(crate) const MAX_TOKEN_LIFETIME_SECS: i64 = 24 * 3600;");
    expect(src("src/admission/read_memory.rs")).toContain("const READ_MEMORY_SHARE_OF_SHED: u64 = 4;");
  });

  test("the quota fields are the policy contract's, in its order", () => {
    const s = schema("project-policies.schema.json") as any;
    expect(Object.keys(s.properties.projects.items.properties.quotas.properties)).toEqual([...QUOTA_FIELDS]);
  });
});
