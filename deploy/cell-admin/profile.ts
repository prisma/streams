// The shared-cell profile as cell-admin reads it: deploy/profiles/*.env
// layered in order (later wins, as the deploy scripts pass them), checked
// for the single-server shared-cell shape, and turned into the quota
// ceilings `admit` enforces. The ceiling rule mirrors the server's
// (src/auth/ceiling.rs, `CellCeiling::ceiling`): on a bounded axis the
// ceiling is max(1, floor(bound / k)); an axis with no shared bound has
// none. The stored-bytes quota has no bound and no ceiling: the profile's
// PROJECT_STORED_BYTES_DEFAULT is what `admit` gives a project that sets
// none, as the server gives it to a 0 (`CellCeiling::effective`).
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { AdminError } from "./errors";

/// Entries of the per-stream maps every project shares: the descriptor
/// cache's REGISTRY_CACHE_MAX (src/registry/cache.rs), also the usage
/// limiter's and the key cache's size. profile.test.ts pins the match.
export const STREAM_MAP_ENTRIES = 65_536;

/// The quota fields of the policy feed (contracts/streams-platform/v1,
/// project-policies.schema.json), in contract order.
export const QUOTA_FIELDS = [
  "requests_per_sec",
  "append_bytes_per_sec",
  "append_records_per_sec",
  "read_bytes_per_sec",
  "max_inflight_requests",
  "max_live_subscriptions",
  "max_streams",
  "queued_append_bytes",
  "max_stored_bytes",
  "request_weight",
  "byte_weight",
] as const;
export type QuotaField = (typeof QUOTA_FIELDS)[number];
export type Quotas = Record<QuotaField, number>;

/// The axes with a shared bound, and so a ceiling. Of the other two,
/// append records/s keeps its feed value on the server too, and stored
/// bytes take the cell's default when the feed sets none.
export const BOUNDED_FIELDS = [
  "requests_per_sec",
  "append_bytes_per_sec",
  "read_bytes_per_sec",
  "max_inflight_requests",
  "max_live_subscriptions",
  "max_streams",
  "queued_append_bytes",
] as const satisfies readonly QuotaField[];
export type BoundedField = (typeof BOUNDED_FIELDS)[number];
export type Ceilings = Record<BoundedField, number>;

/// What a cell is sized by: k and the bound behind each ceiling, and the
/// stored bytes a project is admitted with when it is given none.
export interface CellSizing {
  share_k: number;
  bounds: Ceilings;
  ceilings: Ceilings;
  stored_default: number;
}

/// Read KEY=VALUE files in order; a later file's value wins.
export function readProfiles(paths: readonly string[]): Map<string, string> {
  return parseProfiles(
    paths.map((path) => {
      try {
        return { path, text: readFileSync(path, "utf8") };
      } catch {
        throw new AdminError(`cannot read profile ${path}`);
      }
    }),
  );
}

/// Layer KEY=VALUE texts in order, a later one's value winning: files
/// `readProfiles` read, or copies compiled into a bundle (the issuer's),
/// each named by `path` in a refusal.
export function parseProfiles(files: readonly { path: string; text: string }[]): Map<string, string> {
  const env = new Map<string, string>();
  for (const { path, text } of files) {
    for (const [i, raw] of text.split("\n").entries()) {
      const line = raw.trim();
      if (line === "" || line.startsWith("#")) continue;
      const eq = line.indexOf("=");
      if (eq <= 0 || /\s/.test(line)) {
        throw new AdminError(`${path}:${i + 1}: not a space-free KEY=VALUE line`);
      }
      env.set(line.slice(0, eq), line.slice(eq + 1));
    }
  }
  return env;
}

/// The layered profile's identity: sha256 (hex) over its KEY=VALUE lines
/// sorted by key, one per line. Comments, order and the files' paths do
/// not enter it, so the issuer's bundled copy of the profile and the files
/// `init` read have one digest when they say the same.
export function profileDigest(env: Map<string, string>): string {
  const lines = [...env.entries()]
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
    .map(([k, v]) => `${k}=${v}\n`);
  return createHash("sha256").update(lines.join("")).digest("hex");
}

function positive(env: Map<string, string>, name: string): number {
  const raw = env.get(name);
  if (raw === undefined) throw new AdminError(`profile does not set ${name}`);
  if (!/^[0-9]+$/.test(raw) || Number(raw) <= 0 || !Number.isSafeInteger(Number(raw))) {
    throw new AdminError(`profile ${name}=${raw} is not a positive integer`);
  }
  return Number(raw);
}

function requireValue(env: Map<string, string>, name: string, want: string): void {
  const got = env.get(name);
  if (got !== want) {
    throw new AdminError(`profile ${name}=${got ?? "(unset)"}: a shared cell needs ${name}=${want}`);
  }
}

/// A single-server shared cell's posture (PLAN section 1.2 shape (a)).
/// It is kept awake for good (the owner's public-beta answer (4) of
/// 2026-10-11, "Keep awake"): the wrapper (deploy/app-server/index.ts)
/// holds Compute's keep-awake guard for KEEP_AWAKE=1, for the process's
/// life unless KEEP_AWAKE_UNTIL_MS bounds it.
function checkPosture(env: Map<string, string>): void {
  requireValue(env, "STREAMS_AUTH_MODE", "enforce");
  requireValue(env, "STREAMS_RELEASE_POSTURE", "1");
  requireValue(env, "BILLING_MODE", "required");
  requireValue(env, "ROLLUP", "1");
  requireValue(env, "INITIAL_SHARDS", "1");
  requireValue(env, "KEEP_AWAKE", "1");
  if (env.has("KEEP_AWAKE_UNTIL_MS")) {
    throw new AdminError("profile sets KEEP_AWAKE_UNTIL_MS: a shared cell is kept awake for good");
  }
  for (const name of ["FLEET_PREFIX", "FLEET_INTERNAL_TOKEN", "WORKLOAD_TOKEN_FILE"]) {
    if (env.has(name)) {
      throw new AdminError(`profile sets ${name}: a single-server shared cell runs without it`);
    }
  }
}

const ceilingOf = (bound: number, k: number) => Math.max(1, Math.floor(bound / k));

/// The fewest projects whose memory lines may fill the instance read
/// memory: the owner accepted four (README "Shared cells Q1" (A)).
const READ_MEMORY_FILLERS = 4;

/// The profile's LiveFeed share must be the cell's divided by k: one
/// project at its share leaves the rest to the other k - 1. Its memory
/// line may be larger than the read memory divided by k, but it must take
/// at least READ_MEMORY_FILLERS projects at their lines to fill it, and k
/// projects at their lines must stay within the one shard's unabsorbed-
/// bytes line, where every append to the shard is refused (phase B step
/// 12 on one shard). That is necessary, not the k-tolerance itself: a
/// project's queued bytes can take it past its line (shared-cell.env).
function checkShares(env: Map<string, string>, k: number): void {
  const feedTotal = positive(env, "SSE_FEED_TOTAL_BYTES");
  const feedProject = positive(env, "SSE_FEED_PROJECT_BYTES");
  if (feedProject !== Math.floor(feedTotal / k)) {
    throw new AdminError(
      `profile SSE_FEED_PROJECT_BYTES=${feedProject}: a cell shared ${k} ways needs ` +
        `SSE_FEED_TOTAL_BYTES / k = ${Math.floor(feedTotal / k)}`,
    );
  }
  // The instance read memory is a quarter of the RSS shed line
  // (src/admission/read_memory.rs, READ_MEMORY_SHARE_OF_SHED).
  const readMemory = Math.floor((positive(env, "ADMIT_RSS_SHED_MB") * 1024 * 1024) / 4);
  const line = positive(env, "PROJECT_MEMORY_PRESSURE_BYTES");
  if ((READ_MEMORY_FILLERS - 1) * line >= readMemory) {
    throw new AdminError(
      `profile PROJECT_MEMORY_PRESSURE_BYTES=${line}: ${READ_MEMORY_FILLERS - 1} projects at their lines ` +
        `would fill the instance read memory (${readMemory}); the owner accepted ${READ_MEMORY_FILLERS}`,
    );
  }
  const shard = positive(env, "MAX_UNABSORBED_BYTES_PER_SHARD");
  if (k * line > shard) {
    throw new AdminError(
      `profile PROJECT_MEMORY_PRESSURE_BYTES=${line}: ${k} projects at their memory lines (${k * line}) ` +
        `would pass the shard line MAX_UNABSORBED_BYTES_PER_SHARD=${shard}`,
    );
  }
}

/// Check the layered profile is a single-server shared cell and derive
/// its ceilings.
export function cellSizing(env: Map<string, string>): CellSizing {
  checkPosture(env);
  const k = positive(env, "PROJECT_SHARE_K");
  if (k < 2) throw new AdminError(`profile PROJECT_SHARE_K=${k}: a shared cell needs k >= 2`);
  checkShares(env, k);
  const bounds: Ceilings = {
    requests_per_sec: positive(env, "CELL_ENVELOPE_REQUESTS_PER_SEC"),
    append_bytes_per_sec: positive(env, "CELL_ENVELOPE_APPEND_BYTES_PER_SEC"),
    read_bytes_per_sec: positive(env, "CELL_ENVELOPE_READ_BYTES_PER_SEC"),
    max_inflight_requests: positive(env, "ADMIT_MAX_INFLIGHT"),
    max_live_subscriptions: positive(env, "SSE_MAX_CONNECTIONS"),
    max_streams: STREAM_MAP_ENTRIES,
    queued_append_bytes: positive(env, "MAX_UNABSORBED_BYTES_PER_INSTANCE"),
  };
  const ceilings = Object.fromEntries(
    BOUNDED_FIELDS.map((f) => [f, ceilingOf(bounds[f], k)]),
  ) as Ceilings;
  return { share_k: k, bounds, ceilings, stored_default: positive(env, "PROJECT_STORED_BYTES_DEFAULT") };
}
