// The tenants manifest (tenants.ts, tenants-admit.ts): tenant classes with
// counts, quotas and per-class phase lists, expanded into projects. Pure:
// no I/O beyond reading the file. Shape (bench/k2cost/tenants/*.json):
//
//   {"name": "layer-b", "seed": 1, "duration": 4200,
//    "periods": {"solo": [900, 1800], "noisy": [2100, 4200]},
//    "quota_sets": {"q-hostile": {"requests_per_sec": 20, ...}},
//    "classes": [{"class": "s0", "count": 8, "role": "compliant",
//                 "streams": 3, "prefix": "t/", "quotas": "q-hostile"?,
//                 "phases": [{"mode": "produce", "at": 600, "secs": 3600,
//                             "spread": 60, "repeat": false,
//                             "args": {"rate": 1, "records": 41}}]}],
//    "legs": [{"leg": "probes", "at": 3600, "secs": 400, "args": {...}}]}
//
// Times are seconds from the run's start (scaled by --time-scale). A
// phase's `spread` starts project j of n at at + spread * j / n. `secs`
// bounds the phase; without it a phase ends when its mode is done or at
// the run's `duration`. `repeat` reruns a run-to-completion mode until
// its window ends (never for a mode whose records the audit replays).
// `quotas` names a quota set the project is admitted with; without one it
// takes the cell's ceilings (bound / k). Roles: compliant tenants must see
// no refusal, the victim's latency is judged per period, hostile ones are
// expected to be refused (typed), idle ones only hold state.

import { createHash, createHmac } from "node:crypto";
import { readFileSync } from "node:fs";
import { MODE_FLAGS } from "./common.ts";
import { type Owner, ownerOf } from "./oracle.ts";

export const ROLES = ["compliant", "victim", "hostile", "idle"] as const;
export type Role = (typeof ROLES)[number];

/** The quota fields admit takes (deploy/cell-admin/profile.ts QUOTA_FIELDS). */
export const QUOTA_FIELDS = [
  "requests_per_sec", "append_bytes_per_sec", "append_records_per_sec", "read_bytes_per_sec",
  "max_inflight_requests", "max_live_subscriptions", "max_streams", "queued_append_bytes",
] as const;

/** Flags the runner sets itself; a phase may not. */
const RESERVED = new Set(["base", "headers-file", "out", "ledger", "key", "window", "port", "seed", "duration", "expect"]);
/** Modes whose acked records the audit replays from the seed: a rerun
 * would repeat them, so they never `repeat`. */
const NO_REPEAT = new Set(["produce", "tail", "subs", "group"]);
/** Modes tenants.ts runs besides k2gen's (hostile.ts registers more). */
export const EXTRA_MODES: Record<string, string[]> = { idle: [] };

export interface PhaseSpec {
  mode: string;
  at: number;
  secs?: number;
  spread?: number;
  repeat?: boolean;
  args?: Record<string, string | number>;
}

export interface ClassSpec {
  class: string;
  count: number;
  role: Role;
  quotas?: string;
  streams?: number;
  prefix?: string;
  comment?: string;
  phases: PhaseSpec[];
}

export interface LegSpec {
  leg: string;
  at: number;
  secs?: number;
  args?: Record<string, string | number>;
}

export interface Manifest {
  name: string;
  seed: number;
  duration: number;
  comment?: string;
  periods?: Record<string, [number, number]>;
  quota_sets?: Record<string, Partial<Record<(typeof QUOTA_FIELDS)[number], number>>>;
  classes: ClassSpec[];
  legs?: LegSpec[];
}

export interface Project {
  id: string;
  workspace: string;
  cls: ClassSpec;
  /** Index within the class and across the manifest. */
  index: number;
  global: number;
  seed: number;
  owner: Owner;
  /** The project's record key (Prisma-Encryption-Key), base64. */
  recordKey: string;
  quotaFlags: string[];
}

export interface Plan {
  manifest: Manifest;
  sha256: string;
  scale: number;
  projects: Project[];
}

const ID = /^[a-z0-9][a-z0-9-]{0,23}$/;
const fail = (msg: string): never => {
  throw new Error(`manifest: ${msg}`);
};

function checkTime(where: string, v: unknown, positive = false): number {
  if (typeof v !== "number" || !Number.isFinite(v) || v < 0 || (positive && v === 0)) {
    fail(`${where} must be a ${positive ? "positive" : "non-negative"} number of seconds`);
  }
  return v as number;
}

/** The flags a mode takes, k2gen's or one tenants.ts registers. */
export function modeFlags(mode: string): string[] | undefined {
  return MODE_FLAGS[mode] ?? EXTRA_MODES[mode];
}

function checkPhase(cls: ClassSpec, ph: PhaseSpec, i: number, duration: number): void {
  const where = `class ${cls.class} phase ${i}`;
  const flags = modeFlags(ph.mode);
  if (!flags || ph.mode === "audit" || ph.mode === "corpus-stats") fail(`${where}: unknown mode ${JSON.stringify(ph.mode)}`);
  checkTime(`${where} at`, ph.at);
  if (ph.secs !== undefined) checkTime(`${where} secs`, ph.secs, true);
  if (ph.spread !== undefined) checkTime(`${where} spread`, ph.spread);
  if (ph.at + (ph.spread ?? 0) >= duration) fail(`${where} starts at or after the run's duration ${duration}`);
  if (ph.repeat && NO_REPEAT.has(ph.mode)) fail(`${where}: ${ph.mode} never repeats (its records are replayed by the audit)`);
  for (const k of Object.keys(ph.args ?? {})) {
    if (RESERVED.has(k)) fail(`${where}: --${k} is set by the runner`);
    if (!flags!.includes(k)) fail(`${where}: ${ph.mode} takes no --${k}`);
  }
  const payload = ph.args?.payload;
  if (["produce", "churn"].includes(ph.mode) && payload !== undefined && payload !== "oracle") {
    fail(`${where}: a tenant writes --payload oracle (got ${payload}), so every read can be checked for leaks`);
  }
  if (["walk", "tail", "group", "subs"].includes(ph.mode) && ["rate", "mbps", "concurrency"].some((k) => k in (ph.args ?? {}))) {
    fail(`${where}: a tenant's reader embeds no producer (its records could not be audited); give the class a produce phase`);
  }
}

export function checkManifest(m: Manifest): void {
  if (typeof m.name !== "string" || !ID.test(m.name)) fail(`name must match ${ID.source}`);
  if (!Number.isSafeInteger(m.seed)) fail("seed must be an integer");
  checkTime("duration", m.duration, true);
  for (const [name, span] of Object.entries(m.periods ?? {})) {
    if (name === "run") fail("period name run is the whole run's (tenants.ts)");
    if (!Array.isArray(span) || span.length !== 2 || !(checkTime(`period ${name}`, span[0]) < checkTime(`period ${name}`, span[1]))) {
      fail(`period ${name} must be [start, end) with start < end`);
    }
  }
  for (const [name, set] of Object.entries(m.quota_sets ?? {})) {
    for (const [k, v] of Object.entries(set)) {
      if (!(QUOTA_FIELDS as readonly string[]).includes(k)) fail(`quota set ${name}: unknown field ${k}`);
      if (!Number.isSafeInteger(v) || (v as number) < 1) fail(`quota set ${name}: ${k} must be a positive integer`);
    }
  }
  const seen = new Set<string>();
  for (const c of m.classes ?? fail("classes missing")) {
    if (!ID.test(c.class)) fail(`class ${JSON.stringify(c.class)} must match ${ID.source}`);
    if (seen.has(c.class)) fail(`class ${c.class} appears twice`);
    seen.add(c.class);
    if (!Number.isSafeInteger(c.count) || c.count < 1 || c.count > 9999) fail(`class ${c.class}: count must be 1..9999`);
    if (!(ROLES as readonly string[]).includes(c.role)) fail(`class ${c.class}: role must be one of ${ROLES.join(", ")}`);
    if (c.quotas !== undefined && !(m.quota_sets ?? {})[c.quotas]) fail(`class ${c.class}: no quota set ${c.quotas}`);
    if (c.streams !== undefined && (!Number.isSafeInteger(c.streams) || c.streams < 1)) fail(`class ${c.class}: streams must be >= 1`);
    c.phases.forEach((ph, i) => checkPhase(c, ph, i, m.duration));
  }
  for (const [i, leg] of (m.legs ?? []).entries()) {
    if (typeof leg.leg !== "string") fail(`leg ${i}: no name`);
    checkTime(`leg ${i} at`, leg.at);
    if (leg.secs !== undefined) checkTime(`leg ${i} secs`, leg.secs, true);
  }
}

export function readManifest(path: string): { manifest: Manifest; sha256: string } {
  const text = readFileSync(path, "utf8");
  const manifest = JSON.parse(text) as Manifest;
  checkManifest(manifest);
  return { manifest, sha256: createHash("sha256").update(text).digest("hex") };
}

/** The oracle's secret: the manifest's name and seed. The server never
 * sees it; it only lets the harness tell its tenants' records apart. */
export function oracleSecret(m: Manifest): string {
  return `${m.name}:${m.seed}`;
}

/** Every project of the manifest, in class order. */
export function expand(m: Manifest, sha256: string, scale = 1): Plan {
  if (!(scale > 0)) fail("--time-scale must be > 0");
  const secret = oracleSecret(m);
  const keyRoot = createHash("sha256").update(`k2-record-key-v1:${secret}`).digest();
  const projects: Project[] = [];
  for (const cls of m.classes) {
    const set = cls.quotas ? m.quota_sets![cls.quotas]! : {};
    const quotaFlags = Object.entries(set).map(([k, v]) => `${k}=${v}`);
    for (let i = 0; i < cls.count; i++) {
      const id = `proj_${m.name}_${cls.class}_${String(i).padStart(4, "0")}`.replaceAll("-", "_");
      projects.push({
        id,
        workspace: id.replace(/^proj_/, "wksp_"),
        cls,
        index: i,
        global: projects.length,
        seed: m.seed * 100_003 + projects.length * 1009,
        owner: ownerOf(secret, id),
        recordKey: createHmac("sha256", keyRoot).update(id).digest("base64"),
        quotaFlags,
      });
    }
  }
  return { manifest: m, sha256, scale, projects };
}

/** When project `p`'s phase starts, in seconds from the run's start. */
export function phaseStart(plan: Plan, p: Project, ph: PhaseSpec): number {
  return (ph.at + ((ph.spread ?? 0) * p.index) / p.cls.count) * plan.scale;
}

/** A phase's end, in seconds from the run's start. */
export function phaseEnd(plan: Plan, p: Project, ph: PhaseSpec): number {
  const end = plan.manifest.duration * plan.scale;
  return ph.secs === undefined ? end : Math.min(end, phaseStart(plan, p, ph) + ph.secs * plan.scale);
}

/** The phase's k2gen flags: the class's streams and prefix, then the
 * phase's own arguments. */
export function phaseFlags(p: Project, ph: PhaseSpec): Map<string, string> {
  const f = new Map<string, string>([["streams", String(p.cls.streams ?? 1)], ["prefix", p.cls.prefix ?? "t/"]]);
  for (const [k, v] of Object.entries(ph.args ?? {})) f.set(k, String(v));
  if ((ph.mode === "produce" || ph.mode === "churn") && !f.has("payload")) f.set("payload", "oracle");
  return f;
}
