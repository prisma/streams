// The cell watch's rules (public-beta alerting triage B2; the rows are the
// triage's table, design alerting.md "Signals and thresholds"): one pure
// step, (config, previous state, poll) -> (state, events), with no I/O and
// no clock of its own. poll.ts fills a Poll every 15 s and sinks.ts
// delivers the events (triage B3). Rows 1 and 2 are the paging tool's own
// uptime check and heartbeat monitor; this module only decides when the
// watch withholds its heartbeat (D7).
//
// Sources: `data` is /operator/data.json and `health` /health, both every
// 15 s; `billing` (/operator/billing.json) and `issuerMetrics` are fetched
// every 60 s and are null in the cycles between. A rule whose source a
// poll lacks is not evaluated: it neither opens nor counts as clean. Row
// 20 is the exception that keeps this from going quiet: it notifies when
// /health answers 200 but `data` has been missing for 60 s (a wrong or
// rotated STREAMS_OPERATOR_TOKEN answers 401, blinding every row that
// reads it).
//
// Hysteresis: a rule opens as its row states, resolves after four
// consecutive clean evaluations (one minute at 15 s, four at 60 s), and is
// never triggered again while open, except once to raise NOTIFY to PAGE.
// A fresh watch knows nothing of the incidents an earlier one opened (its
// open rules live in memory), so each rule's first clean evaluation since
// the watch started resolves the rule's keys once: an incident left open
// across a watch redeploy closes as soon as its condition is seen clear,
// and a resolve of a key that is not open changes nothing on the pager.
// NOTIFY_ONLY demotes the level rules' PAGE to NOTIFY until they are
// calibrated (triage B6): the binary rows page from the first deploy.
// Counter baselines reset on the watch's first poll and whenever the
// cell's boot id changes.
//
// Payload allowlist: the cell id, the rule id, named finite numbers, fixed
// enums, the watch's timestamp and a RUNBOOK anchor, and nothing else: no
// response body, log line, project or workspace id, stream hash, object
// path, token or URL (src/operator.rs:4-8; the owner's raw-evidence hold).
// Bodies, headers and error codes are read only to be classified into the
// enums below.

export type Class = "PAGE" | "NOTIFY";
const RANK: Record<Class, number> = { NOTIFY: 1, PAGE: 2 };

const SEC = 1_000;
const MIN = 60 * SEC;
const HOUR = 60 * MIN;
const DAY = 24 * HOUR;
const MIB = 2 ** 20;
const GIB = 2 ** 30;

/// Consecutive clean evaluations that resolve an open rule.
export const RESOLVE_AFTER = 4;
/// Consecutive failed trigger deliveries after which the heartbeat is withheld.
export const WITHHOLD_AFTER = 3;

/// A fetched JSON document; rules read it only through `get`, `num` and `bool`.
export type Json = unknown;

export interface HealthPoll {
  /// The status, or null when no answer came (a timeout or a refused connection).
  status: number | null;
  /// The watch's 5 s timeout fired.
  timedOut: boolean;
  /// `streams-origin` or `prisma-streams-origin` was present (its
  /// value is never kept; the edge strips the `Prisma-` form, #176).
  origin: boolean;
  /// `x-streams-git` and `x-streams-boot-id`, which the cell sends on a 200 only.
  git: string | null;
  bootId: string | null;
  /// The body's first bytes: classified into an enum, never forwarded.
  bodyPrefix: string;
}

export interface IssuerHealthPoll {
  status: number | null;
  timedOut: boolean;
}

/// One step of the canary (triage B7): `ok` as the probe judged it (a
/// read must return the record appended), the status (null: no answer),
/// the body's `error.code`, and whether the cell's origin header came back.
export interface CanaryStep {
  ok: boolean;
  status: number | null;
  errorCode: string | null;
  origin: boolean;
}

export interface CanaryResult {
  /// The exchange at the issuer, with the `endpoint` it answered; null in a
  /// cycle served by the cached token.
  exchange: (CanaryStep & { endpoint: string | null }) | null;
  /// The append and the durable read back; null when not attempted.
  append: CanaryStep | null;
  read: CanaryStep | null;
}

export interface Poll {
  /// The watch's clock, ms.
  at: number;
  health: HealthPoll;
  data: Json | null;
  /// The status /operator/data.json answered, null when no answer came:
  /// `data` is null whenever it is not a 200 with a JSON body.
  dataStatus: number | null;
  billing: Json | null;
  /// Null when ISSUER_ORIGIN is unset or the cycle did not fetch it.
  issuerHealth: IssuerHealthPoll | null;
  issuerMetrics: Json | null;
  /// Null without the canary (no CANARY_CREDENTIAL) and in the cycles
  /// between its rounds (poll.ts: one every fourth).
  canary: CanaryResult | null;
}

export interface Prices {
  /// Dollars per billed Class A request (PUT, multipart, LIST, COPY) and
  /// Class B request (GET, HEAD). A 2xx and a 404 are billed; a 304, a 412,
  /// a failure and every delete are not (src/store_timing.rs `billed`).
  classA: number;
  classB: number;
  /// Dollars per GiB-month stored; the estimate charges writtenBytes x 1.2.
  storageGibMonth: number;
  /// The kept-awake server's flat dollars per day (1 GiB x $0.006/GiB-h x 24).
  serverPerDay: number;
}

export interface RulesConfig {
  /// STREAMS_CELL_ID: the dedup key's prefix.
  cellId: string;
  /// STREAMS_CELL_ORIGIN: the endpoint the issuer's exchange must answer (row 22).
  cellOrigin: string;
  /// EXPECTED_GIT; "" turns row 5 off.
  expectedGit: string;
  /// NOTIFY_ONLY=1: the level rules notify instead of paging (B6).
  notifyOnly: boolean;
  /// ISSUER_ORIGIN is set: rows 21, 22, 27 and 28 run.
  issuer: boolean;
  /// G: stored bytes written per 24 h before row 23 notifies (owner S3).
  storedGrowthPerDay: number;
  /// Row 24's daily budget in dollars (owner S3).
  budgetPerDay: number;
  prices: Prices;
}

export const DEFAULT_PRICES: Prices = { classA: 5e-6, classB: 5e-7, storageGibMonth: 0.015, serverPerDay: 0.144 };
/// The owner's answer S3 of 2026-10-12: G = 50 GiB/day and $10/day per cell.
export const STORED_GROWTH_DEFAULT = 50 * GIB;
/// Row 29's bar is this many G of `bytes_put` per 24 h: every byte the
/// process puts, so the WAL, the L0 flushes, compaction and the history
/// absorber each write a stored byte again. The lead's factor under the
/// owner's delegation of 2026-10-17, uncalibrated until the certification
/// cell measures `bytes_put` per stored byte.
export const BYTES_PUT_FACTOR = 8;
export const BUDGET_DEFAULT = 10;

export function rulesConfig(c: Pick<RulesConfig, "cellId" | "cellOrigin"> & Partial<RulesConfig>): RulesConfig {
  return {
    expectedGit: "",
    notifyOnly: false,
    issuer: false,
    storedGrowthPerDay: STORED_GROWTH_DEFAULT,
    budgetPerDay: BUDGET_DEFAULT,
    prices: DEFAULT_PRICES,
    ...c,
  };
}

export interface WatchEvent {
  kind: "trigger" | "resolve";
  /// `<cell>/<rule>`.
  dedupKey: string;
  class: Class;
  rule: string;
  values: Record<string, number>;
  labels: Record<string, string>;
  /// The watch's clock, ms.
  at: number;
  runbookAnchor: string;
}

interface Sample {
  at: number;
  v: number[];
}

/// A rule's own memory: since-times of held conditions, streaks, marks.
interface Memo {
  since: Record<string, number>;
  streak: Record<string, number>;
  mark: Record<string, number>;
  samples: Record<string, Sample[]>;
}

export interface WatchState {
  polls: number;
  /// The cell's current boot id and when this watch first saw it.
  boot: { id: string; firstSeenAt: number } | null;
  /// Boot ids seen in the last 30 min, with when each was last seen.
  bootIds: Record<string, number>;
  /// The boot ids among them that a newer boot reported, on the operator
  /// surface, as its replaced predecessor which learned of it (a deploy or a
  /// restart, planned), with when the watch learned it. They do not count
  /// toward row 4's crash loop; a boot that went without that link does.
  replaced: Record<string, number>;
  /// Counter baselines, cleared on the first poll and on a new boot id.
  baselines: Record<string, number>;
  open: Record<string, { class: Class; since: number }>;
  /// Consecutive clean evaluations of each open rule.
  clean: Record<string, number>;
  /// The rules this watch has not yet seen answer clean or fire; each one's
  /// first clean answer resolves its keys (an earlier watch's incident).
  unsettled: Record<string, true>;
  memo: Record<string, Memo>;
  /// Consecutive failed trigger deliveries (D7).
  failedTriggers: number;
}

export function initialState(): WatchState {
  const unsettled: Record<string, true> = Object.fromEntries(RULES.map((r) => [r.id, true]));
  return { polls: 0, boot: null, bootIds: {}, replaced: {}, baselines: {}, open: {}, clean: {}, unsettled, memo: {}, failedTriggers: 0 };
}

// ---- the fixed enums -------------------------------------------------------

export const ORIGINS = ["server", "wrapper", "platform"] as const;
export const REASONS = ["tasks", "feeds_unpublished", "superseded", "shard_storage", "billing_not_ready", "other"] as const;
export const ANSWERS = ["status", "timeout", "no_answer"] as const;
export const STEPS = ["append", "read"] as const;
export const FAILURES = ["exchange", "endpoint"] as const;
/// The error codes a page may name: the cell's auth refusals (AuthError::kind,
/// src/auth.rs), its availability and quota refusals, the issuer's refusals
/// and the wrapper's. Any other code is `other`: a body can say anything.
export const ERROR_CODES = [
  "none", "other", "binary_exited",
  "token_too_large", "malformed", "kid_missing", "kid_unknown", "alg_not_allowed", "bad_signature",
  "wrong_issuer", "wrong_audience", "wrong_cell", "expired", "not_yet_valid", "lifetime_too_long",
  "claim_invalid", "empty_prefix_array", "project_not_active", "ownership_version_mismatch",
  "workspace_mismatch", "credential_unknown", "credential_not_active", "credential_expired",
  "credential_project_mismatch", "grant_version_mismatch", "policy_stale", "grants_stale", "keys_stale",
  "missing_scope", "prefix_denied",
  "unauthorized", "not_found", "internal", "temporarily_unavailable", "shard_open_failed",
  "project_rate_limit", "project_concurrency_limit", "project_memory_pressure", "read_memory_busy",
  "queued_bytes", "stream_limit", "limit_bytes_per_sec", "limit_requests_per_sec", "limit_records_per_sec",
  "payload_too_large", "body_too_large", "sealed", "wrong_key", "missing_key", "invalid_key",
  "invalid_credential", "invalid_request", "issuer_unavailable", "rate_limited", "method_not_allowed",
] as const;

type Labels = Record<string, string>;
type LabelSpec = Readonly<Record<string, readonly string[]>>;

const HEALTH_LABELS: LabelSpec = { origin: ORIGINS, reason: REASONS, answer: ANSWERS };
const CANARY_LABELS: LabelSpec = { step: STEPS, origin: ORIGINS, error_code: ERROR_CODES };
const EXCHANGE_LABELS: LabelSpec = { failure: FAILURES, answer: ANSWERS, error_code: ERROR_CODES };
const ISSUER_LABELS: LabelSpec = { answer: ANSWERS };

function answerOf(p: { status: number | null; timedOut: boolean }): (typeof ANSWERS)[number] {
  if (p.timedOut) return "timeout";
  return p.status === null ? "no_answer" : "status";
}

/// Who answered: the server (its origin header), the deploy wrapper holding
/// the port after the binary died (500 binary_exited, no origin header), or
/// the platform (anything else, a timeout and the edge's wall included).
function originOf(status: number | null, origin: boolean, wrapperBody: boolean): (typeof ORIGINS)[number] {
  if (origin) return "server";
  return status === 500 && wrapperBody ? "wrapper" : "platform";
}

/// The server's 503 reason, from the prefix of /health's body (src/http.rs
/// health_axum, src/tasks.rs readiness_reason). `superseded` is an older boot
/// that a newer boot of the cell fenced (edge change #179, a deploy or a
/// restart), not a storage fault.
function reasonOf(status: number | null, body: string): (typeof REASONS)[number] {
  if (status !== 503) return "other";
  if (body.startsWith("runtime ") || body.startsWith("critical task terminated")) return "tasks";
  if (body.startsWith("auth feeds not yet published")) return "feeds_unpublished";
  if (body.startsWith("shard storage unavailable: superseded by a newer boot")) return "superseded";
  if (body.startsWith("shard storage unavailable")) return "shard_storage";
  if (body.startsWith("billing not ready")) return "billing_not_ready";
  return "other";
}

function errorCodeOf(code: string | null): (typeof ERROR_CODES)[number] {
  if (code === null) return "none";
  return (ERROR_CODES as readonly string[]).includes(code) ? (code as (typeof ERROR_CODES)[number]) : "other";
}

// ---- reading documents -----------------------------------------------------

/// `doc` at `path`, or undefined where any step is not an object.
export function get(doc: Json, ...path: string[]): Json {
  let v: Json = doc;
  for (const k of path) {
    if (v === null || typeof v !== "object" || Array.isArray(v)) return undefined;
    v = (v as Record<string, Json>)[k];
  }
  return v;
}
const num = (v: Json): number | null => (typeof v === "number" && Number.isFinite(v) ? v : null);
const bool = (v: Json): boolean | null => (typeof v === "boolean" ? v : null);
const secs = (ms: number) => Math.floor(ms / SEC);
const round = (x: number, places: number) => Math.round(x * 10 ** places) / 10 ** places;

/// Whether the issuer's `endpoint` is the origin `origin` (a trailing slash aside).
export function sameOrigin(endpoint: string | null, origin: string): boolean {
  if (endpoint === null) return false;
  try {
    return new URL(endpoint).href === new URL(origin).href;
  } catch {
    return false;
  }
}

// ---- one evaluation --------------------------------------------------------

interface Fire {
  class: Class;
  values: Record<string, number | null>;
  labels?: Labels;
}
type Answer = "skip" | "clean" | "pending" | Fire;
const fire = (cls: Class, values: Fire["values"], labels?: Labels): Fire => ({ class: cls, values, labels });

interface BootView {
  /// The boot id changed on this poll.
  changed: boolean;
  /// Counter baselines were reset on this poll (the first poll, or a change).
  restarted: boolean;
  firstSeenAt: number | null;
  /// Distinct boot ids seen in the last 30 min, the replaced ones left out.
  idsInWindow: number;
}

class Ctx {
  constructor(
    readonly cfg: RulesConfig,
    readonly poll: Poll,
    readonly boot: BootView,
    readonly memo: Memo,
    private readonly baselines: Record<string, number>,
  ) {}

  get at(): number {
    return this.poll.at;
  }

  /// How long `cond` has held without a break, or null when it does not hold.
  held(key: string, cond: boolean): number | null {
    if (!cond) {
      delete this.memo.since[key];
      return null;
    }
    const since = (this.memo.since[key] ??= this.at);
    return this.at - since;
  }

  /// Consecutive evaluations `cond` held, this one included (0 when not).
  streak(key: string, cond: boolean): number {
    const n = cond ? (this.memo.streak[key] ?? 0) + 1 : 0;
    this.memo.streak[key] = n;
    return n;
  }

  /// The counter's growth since the last poll that read it; 0 on its first
  /// reading after a baseline reset, and when it went down (it reset).
  /// Each counter is read by one rule, once a poll.
  growth(name: string, value: number): number {
    const base = this.baselines[name];
    this.baselines[name] = value;
    return base === undefined ? 0 : Math.max(0, value - base);
  }

  /// The oldest of the samples of `v` kept over the trailing `span` (one
  /// taken every 10 min); restarts when a component of `v` went down.
  window(key: string, v: number[], span: number): Sample {
    let samples = this.memo.samples[key] ?? [];
    const last = samples[samples.length - 1];
    if (last !== undefined && v.some((x, i) => x < (last.v[i] ?? 0))) samples = [];
    const newest = samples[samples.length - 1];
    if (newest === undefined || this.at - newest.at >= 10 * MIN) samples.push({ at: this.at, v });
    samples = samples.filter((s) => s.at >= this.at - span);
    this.memo.samples[key] = samples;
    return samples[0] ?? { at: this.at, v };
  }

  /// The oldest of the samples of `v` kept over the trailing `span`, one a
  /// poll, this one included; restarts when a component of `v` went down.
  trailing(key: string, v: number[], span: number): Sample {
    let samples = this.memo.samples[key] ?? [];
    const last = samples[samples.length - 1];
    if (last !== undefined && v.some((x, i) => x < (last.v[i] ?? 0))) samples = [];
    samples.push({ at: this.at, v });
    samples = samples.filter((s) => s.at >= this.at - span);
    this.memo.samples[key] = samples;
    return samples[0] ?? { at: this.at, v };
  }
}

// ---- the rules: one function per row ----------------------------------------

function noInside2xx(c: Ctx): Answer {
  const h = c.poll.health;
  const ms = c.held("down", h.status !== 200);
  if (ms === null) return "clean";
  if (ms < 2 * MIN) return "pending";
  const origin = originOf(h.status, h.origin, h.bodyPrefix.includes('"binary_exited"'));
  const labels: Labels = { origin, answer: answerOf(h) };
  if (origin === "server") labels.reason = reasonOf(h.status, h.bodyPrefix);
  return fire("PAGE", { status: h.status ?? 0, down_secs: secs(ms) }, labels);
}

function restarts(c: Ctx): Answer {
  const n = c.boot.idsInWindow;
  if (n >= 3) return fire("PAGE", { boot_ids_30m: n });
  return c.boot.changed ? fire("NOTIFY", { boot_ids_30m: n }) : "clean";
}

function wrongBuild(c: Ctx): Answer {
  const { status, git } = c.poll.health;
  if (status !== 200 || git === null) return "skip";
  const ms = c.held("wrong", !git.startsWith(c.cfg.expectedGit));
  if (ms === null) return "clean";
  return ms >= 10 * MIN ? fire("NOTIFY", { wrong_build_secs: secs(ms) }) : "pending";
}

function feedDelivery(c: Ctx): Answer {
  const feeds = get(c.poll.data, "feeds");
  if (feeds === undefined || feeds === null) return "skip";
  const ages = {
    jwks_age_secs: num(get(feeds, "jwks", "ageSecs")),
    policies_age_secs: num(get(feeds, "policies", "ageSecs")),
    grants_age_secs: num(get(feeds, "grants", "ageSecs")),
  };
  const stale = {
    policies_stale: get(feeds, "policies", "stale") === true ? 1 : 0,
    grants_stale: get(feeds, "grants", "stale") === true ? 1 : 0,
  };
  const late = Object.values(ages).some((a) => a !== null && a > 75);
  if (!late && stale.policies_stale + stale.grants_stale === 0) return "clean";
  return fire("PAGE", { ...ages, ...stale });
}

function reservedDropped(c: Ctx): Answer {
  const dropped = num(get(c.poll.data, "feeds", "policies", "reservedDropped"));
  if (dropped === null) return "skip";
  return dropped > 0 ? fire("PAGE", { reserved_dropped: dropped }) : "clean";
}

function rssAtShedLine(c: Ctx): Answer {
  const local = get(c.poll.data, "local");
  const rss = num(get(local, "rss_mb"));
  const reserved = num(get(local, "absorb_reserved_bytes_now"));
  const line = num(get(local, "rss_shed_mb"));
  if (rss === null || reserved === null || line === null || line <= 0) return "skip";
  const pressure = rss + reserved / MIB;
  const over = c.held("over", pressure >= line);
  const near = c.held("near", pressure >= 0.9 * line);
  const values = { pressure_mb: round(pressure, 1), rss_shed_mb: line };
  if (over !== null && over >= 10 * MIN) return fire("PAGE", { ...values, held_secs: secs(over) });
  if (near !== null && near >= 30 * MIN) return fire("NOTIFY", { ...values, held_secs: secs(near) });
  return near === null ? "clean" : "pending";
}

function rssWriteShed(c: Ctx): Answer {
  const shed = num(get(c.poll.data, "local", "admit_shed_rss"));
  if (shed === null) return "skip";
  const g = c.growth("local.admit_shed_rss", shed);
  const n = c.streak("grew", g > 0);
  if (n === 0) return "clean";
  return fire(n >= 2 ? "PAGE" : "NOTIFY", { admit_shed_rss: shed, growth: g, polls: n });
}

function overloadSheds(c: Ctx): Answer {
  const inflight = num(get(c.poll.data, "local", "admit_shed_inflight"));
  const survival = num(get(c.poll.data, "local", "admit_shed_survival"));
  if (inflight === null || survival === null) return "skip";
  const g = c.growth("local.admit_shed_inflight+survival", inflight + survival);
  const n = c.streak("grew", g > 0);
  if (n === 0) return "clean";
  const values = { admit_shed_inflight: inflight, admit_shed_survival: survival, growth: g, polls: n };
  return fire(n >= 3 ? "PAGE" : "NOTIFY", values);
}

function durabilityWedge(c: Ctx): Answer {
  const wedge = num(get(c.poll.data, "local", "wedge_shed"));
  if (wedge === null) return "skip";
  const g = c.growth("local.wedge_shed", wedge);
  return g > 0 ? fire("PAGE", { wedge_shed: wedge, growth: g }) : "clean";
}

function backlogLatch(c: Ctx): Answer {
  const latch = get(c.poll.data, "local", "maintenance_backpressure");
  const engaged = bool(get(latch, "engaged"));
  const shed = num(get(latch, "appends_shed"));
  const lag = num(get(latch, "absorb_lag_secs"));
  const unabsorbed = num(get(latch, "unabsorbed_bytes"));
  if (engaged === null || shed === null || lag === null || unabsorbed === null) return "skip";
  const g = c.growth("local.maintenance_backpressure.appends_shed", shed);
  const values = { engaged: engaged ? 1 : 0, appends_shed_growth: g, absorb_lag_secs: lag, unabsorbed_bytes: unabsorbed };
  if (engaged || g > 0) return fire("PAGE", values);
  if (lag > 450 || unabsorbed > 256 * MIB) return fire("NOTIFY", values);
  return "clean";
}

function historyFlushWait(c: Ctx): Answer {
  const gauges = get(c.poll.data, "ops", "gauges");
  const wait = num(get(gauges, "history_flush_wait_ms_max"));
  if (wait === null) return "skip";
  if (wait <= 30_000) return "clean";
  return fire("PAGE", {
    history_flush_wait_ms_max: wait,
    history_l0_ssts_max: num(get(gauges, "history_l0_ssts_max")),
    history_compacted_runs_max: num(get(gauges, "history_compacted_runs_max")),
  });
}

function readMemory(c: Ctx): Answer {
  const used = num(get(c.poll.data, "local", "read_memory_bytes"));
  const cap = num(get(c.poll.data, "local", "read_memory_capacity_bytes"));
  if (used === null || cap === null || cap <= 0) return "skip";
  const ms = c.held("high", used / cap > 0.8);
  if (ms === null) return "clean";
  if (ms < 5 * MIN) return "pending";
  return fire("PAGE", { read_memory_ratio: round(used / cap, 3), read_memory_capacity_bytes: cap, held_secs: secs(ms) });
}

function livePool(c: Ctx): Answer {
  const sse = num(get(c.poll.data, "local", "sse_connections"));
  const parked = num(get(c.poll.data, "local", "parked"));
  const max = num(get(c.poll.data, "local", "sse_max_connections"));
  if (sse === null || parked === null || max === null || max <= 0) return "skip";
  const ratio = (sse + parked) / max;
  const full = c.held("full", ratio >= 1);
  const near = c.held("near", ratio >= 0.9);
  const values = { live_pool_ratio: round(ratio, 3), sse_connections: sse, parked, sse_max_connections: max };
  if (full !== null && full >= MIN) return fire("PAGE", values);
  if (near !== null && near >= MIN) return fire("NOTIFY", values);
  return near === null ? "clean" : "pending";
}

/// One `local.store.totals.ops` cell's puts, every outcome, and its failed
/// ones: `err` is a failure left after the client's retries, a transport
/// error or a cancelled call; a 412 (a manifest CAS conflict) is `unbilled`.
function putCounts(totals: Json, name: string): [number, number] {
  const count = (k: string) => num(get(totals, "ops", name, k)) ?? 0;
  const err = count("err");
  return [count("ok") + count("not_found") + count("unbilled") + err, err];
}

/// Row 16 counts failed puts from the cumulative `totals`, over the trailing
/// minute: the latency window `local.store.ops` counts as `err` every answer
/// that is not a 2xx or a 304, so an idle cell's occasional refused manifest
/// put (412) read as 1 of 9 failing.
function objectStore(c: Ctx): Answer {
  const ops = get(c.poll.data, "local", "store", "ops");
  const totals = get(c.poll.data, "local", "store", "totals");
  const since = num(get(totals, "since_ms"));
  if (ops === undefined || ops === null || typeof ops !== "object" || since === null) return "skip";
  // A restart starts the counters again (a new `since_ms`): a new baseline.
  if (c.boot.restarted || c.memo.mark.since !== since) delete c.memo.samples.puts;
  c.memo.mark.since = since;
  const now = [...putCounts(totals, "put:wal"), ...putCounts(totals, "put:manifest")];
  const base = c.trailing("puts", now, MIN).v;
  const [walN, walErr, manifestN, manifestErr] = now.map((x, i) => x - (base[i] ?? x));
  const wal = { n: walN ?? 0, err: walErr ?? 0, p99: num(get(ops, "put:wal", "p99_ms")) ?? 0 };
  const manifest = { n: manifestN ?? 0, err: manifestErr ?? 0 };
  const failing = (x: { n: number; err: number }) => x.err > 0 && x.n > 0 && x.err / x.n >= 0.05;
  const n = c.streak("failing", failing(wal) || failing(manifest));
  const slow = c.held("slow", wal.p99 > 2000);
  const values = {
    put_wal_n: wal.n, put_wal_err: wal.err, put_manifest_n: manifest.n, put_manifest_err: manifest.err,
    put_wal_p99_ms: wal.p99,
  };
  if (n >= 2) return fire("PAGE", values);
  if (slow !== null && slow >= 5 * MIN) return fire("NOTIFY", values);
  return n === 0 && slow === null ? "clean" : "pending";
}

function unownedUsage(c: Ctx): Answer {
  const total = num(get(c.poll.data, "ops", "counters", "unowned_meter_events_total"));
  if (total === null) return "skip";
  if (c.growth("ops.counters.unowned_meter_events_total", total) > 0) c.memo.mark.grew = c.at;
  const last = c.memo.mark.grew;
  if (last === undefined || c.at - last > 15 * MIN) return "clean";
  return fire("NOTIFY", { unowned_meter_events_total: total, since_growth_secs: secs(c.at - last) });
}

function billingPipeline(c: Ctx): Answer {
  const b = c.poll.billing;
  if (b === null || typeof b !== "object") return "skip";
  const alerts = get(b, "openAlerts");
  const open = Array.isArray(alerts) ? alerts.length : 0;
  const ready = bool(get(b, "ready"));
  const drain = num(get(b, "drain", "lastOkAgeSecs"));
  if (open === 0 && ready !== false && !(drain !== null && drain > 300)) return "clean";
  return fire("NOTIFY", { open_alerts: open, ready: ready === false ? 0 : 1, drain_last_ok_age_secs: drain });
}

function canary(c: Ctx): Answer {
  const k = c.poll.canary;
  if (k === null) return "skip";
  const failed = [k.append, k.read].find((s): s is CanaryStep => s !== null && !s.ok);
  if (failed === undefined && !(k.append?.ok && k.read?.ok)) return "skip";
  const n = c.streak("failing", failed !== undefined);
  if (failed === undefined) return "clean";
  if (n < 3) return "pending";
  return fire("PAGE", { status: failed.status ?? 0, consecutive_failures: n }, {
    step: failed === k.append ? "append" : "read",
    origin: originOf(failed.status, failed.origin, failed.errorCode === "binary_exited"),
    error_code: errorCodeOf(failed.errorCode),
  });
}

function snapshotStale(c: Ctx): Answer {
  const data = c.poll.data;
  const readable = data !== null && typeof data === "object";
  // /health is 200, so the server answers, but its operator document does
  // not come back: the rows that read it are blind. A cell that is down is
  // row 3's, so an unready /health stops the clock.
  const unread = c.held("unread", !readable && c.poll.health.status === 200);
  if (!readable) {
    if (unread === null) return "skip";
    if (unread < MIN) return "pending";
    return fire("NOTIFY", { data_status: c.poll.dataStatus ?? 0, data_unread_secs: secs(unread) });
  }
  const ops = get(data, "ops");
  if (ops === null || ops === undefined) {
    // Judged only on a poll whose /health answered 200, so the boot id is
    // the running process's: a restart's unready seconds are not the old one's.
    if (c.poll.health.status !== 200 || c.boot.firstSeenAt === null) return "skip";
    const ms = c.at - c.boot.firstSeenAt;
    return ms > MIN ? fire("NOTIFY", { ops_null_secs: secs(ms) }) : "pending";
  }
  const cellNow = num(get(data, "ts_ms"));
  const taken = num(get(ops, "ts_ms"));
  if (cellNow === null || taken === null) return "skip";
  return cellNow - taken > MIN ? fire("NOTIFY", { ops_age_secs: secs(cellNow - taken) }) : "clean";
}

function issuerDown(c: Ctx): Answer {
  const h = c.poll.issuerHealth;
  if (h === null) return "skip";
  const ms = c.held("down", h.status !== 200);
  if (ms === null) return "clean";
  if (ms < 2 * MIN) return "pending";
  return fire("PAGE", { status: h.status ?? 0, down_secs: secs(ms) }, { answer: answerOf(h) });
}

function canaryExchange(c: Ctx): Answer {
  const x = c.poll.canary?.exchange ?? null;
  if (x === null) return "skip";
  const wrongEndpoint = x.ok && !sameOrigin(x.endpoint, c.cfg.cellOrigin);
  const failed = !x.ok || wrongEndpoint;
  const n = c.streak("failing", failed);
  if (!failed) return "clean";
  if (n < 3) return "pending";
  return fire("PAGE", { status: x.status ?? 0, consecutive_failures: n }, {
    failure: wrongEndpoint ? "endpoint" : "exchange",
    answer: answerOf({ status: x.status, timedOut: false }),
    error_code: errorCodeOf(x.errorCode),
  });
}

function storedGrowth(c: Ctx): Answer {
  const written = num(get(c.poll.billing, "rollup", "stored", "writtenBytes"));
  if (written === null) return "skip";
  const first = c.window("written", [written], DAY);
  const growth = written - (first.v[0] ?? written);
  if (growth <= c.cfg.storedGrowthPerDay) return "clean";
  return fire("NOTIFY", { written_growth_bytes: growth, window_secs: secs(c.at - first.at), g_bytes: c.cfg.storedGrowthPerDay });
}

/// `local.store.totals.bytes_put`, every byte the process put since
/// `totals.since_ms`: the bound from above on the bucket's growth that
/// row 23 cannot see (bytes written and deleted within one drain round
/// never reach `writtenBytes`; the owner's call stored-cap #1 (c) of
/// 2026-10-14). Cell-wide and unattributed. A restart starts it again.
function bytesPutGrowth(c: Ctx): Answer {
  if (c.boot.restarted) delete c.memo.samples.put;
  const put = num(get(c.poll.data, "local", "store", "totals", "bytes_put"));
  if (put === null) return "skip";
  const first = c.window("put", [put], DAY);
  const growth = put - (first.v[0] ?? put);
  const bar = BYTES_PUT_FACTOR * c.cfg.storedGrowthPerDay;
  if (growth <= bar) return "clean";
  return fire("NOTIFY", { put_growth_bytes: growth, window_secs: secs(c.at - first.at), bar_bytes: bar });
}

/// Billed Class A and Class B requests in /operator/data.json's
/// `local.store.totals.ops` (cells `<op>:<class>`, counts by outcome).
function billedRequests(ops: Json): [number, number] {
  let a = 0;
  let b = 0;
  if (ops === null || typeof ops !== "object") return [a, b];
  for (const [cell, counts] of Object.entries(ops as Record<string, Json>)) {
    const billed = (num(get(counts, "ok")) ?? 0) + (num(get(counts, "not_found")) ?? 0);
    const op = cell.split(":")[0] ?? "";
    if (["put", "mpu", "list", "copy"].includes(op)) a += billed;
    else if (["get", "head"].includes(op)) b += billed;
  }
  return [a, b];
}

function dailySpend(c: Ctx): Answer {
  if (c.boot.restarted) delete c.memo.samples.requests;
  const written = num(get(c.poll.billing, "rollup", "stored", "writtenBytes"));
  const totals = get(c.poll.data, "local", "store", "totals", "ops");
  if (written === null || totals === undefined || totals === null) return "skip";
  const [a, b] = billedRequests(totals);
  const first = c.window("requests", [a, b], DAY);
  const span = c.at - first.at;
  const p = c.cfg.prices;
  // An hour's requests at least before they are scaled to a day; until
  // then the estimate leaves them out (a lower bound).
  const billed = (a - (first.v[0] ?? a)) * p.classA + (b - (first.v[1] ?? b)) * p.classB;
  const requests = span >= HOUR ? (billed * DAY) / span : 0;
  const storage = ((written / GIB) * 1.2 * p.storageGibMonth) / 30;
  const estimate = requests + storage + p.serverPerDay;
  if (estimate <= c.cfg.budgetPerDay) return "clean";
  return fire("NOTIFY", {
    estimate_per_day: round(estimate, 2), requests_per_day: round(requests, 2), storage_per_day: round(storage, 2),
    server_per_day: p.serverPerDay, budget_per_day: c.cfg.budgetPerDay, window_secs: secs(span),
  });
}

function staleRollup(c: Ctx): Answer {
  const ingest = num(get(c.poll.data, "ops", "counters", "ingest_bytes_total"));
  if (ingest !== null && c.growth("ops.counters.ingest_bytes_total", ingest) > 0) c.memo.mark.ingest = c.at;
  const stored = get(c.poll.billing, "rollup", "stored");
  const age = num(get(c.poll.billing, "rollup", "lastApplyAgeSecs"));
  if (stored === undefined || stored === null || age === null) return "skip";
  // Traffic: ingest grew within the same 10 min the apply is late by.
  const last = c.memo.mark.ingest;
  const traffic = last !== undefined && c.at - last <= 10 * MIN;
  return age > 600 && traffic ? fire("NOTIFY", { last_apply_age_secs: age }) : "clean";
}

function usageOverflow(c: Ctx): Answer {
  const admits = num(get(c.poll.data, "local", "usage_overflow_admits"));
  if (admits === null) return "skip";
  return admits > 0 ? fire("NOTIFY", { usage_overflow_admits: admits }) : "clean";
}

/// /v1/admin/metrics `cache_age_secs` (deploy/issuer/ops.ts).
function issuerCacheAge(c: Ctx): Answer {
  const age = num(get(c.poll.issuerMetrics, "cache_age_secs"));
  if (age === null) return "skip";
  return age > 150 ? fire("NOTIFY", { cache_age_secs: age }) : "clean";
}

/// /v1/admin/metrics `gate_open` (deploy/issuer/ops.ts).
function admissionGateClosed(c: Ctx): Answer {
  const open = bool(get(c.poll.issuerMetrics, "gate_open"));
  if (open === null) return "skip";
  return open ? "clean" : fire("NOTIFY", { gate_open: 0 });
}

// ---- the table -------------------------------------------------------------

/// RUNBOOK.md headings, as GitHub anchors them.
const RUNBOOK = {
  platform: "RUNBOOK.md#75-platform-failure-modes-you-will-meet",
  deploy: "RUNBOOK.md#74-deploy-procedure-per-release",
  monitoring: "RUNBOOK.md#8-monitoring",
  store: "RUNBOOK.md#reading-v1debugstore-the-5-minute-diagnosis",
  billing: "RUNBOOK.md#billing-pipeline-runbooks-round-22-item-d4",
  memory: "RUNBOOK.md#memory-survival-posture-oom-review-2026-08-07",
  shared: "RUNBOOK.md#shared-cells-single-server-design-partners",
} as const;

export interface Rule {
  id: string;
  /// The triage table's row.
  row: number;
  anchor: string;
  /// A level rule (rows 8, 10, 12-16): run NOTIFY_ONLY until calibrated (B6).
  level: boolean;
  labels: LabelSpec;
  enabled: (cfg: RulesConfig) => boolean;
  check: (c: Ctx) => Answer;
}

const always = () => true;
const withIssuer = (cfg: RulesConfig) => cfg.issuer;
const rule = (row: number, id: string, anchor: string, check: Rule["check"], more: Partial<Rule> = {}): Rule => ({
  id: `r${String(row).padStart(2, "0")}_${id}`, row, anchor, level: false, labels: {}, enabled: always, check, ...more,
});

export const RULES: readonly Rule[] = [
  rule(3, "no_2xx_inside", RUNBOOK.platform, noInside2xx, { labels: HEALTH_LABELS }),
  rule(4, "restarts", RUNBOOK.platform, restarts),
  rule(5, "wrong_build", RUNBOOK.deploy, wrongBuild, { enabled: (cfg) => cfg.expectedGit !== "" }),
  rule(6, "feed_delivery", RUNBOOK.shared, feedDelivery),
  rule(7, "reserved_policy_dropped", RUNBOOK.shared, reservedDropped),
  rule(8, "rss_at_shed_line", RUNBOOK.memory, rssAtShedLine, { level: true }),
  rule(9, "rss_write_shed", RUNBOOK.memory, rssWriteShed),
  rule(10, "overload_sheds", RUNBOOK.shared, overloadSheds, { level: true }),
  rule(11, "durability_wedge", RUNBOOK.shared, durabilityWedge),
  rule(12, "backlog_latch", RUNBOOK.shared, backlogLatch, { level: true }),
  rule(13, "history_flush_wait", RUNBOOK.shared, historyFlushWait, { level: true }),
  rule(14, "read_memory", RUNBOOK.shared, readMemory, { level: true }),
  rule(15, "live_pool", RUNBOOK.shared, livePool, { level: true }),
  rule(16, "object_store", RUNBOOK.store, objectStore, { level: true }),
  rule(17, "unowned_usage", RUNBOOK.shared, unownedUsage),
  rule(18, "billing_pipeline", RUNBOOK.billing, billingPipeline),
  rule(19, "canary", RUNBOOK.shared, canary, { labels: CANARY_LABELS }),
  rule(20, "snapshot_stale", RUNBOOK.monitoring, snapshotStale),
  rule(21, "issuer_down", RUNBOOK.shared, issuerDown, { labels: ISSUER_LABELS, enabled: withIssuer }),
  rule(22, "canary_exchange", RUNBOOK.shared, canaryExchange, { labels: EXCHANGE_LABELS, enabled: withIssuer }),
  rule(23, "stored_growth", RUNBOOK.billing, storedGrowth),
  rule(24, "daily_spend", RUNBOOK.billing, dailySpend),
  rule(25, "stale_rollup", RUNBOOK.billing, staleRollup),
  rule(26, "usage_overflow", RUNBOOK.shared, usageOverflow),
  rule(27, "issuer_cache_age", RUNBOOK.shared, issuerCacheAge, { enabled: withIssuer }),
  rule(28, "admission_gate_closed", RUNBOOK.shared, admissionGateClosed, { enabled: withIssuer }),
  rule(29, "bytes_put_growth", RUNBOOK.billing, bytesPutGrowth),
];

// ---- the step --------------------------------------------------------------

/// Only finite numbers, and only the labels the rule declares with a value
/// of their enum: the payload allowlist, enforced where every event is made.
export function event(
  cfg: RulesConfig, r: Rule, kind: WatchEvent["kind"], cls: Class, at: number,
  values: Fire["values"], labels: Labels,
): WatchEvent {
  const nums = Object.fromEntries(
    Object.entries(values).filter((e): e is [string, number] => typeof e[1] === "number" && Number.isFinite(e[1])),
  );
  const enums = Object.fromEntries(Object.entries(labels).filter(([k, v]) => r.labels[k]?.includes(v) === true));
  return { kind, dedupKey: `${cfg.cellId}/${r.id}`, class: cls, rule: r.id, values: nums, labels: enums, at, runbookAnchor: r.anchor };
}

/// The boot a newer boot replaced, from `/operator/data.json`'s `boot`: the
/// serving boot names its predecessor and says whether that boot learned of
/// it (the predecessor's mark, written when it saw a newer boot registered).
/// A predecessor that went before its successor registered (a crash) has no
/// mark, so it is not returned; so is one the successor has not yet decided.
function replacedBoot(data: Json): string | null {
  const predecessor = get(data, "boot", "predecessor");
  if (typeof predecessor !== "string" || predecessor === "") return null;
  return bool(get(data, "boot", "predecessorSuperseded")) === true ? predecessor : null;
}

/// Notes the boot id a 200 carried; reports a change and the 30 min window.
function noteBoot(state: WatchState, poll: Poll): BootView {
  const first = state.polls === 0;
  const { status, bootId } = poll.health;
  let changed = false;
  if (status === 200 && bootId !== null) {
    if (state.boot === null) state.boot = { id: bootId, firstSeenAt: poll.at };
    else if (state.boot.id !== bootId) {
      state.boot = { id: bootId, firstSeenAt: poll.at };
      changed = true;
    }
    state.bootIds[bootId] = poll.at;
  }
  const replaced = replacedBoot(poll.data);
  if (replaced !== null && replaced !== get(poll.data, "boot", "id")) state.replaced[replaced] ??= poll.at;
  for (const [id, seen] of Object.entries(state.bootIds)) if (seen < poll.at - 30 * MIN) delete state.bootIds[id];
  for (const id of Object.keys(state.replaced)) if (state.bootIds[id] === undefined) delete state.replaced[id];
  if (first || changed) state.baselines = {};
  return {
    changed,
    restarted: first || changed,
    firstSeenAt: state.boot?.firstSeenAt ?? null,
    idsInWindow: Object.keys(state.bootIds).filter((id) => state.replaced[id] === undefined).length,
  };
}

function settle(cfg: RulesConfig, state: WatchState, r: Rule, answer: Answer, at: number, events: WatchEvent[]): void {
  if (answer === "skip") return;
  const open = state.open[r.id];
  const inherited = state.unsettled[r.id] === true && answer !== "pending";
  if (answer !== "pending") delete state.unsettled[r.id];
  if (answer === "clean") {
    if (open === undefined && inherited) events.push(event(cfg, r, "resolve", "NOTIFY", at, {}, {}));
    if (open === undefined) return;
    const n = (state.clean[r.id] ?? 0) + 1;
    state.clean[r.id] = n;
    if (n < RESOLVE_AFTER) return;
    delete state.open[r.id];
    delete state.clean[r.id];
    events.push(event(cfg, r, "resolve", open.class, at, { open_secs: secs(at - open.since) }, {}));
    return;
  }
  if (open !== undefined) state.clean[r.id] = 0;
  if (answer === "pending") return;
  const cls: Class = cfg.notifyOnly && r.level ? "NOTIFY" : answer.class;
  if (open !== undefined && RANK[cls] <= RANK[open.class]) return;
  state.open[r.id] = { class: cls, since: open?.since ?? at };
  events.push(event(cfg, r, "trigger", cls, at, answer.values, answer.labels ?? {}));
}

/// One poll: the next state and the events to deliver. `prev` is not changed.
export function step(cfg: RulesConfig, prev: WatchState, poll: Poll): { state: WatchState; events: WatchEvent[] } {
  const state = structuredClone(prev);
  const boot = noteBoot(state, poll);
  const events: WatchEvent[] = [];
  for (const r of RULES) {
    if (!r.enabled(cfg)) continue;
    const memo = (state.memo[r.id] ??= { since: {}, streak: {}, mark: {}, samples: {} });
    const answer = r.check(new Ctx(cfg, poll, boot, memo, state.baselines));
    settle(cfg, state, r, answer, poll.at, events);
  }
  state.polls += 1;
  return { state, events };
}

/// A delivery's outcome (D7): consecutive failed trigger deliveries count;
/// a delivered trigger resets the count; a resolve changes nothing.
export function delivered(prev: WatchState, e: WatchEvent, ok: boolean): WatchState {
  if (e.kind !== "trigger") return prev;
  return { ...prev, failedTriggers: ok ? 0 : prev.failedTriggers + 1 };
}

/// Whether the heartbeat goes out after this cycle: withheld after
/// WITHHOLD_AFTER consecutive failed trigger deliveries, so the dead-man
/// (row 2) pages through the paging tool's other path.
export function heartbeatDue(state: WatchState): boolean {
  return state.failedTriggers < WITHHOLD_AFTER;
}
