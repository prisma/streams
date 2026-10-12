// The cell watch's test rig: the fixtures, and a watch driven one poll at
// a time on a clock of its own.
//
// fixtures/ holds the answers of a local shared cell, captured once at
// cb5e79fa (the S2 operator fields) as cell-admin's e2e test boots one,
// on the alerting lane's tree before it landed on slate (so billing.json
// has no `rollup.stored`, and health.json's `git` names that tree):
// target/debug streams-slate over s3lite, deploy/profiles compute-1g.env
// then shared-cell.env, enforce with the bundle's feeds, one admitted
// project with one stream and twenty appends, then GET /health (its
// status, headers and body as health.json), and /operator/data.json and
// /operator/billing.json with the deployment bearer once `ops` was served.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import {
  initialState, type Json, type Poll, RULES, type RulesConfig, rulesConfig, step, type WatchEvent, type WatchState,
} from "./rules";

const load = (name: string): Json => JSON.parse(readFileSync(join(import.meta.dir, "fixtures", name), "utf8"));

/// GET /operator/data.json, /operator/billing.json and /health of a
/// healthy cell, as captured.
export const DATA = load("data.json");
export const BILLING = load("billing.json");
const HEALTH = load("health.json") as { status: number; origin: boolean; git: string; bootId: string; body: string };
export const GIT = HEALTH.git;
export const BOOT = HEALTH.bootId;

export const ORIGIN = "https://cell.example.test";
export const CFG: RulesConfig = rulesConfig({ cellId: "cell_sc1", cellOrigin: ORIGIN });
/// Every row on: the issuer set, the build expected.
export const ALL_ROWS: RulesConfig = { ...CFG, issuer: true, expectedGit: GIT };

/// A copy of `doc` with each `"a.b.c"` path set to its value.
export function patch(doc: Json, edits: Record<string, Json>): Json {
  const copy = structuredClone(doc);
  for (const [path, value] of Object.entries(edits)) {
    const keys = path.split(".");
    const last = keys.pop() as string;
    let at = copy as Record<string, Json>;
    for (const k of keys) {
      if (at[k] === null || typeof at[k] !== "object") at[k] = {};
      at = at[k] as Record<string, Json>;
    }
    at[last] = value;
  }
  return copy;
}

/// The billing answer once stored-cap's `rollup.stored` is served.
export const billingWithStored = (writtenBytes: number, more: Record<string, Json> = {}): Json =>
  patch(BILLING, {
    "rollup.stored": { projects: 1, totalBytes: writtenBytes, writtenBytes, asOfMs: 0, largest: [] },
    ...more,
  });

export const T0 = 1_800_000_000_000;
export const POLL_MS = 15_000;

/// A poll of the healthy cell at `at`: no billing, no issuer, no canary.
export function healthyPoll(at: number): Poll {
  return {
    at,
    health: { status: 200, timedOut: false, origin: true, git: GIT, bootId: BOOT, bodyPrefix: "ok" },
    data: DATA,
    billing: null,
    issuerHealth: null,
    issuerMetrics: null,
    canary: null,
  };
}

/// A watch on its own clock: each poll is POLL_MS after the one before.
export class Watch {
  state: WatchState = initialState();
  events: WatchEvent[] = [];
  at = T0;

  constructor(readonly cfg: RulesConfig = CFG) {}

  /// One poll of the healthy cell with `over` applied; its events.
  poll(over: Partial<Poll> = {}): WatchEvent[] {
    const p = { ...healthyPoll(this.at), ...over };
    const r = step(this.cfg, this.state, p);
    this.state = r.state;
    this.events.push(...r.events);
    this.at += POLL_MS;
    return r.events;
  }

  /// `n` polls alike; their events.
  polls(n: number, over: Partial<Poll> = {}): WatchEvent[] {
    const out: WatchEvent[] = [];
    for (let i = 0; i < n; i++) out.push(...this.poll(over));
    return out;
  }
}

/// `[kind, class]` of each event, in order.
export const kinds = (events: WatchEvent[]) => events.map((e) => [e.kind, e.class]);

/// The events of one rule, by its row.
export function ofRow(events: WatchEvent[], row: number): WatchEvent[] {
  const id = RULES.find((r) => r.row === row)?.id;
  return events.filter((e) => e.rule === id);
}

export function idOf(row: number): string {
  return RULES.find((r) => r.row === row)?.id ?? "";
}
