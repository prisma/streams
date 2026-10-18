// The cell watch's local drills (public-beta alerting triage B8): one per
// PAGE rule the triage names, each made to happen on a real local cell
// and each reaching the paging tool's local stand-in (pager.ts) as the
// page it must be, then, where the cell can be put right, its resolve.
// The watch is its app in `prisma dev` with NOTIFY_ONLY=0 and the Events
// v2 sink; the cell is the real binary over s3lite on the shared-cell
// profile (e2e-rig.ts). Opt-in, and about 25 minutes, because each rule
// waits as long as it does in production:
//
//   cargo build --locked --bin streams-slate --bin s3lite
//   (cd deploy/cell-watch && bun install --frozen-lockfile)
//   CELL_WATCH_E2E_BIN_DIR=target/debug CELL_WATCH_DRILLS=1 bun test ./deploy/cell-watch/drills.e2e.test.ts
//
// The cell's drills, in order: a reserved project in the bundle (row 7),
// the bundle deleted (row 6), `POST /v1/debug/history-stall` (row 13),
// `absorb-pause` until the absorb-lag latch engages (row 12; the cell runs
// with MAX_ABSORB_LAG_SECS=150 so it does within minutes, not 15), the
// cell stopped (rows 3 and 1) and the watch killed (row 2). The issuer's,
// on a cell with the issuer and the canary: the issuer stopped (row 21)
// and the issuer's key published under another kid for the internal
// audience (row 19, a wrong-`aud` keys feed: every customer token answers
// 401 kid_unknown while /health is green). RUNBOOK's shared-cell "Drills"
// paragraph is the same list; on a deployed cell each drill reaches the
// on-call's phone instead (§7.5).
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdirSync, readFileSync, rmSync } from "node:fs";
import { join, resolve } from "node:path";
import { canaryProjectId } from "../cell-admin/rules";
import { initCell, scratchDir } from "../cell-admin/test-rig";
import { boot } from "../issuer/boot";
import { StateCache } from "../issuer/cache";
import { readConfig } from "../issuer/config";
import { admin, envWith, issuerCell } from "../issuer/rig";
import { Issuer } from "../issuer/server";
import { CANARY_SCOPES, canaryStreamKey, EXCHANGE_PATH, RECORDS_PATH } from "./canary";
import { freePort, onTeardown, publish, startCell, startS3, startWatch, stopCell, teardown, until, writeFeed } from "./e2e-rig";
import { HEARTBEAT_KEY, Pager, type PagerChange, UPTIME_KEY } from "./pager";

const BIN_DIR = process.env.CELL_WATCH_E2E_BIN_DIR;
const SHELL = { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" };
const maybe = BIN_DIR && process.env.CELL_WATCH_DRILLS === "1" ? test : test.skip;
afterAll(teardown);

const ROUTING_KEY = `routing-key-${randomBytes(8).toString("hex")}`;
const page = (rule: string) => `cell_sc1/${rule}/page`;
/// Row 17 (`r17_unowned_usage`), which the cell's own metering opened on
/// every idle enforce cell until round 4's fix (B4's finding; NEXT-WORK
/// 14.7): no drill causes it, so it never opens.
const ROW_17 = "cell_sc1/r17_unowned_usage/notify";

type Seen = [PagerChange["action"], string, boolean];

/// The pager's changes from `from` on, sorted: a drill's changes may land
/// in any order.
const changes = (p: Pager, from: number): Seen[] =>
  p.since(from).sort((a, b) => (a.join() < b.join() ? -1 : 1));

/// Waits up to `secs` for the pager to show as many changes as `want`
/// since `from`, then holds them to exactly `want` (in any order).
async function shows(p: Pager, from: number, secs: number, want: Seen[]): Promise<void> {
  await until("the pager's changes", secs, () => (changes(p, from).length >= want.length ? true : undefined)).catch(() => {});
  expect(changes(p, from)).toEqual([...want].sort((a, b) => (a.join() < b.join() ? -1 : 1)));
}

/// The summary of the incident opened on `key` since `from`.
const summaryOf = (p: Pager, from: number, key: string) =>
  p.changes.slice(from).find((c) => c.action === "open" && c.key === key)?.summary ?? "";

/// The watch's inputs: the cell, the pager's Events v2 endpoint and
/// heartbeat, PAGE on (NOTIFY_ONLY=0), and the issuer's when given.
function inputs(cellOrigin: string, operator: string, git: string, p: Pager, issuer?: Record<string, string>) {
  return {
    STREAMS_OPERATOR_TOKEN: operator,
    ALERT_SINK_URL: p.eventsUrl,
    ALERT_SINK_KEY: ROUTING_KEY,
    HEARTBEAT_URL: p.heartbeatUrl,
    STREAMS_ISSUER_METRICS_TOKEN: "none",
    CANARY_CREDENTIAL: "none",
    STREAMS_CELL_ORIGIN: cellOrigin,
    STREAMS_CELL_ID: "cell_sc1",
    ALERT_SINK_KIND: "events-v2",
    EXPECTED_GIT: git,
    NOTIFY_ONLY: "0",
    ISSUER_ORIGIN: "none",
    ...issuer,
  };
}

function pager(uptimeOrigin: string | null): Pager {
  const p = new Pager({ routingKey: ROUTING_KEY, heartbeatToken: randomBytes(8).toString("hex"), uptimeOrigin });
  onTeardown(() => p.stop());
  return p;
}

maybe("the cell's drills each page the on-call: rows 7, 6, 13, 12, 3 with the uptime check, and the heartbeat", async () => {
  const bin = resolve(BIN_DIR ?? "");
  const c = await initCell();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  const bundle = publish(c.state, feeds);
  const operator = randomBytes(24).toString("hex");
  const spec = {
    shell: SHELL, bin, port: freePort(), s3: startS3(bin), bucket: `cell-watch-drills-${Date.now()}`, feeds, operator,
    log: join(c.root, "server-1.log"), env: { MAX_ABSORB_LAG_SECS: "150" },
  };
  const cell = await startCell(spec);
  const p = pager(cell.origin);
  const watch = await startWatch(SHELL, inputs(cell.origin, operator, cell.git, p));
  await until("two heartbeats", 45, () => (p.beats.length >= 2 ? true : undefined));
  expect(changes(p, 0)).toEqual([]);
  const debug = (path: string) => fetch(`${cell.origin}/v1/debug/${path}`, { method: "POST", headers: { authorization: `Bearer ${operator}` } });
  const v = bundle.policies.feed_version;

  // Row 7: a policy naming the deployment's PROJECT_ID is dropped, and the drop pages.
  let mark = p.changes.length;
  const reserved = { ...(bundle.policies.projects[0] as object), project_id: "proj_deploy", workspace_id: "ws_drill" };
  writeFeed(feeds, "policies", { feed_version: v + 1, projects: [...bundle.policies.projects, reserved] });
  await shows(p, mark, 120, [["open", page("r07_reserved_policy_dropped"), true]]);
  expect(summaryOf(p, mark, page("r07_reserved_policy_dropped"))).toBe("[PAGE] cell_sc1/r07_reserved_policy_dropped reserved_dropped=1");
  writeFeed(feeds, "policies", { feed_version: v + 2, projects: bundle.policies.projects });
  await shows(p, mark, 150, [["open", page("r07_reserved_policy_dropped"), true], ["resolve", page("r07_reserved_policy_dropped"), true]]);

  // Row 6: the bundle deleted (the wrapper's `feeds deleted`) pages once a feed is 75 s old.
  mark = p.changes.length;
  for (const name of ["keys", "policies", "grants"]) rmSync(join(feeds, `${name}.json`));
  await shows(p, mark, 150, [["open", page("r06_feed_delivery"), true]]);
  expect(summaryOf(p, mark, page("r06_feed_delivery"))).toMatch(/^\[PAGE\] cell_sc1\/r06_feed_delivery jwks_age_secs=(7[6-9]|[89]\d) /);
  writeFeed(feeds, "keys", bundle.keys);
  writeFeed(feeds, "policies", { feed_version: v + 2, projects: bundle.policies.projects });
  writeFeed(feeds, "grants", bundle.grants);
  await shows(p, mark, 150, [["open", page("r06_feed_delivery"), true], ["resolve", page("r06_feed_delivery"), true]]);

  // Row 13: a gather's flush held 35 s.
  mark = p.changes.length;
  expect((await debug("history-stall?ms=35000")).status).toBe(200);
  await shows(p, mark, 240, [["open", page("r13_history_flush_wait"), true]]);
  expect(summaryOf(p, mark, page("r13_history_flush_wait"))).toMatch(/ history_flush_wait_ms_max=3[5-9]\d{3} /);
  expect((await debug("history-stall?ms=0")).status).toBe(200);
  await shows(p, mark, 240, [["open", page("r13_history_flush_wait"), true], ["resolve", page("r13_history_flush_wait"), true]]);

  // Row 12: absorption paused until the absorb-lag latch engages.
  mark = p.changes.length;
  expect((await debug("absorb-pause?on=1")).status).toBe(200);
  await shows(p, mark, 300, [["open", page("r12_backlog_latch"), true]]);
  expect(summaryOf(p, mark, page("r12_backlog_latch"))).toMatch(/^\[PAGE\] cell_sc1\/r12_backlog_latch engaged=1 /);
  expect((await debug("absorb-pause?on=0")).status).toBe(200);
  await shows(p, mark, 150, [["open", page("r12_backlog_latch"), true], ["resolve", page("r12_backlog_latch"), true]]);

  // Rows 3 and 1: the cell stopped. The watch's inside check pages after
  // 2 min, the tool's uptime check after 3 failures a minute apart; the
  // cell back pages neither and notifies its restart.
  mark = p.changes.length;
  await stopCell(cell);
  await shows(p, mark, 240, [["open", page("r03_no_2xx_inside"), true], ["open", UPTIME_KEY, true]]);
  expect(summaryOf(p, mark, page("r03_no_2xx_inside"))).toMatch(
    /^\[PAGE\] cell_sc1\/r03_no_2xx_inside origin=platform answer=no_answer status=0 down_secs=1[2-9]\d$/,
  );
  // Back on the profile's 900 s lag bound: what the stopped cell left
  // unabsorbed is older than the drill's 150 s by now, and would engage
  // the latch (row 12) from boot to the first gather.
  await startCell({ ...spec, env: {}, log: join(c.root, "server-2.log") });
  await shows(p, mark, 180, [
    ["open", page("r03_no_2xx_inside"), true], ["open", UPTIME_KEY, true], ["open", "cell_sc1/r04_restarts/notify", false],
    ["resolve", page("r03_no_2xx_inside"), true], ["resolve", UPTIME_KEY, true], ["resolve", "cell_sc1/r04_restarts/notify", false],
  ]);

  // Row 2: the watch killed. Its beats stop, and the tool pages 3 min after the last.
  mark = p.changes.length;
  await watch.stop();
  const stopped = Date.now();
  await shows(p, mark, 240, [["open", HEARTBEAT_KEY, true]]);
  const last = p.beats[p.beats.length - 1] ?? 0;
  expect([last < stopped, p.changes[p.changes.length - 1]!.at - last >= 180_000]).toEqual([true, true]);
  // Row 17 never opened in all of it, and no event carried the operator bearer.
  expect(p.since(0).filter(([, key]) => key === ROW_17)).toEqual([]);
  expect([watch.log(), watch.out()].filter((t) => t.includes(operator) || t.includes(ROUTING_KEY))).toEqual([]);
}, 45 * 60_000);

maybe("the issuer's drills each page the on-call: row 21 with the issuer stopped, row 19 with a wrong-aud keys feed", async () => {
  const bin = resolve(BIN_DIR ?? "");
  const ic = await issuerCell();
  const secretFile = join(ic.root, "canary-watch.secret");
  await admin(ic, "credential", "--project", canaryProjectId("cell_sc1"), "--hidden", "--name", "watch",
    "--scopes", CANARY_SCOPES, "--pepper-file", ic.pepper, "--out", secretFile);
  const credential = readFileSync(secretFile, "utf8").trim();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  const bundle = publish(ic.state, feeds);
  const operator = randomBytes(24).toString("hex");
  const cell = await startCell({
    shell: SHELL, bin, port: freePort(), s3: startS3(bin), bucket: `cell-watch-drills-issuer-${Date.now()}`, feeds, operator,
    log: join(ic.root, "server.log"),
  });

  // The issuer as main.ts runs it, for this cell, on a port it keeps across a restart.
  const cfg = readConfig(envWith(ic, { STREAMS_CELL_ORIGIN: cell.origin }));
  const booted = await boot(cfg);
  const cache = new StateCache(booted.store, booted, { log: () => {} });
  const issuer = new Issuer(cfg, { store: booted.store, cache, log: () => {} });
  onTeardown(cache.start());
  const issuerPort = freePort();
  const serveIssuer = () => Bun.serve({ port: issuerPort, hostname: "127.0.0.1", fetch: (req) => issuer.fetch(req) });
  let server = serveIssuer();
  onTeardown(() => server.stop(true));
  const issuerOrigin = `http://127.0.0.1:${issuerPort}`;

  const p = pager(null);
  await startWatch(SHELL, inputs(cell.origin, operator, cell.git, p, {
    ISSUER_ORIGIN: issuerOrigin,
    STREAMS_ISSUER_METRICS_TOKEN: ic.env.STREAMS_ISSUER_METRICS_TOKEN ?? "",
    CANARY_CREDENTIAL: credential,
  }));
  // The canary's first round has put its record in the stream; nothing is open, row 17 included.
  const token = (await (await fetch(`${issuerOrigin}${EXCHANGE_PATH}`, {
    method: "POST", headers: { authorization: `StreamsCredential ${credential}` },
  })).json() as { accessToken: string }).accessToken;
  await until("the canary's first record", 150, async () => {
    const res = await fetch(`${cell.origin}${RECORDS_PATH}?cursor=beginning`, {
      headers: { authorization: `Bearer ${token}`, "streams-encryption-key": canaryStreamKey("cell_sc1") },
    });
    return res.ok && ((await res.json()) as unknown[]).length > 0 ? true : undefined;
  });
  await until("two heartbeats", 45, () => (p.beats.length >= 2 ? true : undefined));
  expect(changes(p, 0)).toEqual([]);

  // Row 21: the issuer stopped pages after 2 min; back, it resolves.
  let mark = p.changes.length;
  server.stop(true);
  await shows(p, mark, 210, [["open", page("r21_issuer_down"), true]]);
  expect(summaryOf(p, mark, page("r21_issuer_down"))).toMatch(/^\[PAGE\] cell_sc1\/r21_issuer_down answer=no_answer status=0 down_secs=1[2-9]\d$/);
  server = serveIssuer();
  await shows(p, mark, 150, [["open", page("r21_issuer_down"), true], ["resolve", page("r21_issuer_down"), true]]);

  // Row 19: the keys feed carries the issuer's key under another kid for
  // the internal audience. The cell retires the kid the issuer signs
  // with, so every customer token, the canary's too, answers 401
  // kid_unknown while /health is green; three rounds a minute apart page.
  mark = p.changes.length;
  const key = bundle.keys.keys[0] as { kid: string };
  writeFeed(feeds, "keys", { feed_version: bundle.keys.feed_version + 1, keys: [{ ...key, kid: `${key.kid}-drill`, aud: "prisma-streams-internal" }] });
  await shows(p, mark, 330, [["open", page("r19_canary"), true]]);
  expect(summaryOf(p, mark, page("r19_canary"))).toBe(
    "[PAGE] cell_sc1/r19_canary step=append origin=server error_code=kid_unknown status=401 consecutive_failures=3",
  );
  expect((await fetch(`${cell.origin}/health`)).status).toBe(200);
}, 25 * 60_000);
