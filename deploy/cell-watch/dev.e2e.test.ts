// The cell watch's app in `prisma dev` against a local shared cell (public-
// beta alerting triage B4's dev e2e): the real binary over s3lite on the
// shared-cell profile with cell-admin's feeds, as cell-admin's e2e test
// boots it, and the watch deployed by Composer's local target from app.ts,
// with a local webhook sink and heartbeat monitor. Opt-in, because it
// needs built binaries, this directory's packages and Composer's local
// emulator (`prisma dev` builds nothing: the test bundles entry.js first):
//
//   cargo build --locked --bin streams-slate --bin s3lite
//   (cd deploy/cell-watch && bun install --frozen-lockfile)
//   CELL_WATCH_E2E_BIN_DIR=target/debug bun test ./deploy/cell-watch/dev.e2e.test.ts
//
// `prisma dev` runs with an emulator directory of its own (`--fresh`), whose
// compute daemon the test stops at the end. Keep-awake cannot be tested
// here: off Compute the guard is a no-op (triage N4), and the boot line
// says `keep_awake unavailable`.
import { afterAll, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdirSync } from "node:fs";
import { join, resolve } from "node:path";
import { initCell, scratchDir } from "../cell-admin/test-rig";
import { freePort, onTeardown, publish, startCell, startS3, startWatch, stopCell, teardown, until } from "./e2e-rig";
import { FakeServer } from "./fake-cell";

const BIN_DIR = process.env.CELL_WATCH_E2E_BIN_DIR;
const SHELL = { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "" };
const maybe = BIN_DIR ? test : test.skip;
const BEAT = "/beat/heartbeat-secret-dev-e2e";
const SINK_BEARER = `webhook-bearer-${randomBytes(8).toString("hex")}`;
const OPERATOR = randomBytes(24).toString("hex");

afterAll(teardown);

/// Row 17 (`r17_unowned_usage`), which the cell's own metering opened on
/// every idle enforce cell until round 4's fix (NEXT-WORK 14.7: the
/// rollup's reads of its own ledgers meter nothing now).
const ROW_17 = "cell_sc1/r17_unowned_usage/notify";

/// `[kind, alert key]` of each event the webhook got.
const sent = (sink: FakeServer) => sink.seen.map((s) => {
  const b = JSON.parse(s.body);
  return [b.kind, b.dedupKey];
});

maybe("in prisma dev the watch polls a local shared cell, beats each cycle, answers ok, and notifies a restart, then resolves it", async () => {
  const bin = resolve(BIN_DIR ?? "");
  const c = await initCell();
  const feeds = join(scratchDir(), "feeds");
  mkdirSync(feeds);
  publish(c.state, feeds);
  const spec = {
    shell: SHELL, bin, port: freePort(), s3: startS3(bin), bucket: `cell-watch-e2e-${Date.now()}`, feeds, operator: OPERATOR,
    log: join(c.root, "server-1.log"),
  };
  const first = await startCell(spec);

  const sink = new FakeServer({ "/hook": () => ({ status: 202, body: "" }) });
  const monitor = new FakeServer({ [BEAT]: () => ({ status: 200, body: "OK" }) });
  onTeardown(() => sink.stop());
  onTeardown(() => monitor.stop());
  const inputs = {
    STREAMS_OPERATOR_TOKEN: OPERATOR,
    ALERT_SINK_URL: `${sink.origin}/hook`,
    ALERT_SINK_KEY: SINK_BEARER,
    HEARTBEAT_URL: `${monitor.origin}${BEAT}`,
    STREAMS_ISSUER_METRICS_TOKEN: "none",
    CANARY_CREDENTIAL: "none",
    STREAMS_CELL_ORIGIN: first.origin,
    STREAMS_CELL_ID: "cell_sc1",
    ALERT_SINK_KIND: "webhook",
    EXPECTED_GIT: first.git,
    NOTIFY_ONLY: "0",
    ISSUER_ORIGIN: "none",
  };
  const watch = await startWatch(SHELL, inputs);
  const ok = await fetch(`${watch.url}/`);
  expect([ok.status, await ok.text(), (await fetch(`${watch.url}/x`)).status]).toEqual([200, "ok", 404]);
  // Two cycles of a healthy cell: two heartbeats, and no event, row 17 included.
  await until("two heartbeats", 45, () => (monitor.seen.length >= 2 ? true : undefined));
  expect(sent(sink)).toEqual([]);

  // A restart is a new boot id: NOTIFY, then the resolve after four clean
  // cycles, which closes both of the rule's keys.
  await stopCell(first);
  const second = await startCell({ ...spec, log: join(c.root, "server-2.log") });
  expect(second.boot).not.toBe(first.boot);
  await until("the restart's trigger", 60, () => (sent(sink).length >= 1 ? true : undefined));
  await until("its resolve", 120, () => (sent(sink).length >= 3 ? true : undefined));
  expect(sent(sink)).toEqual([
    ["trigger", "cell_sc1/r04_restarts/notify"], ["resolve", "cell_sc1/r04_restarts/notify"], ["resolve", "cell_sc1/r04_restarts/page"],
  ]);
  const trigger = JSON.parse(sink.seen.find((s) => s.body.includes('"r04_restarts"'))!.body);
  expect([trigger.class, trigger.values]).toEqual(["NOTIFY", { boot_ids_30m: 2 }]);
  expect(new Set(sink.seen.map((s) => s.authorization))).toEqual(new Set([`Bearer ${SINK_BEARER}`]));
  // Row 17 never opened: the cell's own metering no longer counts as unowned.
  expect(sent(sink).filter(([, key]) => key === ROW_17)).toEqual([]);
  expect(monitor.seen.every((s) => s.method === "GET")).toBe(true);

  // The service's log (the emulator's): the boot line first; no secret anywhere.
  const log = watch.log();
  expect(log.split("\n")[0]).toBe(
    "cell-watch: cell cell_sc1, sink webhook, notify_only false, issuer false, expected_git true, " +
      `keep_awake unavailable, listening on :${new URL(watch.url).port}`,
  );
  for (const secret of [OPERATOR, SINK_BEARER, BEAT]) expect([log, watch.out()].filter((t) => t.includes(secret))).toEqual([]);
}, 330_000);
