// The cell watch's start (public-beta alerting triage B4) against a local
// cell, sink and heartbeat monitor: the inputs read, the keep-awake guard
// taken, `ok` on the port, the boot line, and the first cycle at once; an
// input refused before any of it. Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { InputError } from "./config";
import { FakeCell, FakeServer, OPERATOR_TOKEN } from "./fake-cell";
import { type Running, runWatch } from "./run";

const KEY = "webhook-bearer-for-the-run-tests";
const BEAT_PATH = "/ping/heartbeat-secret-for-the-run-tests";

const stops: (() => unknown)[] = [];
afterEach(async () => {
  for (const s of stops.splice(0).reverse()) await s();
});

function rig() {
  const cell = new FakeCell();
  const sink = new FakeServer({ "/hook": () => ({ status: 202, body: "" }) });
  const monitor = new FakeServer({ [BEAT_PATH]: () => ({ status: 200, body: "OK" }) });
  stops.push(() => cell.stop(), () => sink.stop(), () => monitor.stop());
  const inputs = {
    STREAMS_OPERATOR_TOKEN: OPERATOR_TOKEN,
    ALERT_SINK_URL: `${sink.origin}/hook`,
    ALERT_SINK_KEY: KEY,
    HEARTBEAT_URL: `${monitor.origin}${BEAT_PATH}`,
    STREAMS_ISSUER_METRICS_TOKEN: "none",
    CANARY_CREDENTIAL: "none",
    STREAMS_CELL_ORIGIN: cell.origin,
    STREAMS_CELL_ID: "cell_sc1",
    ALERT_SINK_KIND: "webhook",
    EXPECTED_GIT: "none",
    NOTIFY_ONLY: "1",
    ISSUER_ORIGIN: "none",
  };
  return { cell, sink, monitor, inputs };
}

async function until(what: string, cond: () => boolean, ms = 5_000): Promise<void> {
  const end = Date.now() + ms;
  while (!cond()) {
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`);
    await Bun.sleep(20);
  }
}

describe("runWatch", () => {
  test("takes the guard, answers ok on its port, logs its boot line and runs the first cycle at once", async () => {
    const r = rig();
    const log: string[] = [];
    const order: string[] = [];
    const running: Running = runWatch(r.inputs, 0, () => {
      order.push("guard");
      return "unavailable";
    }, (l) => log.push(l));
    stops.push(running.stop);
    order.push("returned");
    expect(order).toEqual(["guard", "returned"]);
    expect(log[0]).toBe(
      "cell-watch: cell cell_sc1, sink webhook, notify_only true, issuer false, expected_git false, " +
        `keep_awake unavailable, listening on :${running.port}`,
    );
    const ok = await fetch(`http://127.0.0.1:${running.port}/`);
    expect([ok.status, await ok.text()]).toEqual([200, "ok"]);
    expect((await fetch(`http://127.0.0.1:${running.port}/operator/data.json`)).status).toBe(404);
    await until("the first heartbeat", () => r.monitor.seen.length === 1);
    // The first cycle polled the cell with the operator bearer, then beat.
    const bearer = r.cell.seen.filter((s) => s.path === "/operator/data.json").map((s) => s.authorization);
    expect(bearer).toEqual([`Bearer ${OPERATOR_TOKEN}`]);
    expect(r.monitor.seen.map((s) => [s.method, s.path])).toEqual([["GET", BEAT_PATH]]);
    expect(r.sink.seen).toEqual([]);
    // No line names a URL or a secret.
    for (const secret of [OPERATOR_TOKEN, KEY, BEAT_PATH, r.cell.origin, r.sink.origin]) {
      expect(log.filter((l) => l.includes(secret))).toEqual([]);
    }
    await running.stop();
    await expect(fetch(`http://127.0.0.1:${running.port}/`)).rejects.toThrow();
  });

  test("delivers to the webhook with its bearer once a rule triggers", async () => {
    const r = rig();
    r.cell.health = { status: 200, headers: {}, body: "ok" };
    r.cell.setData({ ts_ms: 1, ops: null, feeds: { policies: { reservedDropped: 3 } } });
    const running = runWatch(r.inputs, 0, () => "held", () => {});
    stops.push(running.stop);
    await until("the trigger", () => r.sink.seen.length === 1);
    const sent = r.sink.seen[0]!;
    expect([sent.method, sent.authorization, sent.contentType]).toEqual(["POST", `Bearer ${KEY}`, "application/json"]);
    // NOTIFY_ONLY=1 demotes the level rules only: binary row 7 pages.
    expect(JSON.parse(sent.body)).toMatchObject({ kind: "trigger", class: "PAGE", dedupKey: "cell_sc1/r07_reserved_policy_dropped/page" });
  });

  test("refuses its inputs before it takes the guard or binds the port", async () => {
    const r = rig();
    const probe = Bun.serve({ port: 0, hostname: "0.0.0.0", fetch: () => new Response() });
    const port = Number(probe.port);
    probe.stop(true);
    let guards = 0;
    const refused = () => runWatch({ ...r.inputs, ALERT_SINK_KIND: "slack" }, port, () => {
      guards += 1;
      return "held";
    });
    expect(refused).toThrow(new InputError("ALERT_SINK_KIND is events-v2 or webhook"));
    expect(guards).toBe(0);
    await expect(fetch(`http://127.0.0.1:${port}/`)).rejects.toThrow();
    await Bun.sleep(100);
    expect([r.cell.seen, r.monitor.seen]).toEqual([[], []]);
  });
});
