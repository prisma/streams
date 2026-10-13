// The cell watch's loop (public-beta alerting triage B3) against a local
// cell, sink and heartbeat monitor: a cycle polls, steps the rules,
// delivers and beats; a NOTIFY rising to PAGE pages; a refusing sink
// withholds the heartbeat (D7); a restarted watch re-triggers on the same
// alert key; a failed cycle sends no heartbeat; the loop's cadence and
// stop; the `ok` port; the log lines. Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { type Answer, CANARY_CREDENTIAL, FakeCell, FakeIssuer, FakeServer, METRICS_TOKEN, OPERATOR_TOKEN } from "./fake-cell";
import { CFG, DATA, patch, POLL_MS, T0 } from "./rig";
import type { Json } from "./rules";
import { CellWatch, type CycleReport, serveOk, type WatchConfig } from "./watch";

const KEY = "routing-key-for-the-watch-tests";
const BEAT_PATH = "/ping/heartbeat-secret-for-the-watch-tests";
const ACCEPTED: Answer = { status: 202, body: '{"status":"success"}' };
const R07 = "cell_sc1/r07_reserved_policy_dropped/page";

const rigs: Rig[] = [];
afterEach(() => {
  for (const r of rigs.splice(0)) r.stop();
});

/// A cell, a sink and a heartbeat monitor on local ports, and the watch's clock.
class Rig {
  readonly cell = new FakeCell();
  sinkAnswer: Answer = ACCEPTED;
  readonly sink = new FakeServer({ "/v2/enqueue": () => this.sinkAnswer });
  readonly monitor = new FakeServer({ [BEAT_PATH]: () => ({ status: 200, body: "OK" }) });
  readonly log: string[] = [];
  at = T0;

  constructor() {
    rigs.push(this);
  }

  config(cellOrigin = this.cell.origin): WatchConfig {
    return {
      rules: CFG,
      poll: { cellOrigin, operatorToken: OPERATOR_TOKEN, issuerOrigin: null, issuerMetricsToken: null, canary: null },
      sink: { kind: "events-v2", url: `${this.sink.origin}/v2/enqueue`, key: KEY },
      heartbeatUrl: `${this.monitor.origin}${BEAT_PATH}`,
    };
  }

  watch(cellOrigin?: string): CellWatch {
    return new CellWatch(this.config(cellOrigin), { now: () => this.at, log: (l) => this.log.push(l) });
  }

  /// `n` cycles, POLL_MS apart on the watch's clock.
  async cycles(w: CellWatch, n: number): Promise<CycleReport[]> {
    const out: CycleReport[] = [];
    for (let i = 0; i < n; i++) {
      out.push(await w.cycle());
      this.at += POLL_MS;
    }
    return out;
  }

  /// Serves /operator/data.json with the paths edited.
  data(edits: Record<string, Json> = {}): void {
    this.cell.setData(patch(DATA, edits));
  }

  /// `[event_action, dedup_key]` of each request the sink got.
  sent(): string[][] {
    return this.sink.seen.map((s) => {
      const b = JSON.parse(s.body);
      return [b.event_action, b.dedup_key];
    });
  }

  beats(): number {
    return this.monitor.seen.length;
  }

  stop(): void {
    for (const s of [this.cell, this.sink, this.monitor]) s.stop();
  }
}

const kinds = (reports: CycleReport[]) => reports.map((r) => r.events.map((e) => `${e.kind} ${e.class}`));

describe("a cycle", () => {
  test("of a healthy cell polls it, raises and delivers nothing, and sends one heartbeat GET", async () => {
    const rig = new Rig();
    const reports = await rig.cycles(rig.watch(), 3);
    expect(reports).toEqual(Array(3).fill({ events: [], queued: 0, heartbeat: "ok" }));
    expect([rig.sink.seen.length, rig.monitor.seen.map((s) => [s.method, s.path])]).toEqual([0, Array(3).fill(["GET", BEAT_PATH])]);
    expect(rig.cell.server.paths().filter((p) => p !== "/operator/billing.json")).toHaveLength(6);
  });

  test("delivers a PAGE rule's trigger on its page key, then its resolve after four clean cycles, beating every cycle", async () => {
    const rig = new Rig();
    const w = rig.watch();
    rig.data({ "feeds.policies.reservedDropped": 2 });
    const opened = await rig.cycles(w, 1);
    rig.data();
    const closed = await rig.cycles(w, 4);
    expect(kinds([...opened, ...closed])).toEqual([["trigger PAGE"], [], [], [], ["resolve PAGE"]]);
    expect([opened[0]?.events[0]?.at, closed[3]?.events[0]?.at]).toEqual([T0, T0 + 4 * POLL_MS]);
    expect([rig.sent(), rig.beats()]).toEqual([[["trigger", R07], ["resolve", R07]], 5]);
    expect(JSON.parse(rig.sink.seen[0]?.body ?? "{}").payload.custom_details.values).toEqual({ reserved_dropped: 2 });
  });

  test("pages a rule that rises from NOTIFY to PAGE on a second key and resolves both", async () => {
    const rig = new Rig();
    const w = rig.watch();
    const reports: CycleReport[] = [];
    for (const shed of [0, 1, 2, 2, 2, 2, 2]) {
      rig.data({ "local.admit_shed_rss": shed });
      reports.push(...(await rig.cycles(w, 1)));
    }
    expect(kinds(reports)).toEqual([[], ["trigger NOTIFY"], ["trigger PAGE"], [], [], [], ["resolve PAGE"]]);
    const key = "cell_sc1/r09_rss_write_shed";
    expect(rig.sent()).toEqual([
      ["trigger", `${key}/notify`], ["trigger", `${key}/page`], ["resolve", `${key}/notify`], ["resolve", `${key}/page`],
    ]);
  });
});

describe("the canary", () => {
  test("pages row 19 after three failed rounds a minute apart, naming the step, and resolves after four that pass", async () => {
    const rig = new Rig();
    const issuer = new FakeIssuer();
    issuer.endpoint = rig.cell.origin;
    try {
      const cfg = rig.config();
      const w = new CellWatch({
        ...cfg,
        rules: { ...CFG, cellOrigin: rig.cell.origin, issuer: true },
        poll: {
          ...cfg.poll,
          issuerOrigin: issuer.origin,
          issuerMetricsToken: METRICS_TOKEN,
          canary: { credential: CANARY_CREDENTIAL, cellId: "cell_sc1" },
        },
      }, { now: () => rig.at, log: (l) => rig.log.push(l) });
      const reports = await rig.cycles(w, 4);
      // The cell refuses the canary's token (the keys feed and the binary
      // disagree): each round's append is a 401, the token dropped and
      // exchanged again, so the issuer's row stays quiet.
      rig.cell.canary.append = { status: 401, headers: { "prisma-streams-origin": "s" }, body: '{"error":{"code":"kid_unknown","message":"x"}}' };
      reports.push(...(await rig.cycles(w, 12)));
      delete rig.cell.canary.append;
      reports.push(...(await rig.cycles(w, 16)));
      const at = reports.flatMap((r, cycle) => r.events.map((e) => [cycle, e.kind, e.dedupKey]));
      expect(at).toEqual([[12, "trigger", "cell_sc1/r19_canary"], [28, "resolve", "cell_sc1/r19_canary"]]);
      expect(rig.sent()).toEqual([["trigger", "cell_sc1/r19_canary/page"], ["resolve", "cell_sc1/r19_canary/page"]]);
      expect(JSON.parse(rig.sink.seen[0]?.body ?? "{}").payload.custom_details).toMatchObject({
        values: { status: 401, consecutive_failures: 3 },
        labels: { step: "append", origin: "server", error_code: "kid_unknown" },
      });
      // Exchanges: the first round's, and one after each 401 but the last.
      expect(issuer.seen.filter((s) => s.method === "POST").length).toBe(4);
      expect(rig.beats()).toBe(32);
    } finally {
      issuer.stop();
    }
  });
});

describe("a sink that refuses (D7)", () => {
  test("keeps the trigger queued, withholds the heartbeat from the third cycle, and beats again once it is delivered", async () => {
    const rig = new Rig();
    const w = rig.watch();
    rig.sinkAnswer = { status: 503, body: "" };
    rig.data({ "feeds.policies.reservedDropped": 1 });
    const refused = await rig.cycles(w, 4);
    rig.sinkAnswer = ACCEPTED;
    const taken = await rig.cycles(w, 1);
    expect([...refused, ...taken].map((r) => [r.queued, r.heartbeat])).toEqual([
      [1, "ok"], [1, "ok"], [1, "withheld"], [1, "withheld"], [0, "ok"],
    ]);
    expect([rig.sent(), rig.beats()]).toEqual([Array(5).fill(["trigger", R07]), 3]);
  });
});

describe("restarts and failures", () => {
  test("a restarted watch re-triggers what still holds on the same alert key", async () => {
    const rig = new Rig();
    rig.data({ "feeds.policies.reservedDropped": 1 });
    await rig.cycles(rig.watch(), 2);
    await rig.cycles(rig.watch(), 2);
    expect(rig.sent()).toEqual([["trigger", R07], ["trigger", R07]]);
  });

  test("a cycle that cannot complete sends no heartbeat, is logged, and the loop carries on", async () => {
    const rig = new Rig();
    const w = rig.watch("not an origin");
    await expect(w.cycle()).rejects.toThrow();
    const stop = w.start(20);
    while (rig.log.filter((l) => l.startsWith("cell-watch: cycle failed: ")).length < 3) await Bun.sleep(10);
    await stop();
    expect([rig.beats(), rig.sink.seen.length]).toEqual([0, 0]);
  });
});

describe("the loop", () => {
  test("runs a cycle every interval, one at a time, until stopped; the stop waits for the cycle in flight", async () => {
    const rig = new Rig();
    rig.cell.health = { ...rig.cell.health, delayMs: 30 };
    const stop = rig.watch().start(10);
    while (rig.beats() < 3) await Bun.sleep(5);
    await stop();
    const health = rig.cell.server.paths().filter((p) => p === "/health").length;
    const beats = rig.beats();
    await Bun.sleep(100);
    expect([rig.beats(), rig.cell.server.paths().filter((p) => p === "/health").length]).toEqual([beats, health]);
    expect(health).toBe(beats);
  });

  test("starts each cycle at least the interval after the one before began", async () => {
    const rig = new Rig();
    const began = Date.now();
    const stop = rig.watch().start(80);
    while (rig.beats() < 3) await Bun.sleep(5);
    const stoppedAt = Date.now();
    await stop();
    expect(rig.beats()).toBeLessThanOrEqual(Math.floor((stoppedAt - began) / 80) + 1);
  });

  test("a stop during a cycle waits for it to end and starts no other", async () => {
    const rig = new Rig();
    rig.cell.health = { ...rig.cell.health, delayMs: 100 };
    const stop = rig.watch().start(10);
    while (rig.cell.server.paths().length === 0) await Bun.sleep(2);
    await stop();
    const ended = rig.beats();
    await Bun.sleep(250);
    expect([ended, rig.beats(), rig.cell.server.paths().filter((p) => p === "/health").length]).toEqual([1, 1, 1]);
  });

  test("a stop between cycles cancels the next one", async () => {
    const rig = new Rig();
    const stop = rig.watch().start(300);
    while (rig.beats() < 1) await Bun.sleep(5);
    await stop();
    await Bun.sleep(400);
    expect([rig.beats(), rig.cell.server.paths().filter((p) => p === "/health").length]).toEqual([1, 1]);
  });
});

describe("$PORT", () => {
  test("answers ok to GET / and 404 with no body to anything else", async () => {
    const server = serveOk(0, "127.0.0.1");
    try {
      const at = (path: string, method = "GET") => fetch(`http://127.0.0.1:${server.port}${path}`, { method });
      const got: unknown[] = [];
      for (const [path, method] of [["/", "GET"], ["/?x=1", "GET"], ["/health", "GET"], ["/operator/data.json", "GET"], ["/", "POST"], ["/", "PUT"]] as const) {
        const res = await at(path, method);
        got.push([path, method, res.status, await res.text()]);
      }
      expect(got).toEqual([
        ["/", "GET", 200, "ok"], ["/?x=1", "GET", 200, "ok"], ["/health", "GET", 404, ""], ["/operator/data.json", "GET", 404, ""],
        ["/", "POST", 404, ""], ["/", "PUT", 404, ""],
      ]);
    } finally {
      server.stop(true);
    }
  });
});

describe("the log", () => {
  test("has one line per event, the event itself, and names no URL, token, key or heartbeat path", async () => {
    const rig = new Rig();
    const w = rig.watch();
    rig.sinkAnswer = { status: 401, body: `{"error":"bad key ${KEY}"}` };
    rig.data({ "feeds.policies.reservedDropped": 1 });
    const reports = await rig.cycles(w, 3);
    rig.sinkAnswer = ACCEPTED;
    rig.data();
    reports.push(...(await rig.cycles(w, 5)));
    const events = reports.flatMap((r) => r.events);
    expect(events.map((e) => e.kind)).toEqual(["trigger", "resolve"]);
    expect(rig.log.filter((l) => l.startsWith("cell-watch: {"))).toEqual(events.map((e) => `cell-watch: ${JSON.stringify(e)}`));
    expect(rig.log.filter((l) => !l.startsWith("cell-watch: {"))).toEqual([
      `cell-watch: delivery of ${R07} (trigger) failed: status 401; 1 queued`,
      `cell-watch: delivery of ${R07} (trigger) failed: status 401; 1 queued`,
      `cell-watch: delivery of ${R07} (trigger) failed: status 401; 1 queued`,
      "cell-watch: heartbeat withheld: 3 cycles in a row with a trigger undelivered",
    ]);
    const secrets = [OPERATOR_TOKEN, KEY, BEAT_PATH, rig.cell.origin, rig.sink.origin, rig.monitor.origin, "http"];
    for (const line of rig.log) expect(secrets.filter((s) => line.includes(s))).toEqual([]);
  });

  test("names a failed heartbeat by its status only", async () => {
    const rig = new Rig();
    rig.monitor.routes[BEAT_PATH] = () => ({ status: 404, body: BEAT_PATH });
    const [report] = await rig.cycles(rig.watch(), 1);
    expect([report?.heartbeat, rig.log]).toEqual([404, ["cell-watch: heartbeat failed: status 404"]]);
  });
});
