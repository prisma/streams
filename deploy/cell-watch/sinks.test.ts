// The cell watch's sinks (public-beta alerting triage B3; D7, D8) against
// local servers: the Events v2 and webhook bodies, the alert key per
// class (a NOTIFY rising to PAGE pages), delivery in order with retries,
// the bounded queue, the withheld heartbeat, the heartbeat GET and the
// log lines. Run with `bun test ./deploy/cell-watch`.
import { afterEach, describe, expect, test } from "bun:test";
import { type Answer, FakeServer } from "./fake-cell";
import { type Class, heartbeatDue, initialState, type WatchEvent, type WatchState, WITHHOLD_AFTER } from "./rules";
import { alertKey, heartbeat, Outbox, OUTBOX_MAX, SEND_TIMEOUT_MS, type SinkConfig } from "./sinks";

const KEY = "routing-key-for-the-watch-tests";
const AT = 1_800_000_000_000;
const ANCHOR = "RUNBOOK.md#8-monitoring";
const ACCEPTED: Answer = { status: 202, headers: { "content-type": "application/json" }, body: '{"status":"success"}' };

const running: FakeServer[] = [];
afterEach(() => {
  for (const s of running.splice(0)) s.stop();
});

/// A sink at /v2/enqueue answering `answer` (changeable while it runs).
function sinkServer(answer: Answer = ACCEPTED): { server: FakeServer; set(a: Answer): void } {
  let current = answer;
  const server = new FakeServer({ "/v2/enqueue": () => current, "/hook": () => current, "/ping/secret": () => current });
  running.push(server);
  return { server, set: (a) => (current = a) };
}

function outbox(server: FakeServer, more: Partial<SinkConfig> = {}, log: string[] = []): Outbox {
  return new Outbox({ kind: "events-v2", url: `${server.origin}/v2/enqueue`, key: KEY, ...more }, (l) => log.push(l));
}

function trigger(rule: string, cls: Class, values: Record<string, number> = {}, labels: Record<string, string> = {}): WatchEvent {
  return { kind: "trigger", dedupKey: `cell_sc1/${rule}`, class: cls, rule, values, labels, at: AT, runbookAnchor: ANCHOR };
}

function resolve(rule: string, cls: Class, openSecs = 60): WatchEvent {
  return { kind: "resolve", dedupKey: `cell_sc1/${rule}`, class: cls, rule, values: { open_secs: openSecs }, labels: {}, at: AT + 60_000, runbookAnchor: ANCHOR };
}

const bodies = (server: FakeServer) => server.seen.map((s) => JSON.parse(s.body));
const actions = (server: FakeServer) => bodies(server).map((b) => [b.event_action, b.dedup_key]);

const R03 = trigger("r03_no_2xx_inside", "PAGE", { status: 503, down_secs: 125 }, { origin: "server", answer: "status", reason: "shard_storage" });

describe("Events v2 (PagerDuty's shape)", () => {
  test("a trigger goes on its class's alert key, severity critical for PAGE and warning for NOTIFY, the numbers and enums as details", async () => {
    const { server } = sinkServer();
    const box = outbox(server);
    box.add([R03, trigger("r17_unowned_usage", "NOTIFY", { unowned_events: 4 })]);
    await box.flush(initialState());
    expect(bodies(server)).toEqual([
      {
        routing_key: KEY,
        event_action: "trigger",
        dedup_key: "cell_sc1/r03_no_2xx_inside/page",
        payload: {
          summary: "[PAGE] cell_sc1/r03_no_2xx_inside origin=server answer=status reason=shard_storage status=503 down_secs=125",
          source: "cell_sc1",
          severity: "critical",
          timestamp: "2027-01-15T08:00:00.000Z",
          class: "r03_no_2xx_inside",
          custom_details: {
            values: { status: 503, down_secs: 125 },
            labels: { origin: "server", answer: "status", reason: "shard_storage" },
            runbook: ANCHOR,
          },
        },
      },
      {
        routing_key: KEY,
        event_action: "trigger",
        dedup_key: "cell_sc1/r17_unowned_usage/notify",
        payload: {
          summary: "[NOTIFY] cell_sc1/r17_unowned_usage unowned_events=4",
          source: "cell_sc1",
          severity: "warning",
          timestamp: "2027-01-15T08:00:00.000Z",
          class: "r17_unowned_usage",
          custom_details: { values: { unowned_events: 4 }, labels: {}, runbook: ANCHOR },
        },
      },
    ]);
    expect(server.seen.map((s) => [s.method, s.path, s.contentType, s.authorization])).toEqual(
      Array(2).fill(["POST", "/v2/enqueue", "application/json", null]),
    );
  });

  test("a rule rising from NOTIFY to PAGE opens a second alert on its page key, and its resolve closes both", async () => {
    const { server } = sinkServer();
    const box = outbox(server);
    box.add([trigger("r09_rss_write_shed", "NOTIFY"), trigger("r09_rss_write_shed", "PAGE"), resolve("r09_rss_write_shed", "PAGE")]);
    await box.flush(initialState());
    expect(actions(server)).toEqual([
      ["trigger", "cell_sc1/r09_rss_write_shed/notify"],
      ["trigger", "cell_sc1/r09_rss_write_shed/page"],
      ["resolve", "cell_sc1/r09_rss_write_shed/notify"],
      ["resolve", "cell_sc1/r09_rss_write_shed/page"],
    ]);
    expect(bodies(server)[2]).toEqual({ routing_key: KEY, event_action: "resolve", dedup_key: "cell_sc1/r09_rss_write_shed/notify" });
  });

  test("a resolve closes only the keys its rule was triggered on since it opened; one the outbox never saw open closes its own class's key", async () => {
    const { server } = sinkServer();
    const box = outbox(server);
    box.add([trigger("r04_restarts", "NOTIFY"), resolve("r04_restarts", "NOTIFY"), R03, resolve("r03_no_2xx_inside", "PAGE")]);
    box.add([trigger("r04_restarts", "PAGE"), resolve("r04_restarts", "PAGE"), resolve("r11_durability_wedge", "PAGE")]);
    await box.flush(initialState());
    expect(actions(server)).toEqual([
      ["trigger", "cell_sc1/r04_restarts/notify"],
      ["resolve", "cell_sc1/r04_restarts/notify"],
      ["trigger", "cell_sc1/r03_no_2xx_inside/page"],
      ["resolve", "cell_sc1/r03_no_2xx_inside/page"],
      ["trigger", "cell_sc1/r04_restarts/page"],
      ["resolve", "cell_sc1/r04_restarts/page"],
      ["resolve", "cell_sc1/r11_durability_wedge/page"],
    ]);
  });

  test("the alert key is the event's dedup key and its class", () => {
    expect([alertKey("c/r05_wrong_build", "NOTIFY"), alertKey("c/r05_wrong_build", "PAGE")]).toEqual([
      "c/r05_wrong_build/notify", "c/r05_wrong_build/page",
    ]);
  });
});

describe("the JSON webhook", () => {
  test("posts the event's fields on the alert key with a text line, and the key as a bearer", async () => {
    const { server } = sinkServer();
    const box = outbox(server, { kind: "webhook", url: `${server.origin}/hook` });
    box.add([R03, resolve("r03_no_2xx_inside", "PAGE", 300)]);
    await box.flush(initialState());
    expect(bodies(server)).toEqual([
      {
        text: "[PAGE] cell_sc1/r03_no_2xx_inside origin=server answer=status reason=shard_storage status=503 down_secs=125",
        kind: "trigger",
        dedupKey: "cell_sc1/r03_no_2xx_inside/page",
        class: "PAGE",
        rule: "r03_no_2xx_inside",
        values: { status: 503, down_secs: 125 },
        labels: { origin: "server", answer: "status", reason: "shard_storage" },
        at: AT,
        runbookAnchor: ANCHOR,
      },
      {
        text: "[resolved PAGE] cell_sc1/r03_no_2xx_inside open_secs=300",
        kind: "resolve",
        dedupKey: "cell_sc1/r03_no_2xx_inside/page",
        class: "PAGE",
        rule: "r03_no_2xx_inside",
        values: { open_secs: 300 },
        labels: {},
        at: AT + 60_000,
        runbookAnchor: ANCHOR,
      },
    ]);
    expect(server.seen.map((s) => [s.method, s.path, s.contentType, s.authorization])).toEqual(
      Array(2).fill(["POST", "/hook", "application/json", `Bearer ${KEY}`]),
    );
  });

  test("a rule rising from NOTIFY to PAGE is two alerts there too, each resolved with its own class", async () => {
    const { server } = sinkServer();
    const box = outbox(server, { kind: "webhook", url: `${server.origin}/hook` });
    box.add([trigger("r09_rss_write_shed", "NOTIFY"), trigger("r09_rss_write_shed", "PAGE"), resolve("r09_rss_write_shed", "PAGE")]);
    await box.flush(initialState());
    expect(bodies(server).map((b) => [b.kind, b.dedupKey, b.class, b.text])).toEqual([
      ["trigger", "cell_sc1/r09_rss_write_shed/notify", "NOTIFY", "[NOTIFY] cell_sc1/r09_rss_write_shed"],
      ["trigger", "cell_sc1/r09_rss_write_shed/page", "PAGE", "[PAGE] cell_sc1/r09_rss_write_shed"],
      ["resolve", "cell_sc1/r09_rss_write_shed/notify", "NOTIFY", "[resolved NOTIFY] cell_sc1/r09_rss_write_shed open_secs=60"],
      ["resolve", "cell_sc1/r09_rss_write_shed/page", "PAGE", "[resolved PAGE] cell_sc1/r09_rss_write_shed open_secs=60"],
    ]);
  });

  test("sends no Authorization when the key is empty", async () => {
    const { server } = sinkServer();
    const box = outbox(server, { kind: "webhook", url: `${server.origin}/hook`, key: "" });
    box.add([R03]);
    await box.flush(initialState());
    expect(server.seen.map((s) => s.authorization)).toEqual([null]);
  });
});

describe("delivery", () => {
  test("a 2xx is delivered; a 3xx, 4xx or 5xx is not, and the next flush retries from it", async () => {
    const sink = sinkServer();
    const box = outbox(sink.server);
    box.add([R03]);
    for (const status of [302, 400, 429, 500, 503]) {
      sink.set({ status, body: "" });
      await box.flush(initialState());
      expect([status, box.size]).toEqual([status, 1]);
    }
    sink.set({ status: 200, body: "" });
    await box.flush(initialState());
    expect(box.size).toBe(0);
    await box.flush(initialState());
    expect(actions(sink.server)).toEqual(Array(6).fill(["trigger", "cell_sc1/r03_no_2xx_inside/page"]));
  });

  test("a redirect is a failure and is not followed: its target sees nothing", async () => {
    const elsewhere = sinkServer();
    const sink = sinkServer({ status: 307, headers: { location: `${elsewhere.server.origin}/v2/enqueue` }, body: "" });
    const log: string[] = [];
    const box = outbox(sink.server, {}, log);
    box.add([R03]);
    await box.flush(initialState());
    expect([box.size, elsewhere.server.seen.length, log]).toEqual([
      1, 0, ["cell-watch: delivery of cell_sc1/r03_no_2xx_inside/page (trigger) failed: status 307; 1 queued"],
    ]);
  });

  test("a timeout and a refused connection are failures, logged as such", async () => {
    const sink = sinkServer({ ...ACCEPTED, delayMs: 400 });
    const log: string[] = [];
    const slow = new Outbox({ kind: "events-v2", url: `${sink.server.origin}/v2/enqueue`, key: KEY, timeoutMs: 100 }, (l) => log.push(l));
    slow.add([R03]);
    await slow.flush(initialState());
    const gone = new FakeServer({});
    const url = `${gone.origin}/v2/enqueue`;
    gone.stop();
    const refused = new Outbox({ kind: "events-v2", url, key: KEY }, (l) => log.push(l));
    refused.add([R03]);
    await refused.flush(initialState());
    expect([slow.size, refused.size]).toEqual([1, 1]);
    expect(log).toEqual([
      "cell-watch: delivery of cell_sc1/r03_no_2xx_inside/page (trigger) failed: timeout; 1 queued",
      "cell-watch: delivery of cell_sc1/r03_no_2xx_inside/page (trigger) failed: no_answer; 1 queued",
    ]);
    expect([SEND_TIMEOUT_MS, OUTBOX_MAX]).toEqual([5_000, 256]);
  });

  test("a failure holds every delivery behind it, so a resolve never overtakes its trigger", async () => {
    const sink = sinkServer();
    const box = outbox(sink.server);
    box.add([trigger("r07_reserved_policy_dropped", "PAGE")]);
    sink.set({ status: 503, body: "" });
    await box.flush(initialState());
    box.add([resolve("r07_reserved_policy_dropped", "PAGE"), trigger("r11_durability_wedge", "PAGE")]);
    await box.flush(initialState());
    expect(box.size).toBe(3);
    sink.set(ACCEPTED);
    await box.flush(initialState());
    expect(actions(sink.server)).toEqual([
      ["trigger", "cell_sc1/r07_reserved_policy_dropped/page"],
      ["trigger", "cell_sc1/r07_reserved_policy_dropped/page"],
      ["trigger", "cell_sc1/r07_reserved_policy_dropped/page"],
      ["resolve", "cell_sc1/r07_reserved_policy_dropped/page"],
      ["trigger", "cell_sc1/r11_durability_wedge/page"],
    ]);
  });

  test(`keeps at most ${OUTBOX_MAX} deliveries, dropping the oldest, and logs how many`, async () => {
    const sink = sinkServer({ status: 503, body: "" });
    const log: string[] = [];
    const box = outbox(sink.server, {}, log);
    const events = Array.from({ length: OUTBOX_MAX + 3 }, (_, i) => trigger(`r${i}`, "NOTIFY"));
    box.add(events);
    expect([box.size, log]).toEqual([OUTBOX_MAX, ["cell-watch: outbox full: dropped the 3 oldest deliveries"]]);
    sink.set(ACCEPTED);
    await box.flush(initialState());
    const keys = actions(sink.server).map((a) => a[1]);
    expect([keys.length, keys[0], keys[keys.length - 1]]).toEqual([OUTBOX_MAX, "cell_sc1/r3/notify", `cell_sc1/r${OUTBOX_MAX + 2}/notify`]);
  });
});

describe("the withheld heartbeat (D7)", () => {
  test(`${WITHHOLD_AFTER} flushes in a row ending with a trigger undelivered withhold it; a delivered trigger restores it`, async () => {
    const sink = sinkServer({ status: 503, body: "" });
    const box = outbox(sink.server);
    let state: WatchState = initialState();
    box.add([R03]);
    const due: boolean[] = [];
    for (let i = 0; i < 4; i++) {
      state = await box.flush(state);
      due.push(heartbeatDue(state));
    }
    expect([due, state.failedTriggers]).toEqual([[true, true, false, false], 4]);
    sink.set(ACCEPTED);
    state = await box.flush(state);
    expect([heartbeatDue(state), state.failedTriggers, box.size]).toEqual([true, 0, 0]);
  });

  test("one flush with several triggers undelivered counts once", async () => {
    const sink = sinkServer({ status: 500, body: "" });
    const box = outbox(sink.server);
    box.add([R03, trigger("r06_feed_delivery", "PAGE"), trigger("r07_reserved_policy_dropped", "PAGE")]);
    const state = await box.flush(initialState());
    expect([state.failedTriggers, sink.server.seen.length]).toEqual([1, 1]);
  });

  test("a resolve that fails ahead of a trigger counts as that trigger's failed delivery", async () => {
    const sink = sinkServer();
    const box = outbox(sink.server);
    box.add([R03]);
    let state = await box.flush(initialState());
    sink.set({ status: 503, body: "" });
    box.add([resolve("r03_no_2xx_inside", "PAGE"), trigger("r06_feed_delivery", "PAGE")]);
    for (let i = 0; i < WITHHOLD_AFTER; i++) state = await box.flush(state);
    expect([heartbeatDue(state), bodies(sink.server).map((b) => b.event_action)]).toEqual([
      false, ["trigger", "resolve", "resolve", "resolve"],
    ]);
  });

  test("a failed resolve with no trigger behind it, and a flush with nothing queued, count nothing", async () => {
    const sink = sinkServer();
    const box = outbox(sink.server);
    box.add([R03]);
    let state = await box.flush(initialState());
    sink.set({ status: 503, body: "" });
    box.add([resolve("r03_no_2xx_inside", "PAGE")]);
    for (let i = 0; i < 5; i++) state = await box.flush(state);
    const idle = await outbox(sinkServer().server).flush(state);
    expect([state.failedTriggers, idle.failedTriggers, heartbeatDue(state), box.size]).toEqual([0, 0, true, 1]);
  });

  test("a delivered trigger resets a count that a later trigger in the same flush starts again", async () => {
    const sink = sinkServer();
    let calls = 0;
    sink.server.routes["/v2/enqueue"] = () => (++calls === 1 ? ACCEPTED : { status: 503, body: "" });
    const box = outbox(sink.server);
    box.add([R03, trigger("r06_feed_delivery", "PAGE")]);
    const state = await box.flush({ ...initialState(), failedTriggers: 2 });
    expect([state.failedTriggers, box.size]).toEqual([1, 1]);
  });
});

describe("the heartbeat GET", () => {
  test("goes to the configured URL only, with no body and no bearer; a 2xx is ok", async () => {
    const sink = sinkServer({ status: 200, body: "OK" });
    expect(await heartbeat(`${sink.server.origin}/ping/secret`)).toBe("ok");
    expect(sink.server.seen.map((s) => [s.method, s.path, s.authorization, s.body])).toEqual([["GET", "/ping/secret", null, ""]]);
  });

  test("any other answer is a failure, returned and never thrown: a redirect (not followed), a 5xx, a timeout, a refusal", async () => {
    const elsewhere = sinkServer();
    const sink = sinkServer({ status: 302, headers: { location: `${elsewhere.server.origin}/ping/secret` }, body: "" });
    const url = `${sink.server.origin}/ping/secret`;
    const got: unknown[] = [await heartbeat(url)];
    sink.set({ status: 503, body: "" });
    got.push(await heartbeat(url));
    sink.set({ status: 200, body: "", delayMs: 400 });
    got.push(await heartbeat(url, 100));
    const gone = new FakeServer({});
    const closed = `${gone.origin}/ping/secret`;
    gone.stop();
    got.push(await heartbeat(closed));
    expect([got, elsewhere.server.seen.length]).toEqual([[302, 503, "timeout", "no_answer"], 0]);
  });
});

describe("what leaves the watch", () => {
  test("no log line names a URL or a key", async () => {
    const sink = sinkServer({ status: 503, body: `{"echo":"${KEY}"}` });
    const log: string[] = [];
    const box = outbox(sink.server, {}, log);
    box.add(Array.from({ length: OUTBOX_MAX + 1 }, () => R03));
    await box.flush(initialState());
    expect(log.length).toBe(2);
    for (const line of log) expect([line.includes(KEY), line.includes(sink.server.origin), line.includes("http")]).toEqual([false, false, false]);
  });
});
