// The cell watch's inputs (public-beta alerting triage B4): each read under
// its platform name into the watch's configuration, `none` marking an
// optional one absent, and a missing or malformed one refused by its name
// alone, never its value. Run with `bun test ./deploy/cell-watch`.
import { describe, expect, test } from "bun:test";
import { type InputName, INPUTS, InputError, type Inputs, PARAM_INPUTS, readWatchConfig, SECRET_INPUTS } from "./config";
import { DEFAULT_PRICES, rulesConfig } from "./rules";

/// A value no message may quote: every input carries it.
const MARK = "zq7mark";

const FULL: Record<InputName, string> = {
  STREAMS_OPERATOR_TOKEN: `operator-bearer-${MARK}`,
  ALERT_SINK_URL: `https://events.example.com/v2/enqueue?${MARK}`,
  ALERT_SINK_KEY: `routing-key-${MARK}`,
  HEARTBEAT_URL: `https://beats.example.com/ping/${MARK}`,
  STREAMS_ISSUER_METRICS_TOKEN: `issuer-metrics-${MARK}`,
  STREAMS_CELL_ORIGIN: "https://cell.example.com",
  STREAMS_CELL_ID: "cell_sc1",
  ALERT_SINK_KIND: "events-v2",
  EXPECTED_GIT: "0123456789abcdef0123456789abcdef01234567",
  NOTIFY_ONLY: "1",
  ISSUER_ORIGIN: "https://issuer.example.com",
};

/// FULL with the issuer, the expected build and the sink key off.
const BARE: Record<InputName, string> = {
  ...FULL,
  ALERT_SINK_KIND: "webhook",
  ALERT_SINK_KEY: "none",
  STREAMS_ISSUER_METRICS_TOKEN: "none",
  EXPECTED_GIT: "none",
  NOTIFY_ONLY: "0",
  ISSUER_ORIGIN: "none",
};

/// The refusal `inputs` meets, or null when they are read.
function refusal(inputs: Inputs): string | null {
  try {
    readWatchConfig(inputs);
    return null;
  } catch (e) {
    if (!(e instanceof InputError)) throw e;
    return e.message;
  }
}

describe("the cell watch's inputs", () => {
  test("are the five secrets and the six settings the triage names, each once", () => {
    expect([...SECRET_INPUTS]).toEqual([
      "STREAMS_OPERATOR_TOKEN", "ALERT_SINK_URL", "ALERT_SINK_KEY", "HEARTBEAT_URL", "STREAMS_ISSUER_METRICS_TOKEN",
    ]);
    expect([...PARAM_INPUTS]).toEqual([
      "STREAMS_CELL_ORIGIN", "STREAMS_CELL_ID", "ALERT_SINK_KIND", "EXPECTED_GIT", "NOTIFY_ONLY", "ISSUER_ORIGIN",
    ]);
    expect([...INPUTS]).toEqual([...SECRET_INPUTS, ...PARAM_INPUTS]);
    expect(Object.keys(FULL).sort()).toEqual([...INPUTS].sort());
  });

  test("become the watch's configuration: the rules', the poller's, the sink's and the heartbeat's", () => {
    expect(readWatchConfig(FULL)).toEqual({
      rules: rulesConfig({
        cellId: "cell_sc1",
        cellOrigin: "https://cell.example.com",
        expectedGit: "0123456789abcdef0123456789abcdef01234567",
        notifyOnly: true,
        issuer: true,
      }),
      poll: {
        cellOrigin: "https://cell.example.com",
        operatorToken: `operator-bearer-${MARK}`,
        issuerOrigin: "https://issuer.example.com",
        issuerMetricsToken: `issuer-metrics-${MARK}`,
        canary: null,
      },
      sink: { kind: "events-v2", url: `https://events.example.com/v2/enqueue?${MARK}`, key: `routing-key-${MARK}` },
      heartbeatUrl: `https://beats.example.com/ping/${MARK}`,
    });
    // The prices, the budget and G stay the rules' defaults: no input names them.
    expect(readWatchConfig(FULL).rules.prices).toEqual(DEFAULT_PRICES);
  });

  test("take none for an optional input: no issuer and no metrics token, no expected build, a webhook with no bearer", () => {
    const cfg = readWatchConfig(BARE);
    expect([cfg.rules.issuer, cfg.rules.expectedGit, cfg.rules.notifyOnly]).toEqual([false, "", false]);
    expect([cfg.poll.issuerOrigin, cfg.poll.issuerMetricsToken]).toEqual([null, null]);
    expect(cfg.sink).toEqual({ kind: "webhook", url: FULL.ALERT_SINK_URL, key: "" });
    // A webhook may still carry a bearer.
    expect(readWatchConfig({ ...BARE, ALERT_SINK_KEY: "webhook-bearer" }).sink.key).toBe("webhook-bearer");
  });

  test("normalise an origin and accept http only to this machine", () => {
    const cfg = readWatchConfig({ ...FULL, STREAMS_CELL_ORIGIN: "https://Cell.Example.com:443/", ISSUER_ORIGIN: "http://127.0.0.1:8091" });
    expect([cfg.rules.cellOrigin, cfg.poll.cellOrigin, cfg.poll.issuerOrigin]).toEqual([
      "https://cell.example.com", "https://cell.example.com", "http://127.0.0.1:8091",
    ]);
    for (const local of ["http://localhost:8080", "http://[::1]:9", "http://127.0.0.1:1"]) {
      expect(refusal({ ...FULL, STREAMS_CELL_ORIGIN: local, ALERT_SINK_URL: `${local}/sink`, HEARTBEAT_URL: `${local}/beat` })).toBeNull();
    }
  });

  test("refuse a missing or empty input, and none for a required one, by its name", () => {
    const required = ["STREAMS_OPERATOR_TOKEN", "ALERT_SINK_URL", "HEARTBEAT_URL", "STREAMS_CELL_ORIGIN", "STREAMS_CELL_ID",
      "ALERT_SINK_KIND", "NOTIFY_ONLY"];
    for (const name of INPUTS) {
      const { [name]: _, ...rest } = FULL;
      expect([name, refusal(rest)]).toEqual([name, `${name} is required`]);
      expect([name, refusal({ ...FULL, [name]: "" })]).toEqual([name, `${name} is required`]);
      const none = refusal({ ...FULL, [name]: "none" });
      if (required.includes(name)) expect([name, none]).toEqual([name, `${name} is required; none is for an optional input`]);
    }
  });

  test("refuse a malformed value by the input's name, without quoting it", () => {
    const cases: [Partial<Record<InputName, string>>, string][] = [
      [{ STREAMS_CELL_ID: `cell/${MARK}` }, "STREAMS_CELL_ID is not a valid id (^[A-Za-z0-9_-]{1,128}$)"],
      [{ STREAMS_CELL_ORIGIN: `cell-${MARK}` }, "STREAMS_CELL_ORIGIN is not a URL"],
      [{ STREAMS_CELL_ORIGIN: `ftp://${MARK}.example.com` }, "STREAMS_CELL_ORIGIN must be https (http only to this machine)"],
      [{ STREAMS_CELL_ORIGIN: `http://${MARK}.example.com` }, "STREAMS_CELL_ORIGIN must be https (http only to this machine)"],
      [{ STREAMS_CELL_ORIGIN: `https://cell.example.com/${MARK}` }, "STREAMS_CELL_ORIGIN must be an origin, without a path"],
      [{ STREAMS_CELL_ORIGIN: `https://cell.example.com?${MARK}` }, "STREAMS_CELL_ORIGIN must be an origin, without a path"],
      [{ ISSUER_ORIGIN: `https://issuer.example.com/${MARK}` }, "ISSUER_ORIGIN must be an origin, without a path"],
      [{ ALERT_SINK_URL: `http://${MARK}.example.com/v2/enqueue` }, "ALERT_SINK_URL must be https (http only to this machine)"],
      [{ ALERT_SINK_URL: `sink ${MARK}` }, "ALERT_SINK_URL is not a URL"],
      [{ HEARTBEAT_URL: `http://beats.example.com/${MARK}` }, "HEARTBEAT_URL must be https (http only to this machine)"],
      [{ ALERT_SINK_KIND: `pagerduty-${MARK}` }, "ALERT_SINK_KIND is events-v2 or webhook"],
      [{ NOTIFY_ONLY: `yes-${MARK}` }, "NOTIFY_ONLY is 0 or 1"],
      [{ EXPECTED_GIT: `main-${MARK}` }, "EXPECTED_GIT is a commit (7 to 40 lowercase hex digits) or none"],
      [{ EXPECTED_GIT: "0123ABC" }, "EXPECTED_GIT is a commit (7 to 40 lowercase hex digits) or none"],
      [{ EXPECTED_GIT: "012345" }, "EXPECTED_GIT is a commit (7 to 40 lowercase hex digits) or none"],
      [{ ALERT_SINK_KEY: "none" }, "ALERT_SINK_KEY is the events-v2 routing key; none is for a webhook only"],
      [{ STREAMS_ISSUER_METRICS_TOKEN: "none" }, "STREAMS_ISSUER_METRICS_TOKEN is required when ISSUER_ORIGIN is set"],
      [{ ISSUER_ORIGIN: "none" }, "STREAMS_ISSUER_METRICS_TOKEN is set but ISSUER_ORIGIN is none"],
    ];
    for (const [patch, message] of cases) {
      expect(refusal({ ...FULL, ...patch })).toBe(message);
      expect(message).not.toContain(MARK);
    }
    // A short commit is accepted (row 5 compares a prefix).
    expect(readWatchConfig({ ...FULL, EXPECTED_GIT: "0123abc" }).rules.expectedGit).toBe("0123abc");
  });

  test("read no name but their own", () => {
    const laced = { ...BARE, NOTIFY_ONLY_EXTRA: "1", ISSUER_ORIGIN_OVERRIDE: "https://other.example.com", PORT: "1" };
    expect(readWatchConfig(laced)).toEqual(readWatchConfig(BARE));
  });
});
