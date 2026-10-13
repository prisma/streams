// The cell watch's loop (public-beta alerting triage B3): one cycle every
// 15 s, poll the cell and the issuer (poll.ts), step the rules (rules.ts),
// deliver the events (sinks.ts), then the heartbeat GET unless D7
// withholds it; and $PORT, which answers `ok` to `GET /` and nothing else
// (no rule state, no proxying).
//
// State lives in memory only. A restarted watch starts from rules.ts's
// initial state and re-triggers whatever still holds, on the same alert
// keys, which the pager folds into the alerts already open; a resolve
// closes both of its rule's keys (sinks.ts), so it also closes a key only
// the watch before the restart had opened.
//
// A cycle that throws (poll.ts answers every network failure as data, so
// this is a bug or an unusable input) sends no heartbeat: a watch that
// cannot complete its cycles goes quiet and the dead-man (row 2) pages.
// The log has one line per event (the allowlisted event itself) and one
// per failed delivery, failed or withheld heartbeat and failed cycle;
// none names a URL, a token or a body.
import { type PollConfig, POLL_MS, Poller } from "./poll";
import { heartbeatDue, initialState, type RulesConfig, step, type WatchEvent, type WatchState } from "./rules";
import { heartbeat, type Outcome, outcomeText, Outbox, type SinkConfig } from "./sinks";

export interface WatchConfig {
  rules: RulesConfig;
  poll: PollConfig;
  sink: SinkConfig;
  /// HEARTBEAT_URL.
  heartbeatUrl: string;
}

export interface CycleReport {
  /// The events this cycle's poll raised.
  events: WatchEvent[];
  /// Deliveries still queued once it ended.
  queued: number;
  heartbeat: Outcome | "withheld";
}

export class CellWatch {
  private state: WatchState = initialState();
  private readonly poller: Poller;
  private readonly outbox: Outbox;
  private readonly log: (line: string) => void;

  constructor(
    private readonly cfg: WatchConfig,
    deps: { now?: () => number; log?: (line: string) => void } = {},
  ) {
    this.log = deps.log ?? console.log;
    this.poller = new Poller(cfg.poll, deps.now);
    this.outbox = new Outbox(cfg.sink, this.log);
  }

  /// One cycle: poll, step, deliver, beat.
  async cycle(): Promise<CycleReport> {
    const poll = await this.poller.poll();
    const { state, events } = step(this.cfg.rules, this.state, poll);
    for (const e of events) this.log(`cell-watch: ${JSON.stringify(e)}`);
    this.outbox.add(events);
    this.state = await this.outbox.flush(state);
    const report = (beat: CycleReport["heartbeat"]) => ({ events, queued: this.outbox.size, heartbeat: beat });
    if (!heartbeatDue(this.state)) {
      this.log(`cell-watch: heartbeat withheld: ${this.state.failedTriggers} cycles in a row with a trigger undelivered`);
      return report("withheld");
    }
    const beat = await heartbeat(this.cfg.heartbeatUrl);
    if (beat !== "ok") this.log(`cell-watch: heartbeat failed: ${outcomeText(beat)}`);
    return report(beat);
  }

  /// Runs cycles one at a time, each `everyMs` after the one before began
  /// (at once when that one ran longer); a cycle that throws is logged by
  /// its message. The returned stop ends the loop once the cycle in flight
  /// has.
  start(everyMs = POLL_MS): () => Promise<void> {
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let inflight: Promise<void> = Promise.resolve();
    const run = (): void => {
      const began = Date.now();
      inflight = this.cycle()
        .then(
          () => undefined,
          (e: unknown) => this.log(`cell-watch: cycle failed: ${e instanceof Error ? e.message : "unknown"}`),
        )
        .then(() => {
          if (!stopped) timer = setTimeout(run, Math.max(0, began + everyMs - Date.now()));
        });
    };
    run();
    return async () => {
      stopped = true;
      clearTimeout(timer);
      await inflight;
    };
  }
}

/// $PORT: `ok` to `GET /`, a 404 with no body to anything else.
export function serveOk(port: number, hostname = "0.0.0.0"): ReturnType<typeof Bun.serve> {
  return Bun.serve({
    port,
    hostname,
    fetch: (req) =>
      req.method === "GET" && new URL(req.url).pathname === "/" ? new Response("ok") : new Response(null, { status: 404 }),
  });
}
