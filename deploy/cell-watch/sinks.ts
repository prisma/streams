// The cell watch's sinks (public-beta alerting triage B3; D7, D8): the
// events rules.ts raises go to the one configured events sink, in one of
// two shapes, and the watch sends a heartbeat GET after each cycle.
//
//   events-v2  PagerDuty's Events API v2: POST {routing_key, event_action
//              trigger|resolve, dedup_key, payload} with ALERT_SINK_KEY as
//              the routing key; PAGE is severity critical, NOTIFY warning.
//   webhook    a generic JSON webhook: POST the event's fields and a `text`
//              line, with ALERT_SINK_KEY, when set, as the bearer.
//
// Alert keys carry the class (`<cell>/<rule>/page`, `<cell>/<rule>/notify`):
// a rule that opens at NOTIFY and rises to PAGE then opens a second alert,
// which pages, where a trigger on the first key would only be folded into
// the open low-urgency alert. A resolve closes every key its rule was
// triggered on (the open keys are kept in memory, like the rules' state).
//
// Delivery: the events of a cycle join the queue in order and the queue is
// delivered at once, front first. A delivery that fails (no 2xx within 5 s;
// a redirect is a failure and is not followed) stays at the front with
// everything behind it, and the next cycle retries from it: a resolve never
// overtakes its trigger, and a dead sink costs one timeout a cycle. A cycle
// that ends with a trigger still queued is one failed trigger delivery
// (rules.ts `delivered`): after three in a row the heartbeat is withheld
// (D7), so the dead-man (row 2) pages through the paging tool's other
// path. The queue keeps the newest OUTBOX_MAX deliveries.
//
// Only the configured URLs are sent anything. A body carries the event's
// allowlisted fields (rules.ts) and, for Events v2, the routing key; what a
// sink or the heartbeat monitor answers is read for its status only. Log
// lines name an alert key and a status, never a URL or a key.
import { type Class, delivered, type WatchEvent, type WatchState } from "./rules";

export const SEND_TIMEOUT_MS = 5_000;
/// Deliveries kept for the next cycle; beyond it the oldest are dropped.
export const OUTBOX_MAX = 256;

export interface SinkConfig {
  kind: "events-v2" | "webhook";
  /// ALERT_SINK_URL: every delivery is POSTed here and nowhere else.
  url: string;
  /// ALERT_SINK_KEY: the routing key (events-v2), or the webhook's bearer
  /// ("" sends none).
  key: string;
  timeoutMs?: number;
}

/// What one request came to: delivered, the status of any other answer,
/// the timeout, or no answer at all (a refused or broken connection).
export type Outcome = "ok" | number | "timeout" | "no_answer";

/// One alert on the pager: an event on its class's key.
interface Delivery {
  event: WatchEvent;
  key: string;
  class: Class;
}

/// The pager's key for a rule's alert at one class.
export const alertKey = (dedupKey: string, cls: Class): string => `${dedupKey}/${cls.toLowerCase()}`;

/// The line a person reads: the class (or "resolved" and the class), the
/// event's dedup key, then its enums and numbers.
function summary(d: Delivery): string {
  const e = d.event;
  const head = e.kind === "trigger" ? `[${d.class}]` : `[resolved ${d.class}]`;
  const facts = [...Object.entries(e.labels), ...Object.entries(e.values)].map(([k, v]) => `${k}=${v}`);
  return [head, e.dedupKey, ...facts].join(" ");
}

/// The cell id: the event's dedup key is `<cell>/<rule>`.
const cellOf = (e: WatchEvent): string => e.dedupKey.slice(0, e.dedupKey.length - e.rule.length - 1);

function eventsV2(key: string, d: Delivery): unknown {
  const e = d.event;
  if (e.kind === "resolve") return { routing_key: key, event_action: "resolve", dedup_key: d.key };
  return {
    routing_key: key,
    event_action: "trigger",
    dedup_key: d.key,
    payload: {
      summary: summary(d),
      source: cellOf(e),
      severity: d.class === "PAGE" ? "critical" : "warning",
      timestamp: new Date(e.at).toISOString(),
      class: e.rule,
      custom_details: { values: e.values, labels: e.labels, runbook: e.runbookAnchor },
    },
  };
}

function webhook(d: Delivery): unknown {
  const e = d.event;
  return {
    text: summary(d),
    kind: e.kind,
    dedupKey: d.key,
    class: d.class,
    rule: e.rule,
    values: e.values,
    labels: e.labels,
    at: e.at,
    runbookAnchor: e.runbookAnchor,
  };
}

/// One request under the timeout, no redirect followed; only the answer's
/// status is read.
async function call(url: string, init: RequestInit, timeoutMs: number): Promise<Outcome> {
  const signal = AbortSignal.timeout(timeoutMs);
  try {
    const res = await fetch(url, { ...init, signal, redirect: "manual" });
    return res.ok ? "ok" : res.status;
  } catch {
    return signal.aborted ? "timeout" : "no_answer";
  }
}

const said = (o: Outcome): string => (typeof o === "number" ? `status ${o}` : o);

/// The heartbeat: one GET of HEARTBEAT_URL.
export function heartbeat(url: string, timeoutMs = SEND_TIMEOUT_MS): Promise<Outcome> {
  return call(url, { method: "GET" }, timeoutMs);
}

export class Outbox {
  private queue: Delivery[] = [];
  /// The classes each rule's open alert was triggered at, by dedup key.
  private readonly open = new Map<string, Set<Class>>();

  constructor(
    private readonly sink: SinkConfig,
    private readonly log: (line: string) => void = console.log,
  ) {}

  /// Deliveries waiting.
  get size(): number {
    return this.queue.length;
  }

  /// Queues the events in order, each as the deliveries its alert keys take.
  add(events: readonly WatchEvent[]): void {
    for (const e of events) {
      if (e.kind === "trigger") {
        const classes = this.open.get(e.dedupKey) ?? new Set<Class>();
        this.open.set(e.dedupKey, classes.add(e.class));
        this.queue.push({ event: e, key: alertKey(e.dedupKey, e.class), class: e.class });
        continue;
      }
      const classes = this.open.get(e.dedupKey) ?? new Set([e.class]);
      this.open.delete(e.dedupKey);
      for (const cls of classes) this.queue.push({ event: e, key: alertKey(e.dedupKey, cls), class: cls });
    }
    const over = this.queue.length - OUTBOX_MAX;
    if (over > 0) {
      this.queue.splice(0, over);
      this.log(`cell-watch: outbox full: dropped the ${over} oldest deliveries`);
    }
  }

  /// Delivers the queue front first until one fails; `state` with D7's count.
  async flush(state: WatchState): Promise<WatchState> {
    let next = state;
    for (let d = this.queue[0]; d !== undefined; d = this.queue[0]) {
      const outcome = await this.send(d);
      if (outcome !== "ok") {
        this.log(`cell-watch: delivery of ${d.key} (${d.event.kind}) failed: ${said(outcome)}; ${this.queue.length} queued`);
        break;
      }
      this.queue.shift();
      next = delivered(next, d.event, true);
    }
    const stuck = this.queue.find((d) => d.event.kind === "trigger");
    return stuck === undefined ? next : delivered(next, stuck.event, false);
  }

  private send(d: Delivery): Promise<Outcome> {
    const { kind, url, key } = this.sink;
    const headers: Record<string, string> = { "content-type": "application/json" };
    if (kind === "webhook" && key !== "") headers.authorization = `Bearer ${key}`;
    const body = JSON.stringify(kind === "events-v2" ? eventsV2(key, d) : webhook(d));
    return call(url, { method: "POST", headers, body }, this.sink.timeoutMs ?? SEND_TIMEOUT_MS);
  }
}
