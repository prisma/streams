/**
 * The client's multiplexed wait (`POST /v1/projects/{p}/live:wait`, layout
 * 6A package 2): every `subscribe()` tail, consumer iterator and watch
 * subscription of one client waits through one long-poll, not one each.
 *
 * A subscription takes part in a request only while its iterator waits for
 * its next answer (`next()`), so an iterator still processing what it was
 * given is never answered again under it, and its cursor is the one it
 * passed. Every answer ends its request; the subscriptions it did not
 * answer join the next one at once.
 *
 * A request that carries a consumer group is never aborted: an answer lost
 * after the server leased messages for it would hold them until their
 * visibility window passes. A request without one is aborted and sent
 * again whenever a subscription joins or leaves, so a new subscription
 * waits at once. A subscription that joins while a group's request is in
 * flight waits in a second request that ends when that one does, and the
 * two merge at the next answer.
 *
 * The server keeps nothing between requests, so a restart or a lost
 * connection costs one request; transport failures and retryable refusals
 * reconnect with backoff. A cell without the route (`404`/`405`) makes the
 * multiplexer unavailable for the client's life: every waiting
 * subscription is told so and continues with its per-stream long-poll.
 */

/** A subscription as the request names it. */
export interface MuxSpec {
  kind: "tail" | "group" | "watch";
  stream: string;
  key: string;
  cursor?: string;
  routingKey?: string;
  group?: string;
  max?: number;
  visibilityMs?: number;
  watch?: string;
  watchKey?: string;
}

/** One answered subscription, as the server renders it. */
export interface MuxEntry {
  id: string;
  records?: unknown[];
  cursor?: string;
  upToDate?: boolean;
  sealed?: boolean;
  backlog?: number;
  messages?: unknown[];
  event?: unknown;
  status?: number;
  error?: { code: string; message: string; retryable: boolean; details?: unknown };
}

/** How one request ended, as the transport classifies it. */
export type MuxFailure =
  | { kind: "unsupported" }
  | { kind: "retry" }
  | { kind: "fatal"; error: unknown };

/** The transport the client gives the multiplexer: one request with the
 * client's credentials, and how its failures read. */
export interface MuxTransport {
  send(body: string, signal: AbortSignal): Promise<{ results: MuxEntry[] }>;
  classify(error: unknown): MuxFailure;
}

/** Thrown to a waiting subscription when the cell has no multiplexed wait:
 * it continues with its per-stream long-poll. */
export class MuxUnavailable extends Error {
  constructor() {
    super("the cell does not serve the multiplexed wait");
    this.name = "MuxUnavailable";
  }
}

/** The longest wait, as the server caps it. */
const WAIT_MS = 25_000;
/** How long joins gather before a request in flight is ended and sent
 * again with them, so subscriptions opened one after another (a client
 * starting) cost one re-send, not one each. */
const JOIN_DEBOUNCE_MS = 20;
/** The most subscriptions one request carries, as the server caps it. */
const MAX_SUBS = 256;
/** The shortest wait a second request takes beside a group's: it ends
 * with that one, but never sooner than this, so a group's answer that
 * arrives after its client-side deadline does not make a burst of
 * zero-length waits. */
const MIN_SIDE_WAIT_MS = 1000;

interface Waiter {
  resolve(entry: MuxEntry): void;
  reject(error: unknown): void;
}

interface Flight {
  subs: Set<MuxSub>;
  controller: AbortController;
  groups: boolean;
  deadline: number;
}

export class LiveMux {
  private readonly subs = new Set<MuxSub>();
  private readonly flights = new Set<Flight>();
  private scheduled = false;
  private backoffMs = 1000;
  private backoffUntil = 0;
  private nextId = 0;
  /** Set once a cell answered that it has no multiplexed wait. */
  unavailable = false;

  constructor(private readonly transport: MuxTransport) {}

  /** A new subscription; it waits only when its `next()` is called. */
  open(spec: MuxSpec): MuxSub {
    const sub = new MuxSub(this, `s${this.nextId++}`, spec);
    this.subs.add(sub);
    return sub;
  }

  /** `sub` leaves: a request without a group that carries it is sent again
   * without it. */
  close(sub: MuxSub): void {
    this.subs.delete(sub);
    for (const flight of this.flights) {
      if (flight.subs.has(sub) && !flight.groups) flight.controller.abort();
    }
    this.schedule();
  }

  /** Plan a request for every waiting subscription not in one yet, once
   * the current task has registered whatever else it opens (and, with a
   * request in flight, once joins have gathered for a moment). */
  schedule(): void {
    if (this.scheduled) return;
    this.scheduled = true;
    const gather = this.flights.size > 0 ? JOIN_DEBOUNCE_MS : 0;
    setTimeout(() => {
      this.scheduled = false;
      this.dispatch();
    }, Math.max(gather, this.backoffUntil - Date.now()));
  }

  private inFlight(sub: MuxSub): boolean {
    for (const flight of this.flights) if (flight.subs.has(sub)) return true;
    return false;
  }

  private dispatch(): void {
    if (this.unavailable) return;
    if (Date.now() < this.backoffUntil) {
      this.schedule();
      return;
    }
    const pending = [...this.subs].filter((s) => s.waiting() && !this.inFlight(s));
    if (pending.length === 0) return;
    // Merge: a request that leases nothing is ended and sent again with
    // the newcomers.
    for (const flight of [...this.flights]) {
      if (flight.groups) continue;
      this.flights.delete(flight);
      flight.controller.abort();
      pending.push(...[...flight.subs].filter((s) => this.subs.has(s) && s.waiting()));
    }
    const groupDeadline = Math.max(0, ...[...this.flights].map((f) => f.deadline));
    const waitMs = groupDeadline > 0
      ? Math.max(MIN_SIDE_WAIT_MS, Math.min(WAIT_MS, groupDeadline - Date.now()))
      : WAIT_MS;
    for (let i = 0; i < pending.length; i += MAX_SUBS) {
      void this.run(new Set(pending.slice(i, i + MAX_SUBS)), waitMs);
    }
  }

  private async run(subs: Set<MuxSub>, waitMs: number): Promise<void> {
    const flight: Flight = {
      subs,
      controller: new AbortController(),
      groups: [...subs].some((s) => s.spec.kind === "group"),
      deadline: Date.now() + waitMs,
    };
    this.flights.add(flight);
    const body = JSON.stringify({
      waitMs,
      subs: [...subs].map((s) => ({ id: s.id, ...s.spec })),
    });
    let results: MuxEntry[] | undefined;
    let failure: MuxFailure | undefined;
    try {
      results = (await this.transport.send(body, flight.controller.signal)).results;
    } catch (error) {
      failure = flight.controller.signal.aborted ? undefined : this.transport.classify(error);
    }
    this.flights.delete(flight);
    // An aborted request's subscriptions already wait in its successor: an
    // answer that raced the abort is dropped (it leased nothing).
    if (flight.controller.signal.aborted) results = undefined;
    if (results !== undefined) {
      this.backoffMs = 1000;
      const byId = new Map(results.map((entry) => [entry.id, entry]));
      for (const sub of subs) {
        const entry = byId.get(sub.id);
        if (entry !== undefined) sub.answer(entry);
      }
    } else if (failure?.kind === "unsupported") {
      // Subscriptions in another request (a group's, never aborted) take
      // their answer and learn it at their next `next()`.
      this.unavailable = true;
      for (const sub of this.subs) if (!this.inFlight(sub)) sub.fail(new MuxUnavailable());
      return;
    } else if (failure?.kind === "fatal") {
      for (const sub of subs) sub.fail(failure.error);
    } else if (failure?.kind === "retry") {
      this.backoffUntil = Date.now() + this.backoffMs;
      this.backoffMs = Math.min(this.backoffMs * 2, 5000);
    }
    this.schedule();
  }
}

/** One subscription's place in the multiplexer. Its iterator sets the
 * spec's cursor before each `next()`. */
export class MuxSub {
  private waiter: Waiter | undefined;

  constructor(
    private readonly mux: LiveMux,
    readonly id: string,
    readonly spec: MuxSpec,
  ) {}

  /** Whether the iterator waits for an answer. */
  waiting(): boolean {
    return this.waiter !== undefined;
  }

  /** Wait for this subscription's next answer. Rejects with
   * `MuxUnavailable` when the cell has no multiplexed wait, with the
   * request's failure when it is not retryable, and with the signal's
   * reason when aborted (the subscription then leaves). */
  next(signal?: AbortSignal): Promise<MuxEntry> {
    if (this.mux.unavailable) return Promise.reject(new MuxUnavailable());
    return new Promise<MuxEntry>((resolve, reject) => {
      const abort = () => {
        this.waiter = undefined;
        this.mux.close(this);
        reject(signal?.reason);
      };
      if (signal?.aborted) return abort();
      signal?.addEventListener("abort", abort, { once: true });
      const done = () => signal?.removeEventListener("abort", abort);
      this.waiter = {
        resolve: (entry) => {
          done();
          resolve(entry);
        },
        reject: (error) => {
          done();
          reject(error);
        },
      };
      this.mux.schedule();
    });
  }

  /** Leave the multiplexer. */
  close(): void {
    this.waiter = undefined;
    this.mux.close(this);
  }

  answer(entry: MuxEntry): void {
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.resolve(entry);
  }

  fail(error: unknown): void {
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.reject(error);
  }
}
