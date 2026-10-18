// The issuer's rate limits (public-beta issuer triage B2.7; design §8):
// token buckets in memory, per instance (two instances during a deploy
// briefly allow twice as much, which is harmless), each taken before the
// work it protects. A refusal is 429 rate_limited with Retry-After, and
// counted per bucket for the metrics. No bucket a stranger can spend
// stands in front of a valid credential: the exchange's instance and
// per-credential budgets are taken only once a secret has verified (the
// HMAC costs microseconds), malformed requests and failed secrets each
// spend their own (exchange.ts).
//
// The per-source-IP buckets are built but wired only once Compute names
// the header its edge sets and strips (question K4): a header a client can
// forge would let it spend another client's bucket, or dodge its own. An
// address is charged only for exchanges that fail, never in front of a
// secret that verifies (adversarial pass B8, M2): many customers' apps
// share one address (Compute's egress, a NAT), and one of them failing
// must not lock the others out.

/// K4: the edge's trusted client-IP header, unknown until Compute answers.
export const TRUSTED_CLIENT_IP_HEADER: string | null = null;

export interface Limit {
  /// Tokens added per second, up to `burst`.
  perSec: number;
  burst: number;
  /// For a failure budget: once it is spent, refuse for this long.
  lockoutMs?: number;
}

const MINUTE = 60;
const DAY = 86_400;

export const LIMITS = {
  /// Tokens the whole instance mints (secrets that verified).
  exchange_global: { perSec: 200, burst: 200 },
  /// Tokens one credential gets: a fleet of 60 cold instances at once.
  exchange_credential: { perSec: 2, burst: 60 },
  /// Exchanges without a credential's form, the whole instance.
  exchange_malformed: { perSec: 100, burst: 100 },
  /// Wrong secrets presented for one credential id: once spent, failures
  /// for that id answer 429; a valid secret never takes from it.
  exchange_credential_failed: { perSec: 20 / MINUTE, burst: 20 },
  /// Management calls of one browser session.
  management_session: { perSec: 60 / MINUTE, burst: 60 },
  /// Sign-ins started, the whole instance.
  oauth_start: { perSec: 30 / MINUTE, burst: 30 },
  /// Projects one workspace creates.
  project_create_workspace: { perSec: 5 / DAY, burst: 5 },
  /// Credentials one project creates or rotates.
  credential_change_project: { perSec: 20 / DAY, burst: 20 },
  /// Per source IP (K4): failed exchanges, sign-ins started. An
  /// address's sign-ins are taken before the instance's and are a sixth
  /// of them, so one address cannot spend the instance's budget.
  exchange_failed_ip: { perSec: 20 / MINUTE, burst: 20, lockoutMs: 600_000 },
  oauth_start_ip: { perSec: 5 / MINUTE, burst: 5 },
} as const satisfies Record<string, Limit>;

export type LimitName = keyof typeof LIMITS;

/// At this many keys, a new key first scans the table: a bucket that has
/// refilled is forgotten (it is the same as a new one), then, while more
/// than KEEP_AFTER_PRUNE remain, the oldest (first seen first), a
/// locked-out one only once no other is left. Keys come from requests
/// (credential ids, IPs): a spray faster than any bucket refills leaves
/// nothing refilled, and the cut to KEEP_AFTER_PRUNE keeps the table
/// bounded and its scans to one per 5,000 new keys (adversarial pass B8,
/// H1). A forgotten key's budget starts full again.
const PRUNE_AT = 10_000;
const KEEP_AFTER_PRUNE = 5_000;

interface Bucket {
  tokens: number;
  at: number;
  lockedUntil: number;
}

class Buckets {
  private readonly buckets = new Map<string, Bucket>();
  /// Scans of the whole table since boot.
  scans = 0;

  constructor(private readonly limit: Limit) {}

  get size(): number {
    return this.buckets.size;
  }

  /// `key`'s bucket refilled to `now`.
  private bucket(key: string, now: number): Bucket {
    let b = this.buckets.get(key);
    if (b === undefined) {
      if (this.buckets.size >= PRUNE_AT) this.prune(now);
      b = { tokens: this.limit.burst, at: now, lockedUntil: 0 };
      this.buckets.set(key, b);
    }
    b.tokens = Math.min(this.limit.burst, b.tokens + (Math.max(0, now - b.at) / 1000) * this.limit.perSec);
    b.at = now;
    return b;
  }

  private prune(now: number): void {
    this.scans += 1;
    for (const [key, b] of this.buckets) {
      const full = b.tokens + (Math.max(0, now - b.at) / 1000) * this.limit.perSec >= this.limit.burst;
      if (full && b.lockedUntil <= now) this.buckets.delete(key);
    }
    for (const lockedToo of [false, true]) {
      for (const [key, b] of this.buckets) {
        if (this.buckets.size <= KEEP_AFTER_PRUNE) return;
        if (lockedToo || b.lockedUntil <= now) this.buckets.delete(key);
      }
    }
  }

  private wait(b: Bucket): number {
    return Math.max(1, Math.ceil((1 - b.tokens) / this.limit.perSec));
  }

  /// Take one token: 0, or the seconds until one is there.
  take(key: string, now: number): number {
    const b = this.bucket(key, now);
    if (b.tokens < 1) return this.wait(b);
    b.tokens -= 1;
    return 0;
  }

  /// A failure budget: the seconds `key` stays refused, or 0.
  locked(key: string, now: number): number {
    const b = this.buckets.get(key);
    return b && b.lockedUntil > now ? Math.ceil((b.lockedUntil - now) / 1000) : 0;
  }

  /// Spend one failure; the last one locks `key` for lockoutMs.
  charge(key: string, now: number): void {
    const b = this.bucket(key, now);
    b.tokens = Math.max(0, b.tokens - 1);
    if (b.tokens < 1) b.lockedUntil = now + (this.limit.lockoutMs ?? 0);
  }
}

export class RateLimits {
  /// Refusals per bucket since boot.
  readonly refused = Object.fromEntries(Object.keys(LIMITS).map((n) => [n, 0])) as Record<LimitName, number>;
  private readonly buckets = Object.fromEntries(
    Object.entries(LIMITS).map(([n, l]) => [n, new Buckets(l)]),
  ) as Record<LimitName, Buckets>;

  constructor(private readonly now: () => number = Date.now) {}

  /// Take one from `name`'s bucket for `key`: 0, or Retry-After seconds.
  take(name: LimitName, key: string): number {
    const wait = this.buckets[name].take(key, this.now());
    if (wait > 0) this.refused[name] += 1;
    return wait;
  }

  /// The seconds `key` stays locked out of `name`, a failure budget.
  locked(name: LimitName, key: string): number {
    const wait = this.buckets[name].locked(key, this.now());
    if (wait > 0) this.refused[name] += 1;
    return wait;
  }

  charge(name: LimitName, key: string): void {
    this.buckets[name].charge(key, this.now());
  }

  /// Keys `name` remembers (pruning keeps it bounded).
  keys(name: LimitName): number {
    return this.buckets[name].size;
  }

  /// How often `name`'s table has been scanned for keys to forget.
  scans(name: LimitName): number {
    return this.buckets[name].scans;
  }
}

/// The client's address from the edge's trusted header, when K4 names one.
export function clientIp(req: Request, header: string | null = TRUSTED_CLIENT_IP_HEADER): string | null {
  if (header === null) return null;
  return req.headers.get(header)?.trim() || null;
}
