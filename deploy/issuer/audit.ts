// The issuer's audit trail (public-beta issuer triage B2.8; design §9).
//
// - Every change the issuer makes to the state, and every refusal of a
//   management call, is one JSON object under `audit/YYYY/MM/DD/` in the
//   control bucket (StateStore.putAudit, created once, never replaced):
//   when, who, what, the ids and versions before and after, the outcome.
// - Exchanges are not recorded one by one. Per credential, the counts by
//   outcome are logged to stdout every minute, and every hour one object
//   holds the hour's counts and the jti of every token minted (for
//   incident lookup; the cell publishes no jti list, MULTITENANCY §4.4).
//
// No record holds a secret, a token, a key or a pepper. The audit prefix
// is writable by anything that holds the control bucket's writer key, the
// issuer included (skeptic S2): it is a record, not tamper evidence.
// Retention (13 months) is a lifecycle rule on the prefix if Prisma
// Buckets offer one, else a sweep (not built: nothing ages out before
// 2027-11).
import { auditKey, type StateStore } from "../cell-admin/store";

/// A signed-in user acting through the management API, or a process:
/// `cli:<host>` for the owner's cell-admin, `issuer:<task>` for the
/// issuer's own jobs.
export type Actor = { user_id: string; email: string; workspace_id: string } | string;

export interface Versions {
  feed_version?: number;
  project_policy_version?: number;
  grant_version?: number;
}

export interface AuditEvent {
  actor: Actor;
  action: string;
  outcome: "done" | "refused";
  reason?: string;
  project_id?: string;
  credential_ids?: string[];
  before?: Versions;
  after?: Versions;
  request_id?: string;
  /// Only once the edge's client-IP header is known (K4).
  source_ip?: string;
}

export type ExchangeOutcome =
  | "issued"
  | "invalid_request"
  | "invalid_credential"
  | "credential_not_active"
  | "project_not_active"
  | "rate_limited"
  | "issuer_unavailable";

/// Where an exchange is counted: the credential, when the state holds the
/// id it named; else one bucket for every id it does not know, so a
/// client spraying ids cannot grow the counts.
export const UNKNOWN_CREDENTIAL = "(unknown)";

/// A credential's successful mints an hour keeps the jti of.
export const JTIS_PER_HOUR = 10_000;

const MINUTE_MS = 60_000;
const HOUR_MS = 3_600_000;

interface Counts {
  project_id?: string;
  outcomes: Partial<Record<ExchangeOutcome, number>>;
}

interface HourCounts extends Counts {
  jtis: string[];
  jtis_dropped: number;
}

export interface AuditOptions {
  now?: () => number;
  /// One line of stdout.
  log?: (line: string) => void;
}

export class Audit {
  /// Exchanges by outcome since boot, for the metrics route.
  readonly totals: Record<ExchangeOutcome, number> = {
    issued: 0,
    invalid_request: 0,
    invalid_credential: 0,
    credential_not_active: 0,
    project_not_active: 0,
    rate_limited: 0,
    issuer_unavailable: 0,
  };
  private readonly now: () => number;
  private readonly log: (line: string) => void;
  private minute: number;
  private hour: number;
  private minuteCounts = new Map<string, Counts>();
  private hourCounts = new Map<string, HourCounts>();
  /// Hourly objects being written, so a stop can wait for them.
  private writing = new Set<Promise<void>>();

  constructor(
    private readonly store: StateStore,
    private readonly cellId: string,
    opts: AuditOptions = {},
  ) {
    this.now = opts.now ?? Date.now;
    this.log = opts.log ?? console.log;
    this.minute = Math.floor(this.now() / MINUTE_MS);
    this.hour = Math.floor(this.now() / HOUR_MS);
  }

  /// One audit object for a change or a refused management call.
  async record(e: AuditEvent): Promise<void> {
    const at = new Date(this.now());
    await this.store.putAudit(auditKey(at), { kind: "management", at: at.toISOString(), cell_id: this.cellId, ...e });
  }

  /// Count one exchange under `credential` (UNKNOWN_CREDENTIAL when the
  /// state does not hold the id), with the jti of a minted token.
  exchange(outcome: ExchangeOutcome, credential: string, projectId?: string, jti?: string): void {
    this.tick();
    this.totals[outcome] += 1;
    const m = entry<Counts>(this.minuteCounts, credential, projectId, () => ({ outcomes: {} }));
    m.outcomes[outcome] = (m.outcomes[outcome] ?? 0) + 1;
    const h = entry<HourCounts>(this.hourCounts, credential, projectId, () => ({ outcomes: {}, jtis: [], jtis_dropped: 0 }));
    h.outcomes[outcome] = (h.outcomes[outcome] ?? 0) + 1;
    if (jti === undefined) return;
    if (h.jtis.length < JTIS_PER_HOUR) h.jtis.push(jti);
    else h.jtis_dropped += 1;
  }

  /// Close the minute and the hour that have ended: the minute's counts to
  /// stdout, the hour's to one audit object.
  tick(): void {
    const minute = Math.floor(this.now() / MINUTE_MS);
    if (minute !== this.minute) this.closeMinute(minute);
    const hour = Math.floor(this.now() / HOUR_MS);
    if (hour !== this.hour) this.closeHour(hour);
  }

  private closeMinute(next: number): void {
    const at = new Date(this.minute * MINUTE_MS).toISOString();
    for (const [credential_id, c] of this.minuteCounts) {
      this.log(JSON.stringify({ kind: "exchanges", minute: at, cell_id: this.cellId, credential_id, ...c }));
    }
    this.minuteCounts = new Map();
    this.minute = next;
  }

  private closeHour(next: number): void {
    if (this.hourCounts.size > 0) {
      const doc = {
        kind: "exchanges_hour",
        hour: new Date(this.hour * HOUR_MS).toISOString(),
        cell_id: this.cellId,
        credentials: Object.fromEntries(this.hourCounts),
      };
      const write = this.store
        .putAudit(auditKey(new Date(this.now())), doc)
        .catch((e) => this.log(`writing the exchanges of ${doc.hour} failed: ${(e as Error).message}`))
        .finally(() => this.writing.delete(write));
      this.writing.add(write);
    }
    this.hourCounts = new Map();
    this.hour = next;
  }

  /// Close the open minute and hour now (at shutdown), and wait for every
  /// hourly object being written.
  async flush(): Promise<void> {
    this.closeMinute(this.minute);
    this.closeHour(this.hour);
    await Promise.all([...this.writing]);
  }

  /// Tick every `everyMs` (10 s), so a quiet minute is still closed on time.
  start(everyMs = 10_000): () => void {
    const timer = setInterval(() => this.tick(), everyMs);
    return () => clearInterval(timer);
  }
}

/// `key`'s counts in `counts`, made on first use.
function entry<T extends Counts>(counts: Map<string, T>, key: string, projectId: string | undefined, make: () => T): T {
  let c = counts.get(key);
  if (c === undefined) {
    c = make();
    if (projectId !== undefined) c.project_id = projectId;
    counts.set(key, c);
  }
  return c;
}
