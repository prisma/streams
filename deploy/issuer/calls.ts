// What every management handler is given, and how it answers
// (management.ts routes the calls; projects.ts and credentials.ts handle
// them). A change the issuer makes, and every refusal of a change once
// the caller is signed in, is one audit object (audit.ts). A read is
// never audited, and neither is a refusal before the session or of its
// call budget: an unauthenticated caller must not be able to make the
// issuer write.
import { AdminError, StoreUnavailable } from "../cell-admin/errors";
import type { CellState } from "../cell-admin/state";
import type { AllowEntry, StateStore } from "../cell-admin/store";
import type { AllowlistReader } from "./allowlist";
import type { Audit, AuditEvent } from "./audit";
import type { StateCache } from "./cache";
import { UNAVAILABLE_RETRY_SECS } from "./exchange";
import type { RateLimits } from "./limits";
import { refuse } from "./respond";
import type { Session } from "./session";

export interface ManagementDeps {
  cache: StateCache;
  store: StateStore;
  limits: RateLimits;
  audit: Audit;
  allowlist: AllowlistReader;
  now: () => number;
  log: (line: string) => void;
}

/// A signed-in caller asking for `action`.
export interface Asker {
  session: Session;
  requestId: string;
  /// What the route does, as the audit names it (`project.create`).
  action: string;
  /// Whether the call changes anything (every method but GET).
  change: boolean;
}

/// A call that passed every check management.ts makes.
export interface Call extends Asker {
  req: Request;
  /// The path's ids, decoded.
  ids: string[];
}

/// A call that gives the workspace something new (a project, a credential
/// or a secret), which only a workspace on the allow-list makes.
export interface GrowthCall extends Call {
  /// The session's workspace's allow-list entry.
  entry: AllowEntry;
}

export const actorOf = (s: Session) => ({ user_id: s.user_id, email: s.email, workspace_id: s.workspace_id });

/// Write one audit object for `asker`; a failure is logged, never
/// answered: the change it records is already made.
export async function audited(
  deps: ManagementDeps,
  asker: Asker,
  e: Omit<AuditEvent, "actor" | "action" | "request_id">,
): Promise<void> {
  try {
    await deps.audit.record({ actor: actorOf(asker.session), action: asker.action, ...e, request_id: asker.requestId });
  } catch (err) {
    deps.log(`writing the audit of ${asker.action} (request ${asker.requestId}) failed: ${(err as Error).message}`);
  }
}

/// Refuse `asker` in the product envelope, audited when it asked for a
/// change; `detail` is what the audit records beyond the answer's message.
export async function refused(
  deps: ManagementDeps,
  asker: Asker,
  r: { status: number; code: string; message: string; detail?: string; headers?: Record<string, string> },
): Promise<Response> {
  if (asker.change) {
    await audited(deps, asker, { outcome: "refused", reason: `${r.code}: ${r.detail ?? r.message}` });
  }
  return refuse(r.status, r.code, r.message, r.headers);
}

/// A refusal a change throws from inside its compare-and-swap (the state
/// it lands on refuses it), answered by `commit`.
export class Refusal extends Error {
  constructor(
    readonly status: number,
    readonly code: string,
    message: string,
    readonly detail?: string,
    readonly headers?: Record<string, string>,
  ) {
    super(message);
  }
}

/// The refusal for what a store or compare-and-swap path threw: a Refusal
/// as itself; five lost compare-and-swaps (cell-admin's exit 3), and a
/// store that timed out, could not be reached or failed (StoreUnavailable),
/// as 503 issuer_unavailable with Retry-After; null for anything else. A
/// store failure during a change's write may hide a write that landed (its
/// answer lost): the change may have been made.
export function refusalOf(e: unknown): Refusal | null {
  if (e instanceof Refusal) return e;
  const retry = { "retry-after": String(UNAVAILABLE_RETRY_SECS) };
  if (e instanceof StoreUnavailable) {
    return new Refusal(503, "issuer_unavailable", "the issuer cannot reach the cell's state; retry shortly", e.message, retry);
  }
  if (e instanceof AdminError && e.code === 3) {
    return new Refusal(503, "issuer_unavailable", "the cell's state is busy; retry shortly", e.message, retry);
  }
  return null;
}

/// Make `change` on the issuer's copy of the state (StateCache.mutate:
/// one compare-and-swap, re-applied on a lost one, then the bundle
/// published), answering what it throws by refusalOf.
export async function commit<T>(
  deps: ManagementDeps,
  asker: Asker,
  change: (state: CellState) => T,
): Promise<{ result: T; state: CellState } | Response> {
  try {
    return await deps.cache.mutate(change);
  } catch (e) {
    const refusal = refusalOf(e);
    if (refusal === null) throw e;
    return refused(deps, asker, refusal);
  }
}

const MAX_BODY_BYTES = 4096;

/// The request's body as a JSON object of the `members` named, or why it
/// is refused. An empty body is `{}` when `emptyIsObject`.
export async function readObject(
  req: Request,
  members: readonly string[],
  emptyIsObject = false,
): Promise<Record<string, unknown> | string> {
  if (Number(req.headers.get("content-length") ?? 0) > MAX_BODY_BYTES) {
    return `the body is larger than ${MAX_BODY_BYTES} bytes`;
  }
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return `the body is larger than ${MAX_BODY_BYTES} bytes`;
  if (text.trim() === "" && emptyIsObject) return {};
  let body: unknown;
  try {
    body = JSON.parse(text);
  } catch {
    return "the body is not JSON";
  }
  if (typeof body !== "object" || body === null || Array.isArray(body)) return "the body is not a JSON object";
  const unknown = Object.keys(body).find((k) => !members.includes(k));
  if (unknown !== undefined) {
    return `unknown member ${JSON.stringify(unknown)}: the body takes ${members.length ? members.join(", ") : "nothing"}`;
  }
  return body as Record<string, unknown>;
}

/// A display name: 1 to 64 characters once trimmed, no control
/// characters; null otherwise.
export function displayNameOf(v: unknown): string | null {
  if (typeof v !== "string" || /[\u0000-\u001f\u007f]/.test(v)) return null;
  const name = v.trim();
  return name.length >= 1 && name.length <= 64 ? name : null;
}
