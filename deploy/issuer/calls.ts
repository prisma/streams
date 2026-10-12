// What every management handler is given, and how it answers
// (management.ts routes the calls; projects.ts handles them). A change
// the issuer makes, and every refusal of a change once the caller is
// signed in, is one audit object (audit.ts). A read is never audited, and
// neither is a refusal before the session or of its call budget: an
// unauthenticated caller must not be able to make the issuer write.
import type { AllowEntry, StateStore } from "../cell-admin/store";
import type { Audit, AuditEvent } from "./audit";
import type { StateCache } from "./cache";
import type { RateLimits } from "./limits";
import { refuse } from "./respond";
import type { Session } from "./session";

export interface ManagementDeps {
  cache: StateCache;
  store: StateStore;
  limits: RateLimits;
  audit: Audit;
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
