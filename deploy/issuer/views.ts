// What the management API shows of the state: a project, and a
// credential in the contract's `Credential` shape
// (contracts/streams-platform/v1/management.openapi.yaml). Never a
// secret's hash or its pepper version; the hint is the secret's last six
// characters.
import { isCanary } from "../cell-admin/rules";
import type { Actor, CellState, CredentialRecord, ProjectRecord } from "../cell-admin/state";
import type { IssuerConfig } from "./config";

const byOf = (by: Actor | undefined) => (by ? { createdBy: { userId: by.user_id, email: by.email } } : {});

/// A project as the API shows it. `status` is `deleting` while it is
/// being offboarded.
export function projectView(cfg: IssuerConfig, state: CellState, id: string, p: ProjectRecord) {
  return {
    id,
    workspaceId: p.workspace_id,
    displayName: p.display_name ?? id,
    status: p.phase === "active" ? p.status : "deleting",
    createdAt: p.admitted_at,
    ...byOf(p.created_by),
    cellId: state.cell.cell_id,
    endpoint: cfg.cellOrigin,
    quotas: { ...p.quotas },
  };
}

/// A credential as the API shows it.
export function credentialView(id: string, c: CredentialRecord) {
  return {
    id,
    projectId: c.project_id,
    displayName: c.display_name ?? id,
    scopes: c.scopes.split(" "),
    streamPrefixes: c.stream_prefixes ? [...c.stream_prefixes] : null,
    grantVersion: c.grant_version,
    status: c.status,
    valueHint: c.hint ?? "",
    createdAt: c.created_at ?? null,
    expiresAt: c.expires_at === null ? null : new Date(c.expires_at * 1000).toISOString(),
    ...byOf(c.created_by),
  };
}

/// The customer projects of `workspace` a user sees: not the canary, not
/// offboarded; oldest first.
export function workspaceProjects(state: CellState, workspace: string): [string, ProjectRecord][] {
  return Object.entries(state.projects)
    .filter(([id, p]) => p.workspace_id === workspace && p.phase !== "offboarded" && !isCanary(id))
    .sort(([a, p], [b, q]) => (p.admitted_at < q.admitted_at ? -1 : p.admitted_at > q.admitted_at ? 1 : a < b ? -1 : 1));
}

/// `workspace`'s project `id` as a user sees it: not the canary, not
/// offboarded; else undefined.
export function ownProject(state: CellState, workspace: string, id: string): ProjectRecord | undefined {
  const p = state.projects[id];
  return p && p.workspace_id === workspace && p.phase !== "offboarded" && !isCanary(id) ? p : undefined;
}
