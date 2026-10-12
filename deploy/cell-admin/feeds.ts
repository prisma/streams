// The cell's feed bundle, built from the state and nothing else:
// {keys, policies, grants}, each a full snapshot at the state's
// feed_version, in the shape deploy/app-server materializes from
// FEEDS_S3_KEY into the three STREAMS_AUTH_*_FILE files. Every part is
// validated against contracts/streams-platform/v1 before it is written.
// The schemas are compiled in, not read at run time: the interim issuer
// runs this library as one bundled file, where the repository is absent.
import credentialGrants from "../../contracts/streams-platform/v1/credential-grants.schema.json";
import customerTokenClaims from "../../contracts/streams-platform/v1/customer-token-claims.schema.json";
import keys from "../../contracts/streams-platform/v1/keys.schema.json";
import projectPolicies from "../../contracts/streams-platform/v1/project-policies.schema.json";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { AdminError } from "./errors";
import type { CellState, CredentialRecord, ProjectRecord } from "./state";

const SCHEMAS: Record<string, unknown> = {
  "credential-grants.schema.json": credentialGrants,
  "customer-token-claims.schema.json": customerTokenClaims,
  "keys.schema.json": keys,
  "project-policies.schema.json": projectPolicies,
};

export function schema(name: string): unknown {
  const s = SCHEMAS[name];
  if (s === undefined) throw new Error(`no contract schema ${name} is compiled into cell-admin`);
  return s;
}

/// Throw unless `doc` satisfies the named contract schema.
export function validate(doc: unknown, schemaName: string, what: string): void {
  const errs: string[] = validateDocument(doc, schema(schemaName));
  if (errs.length > 0) {
    throw new AdminError(`${what} violates ${schemaName}: ${errs[0]}`);
  }
}

export interface Bundle {
  keys: { feed_version: number; keys: unknown[] };
  policies: { feed_version: number; projects: unknown[] };
  grants: { feed_version: number; credentials: unknown[] };
}

const sortedEntries = <T>(rec: Record<string, T>) =>
  Object.entries(rec).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));

/// A project's entry in the policy feed: every field the cell reads, and
/// nothing else (no timestamp, no display name).
export const policyEntry = (state: CellState, id: string, p: ProjectRecord) => ({
  project_id: id,
  workspace_id: p.workspace_id,
  cell_id: state.cell.cell_id,
  project_policy_version: p.project_policy_version,
  ownership_version: p.ownership_version,
  status: p.status,
  quotas: { ...p.quotas },
});

/// A credential's entry in the grants feed (never its secret's hash).
export const grantEntry = (id: string, c: CredentialRecord) => ({
  credential_id: id,
  project_id: c.project_id,
  grant_version: c.grant_version,
  status: c.status,
  scopes: c.scopes,
  ...(c.stream_prefixes ? { stream_prefixes: [...c.stream_prefixes] } : {}),
  expires_at: c.expires_at,
});

/// The full snapshots the state implies: every project not yet offboarded,
/// every credential not omitted, every key.
export function bundleOf(state: CellState): Bundle {
  const v = state.feed_version;
  const projects = sortedEntries(state.projects)
    .filter(([, p]) => p.phase !== "offboarded")
    .map(([id, p]) => policyEntry(state, id, p));
  const credentials = sortedEntries(state.credentials)
    .filter(([, c]) => !c.omitted)
    .map(([id, c]) => grantEntry(id, c));
  const bundle: Bundle = {
    keys: { feed_version: v, keys: state.keys.map((k) => ({ ...k })) },
    policies: { feed_version: v, projects },
    grants: { feed_version: v, credentials },
  };
  validate(bundle.keys, "keys.schema.json", "keys snapshot");
  validate(bundle.policies, "project-policies.schema.json", "policy snapshot");
  validate(bundle.grants, "credential-grants.schema.json", "grant snapshot");
  return bundle;
}

/// The text published for a bundle: one line of JSON.
export const bundleText = (bundle: Bundle) => `${JSON.stringify(bundle)}\n`;
