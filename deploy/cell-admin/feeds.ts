// The cell's feed bundle, built from the state and nothing else:
// {keys, policies, grants}, each a full snapshot at the state's
// feed_version, in the shape deploy/app-server materializes from
// FEEDS_S3_KEY into the three STREAMS_AUTH_*_FILE files. Every part is
// validated against contracts/streams-platform/v1 before it is written.
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { validateDocument } from "../../platform-demo/src/validate.mjs";
import { AdminError } from "./errors";
import type { CellState } from "./state";

const SCHEMAS = join(import.meta.dir, "..", "..", "contracts", "streams-platform", "v1");

export function schema(name: string): unknown {
  return JSON.parse(readFileSync(join(SCHEMAS, name), "utf8"));
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

/// The full snapshots the state implies: every project not yet offboarded,
/// every credential not omitted, every key.
export function bundleOf(state: CellState): Bundle {
  const v = state.feed_version;
  const projects = sortedEntries(state.projects)
    .filter(([, p]) => p.phase !== "offboarded")
    .map(([id, p]) => ({
      project_id: id,
      workspace_id: p.workspace_id,
      cell_id: state.cell.cell_id,
      project_policy_version: p.project_policy_version,
      ownership_version: p.ownership_version,
      status: "active",
      quotas: { ...p.quotas },
    }));
  const credentials = sortedEntries(state.credentials)
    .filter(([, c]) => !c.omitted)
    .map(([id, c]) => ({
      credential_id: id,
      project_id: c.project_id,
      grant_version: c.grant_version,
      status: c.status,
      scopes: c.scopes,
      ...(c.stream_prefixes ? { stream_prefixes: [...c.stream_prefixes] } : {}),
      expires_at: null,
    }));
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

export const bundleText = (state: CellState) => `${JSON.stringify(bundleOf(state))}\n`;
