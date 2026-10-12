// init, admit, token, list and bundle. offboard lives in offboard.ts, the
// owner's controls in owner.ts, and the rules every writer follows in
// rules.ts.
import { join, resolve } from "node:path";
import type { Flags } from "./args";
import { AdminError } from "./errors";
import { BOUNDED_FIELDS, cellSizing, profileDigest, QUOTA_FIELDS, readProfiles } from "./profile";
import { BundlePublisher, burnId, loadChecked, mutate } from "./publish";
import {
  admitProject,
  admittedQuotas,
  bumpFeed,
  checkId,
  checkPlacement,
  checkSizing,
  newCredentialId,
  prefixesOf,
  SYSTEM_PROJECT,
  scopesOf,
} from "./rules";
import { type CellState, journal, STATE_FORMAT, writeDurable } from "./state";
import { Conflict } from "./store";
import { openStore, publishNote } from "./stores";
import { cellSigningKey, customerClaims, loadSigningKey, signJwt } from "./tokens";

export interface Io {
  out: (line: string) => void;
}

const PROFILE_DIR = join(import.meta.dir, "..", "profiles");
export const DEFAULT_PROFILES = [join(PROFILE_DIR, "compute-1g.env"), join(PROFILE_DIR, "shared-cell.env")];

export async function init(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f, { create: true });
  const cell = f.must("cell-id");
  const deployment = f.must("deployment-project");
  const account = f.must("account");
  checkId("--cell-id", cell);
  checkId("--deployment-project", deployment);
  checkId("--account", account);
  if (deployment === "proj_local" || deployment === SYSTEM_PROJECT || account === "acct_local") {
    throw new AdminError("a shared cell needs a real PROJECT_ID and ACCOUNT_ID, not a placeholder");
  }
  const profiles = profilesOf(f);
  const env = readProfiles(profiles);
  const sizing = cellSizing(env);
  const sk = loadSigningKey(f.must("key-file"));
  const [workspace_cap, max_projects] = [f.count("workspace-cap", 1), f.count("max-projects", 1000)];
  const denylist = store.denylistAt(f.get("denylist"));
  const state: CellState = {
    format: STATE_FORMAT,
    cell: {
      cell_id: cell,
      deployment_project_id: deployment,
      account_id: account,
      issuer: f.get("issuer") ?? "https://auth.prisma.io",
      share_k: sizing.share_k,
      ceilings: sizing.ceilings,
      profiles,
      profile_digest: profileDigest(env),
      workspace_cap,
      max_projects,
      denylist,
    },
    keys: [sk.record],
    feed_version: 1,
    projects: {},
    credentials: {},
    journal: [],
  };
  await store.exclusive(async () => {
    journal(state, "init", `k=${sizing.share_k} kid=${sk.record.kid}`);
    try {
      await store.save(state, null);
    } catch (e) {
      if (e instanceof Conflict) throw new AdminError(`${store.statePath} exists: a cell is initialised once`);
      throw e;
    }
    await new BundlePublisher(store).publish(state);
  });
  io.out(`cell ${cell}: k=${sizing.share_k}, key ${sk.record.kid} (${sk.record.alg}, aud prisma-streams-data)`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${sizing.ceilings[q]}`).join(" ")}`);
  io.out(`denylist: ${denylist}`);
  io.out(`bundle: ${store.bundlePath} (feed_version 1)`);
}

/// The profile files a command reads: `--profile` (repeatable, later
/// wins), or the two this tool ships with.
const profilesOf = (f: Flags) =>
  (f.all("profile").length > 0 ? f.all("profile") : DEFAULT_PROFILES).map((p) => resolve(p));

export async function admit(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  const workspace = f.must("workspace");
  await store.exclusive(async () => {
    const { state: first } = await loadChecked(store);
    checkSizing(first, profilesOf(f));
    const { ids: denylist } = await store.readDenylist();
    checkPlacement(first, project, workspace, denylist);
    const quotas = admittedQuotas(first, f.all("quota"));
    const scopes = scopesOf(f.get("scopes"));
    const stream_prefixes = prefixesOf(f.all("prefix"));
    await burnId(store, project);
    const { result: credential, state } = await mutate(store, (state) => {
      checkPlacement(state, project, workspace, denylist);
      const credential = newCredentialId(state);
      admitProject(state, { project, workspace, quotas, scopes, stream_prefixes, credential: { id: credential } });
      journal(state, "admit", `${project} workspace=${workspace} credential=${credential}`);
      return credential;
    });
    io.out(`admitted ${project} (workspace ${workspace}) with credential ${credential}`);
    io.out(`quotas: ${QUOTA_FIELDS.map((q) => `${q}=${quotas[q]}`).join(" ")}`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}

export async function token(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const credential = f.must("credential");
  const out = resolve(f.must("out"));
  await store.exclusive(async () => {
    const { state } = await loadChecked(store);
    const cred = state.credentials[credential];
    if (!cred || cred.role !== "customer") {
      throw new AdminError(`credential ${credential} is not a customer credential of this cell`);
    }
    if (cred.status !== "active" || cred.omitted || state.projects[cred.project_id].phase !== "active") {
      throw new AdminError(`credential ${credential} is not active (project ${cred.project_id})`);
    }
    const sk = cellSigningKey(state, f.must("key-file"));
    const ttl = Number(f.get("ttl") ?? "3600");
    const claims = customerClaims(state, credential, ttl, f.get("sub") ?? "design-partner");
    writeDurable(out, signJwt(sk, claims));
    const exp = new Date(Number(claims.exp) * 1000).toISOString();
    io.out(`token for ${cred.project_id} (credential ${credential}, kid ${sk.record.kid}) expires ${exp}`);
    io.out(`written to ${out} (mode 0600); it is a bearer credential: hand it over, never paste it`);
  });
}

export async function bundle(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  await store.exclusive(async () => {
    const { state } = await mutate(store, (state) => {
      bumpFeed(state);
      journal(state, "bundle", "rewritten");
    });
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version})`);
  });
}

export async function list(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const { state } = await loadChecked(store);
  const published = await store.readBundle();
  if (f.has("json")) {
    const { journal: _journal, ...view } = state;
    io.out(JSON.stringify({ ...view, keys: state.keys.map((k) => ({ kid: k.kid, alg: k.alg, aud: k.aud })) }));
    return;
  }
  const c = state.cell;
  const projects = Object.entries(state.projects);
  const placed = projects.filter(([, p]) => p.phase !== "offboarded").length;
  io.out(`cell ${c.cell_id}: k=${c.share_k}, feed_version ${state.feed_version}, keys ${state.keys.map((k) => k.kid).join(",")}`);
  io.out(`reserved: PROJECT_ID ${c.deployment_project_id}, ACCOUNT_ID ${c.account_id}, ${SYSTEM_PROJECT}`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${c.ceilings[q]}`).join(" ")}`);
  io.out(`projects: ${placed} placed of ${c.max_projects}, ${projects.length - placed} offboarded; workspace cap ${c.workspace_cap}`);
  for (const [id, p] of projects.sort(([a], [b]) => (a < b ? -1 : 1))) {
    const creds = Object.entries(state.credentials)
      .filter(([, cr]) => cr.project_id === id)
      .map(([cid, cr]) => `${cid} ${cr.role} ${cr.omitted ? "omitted" : cr.status} v${cr.grant_version}`);
    io.out(`${id} workspace=${p.workspace_id} ${p.phase} policy=v${p.project_policy_version}`);
    io.out(`  quotas: ${QUOTA_FIELDS.map((q) => `${q}=${p.quotas[q]}`).join(" ")}`);
    io.out(`  credentials: ${creds.join("; ") || "none"}`);
  }
  if (!published) io.out("bundle: missing; run cell-admin bundle");
}

