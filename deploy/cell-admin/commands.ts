// init, admit, check-sizing, token, list and bundle. offboard lives in
// offboard.ts, the owner's controls in owner.ts, and the rules every
// writer follows in rules.ts.
import { join, resolve } from "node:path";
import type { Flags } from "./args";
import { CELL_ENV_KEYS, stageCellEnv } from "./cell-env";
import { AdminError } from "./errors";
import {
  BOUNDED_FIELDS,
  type CellSizing,
  cellSizing,
  profileDigest,
  QUOTA_FIELDS,
  type Quotas,
  readProfiles,
} from "./profile";
import { BundlePublisher, burnIds, loadChecked, mutate } from "./publish";
import {
  admitProject,
  admittedQuotas,
  checkPacking,
  bumpFeed,
  canaryProjectId,
  checkId,
  checkPlacement,
  checkSizing,
  classPreset,
  isCanary,
  newCredentialId,
  nextCredentialSeq,
  type PlacementTally,
  placeCanary,
  placedProjects,
  placementTally,
  kindOf,
  prefixesOf,
  SYSTEM_PROJECT,
  scopesOf,
  type Weights,
  weightValues,
} from "./rules";
import { type CellState, type ContainerKind, GATE_DEFAULT, journal, STATE_FORMAT, writeDurable } from "./state";
import { Conflict, type StateStore } from "./store";
import { openStore, publishNote } from "./stores";
import { cellSigningKey, customerClaims, loadSigningKey, readPublicKey, signJwt } from "./tokens";

export interface Io {
  out: (line: string) => void;
}

const PROFILE_DIR = join(import.meta.dir, "..", "profiles");
export const DEFAULT_PROFILES = [join(PROFILE_DIR, "compute-1g.env"), join(PROFILE_DIR, "shared-cell.env")];
/// A dedicated cell's (k = 1): `init` takes them by `--profile`; `admit`
/// defaults to them for a k = 1 cell.
export const DEDICATED_PROFILES = [join(PROFILE_DIR, "compute-1g.env"), join(PROFILE_DIR, "dedicated-cell.env")];

export async function init(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f, { create: true });
  const cell = f.must("cell-id");
  const deployment = f.must("deployment-project");
  checkId("--cell-id", cell);
  if (cell.length > 118) throw new AdminError("--cell-id: at most 118 characters (its canary is sp_canary_<cell id>)", 2);
  checkId("--deployment-project", deployment);
  if (deployment === "proj_local" || deployment === SYSTEM_PROJECT) {
    throw new AdminError("a shared cell needs a real CONTAINER_ID, not a placeholder");
  }
  const profiles = profilesOf(f.all("profile"));
  const env = readProfiles(profiles);
  const sizing = cellSizing(env);
  if (f.has("key-file") === f.has("public-key")) {
    throw new AdminError("give one of --key-file K or --public-key FILE (the public half only)", 2);
  }
  const key = f.has("public-key") ? readPublicKey(resolve(f.must("public-key"))) : loadSigningKey(f.must("key-file")).record;
  const [workspace_cap, max_projects] = [f.count("workspace-cap", 1000), f.count("max-projects", sizing.dedicated ? 1 : 1000)];
  if (sizing.dedicated && max_projects !== 1) {
    throw new AdminError(
      `--max-projects ${max_projects}: a dedicated cell (k = 1) holds one project; --max-projects must be 1`,
    );
  }
  const denylist = store.denylistAt(f.get("denylist"));
  const state: CellState = {
    format: STATE_FORMAT,
    cell: {
      cell_id: cell,
      deployment_project_id: deployment,
      issuer: f.get("issuer") ?? "https://auth.prisma.io",
      share_k: sizing.share_k,
      ceilings: sizing.ceilings,
      profiles,
      profile_digest: profileDigest(env),
      workspace_cap,
      max_projects,
      denylist,
      gate: { ...GATE_DEFAULT },
    },
    keys: [key],
    retired_kids: [],
    // Placing the canary publishes the first generation, 1.
    feed_version: 0,
    projects: {},
    credentials: {},
    journal: [],
  };
  placeCanary(state, sizing.stored_default);
  // The cells file is written once the state is saved, so it never names a
  // cell that does not exist, and staged first, so a file or a directory it
  // cannot write refuses before the cell exists.
  const cellEnv = f.has("cell-env") ? resolve(f.must("cell-env")) : undefined;
  const staged = cellEnv === undefined ? undefined : stageCellEnv(cellEnv, state.cell);
  try {
    await store.exclusive(async () => {
      await createCell(store, state, `k=${sizing.share_k} kid=${key.kid} canary=${canaryProjectId(cell)}`);
      staged?.keep();
      await new BundlePublisher(store).publish(state);
    });
  } finally {
    staged?.drop();
  }
  io.out(`cell ${cell}: k=${sizing.share_k}, key ${key.kid} (${key.alg}, aud prisma-streams-data)`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${sizing.ceilings[q]}`).join(" ")}`);
  io.out(`denylist: ${denylist}`);
  io.out(`bundle: ${store.bundlePath} (feed_version 1)`);
  if (cellEnv !== undefined) io.out(`cells file: ${cellEnv} (${CELL_ENV_KEYS.join(", ")})`);
}

/// Save a new cell's state, refused when the store holds one, or a bundle.
async function createCell(store: StateStore, state: CellState, detail: string): Promise<void> {
  // A bundle without a state: the state was lost, not never made. A new
  // state starts at feed_version 1, below the generation the cell holds,
  // so nothing it wrote would ever publish, and every later command
  // would refuse it as a stale restore.
  const published = await store.readBundle();
  if (published) {
    const stateToo = await store.load().then(() => true, () => false);
    throw new AdminError(
      stateToo
        ? `${store.statePath} exists: a cell is initialised once`
        : `${store.bundlePath} exists at feed_version ${published.version}: this cell has published, and a new ` +
            "state would start below what it serves; recover the newest state, or initialise a new cell id",
    );
  }
  journal(state, "init", detail);
  try {
    await store.save(state, null);
  } catch (e) {
    if (e instanceof Conflict) throw new AdminError(`${store.statePath} exists: a cell is initialised once`);
    throw e;
  }
}

/// The profile files a command reads: `--profile` (repeatable, later
/// wins), or the two this tool ships with for the cell's kind: the shared
/// ones, or the dedicated ones for a cell of k = 1.
const profilesOf = (given: readonly string[], shareK?: number) =>
  (given.length > 0 ? given : shareK === 1 ? DEDICATED_PROFILES : DEFAULT_PROFILES).map((p) => resolve(p));

/// One project going onto the cell, all that its placement decides from.
interface Placement {
  project: string;
  workspace: string;
  kind: ContainerKind;
  weights: Weights;
  quotas: Quotas;
  scopes: string;
  stream_prefixes?: string[];
}

/// The placement `admit` and `admitMany` both make: the checks, the project
/// with its first credential, its journal entry. The id is burned already.
/// `kept` is what a change placing many carries from one placement to the
/// next, so that this one need not walk the state.
function place(
  state: CellState,
  p: Placement,
  sizing: CellSizing,
  denylist: Set<string>,
  kept: { tally?: PlacementTally; seq?: number } = {},
): string {
  checkPlacement(state, p.project, p.workspace, denylist, { tally: kept.tally });
  checkPacking(state, p.weights, sizing);
  const credential = newCredentialId(state);
  const { project, workspace, kind, quotas, scopes, stream_prefixes } = p;
  admitProject(state, { project, workspace, kind, quotas, scopes, stream_prefixes, credential: { id: credential, seq: kept.seq } });
  journal(state, "admit", `${project} workspace=${workspace} kind=${kind} credential=${credential}`);
  return credential;
}

export async function admit(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  const workspace = f.must("workspace");
  const kind = kindOf(f.get("kind"));
  await store.exclusive(async () => {
    const { state: first } = await loadChecked(store);
    const sizing = checkSizing(first, readProfiles(profilesOf(f.all("profile"), first.cell.share_k)));
    const { ids: denylist } = await store.readDenylist();
    const preset = classPreset(first, f.get("class"), f.all("weight"));
    checkPlacement(first, project, workspace, denylist);
    const weights = weightValues(first, preset.weights);
    checkPacking(first, weights, sizing);
    const quotas = admittedQuotas(first, f.all("quota"), sizing.stored_default, weights);
    const scopes = scopesOf(f.get("scopes"));
    const stream_prefixes = prefixesOf(f.all("prefix"));
    await burnIds(store, [project]);
    const placement = { project, workspace, kind, weights, quotas, scopes, stream_prefixes };
    const { result: credential, state } = await mutate(store, (state) => place(state, placement, sizing, denylist));
    io.out(`admitted ${project} (workspace ${workspace}, ${kind}) with credential ${credential}`);
    io.out(`quotas: ${QUOTA_FIELDS.map((q) => `${q}=${quotas[q]}`).join(" ")}`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}

/// A project of `admitMany`, as `admit`'s flags name it.
export interface Admission {
  project: string;
  workspace: string;
  /// Streams unless given.
  kind?: ContainerKind;
  quota: readonly string[];
  weight: readonly string[];
}

export interface AdmitManyResult {
  placed: Admission[];
  failed: { project: string; why: string }[];
}

/// Place each of `wanted` on `state`, in order, by `admit`'s rules at the
/// cell's default profile and scopes; one refused is reported and the next
/// tried. Whole-state walks happen once here, not per project.
function placeEach(
  state: CellState,
  wanted: readonly Admission[],
  sizing: CellSizing,
  denylist: Set<string>,
): AdmitManyResult {
  const result: AdmitManyResult = { placed: [], failed: [] };
  const tally = placementTally(state);
  const scopes = scopesOf(undefined);
  let seq = nextCredentialSeq(state);
  for (const a of wanted) {
    try {
      const weights = weightValues(state, a.weight);
      const quotas = admittedQuotas(state, a.quota, sizing.stored_default, weights);
      const { project, workspace, kind = "streams" } = a;
      place(state, { project, workspace, kind, weights, quotas, scopes }, sizing, denylist, { tally, seq });
      seq += 1;
      tally.placed += 1;
      tally.active.set(workspace, (tally.active.get(workspace) ?? 0) + 1);
      result.placed.push(a);
    } catch (e) {
      if (!(e instanceof AdminError)) throw e;
      result.failed.push({ project: a.project, why: e.message });
    }
  }
  return result;
}

/// Many projects as one change: one denylist write, one state write, one
/// new generation, so the cost per project does not grow with the cell, where
/// one `admit` per project rewrites the whole state and bundle each time. For
/// an operator filling a cell (bench/k2cost/tenants-admit.ts); `admit` stays
/// the one-project command and takes the same path through `place`. As in
/// `admit`, the ids burn before the commit: a crash between the two leaves
/// them all burned, so a cell being filled that crashes is started again.
export async function admitMany(store: StateStore, wanted: readonly Admission[]): Promise<AdmitManyResult> {
  if (wanted.length === 0) return { placed: [], failed: [] };
  return store.exclusive(async () => {
    // The first pass runs on this call's own parse of the state, which goes
    // no further: it finds who is placed, so only they burn.
    const { state: first } = await loadChecked(store);
    const sizing = checkSizing(first, readProfiles(profilesOf([], first.cell.share_k)));
    const { ids: denylist } = await store.readDenylist();
    const { placed, failed } = placeEach(first, wanted, sizing, denylist);
    if (placed.length === 0) return { placed, failed };
    await burnIds(store, placed.map((a) => a.project));
    const { result } = await mutate(store, (state) => placeEach(state, placed, sizing, denylist));
    return { placed: result.placed, failed: [...failed, ...result.failed] };
  });
}

/// `check-sizing`: whether `--profile` (a release's copies, given in full)
/// keep the cell's sizing, by `admit`'s own check (checkSizing): k, the
/// ceilings and the layered profile's digest `init` fixed. Exit 0, or 1
/// with admit's refusal. It changes neither the state nor the bundle.
export async function sizingCheck(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const { state } = await loadChecked(store);
  checkSizing(state, readProfiles(profilesOf(f.all("profile"))));
  const c = state.cell;
  io.out(
    `cell ${c.cell_id}: the profiles give its fixed sizing (k=${c.share_k}, the ceilings init fixed, ` +
      `profile digest ${c.profile_digest.slice(0, 12)})`,
  );
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
  const projects = Object.entries(state.projects).filter(([id]) => !isCanary(id));
  const placed = placedProjects(state).length;
  io.out(`cell ${c.cell_id}: k=${c.share_k}, feed_version ${state.feed_version}, keys ${state.keys.map((k) => k.kid).join(",")}`);
  io.out(`reserved: CONTAINER_ID ${c.deployment_project_id}, ${SYSTEM_PROJECT}`);
  io.out(`ceilings: ${BOUNDED_FIELDS.map((q) => `${q}=${c.ceilings[q]}`).join(" ")}`);
  io.out(`projects: ${placed} placed of ${c.max_projects}, ${projects.length - placed} offboarded; workspace cap ${c.workspace_cap}`);
  for (const [id, p] of projects.sort(([a], [b]) => (a < b ? -1 : 1))) {
    const creds = Object.entries(state.credentials)
      .filter(([, cr]) => cr.project_id === id && !cr.hidden)
      .map(([cid, cr]) => `${cid} ${cr.role} ${cr.omitted ? "omitted" : cr.status} v${cr.grant_version}`);
    io.out(`${id} workspace=${p.workspace_id} ${p.phase} policy=v${p.project_policy_version}`);
    io.out(`  quotas: ${QUOTA_FIELDS.map((q) => `${q}=${p.quotas[q]}`).join(" ")}`);
    io.out(`  credentials: ${creds.join("; ") || "none"}`);
  }
  if (!published) io.out("bundle: missing; run cell-admin bundle");
}

