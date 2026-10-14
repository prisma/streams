// The owner's controls over a shared cell beside the interim issuer
// (public-beta issuer triage B1.8): suspend, resume, quota and gate, each
// one change through `mutate`; allow and disallow, each one compare-and-swap
// of the allow-list. Both land safely beside the issuer's writes.
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError, notYet } from "./errors";
import { CAS_ATTEMPTS, loadChecked, mutate } from "./publish";
import { CANARY_WORKSPACE, checkId, placedProjects, projectOf, quotaValues, refuseCanary, setProjectField } from "./rules";
import { type CellState, journal } from "./state";
import { type AllowEntry, type Allowlist, Conflict, type StateStore } from "./store";
import { openStore, publishNote } from "./stores";

/// Suspend (the cell answers the project's requests 403
/// project_not_active, and the issuer mints it no token) or resume.
async function setStatus(f: Flags, io: Io, status: "active" | "suspended"): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  refuseCanary(project);
  const command = status === "suspended" ? "suspend" : "resume";
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      const p = projectOf(s, project);
      if (p.phase !== "active") {
        throw new AdminError(`project ${project} is ${p.phase}: only an active project is suspended or resumed`);
      }
      if (p.status === status) throw new AdminError(`project ${project} is already ${status}`);
      setProjectField(s, project, { status });
      journal(s, command, `${project} policy=v${p.project_policy_version}`);
    });
    const v = state.projects[project].project_policy_version;
    io.out(`${project} is ${status} at project_policy_version ${v}`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}

export const suspend = (f: Flags, io: Io) => setStatus(f, io, "suspended");
export const resume = (f: Flags, io: Io) => setStatus(f, io, "active");

/// Set some of an active project's quotas (`--quota field=value`, each at
/// most its ceiling), keeping the others, at a newer policy version.
export async function quota(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
  if (f.all("quota").length === 0) throw new AdminError("--quota field=value is required", 2);
  refuseCanary(project);
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      const p = projectOf(s, project);
      if (p.phase !== "active") throw new AdminError(`project ${project} is ${p.phase}: its quotas are final`);
      const quotas = { ...p.quotas, ...quotaValues(s, f.all("quota")) };
      if (!setProjectField(s, project, { quotas })) {
        throw new AdminError(`project ${project} already has these quotas`);
      }
      journal(s, "quota", `${project} ${f.all("quota").join(" ")}`);
    });
    const p = state.projects[project];
    io.out(`${project} at project_policy_version ${p.project_policy_version}: ` +
      `${Object.entries(p.quotas).map(([q, v]) => `${q}=${v}`).join(" ")}`);
    io.out(`bundle: ${store.bundlePath} (feed_version ${state.feed_version}); ${publishNote(store)}`);
  });
}

/// Apply `change` to the allow-list (an empty one when there is none) and
/// write it by compare-and-swap, re-reading on a lost one.
async function updateAllowlist(store: StateStore, change: (doc: Allowlist) => void): Promise<Allowlist> {
  for (let attempt = 1; ; attempt++) {
    const got = await store.readAllowlist();
    const doc: Allowlist = got ? structuredClone(got.doc) : { version: 0, entries: [] };
    change(doc);
    doc.version += 1;
    doc.entries.sort((a, b) => (a.workspace_id < b.workspace_id ? -1 : a.workspace_id > b.workspace_id ? 1 : 0));
    try {
      await store.writeAllowlist(doc, got?.etag ?? null);
      return doc;
    } catch (e) {
      if (!(e instanceof Conflict)) throw e;
      if (attempt >= CAS_ATTEMPTS) throw notYet(`the allow-list changed under ${CAS_ATTEMPTS} attempts: run again`);
    }
  }
}

/// A workspace that may hold projects: a valid id, not one the cell
/// reserves.
function checkWorkspace(state: CellState, workspace: string): void {
  checkId("--workspace", workspace);
  if (workspace === state.cell.account_id) {
    throw new AdminError(`workspace ${workspace} is the cell's ACCOUNT_ID, the unowned-event sink`);
  }
  if (workspace === CANARY_WORKSPACE) throw new AdminError(`workspace ${workspace} is the issuer's canary workspace`);
}

/// Put a workspace on the allow-list, or replace its limits (keeping when
/// it was first added): `--max-projects` over the cell's workspace cap,
/// `--requests-per-sec` (at most the cell's ceiling) over the free-tier
/// rate.
export async function allow(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const workspace = f.must("workspace");
  const { state } = await loadChecked(store);
  checkWorkspace(state, workspace);
  const max_projects = f.has("max-projects") ? f.count("max-projects", 1) : undefined;
  const rps = f.has("requests-per-sec") ? f.count("requests-per-sec", 1) : undefined;
  const ceiling = state.cell.ceilings.requests_per_sec;
  if (rps !== undefined && rps > ceiling) {
    throw new AdminError(`--requests-per-sec ${rps} exceeds the cell's ceiling ${ceiling}`);
  }
  const doc = await store.exclusive(() => updateAllowlist(store, (d) => {
    const was = d.entries.find((e) => e.workspace_id === workspace);
    const entry: AllowEntry = {
      workspace_id: workspace,
      ...(max_projects !== undefined ? { max_projects } : {}),
      ...(rps !== undefined ? { requests_per_sec: rps } : {}),
      note: f.get("note") ?? "",
      added_at: was?.added_at ?? new Date().toISOString(),
    };
    d.entries = [...d.entries.filter((e) => e.workspace_id !== workspace), entry];
  }));
  io.out(`allow-list v${doc.version}: ${workspace} may create projects (${doc.entries.length} workspace(s))`);
}

/// Take a workspace off the allow-list. Under the owner's question I8's
/// answer (a), it creates no new project and its projects keep running
/// until the owner suspends them; under call 16 (2026-10-13, the reduced
/// mode), it gets no new credential or secret either, and while it holds
/// a project its users keep sign-in, reads, revoke and delete at the
/// issuer. The output names the levers against a hostile session there.
export async function disallow(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const workspace = f.must("workspace");
  const { state } = await loadChecked(store);
  const doc = await store.exclusive(() => updateAllowlist(store, (d) => {
    if (!d.entries.some((e) => e.workspace_id === workspace)) {
      throw new AdminError(`workspace ${workspace} is not on the allow-list`);
    }
    d.entries = d.entries.filter((e) => e.workspace_id !== workspace);
  }));
  const running = placedProjects(state)
    .filter(([, p]) => p.workspace_id === workspace)
    .map(([id]) => id);
  io.out(`allow-list v${doc.version}: ${workspace} removed`);
  const none = `${workspace} gets no new project, credential or secret within 5 s`;
  if (running.length === 0) {
    io.out(`${none}; it holds no project, so its users sign in no more`);
    return;
  }
  io.out(`${none}; its ${running.length} project(s) keep running: ${running.join(", ")}`);
  io.out(
    "while it holds a project its users keep sign-in in the issuer's reduced mode: they see its projects and " +
      "credentials, revoke a credential and delete a project",
  );
  io.out(
    "levers against a hostile session: cell-admin suspend --project P stops a delete, not a revoke; rotating " +
      "STREAMS_ISSUER_SESSION_KEY ends every session; cell-admin credential --project P (or allow) replaces a " +
      "credential it revoked",
  );
}

/// Open or close self-serve admission, or set how many customer projects
/// it places (`--n-max`). The gate is the issuer's, not the cell's: it
/// publishes nothing.
export async function gate(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const [open, close] = [f.has("open"), f.has("close")];
  if (open && close) throw new AdminError("give --open or --close, not both", 2);
  if (!open && !close && !f.has("n-max")) throw new AdminError("give --open, --close or --n-max N", 2);
  const n_max = f.has("n-max") ? f.count("n-max", 1) : undefined;
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      s.cell.gate = { open: open ? true : close ? false : s.cell.gate.open, n_max: n_max ?? s.cell.gate.n_max };
      journal(s, "gate", `${s.cell.gate.open ? "open" : "closed"} n_max=${s.cell.gate.n_max}`);
    });
    const g = state.cell.gate;
    io.out(`gate: ${g.open ? "open" : "closed"}, n_max ${g.n_max} (${placedProjects(state).length} project(s) placed)`);
  });
}
