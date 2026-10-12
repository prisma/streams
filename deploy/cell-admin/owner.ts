// The owner's controls over a shared cell beside the interim issuer
// (public-beta issuer triage B1.8): suspend, resume and quota. Each is one
// change through `mutate`, so it lands safely beside the issuer's writes.
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError } from "./errors";
import { mutate } from "./publish";
import { projectOf, quotaValues, setProjectField } from "./rules";
import { journal } from "./state";
import { openStore, publishNote } from "./stores";

/// Suspend (the cell answers the project's requests 403
/// project_not_active, and the issuer mints it no token) or resume.
async function setStatus(f: Flags, io: Io, status: "active" | "suspended"): Promise<void> {
  const store = await openStore(f);
  const project = f.must("project");
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
