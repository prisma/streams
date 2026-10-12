// The owner's controls over a shared cell beside the interim issuer
// (public-beta issuer triage B1.8): suspend and resume. Each is one
// change through `mutate`, so it lands safely beside the issuer's writes.
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError } from "./errors";
import { mutate } from "./publish";
import { projectOf, setProjectField } from "./rules";
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
