// `reprofile`: the one change a cell's layered profile takes on its life
// (lead decision 2026-10-20 02:00, field round 6). `init` records the
// profile's digest, and `admit`, `check-sizing` and the issuer refuse
// any other, so a release that only retunes the block caches could
// reach no existing cell. The state holds the digest, not the values:
// the caller gives the profile the cell records (`--recorded-profile`,
// the running release's files), which must give that digest, and the
// new one (`--profile`, the release's files). Only REPROFILE_KEYS may
// differ between them; the new digest is recorded, with a journal entry
// naming each key.
import { resolve } from "node:path";
import type { Flags } from "./args";
import type { Io } from "./commands";
import { AdminError } from "./errors";
import { profileDigest, readProfiles } from "./profile";
import { mutate } from "./publish";
import { checkSizing } from "./rules";
import { journal } from "./state";
import { openStore } from "./stores";

/// The keys a reprofile may change, add or remove: the block-cache sizes
/// (src/config/cli.rs, load.rs). They size the server's memory, not a
/// project's ceiling: no sizing rule (cellSizing, checkSizing) reads one,
/// which reprofile.test.ts pins by recording every key cellSizing reads.
/// A key any sizing rule, or step 6 of deploy-release, reads never joins.
export const REPROFILE_KEYS = [
  "SHARED_CACHE_BYTES",
  "TELEMETRY_CACHE_BYTES",
] as const;

/// Every key whose value differs between two layered profiles (absent on
/// one side included), sorted, as `KEY was -> now`.
function changedKeys(was: Map<string, string>, now: Map<string, string>): { key: string; text: string }[] {
  const keys = [...new Set([...was.keys(), ...now.keys()])].filter((k) => was.get(k) !== now.get(k)).sort();
  return keys.map((key) => ({ key, text: `${key} ${was.get(key) ?? "(unset)"} -> ${now.get(key) ?? "(unset)"}` }));
}

/// Refuse a change outside REPROFILE_KEYS, naming every key, and a cache
/// size the server would not parse as one.
function refuseChanges(now: Map<string, string>, changes: { key: string; text: string }[]): void {
  const allowed = REPROFILE_KEYS as readonly string[];
  const offending = changes.filter((c) => !allowed.includes(c.key));
  if (offending.length > 0) {
    throw new AdminError(
      `the new profile changes ${offending.map((c) => c.text).join(", ")}: only ${REPROFILE_KEYS.join(", ")} ` +
        "change on a cell's life; any other change needs a new cell",
    );
  }
  for (const { key } of changes) {
    const value = now.get(key);
    if (value !== undefined && !(/^[0-9]+$/.test(value) && Number(value) > 0 && Number.isSafeInteger(Number(value)))) {
      throw new AdminError(`profile ${key}=${value} is not a positive integer`);
    }
  }
}

export async function reprofile(f: Flags, io: Io): Promise<void> {
  const store = await openStore(f);
  const was = readProfiles(f.all("recorded-profile"));
  const files = f.all("profile").map((p) => resolve(p));
  const now = readProfiles(files);
  const [from, to] = [profileDigest(was), profileDigest(now)];
  const changes = changedKeys(was, now);
  const detail = `digest ${from.slice(0, 12)} -> ${to.slice(0, 12)}: ${changes.map((c) => c.text).join(", ")}`;
  await store.exclusive(async () => {
    const { state } = await mutate(store, (s) => {
      const recorded = s.cell.profile_digest;
      if (from !== recorded) {
        throw new AdminError(
          `the --recorded-profile files give digest ${from.slice(0, 12)}, the cell records ${recorded.slice(0, 12)}: ` +
            "give the profiles the cell was initialised or last reprofiled with (the running release's)",
        );
      }
      if (to === recorded) {
        throw new AdminError(`the new profile is the one the cell records (digest ${recorded.slice(0, 12)}): nothing to change`);
      }
      refuseChanges(now, changes);
      s.cell.profile_digest = to;
      s.cell.profiles = files;
      // What every later admission and deploy runs: k, the ceilings and
      // now the digest, against the new profile.
      checkSizing(s, now);
      journal(s, "reprofile", detail);
    });
    io.out(
      `cell ${state.cell.cell_id}: profile ${detail.replace(": ", " (")}); k and the ceilings are unchanged`,
    );
    io.out("the running issuer refuses every create (profile_ok false) until a release with this profile deploys: deploy it now");
  });
}
