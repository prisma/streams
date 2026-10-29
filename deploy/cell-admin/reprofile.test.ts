// cell-admin reprofile (lead decision 2026-10-20 02:00): a cell's layered
// profile changes on its life only where no sizing rule reads, the
// block-cache sizes. Run with `bun test ./deploy/cell-admin`.
import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { cellSizing, profileDigest, readProfiles } from "./profile";
import { REPROFILE_KEYS } from "./reprofile";
import {
  cleanup,
  initCell,
  readBundleText,
  readState,
  readStateText,
  run,
  STORES,
  scratchDir,
  useStore,
} from "./test-rig";

afterEach(cleanup);

const shipped = (name: string) => readFileSync(join(import.meta.dir, "..", "profiles", name), "utf8");

/// shared-cell.env before f8456a52 added the block-cache lines: the profile
/// every cell initialised before the RC recorded (field round 6).
const BEFORE_CACHES = shipped("shared-cell.env").replace(/^SHARED_CACHE_BYTES=.*\n/gm, "");

/// A release's two profile files, compute-1g.env then `shared` (the
/// shipped shared-cell.env unless given) with `extra` appended, as `flag`
/// arguments.
function release(flag: string, o: { shared?: string; extra?: string } = {}): string[] {
  const root = scratchDir();
  return ["compute-1g.env", "shared-cell.env"].flatMap((name, i) => {
    const path = join(root, name);
    writeFileSync(path, i === 0 ? shipped(name) : `${o.shared ?? shipped(name)}${o.extra ?? ""}`);
    return [flag, path];
  });
}

const digestOf = (args: string[]) => profileDigest(readProfiles(args.filter((_, i) => i % 2 === 1)));

for (const kind of STORES) {
  describe(`reprofile on the ${kind} store`, () => {
    beforeEach(() => useStore(kind));

    test("takes a change of the block-cache sizes alone: records the new digest and the keys, and the release then passes", async () => {
      const c = await initCell(release("--profile", { shared: BEFORE_CACHES }));
      const rc = release("--profile");
      const refused = await run("check-sizing", "--state", c.state, ...rc);
      expect(refused.code).toBe(1);
      const [was, now] = [digestOf(release("--profile", { shared: BEFORE_CACHES })), digestOf(rc)];
      expect(readState(c.state).cell.profile_digest).toBe(was);
      const bundle = readBundleText(c.state);
      const r = await run("reprofile", "--state", c.state, ...release("--recorded-profile", { shared: BEFORE_CACHES }), ...rc);
      const keys = "SHARED_CACHE_BYTES 134217728 -> 33554432";
      expect([r.code, r.err]).toEqual([0, ""]);
      expect(r.out.split("\n")).toEqual([
        `cell cell_sc1: profile digest ${was.slice(0, 12)} -> ${now.slice(0, 12)} (${keys}); k and the ceilings are unchanged`,
        "the running issuer refuses every create (profile_ok false) until a release with this profile deploys: deploy it now",
      ]);
      const state = readState(c.state);
      expect([state.cell.profile_digest, state.cell.profiles]).toEqual([now, rc.filter((_, i) => i % 2 === 1)]);
      expect(state.journal.map((j) => [j.command, j.detail]).at(-1)).toEqual([
        "reprofile", `digest ${was.slice(0, 12)} -> ${now.slice(0, 12)}: ${keys}`,
      ]);
      // No published field changed: the bundle and its generation are as they were.
      expect([state.feed_version, readBundleText(c.state)]).toEqual([1, bundle]);
      expect((await run("check-sizing", "--state", c.state, ...rc)).code).toBe(0);
      expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w", ...rc)).code).toBe(0);
      // The way back (a rollback to the release before): the same rule, reversed.
      const back = await run("reprofile", "--state", c.state, ...release("--recorded-profile"),
        ...release("--profile", { shared: BEFORE_CACHES }));
      expect([back.code, readState(c.state).cell.profile_digest]).toEqual([0, was]);
    });

    test("refuses a change to any other key, naming each one, and changes nothing", async () => {
      const c = await initCell();
      const before = readStateText(c.state);
      const recorded = release("--recorded-profile");
      const cases: [string, string][] = [
        ["PROJECT_SHARE_K=4\nSHARED_CACHE_BYTES=1048576\n", "PROJECT_SHARE_K 8 -> 4"],
        ["SSE_MAX_CONNECTIONS=800\n", "SSE_MAX_CONNECTIONS 1200 -> 800"],
        ["CELL_ENVELOPE_REQUESTS_PER_SEC=2000\nPROJECT_STORED_BYTES_DEFAULT=1\n",
          "CELL_ENVELOPE_REQUESTS_PER_SEC 1411 -> 2000, PROJECT_STORED_BYTES_DEFAULT 10737418240 -> 1"],
        ["MAX_REQUEST_BODY_BYTES=16777216\n", "MAX_REQUEST_BODY_BYTES 8388608 -> 16777216"],
        ["SEGMENT_TARGET_BYTES=1\n", "SEGMENT_TARGET_BYTES (unset) -> 1"],
      ];
      for (const [extra, offending] of cases) {
        const r = await run("reprofile", "--state", c.state, ...recorded, ...release("--profile", { extra }));
        expect([r.code, r.out, r.err]).toEqual([1, "",
          `cell-admin reprofile: the new profile changes ${offending}: only ${REPROFILE_KEYS.join(", ")} change ` +
            "on a cell's life; any other change needs a new cell"]);
      }
      const removed = shipped("shared-cell.env").replace(/^PROJECT_SHARE_K=.*\n/m, "");
      const r = await run("reprofile", "--state", c.state, ...recorded, ...release("--profile", { shared: removed }));
      expect(r.err).toStartWith("cell-admin reprofile: the new profile changes PROJECT_SHARE_K 8 -> (unset): ");
      for (const value of ["0", "32MiB"]) {
        const bad = await run("reprofile", "--state", c.state, ...recorded,
          ...release("--profile", { extra: `SHARED_CACHE_BYTES=${value}\n` }));
        expect([bad.code, bad.err]).toEqual([1,
          `cell-admin reprofile: profile SHARED_CACHE_BYTES=${value} is not a positive integer`]);
      }
      expect(readStateText(c.state)).toBe(before);
    });

    test("refuses a --recorded-profile that is not the profile the cell recorded, and a profile that is already it", async () => {
      const c = await initCell(release("--profile", { shared: BEFORE_CACHES }));
      const before = readStateText(c.state);
      const recorded = digestOf(release("--profile", { shared: BEFORE_CACHES }));
      // The release's own profile given as the recorded one: the field's
      // mistake, which would otherwise diff the release against itself.
      let r = await run("reprofile", "--state", c.state, ...release("--recorded-profile"), ...release("--profile"));
      expect([r.code, r.err]).toEqual([1,
        `cell-admin reprofile: the --recorded-profile files give digest ${digestOf(release("--profile")).slice(0, 12)}, ` +
          `the cell records ${recorded.slice(0, 12)}: give the profiles the cell was initialised or last reprofiled ` +
          "with (the running release's)"]);
      r = await run("reprofile", "--state", c.state, ...release("--recorded-profile", { shared: BEFORE_CACHES }),
        ...release("--profile", { shared: BEFORE_CACHES, extra: "# a comment the digest ignores\n" }));
      expect([r.code, r.err]).toEqual([1,
        `cell-admin reprofile: the new profile is the one the cell records (digest ${recorded.slice(0, 12)}): nothing to change`]);
      r = await run("reprofile", "--state", c.state, ...release("--profile"));
      expect([r.code, r.err.split("\n")[0]]).toEqual([2, "cell-admin reprofile: --recorded-profile is required"]);
      expect(readStateText(c.state)).toBe(before);
    });
  });
}

describe("the keys reprofile takes", () => {
  test("are no key a sizing rule reads, and only the block-cache sizes", () => {
    const read = new Set<string>();
    class Recording extends Map<string, string> {
      override get(key: string) {
        read.add(key);
        return super.get(key);
      }
      override has(key: string) {
        read.add(key);
        return super.has(key);
      }
    }
    cellSizing(new Recording(readProfiles(release("--profile").filter((_, i) => i % 2 === 1))));
    expect(read.has("PROJECT_SHARE_K") && read.has("ADMIT_MAX_INFLIGHT") && read.has("ADMIT_RSS_SHED_MB")).toBe(true);
    expect(REPROFILE_KEYS.filter((k) => read.has(k))).toEqual([]);
    expect([...REPROFILE_KEYS]).toEqual(["SHARED_CACHE_BYTES", "TELEMETRY_CACHE_BYTES"]);
    for (const k of REPROFILE_KEYS) expect(readFileSync(join(import.meta.dir, "..", "..", "src", "config", "load.rs"), "utf8")
      + readFileSync(join(import.meta.dir, "..", "..", "src", "config", "cli.rs"), "utf8")).toContain(`"${k}"`);
  });
});
