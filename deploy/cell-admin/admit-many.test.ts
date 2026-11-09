// admitMany, on both stores: many projects as one change, by the rules one
// `admit` follows. Run with `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { admitMany } from "./commands";
import { QUOTA_FIELDS } from "./profile";
import { burnIds } from "./publish";
import { canaryProjectId } from "./rules";
import { readS3Credentials, S3Store } from "./s3store";
import { DirStore, type StateStore } from "./store";
import {
  auditTexts,
  cleanup,
  credentialsOf,
  denylistText,
  fakeS3,
  initCell,
  readBundle,
  readBundleText,
  readState,
  run,
  STORES,
  useStore,
  writeStateText,
} from "./test-rig";

afterEach(cleanup);

const openedStore = async (state: string): Promise<StateStore> => {
  const creds = credentialsOf(state);
  return creds === undefined ? new DirStore(state) : S3Store.open(state, readS3Credentials(creds));
};

const wanted = (project: string, workspace = `w_${project}`, quota: string[] = [], weight: string[] = []) => ({
  project, workspace, quota, weight,
});

const ids = (r: { placed: { project: string }[] }) => r.placed.map((p) => p.project);
const refusals = (r: { failed: { project: string; why: string }[] }) => r.failed.map((f) => [f.project, f.why]);

for (const kind of STORES) {
  describe(`admitMany on the ${kind} store`, () => {
    test("places what one admit per project places, in one generation", async () => {
      useStore(kind);
      const [one, many] = [await initCell(), await initCell()];
      for (const a of ["p1", "p2", "p3"]) {
        const r = await run("admit", "--state", one.state, "--project", a, "--workspace", `w_${a}`, "--quota", "max_streams=7", "--weight", "request=2");
        expect([a, r.code]).toEqual([a, 0]);
      }
      const r = await admitMany(await openedStore(many.state), ["p1", "p2", "p3"].map((a) => wanted(a, `w_${a}`, ["max_streams=7"], ["request=2"])));
      expect([ids(r), r.failed]).toEqual([["p1", "p2", "p3"], []]);
      const [a, b] = [readState(one.state), readState(many.state)];
      expect(b.feed_version).toBe(2);
      expect(a.feed_version).toBe(4);
      // Equal but for what a run makes fresh: times and credential ids. The
      // bundle's entries compare whole: a cell cannot tell the two apart.
      const fresh = ({ admitted_at: _t, ...rest }: Record<string, unknown>) => rest;
      const projects = (s: typeof a) => Object.fromEntries(Object.entries(s.projects).map(([id, p]) => [id, fresh(p as never)]));
      expect(projects(b)).toEqual(projects(a));
      const creds = (s: typeof a) =>
        Object.values(s.credentials)
          .map(({ created_at: _t, ...rest }) => rest)
          .sort((x, y) => x.seq - y.seq);
      expect(creds(b)).toEqual(creds(a));
      const noCred = (where: string) =>
        (readBundle(where).grants.credentials as { credential_id: string }[]).map(({ credential_id: _c, ...rest }) => rest).sort((x, y) => JSON.stringify(x).localeCompare(JSON.stringify(y)));
      expect(noCred(many.state)).toEqual(noCred(one.state));
      expect(readBundle(many.state).policies.containers).toEqual(readBundle(one.state).policies.containers);
      expect(denylistText(many.state)).toBe(denylistText(one.state));
      expect(readBundle(many.state).keys.feed_version).toBe(2);
      // The ordinary path takes the state on from here.
      expect((await run("admit", "--state", many.state, "--project", "p4", "--workspace", "w_p4")).code).toBe(0);
      const after = readState(many.state);
      expect(Object.values(after.credentials).find((x) => x.project_id === "p4")!.seq).toBe(5);
    });

    test("holds each cap across the batch, and refuses per project without burning the refused id", async () => {
      useStore(kind);
      const c = await initCell(["--max-projects", "3"]);
      const r = await admitMany(await openedStore(c.state), [
        wanted("a", "w1"),
        wanted("b", "w1"), // the workspace's cap, 1, is held by a
        wanted("c", "w2", ["requests_per_sec=999999"]), // over the ceiling
        wanted("system", "w3"), // reserved
        wanted("d", "w4"),
        wanted("a", "w5"), // the same id again
        wanted("e", "w6"),
        wanted("f", "w7"), // the cell's cap, 3, is held by a, d and e
      ]);
      expect(ids(r)).toEqual(["a", "d", "e"]);
      const k = readState(c.state).cell.ceilings.requests_per_sec;
      expect(refusals(r)).toEqual([
        ["b", "workspace w1 already has 1 project(s) on cell cell_sc1 (cap 1)"],
        ["c", `--quota requests_per_sec=999999 exceeds the cell's ceiling ${k} (bound / k, k = 8)`],
        ["system", "project system is reserved on cell cell_sc1"],
        ["a", "project id a was used before: an id is never placed twice"],
        ["f", "cell cell_sc1 already holds its 3 projects"],
      ]);
      const st = readState(c.state);
      expect(Object.keys(st.projects).sort()).toEqual(["a", "d", "e", canaryProjectId("cell_sc1")].sort());
      expect(denylistText(c.state)).toBe("a\nd\ne\n");
      expect(st.journal.filter((j) => j.command === "admit").length).toBe(3);
      for (const q of QUOTA_FIELDS) expect(st.projects.a.quotas[q]).toBeDefined();
    });

    test("holds the packing rule across the batch: the heavy projects' weights sum to at most k - 1", async () => {
      useStore(kind);
      const c = await initCell();
      const r = await admitMany(await openedStore(c.state), [
        wanted("h1", "w1", [], ["request=4"]),
        wanted("h2", "w2", [], ["request=3"]),
        wanted("h3", "w3", [], ["request=2"]), // 4 + 3 + 2 > 7
        wanted("u", "w4"),
      ]);
      expect(ids(r)).toEqual(["h1", "h2", "u"]);
      expect(refusals(r)).toEqual([
        ["h3", "--weight request=2: the cell's heavy projects would hold 9 request_weight units, over k - 1 = 7 (they hold 7)"],
      ]);
      expect(readState(c.state).projects.h2.quotas.request_weight).toBe(3);
    });

    test("does nothing when nothing is placed: no generation, no burn, no journal", async () => {
      useStore(kind);
      const c = await initCell();
      const [state, bundle] = [readState(c.state), readBundleText(c.state)];
      const store = await openedStore(c.state);
      expect(await admitMany(store, [])).toEqual({ placed: [], failed: [] });
      const r = await admitMany(store, [wanted("system")]);
      expect(ids(r)).toEqual([]);
      expect(readState(c.state)).toEqual(state);
      expect(readBundleText(c.state)).toBe(bundle);
      expect((await store.readDenylist()).ids.size).toBe(0);
    });

    test("spills the journal past its cap into one audit object, losing no entry", async () => {
      useStore(kind);
      const c = await initCell();
      const projects = Array.from({ length: 150 }, (_, i) => `p${i}`);
      const r = await admitMany(await openedStore(c.state), projects.map((p) => wanted(p)));
      expect(r.failed).toEqual([]);
      const st = readState(c.state);
      expect(st.journal.length).toBe(100);
      const audits = auditTexts(c.state).map((t) => JSON.parse(t));
      expect(audits.length).toBe(1);
      const spilled = audits[0].entries as { command: string; detail: string }[];
      expect([...spilled, ...st.journal].filter((j) => j.command === "admit").map((j) => j.detail.split(" ")[0])).toEqual(projects);
    });

    test("refuses a state behind its bundle, as every command does", async () => {
      useStore(kind);
      const c = await initCell();
      const old = readState(c.state);
      const store = await openedStore(c.state);
      await admitMany(store, [wanted("a")]);
      writeStateText(c.state, `${JSON.stringify(old)}\n`);
      await expect(admitMany(store, [wanted("b")])).rejects.toThrow("the state was restored from a stale copy");
      expect(denylistText(c.state)).toBe("a\n");
    });
  });
}

describe("admitMany against another writer", () => {
  test("re-applies on a lost compare-and-swap: each project once, each id burned once", async () => {
    useStore("s3");
    const c = await initCell();
    const store = await openedStore(c.state);
    let raced = false;
    fakeS3().conflictOnce = (key) => {
      if (raced || !key.endsWith("cell-state.json")) return false;
      raced = true;
      return true;
    };
    const r = await admitMany(store, [wanted("a"), wanted("b")]);
    expect([raced, ids(r), r.failed]).toEqual([true, ["a", "b"], []]);
    const st = readState(c.state);
    expect(Object.keys(st.projects).sort()).toEqual(["a", "b", canaryProjectId("cell_sc1")].sort());
    expect(Object.values(st.credentials).map((x) => x.seq).sort()).toEqual([1, 2, 3]);
    expect(st.journal.filter((j) => j.command === "admit").length).toBe(2);
    expect(denylistText(c.state)).toBe("a\nb\n");
  });
});

describe("burnIds", () => {
  for (const kind of STORES) {
    test(`burns every id or none, in one write, on the ${kind} store`, async () => {
      useStore(kind);
      const c = await initCell();
      const store = await openedStore(c.state);
      await store.load();
      await burnIds(store, ["x", "y"]);
      expect(denylistText(c.state)).toBe("x\ny\n");
      await expect(burnIds(store, ["z", "y"])).rejects.toThrow("project id y was used before: an id is never placed twice");
      expect(denylistText(c.state)).toBe("x\ny\n");
      await burnIds(store, ["z"]);
      expect(denylistText(c.state)).toBe("x\ny\nz\n");
    });
  }
});
