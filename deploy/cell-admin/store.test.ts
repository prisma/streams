// The state store and the one write path every change takes (public-beta
// issuer triage B1.1 and B1.3). Run with `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { chmodSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { BundlePublisher, burnId, mutate, unversionedChanges } from "./publish";
import { stateText } from "./state";
import { readS3Credentials, S3Store } from "./s3store";
import { Conflict, DirStore, type StateStore } from "./store";
import {
  cleanup,
  credentialsOf,
  customerCredential,
  customerOnly,
  denylistText,
  fakeS3,
  freshState,
  initCell,
  keyFile,
  readBundle,
  readState,
  run,
  STORES,
  scratchDir,
  stateExists,
  useStore,
  writeStateText,
} from "./test-rig";

afterEach(cleanup);

describe("the directory store", () => {
  test("replaces the state only from the copy it was read as, and creates it only once", async () => {
    const c = await initCell();
    const store = new DirStore(c.state);
    const { state, etag } = await store.load();
    const next = await store.save({ ...state, feed_version: state.feed_version + 1 }, etag);
    await expect(store.save(state, etag)).rejects.toBeInstanceOf(Conflict);
    await expect(store.save(state, null)).rejects.toBeInstanceOf(Conflict);
    expect((await store.load()).etag).toBe(next);
    expect(readState(c.state).feed_version).toBe(2);
  });
});

describe("loadChanged", () => {
  for (const kind of STORES) {
    test(`answers null for the state as read, and the state another writer committed (${kind})`, async () => {
      useStore(kind);
      const c = await initCell();
      const store: StateStore =
        kind === "dir" ? new DirStore(c.state) : await S3Store.open(c.state, readS3Credentials(credentialsOf(c.state)!));
      // The status of the store's last read of the state (a bucket's).
      const lastRead = () =>
        kind === "dir" ? "file" : fakeS3().requests.filter((q) => q.method === "GET" && q.key.endsWith("/cell-state.json")).at(-1)?.status;
      const { etag } = await store.load();
      expect([await store.loadChanged(etag), lastRead()]).toEqual([null, kind === "dir" ? "file" : 304]);
      expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).code).toBe(0);
      const changed = await store.loadChanged(etag);
      expect([changed?.state.projects.proj_a?.workspace_id, changed?.state.feed_version, lastRead()]).toEqual(
        ["w", 2, kind === "dir" ? "file" : 200],
      );
      expect([await store.loadChanged(changed!.etag), lastRead()]).toEqual([null, kind === "dir" ? "file" : 304]);
    });
  }
});

describe("mutate", () => {
  test("a change that lost its compare-and-swap is applied again to the state that won", async () => {
    const c = await initCell();
    const store = new DirStore(c.state);
    let calls = 0;
    const done = await mutate(store, (state) => {
      calls += 1;
      if (calls === 1) {
        // Another writer commits between this read and its write.
        const other = readState(c.state);
        other.journal.push({ at: "t", command: "other", detail: "won" });
        writeFileSync(join(c.state, "cell-state.json"), stateText(other));
      }
      state.feed_version += 1;
      state.journal.push({ at: "t", command: "mine", detail: `attempt ${calls}` });
    });
    expect(calls).toBe(2);
    expect(readState(c.state).journal.map((j) => j.command)).toEqual(["init", "other", "mine"]);
    expect([done.state.feed_version, readBundle(c.state).keys.feed_version]).toEqual([2, 2]);
  });

  test("a refusal commits and publishes nothing", async () => {
    const c = await initCell();
    const before = readFileSync(join(c.state, "cell-state.json"), "utf8");
    await expect(mutate(new DirStore(c.state), () => {
      throw new Error("refused");
    })).rejects.toThrow("refused");
    expect(readFileSync(join(c.state, "cell-state.json"), "utf8")).toBe(before);
    expect(readBundle(c.state).keys.feed_version).toBe(1);
  });
});

describe("mutate from a copy (the issuer's)", () => {
  const s3Store = async (state: string) => S3Store.open(state, readS3Credentials(credentialsOf(state)!));
  const stateGets = () => fakeS3().requests.filter((q) => q.method === "GET" && q.key.endsWith("/cell-state.json")).length;

  test("commits from a current copy without reading the state, and returns the new ETag", async () => {
    useStore("s3");
    const c = await initCell();
    const store = await s3Store(c.state);
    const copy = await store.load();
    const gets = stateGets();
    const done = await mutate(store, (s) => {
      s.journal.push({ at: "t", command: "mine", detail: "" });
    }, copy);
    expect(stateGets()).toBe(gets);
    expect(readState(c.state).journal.at(-1)?.command).toBe("mine");
    expect((await store.load()).etag).toBe(done.etag);
    // The copy itself is not changed: the attempt works on a clone.
    expect(copy.state.journal.at(-1)?.command).toBe("init");
  });

  test("a copy another writer moved past loses its compare-and-swap, and the change is applied to the winner's state", async () => {
    useStore("s3");
    const c = await initCell();
    const store = await s3Store(c.state);
    const copy = await store.load();
    const other = readState(c.state);
    other.journal.push({ at: "t", command: "other", detail: "won" });
    writeStateText(c.state, JSON.stringify(other));
    let calls = 0;
    await mutate(store, (s) => {
      calls += 1;
      s.journal.push({ at: "t", command: "mine", detail: "" });
    }, copy);
    expect([calls, readState(c.state).journal.map((j) => j.command)]).toEqual([2, ["init", "other", "mine"]]);
  });

  test("a copy older than the published bundle starts from the stored state, not as a stale restore", async () => {
    useStore("s3");
    const c = await initCell();
    const store = await s3Store(c.state);
    const copy = await store.load();
    expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).code).toBe(0);
    let calls = 0;
    const done = await mutate(store, (s) => {
      calls += 1;
      s.journal.push({ at: "t", command: "mine", detail: "" });
    }, copy);
    expect([calls, done.state.projects.proj_a?.workspace_id, readState(c.state).journal.at(-1)?.command]).toEqual([
      1, "w", "mine",
    ]);
  });
});

describe("the bundle publisher", () => {
  test("publishes only forward: a published bundle as new as the state is kept", async () => {
    const c = await initCell();
    const store = new DirStore(c.state);
    const { state } = await store.load();
    const before = readFileSync(join(c.state, "feeds-bundle.json"), "utf8");
    expect(await new BundlePublisher(store).publish(state)).toBe("current");
    expect(await new BundlePublisher(store).publish({ ...state, feed_version: 0 })).toBe("current");
    expect(readFileSync(join(c.state, "feeds-bundle.json"), "utf8")).toBe(before);
  });

  test("a publish that lost its compare-and-swap re-reads, and keeps a winner as new as itself", async () => {
    const c = await initCell();
    const store = new DirStore(c.state);
    const { state } = await store.load();
    state.feed_version = 2;
    let raced = false;
    const racing = Object.create(store, {
      publishBundle: {
        value: async (text: string, etag: string | null) => {
          if (!raced) {
            raced = true;
            const winner = { ...readBundle(c.state) };
            for (const feed of ["keys", "policies", "grants"] as const) winner[feed] = { ...winner[feed], feed_version: 2 };
            writeFileSync(join(c.state, "feeds-bundle.json"), JSON.stringify(winner));
          }
          return store.publishBundle(text, etag);
        },
      },
    });
    expect(await new BundlePublisher(racing).publish(state)).toBe("current");
    expect(raced).toBe(true);
  });

  test("names every entry changed at an unchanged version, a key's material included", async () => {
    const c = await initCell();
    const published = readBundle(c.state);
    const ours = structuredClone(published);
    (ours.keys.keys[0] as any).pem = "-----BEGIN PUBLIC KEY-----\nAAAA\n-----END PUBLIC KEY-----\n";
    expect(unversionedChanges(published, ours)).toEqual([`key ${(published.keys.keys[0] as any).kid}`]);
    expect(unversionedChanges(published, published)).toEqual([]);
  });
});

describe("burnId", () => {
  test("appends an id once and refuses it ever after", async () => {
    const c = await initCell();
    const store = new DirStore(c.state);
    await store.load();
    await burnId(store, "proj_x");
    await expect(burnId(store, "proj_x")).rejects.toThrow("project id proj_x was used before: an id is never placed twice");
    expect(readFileSync(join(c.state, "denylist.txt"), "utf8")).toBe("proj_x\n");
  });
});

describe("the S3 store", () => {
  const init = (state: string, key: string, extra: string[] = []) =>
    run("init", "--state", state, "--cell-id", "cell_sc1", "--deployment-project", "proj_deploy",
      "--account", "acct_sink", "--key-file", key, ...extra);

  test("refuses to run on a bucket that does not honour conditional writes, and leaves nothing", async () => {
    useStore("s3");
    const root = scratchDir();
    const state = freshState(root);
    fakeS3().honourPreconditions = false;
    const r = await init(state, keyFile(root));
    expect([r.code, r.err.replace(/:\d+ /, ":<port> ")]).toEqual([1, "cell-admin init: http://127.0.0.1:<port> " +
      "bucket ctl-1 does not honour conditional writes (If-None-Match: * over an existing object answered 200, " +
      "not 412): cell-admin refuses to run without them"]);
    expect([...fakeS3().objects.keys()]).toEqual([]);
  });

  test("refuses to run with a key that cannot write the bucket, naming the refusal", async () => {
    useStore("s3");
    const root = scratchDir();
    const state = freshState(root);
    fakeS3().denyWrites = true;
    const r = await init(state, keyFile(root));
    expect([r.code, r.err.replace(/:\d+ /, ":<port> ")]).toEqual([1, "cell-admin init: http://127.0.0.1:<port> " +
      "bucket ctl-1 refused a probe write (If-None-Match: * on a new key answered 403)"]);
  });

  test("two writers on one state lose no update", async () => {
    useStore("s3");
    const c = await initCell(["--workspace-cap", "1"]);
    const admitted = await Promise.all(["a", "b", "c", "d"].map((x) =>
      run("admit", "--state", c.state, "--project", `proj_${x}`, "--workspace", `w_${x}`)));
    expect(admitted.map((r) => [r.code, r.err])).toEqual([[0, ""], [0, ""], [0, ""], [0, ""]]);
    expect(Object.keys(readState(c.state).projects).sort()).toEqual(["proj_a", "proj_b", "proj_c", "proj_d", "sp_canary_cell_sc1"]);
    const b = readBundle(c.state);
    expect([b.keys.feed_version, (customerOnly(b).policies.projects as any[]).length]).toEqual([5, 4]);
    expect(denylistText(c.state).split("\n").filter(Boolean).sort()).toEqual(["proj_a", "proj_b", "proj_c", "proj_d"]);
  });

  test("a write another writer beat is applied again to the state that won", async () => {
    useStore("s3");
    const c = await initCell();
    const [bucket] = c.state.slice("s3://".length).split("/");
    let raced = false;
    fakeS3().beforePut = (key) => {
      if (raced || key !== `${bucket}/cells/cell_sc1/cell-state.json`) return;
      raced = true;
      const other = readState(c.state);
      other.journal.push({ at: "t", command: "other", detail: "won" });
      writeStateText(c.state, JSON.stringify(other));
    };
    expect((await run("admit", "--state", c.state, "--project", "proj_a", "--workspace", "w")).code).toBe(0);
    expect(readState(c.state).journal.map((j) => j.command)).toEqual(["init", "other", "admit"]);
    expect(customerCredential(c.state, "proj_a")).toMatch(/^strcred_/);
    expect(fakeS3().requests.filter((q) => q.status === 412 && q.key.endsWith("cell-state.json"))).toHaveLength(1);
  });

  test("reads the bucket keys only from a file only its owner can read, naming the feeds bucket (D3 (a))", async () => {
    useStore("s3");
    const root = scratchDir();
    const state = freshState(root);
    const creds = credentialsOf(state)!;
    chmodSync(creds, 0o644);
    let r = await init(state, keyFile(root));
    expect(r.err).toBe(`cell-admin init: credentials file ${creds} has mode 0644: group or others can read ` +
      "the bucket keys; chmod 600 it");
    const { feeds: _feeds, ...controlOnly } = JSON.parse(readFileSync(creds, "utf8"));
    writeFileSync(creds, JSON.stringify(controlOnly), { mode: 0o600 });
    chmodSync(creds, 0o600);
    r = await init(state, keyFile(root, "rsa", 0o600, "k2.pem"));
    expect(r.err).toBe(`cell-admin init: credentials file ${creds} lacks feeds: the bundle lives in its own bucket (D3 (a))`);
    expect(stateExists(state)).toBe(false);
    r = await run("list", "--state", "s3://elsewhere/cells/cell_sc1");
    expect([r.code, r.err.split("\n")[0]]).toEqual([2,
      "cell-admin list: --state s3://elsewhere/cells/cell_sc1 needs --s3-credentials FILE"]);
  });

  test("refuses a state that is another cell's", async () => {
    useStore("s3");
    const c = await initCell();
    writeStateText(c.state, JSON.stringify({ ...readState(c.state), cell: { ...readState(c.state).cell, cell_id: "x" } }));
    const r = await run("list", "--state", c.state);
    expect(r.err).toBe(`cell-admin list: ${c.state}/cell-state.json holds cell x, not cell_sc1`);
  });
});
