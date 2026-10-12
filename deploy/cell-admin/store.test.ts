// The state store and the one write path every change takes (public-beta
// issuer triage B1.1 and B1.3). Run with `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { BundlePublisher, burnId, mutate, unversionedChanges } from "./publish";
import { stateText } from "./state";
import { Conflict, DirStore } from "./store";
import { cleanup, initCell, readBundle, readState } from "./test-rig";

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
