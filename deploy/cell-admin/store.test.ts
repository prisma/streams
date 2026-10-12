// The state store and the one write path every change takes (public-beta
// issuer triage B1.1 and B1.3). Run with `bun test ./deploy/cell-admin`.
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { burnId, mutate } from "./publish";
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
