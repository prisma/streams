// The issuer's copy of the state (public-beta issuer triage B2.4).
// Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { bundleOf, bundleText } from "../cell-admin/feeds";
import { cleanup, readBundle, readBundleText, readState, readStateText, writeStateText } from "../cell-admin/test-rig";
import { boot } from "./boot";
import { MAX_AGE_MS, StateCache } from "./cache";
import { readConfig } from "./config";
import { CELL, cellState, customerSecret, fakeS3, issuerCell } from "./rig";

afterEach(cleanup);

const stateReads = () =>
  fakeS3().requests.filter((q) => q.method === "GET" && q.key.endsWith(`/cells/${CELL}/cell-state.json`));

describe("the state cache", () => {
  test("takes a change cell-admin committed at its next revalidation, and an unchanged state costs a 304", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    const cache = new StateCache(b.store, b);
    await cache.refresh();
    expect(stateReads().at(-1)?.status).toBe(304);
    const { id } = await customerSecret(cell, "sp_a");
    expect(cache.state.credentials[id]).toBeUndefined();
    await cache.refresh();
    expect(stateReads().at(-1)?.status).toBe(200);
    expect(cache.state.credentials[id]?.project_id).toBe("sp_a");
    expect(cache.state.feed_version).toBe(cellState(cell).feed_version);
    await cache.refresh();
    expect(stateReads().at(-1)?.status).toBe(304);
  });

  test("ages while revalidation fails, is not fresh past 300 s, and is fresh again once one succeeds", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    let t = 1_000_000;
    const log: string[] = [];
    const cache = new StateCache(b.store, b, { now: () => t, log: (l) => log.push(l) });
    fakeS3().failReads = true;
    t += MAX_AGE_MS;
    await cache.refresh();
    expect([cache.ageMs(), cache.fresh()]).toEqual([300_000, true]);
    t += 1_000;
    expect([cache.ageMs(), cache.fresh()]).toEqual([301_000, false]);
    expect(log.at(-1)).toBe(
      `state revalidation failed (copy 300 s old): GET s3://${cell.env.STREAMS_CONTROL_S3_BUCKET}/cells/${CELL}/` +
        "cell-state.json answered 500 InternalError",
    );
    fakeS3().failReads = false;
    await cache.refresh();
    expect([cache.ageMs(), cache.fresh()]).toEqual([0, true]);
  });

  test("revalidates on its interval once started, until stopped", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    const cache = new StateCache(b.store, b, { refreshMs: 20 });
    const stop = cache.start();
    try {
      await customerSecret(cell, "sp_a");
      const deadline = Date.now() + 2_000;
      while (cache.state.projects.sp_a === undefined && Date.now() < deadline) await Bun.sleep(10);
      expect(cache.state.projects.sp_a?.workspace_id).toBe("ws_sp_a");
    } finally {
      stop();
    }
    await Bun.sleep(50); // a revalidation in flight at the stop lands
    const reads = stateReads().length;
    await Bun.sleep(100);
    expect(stateReads().length).toBe(reads);
  });
});

describe("the bundle repair", () => {
  const bundleKey = `cells/${CELL}/feeds-bundle.json`;
  const bundlePuts = () => fakeS3().requests.filter((q) => q.method === "PUT" && q.key.endsWith(bundleKey));

  /// A cell whose last writer committed the state and stopped before it
  /// published: the bundle is the one before that commit.
  async function stoppedWriter() {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    const log: string[] = [];
    const cache = new StateCache(b.store, b, { log: (l) => log.push(l) });
    await cache.refresh();
    const before = readBundleText(cell.state);
    const { id } = await customerSecret(cell, "sp_a");
    fakeS3().put(cell.env.STREAMS_FEEDS_S3_BUCKET, bundleKey, before);
    return { cell, cache, log, id };
  }

  test("republishes the bundle a state is ahead of, as after a writer that stopped between its commit and its publish", async () => {
    const { cell, cache, log, id } = await stoppedWriter();
    expect(readBundle(cell.state).keys.feed_version).toBe(1);
    await cache.refresh();
    const state = readState(cell.state);
    expect(readBundleText(cell.state)).toBe(bundleText(bundleOf(state)));
    expect((readBundle(cell.state).grants.credentials as { credential_id: string }[]).map((c) => c.credential_id))
      .toContain(id);
    expect([cache.publishedVersion, state.feed_version]).toEqual([3, 3]);
    expect(log.at(-1)).toBe("republished the bundle at feed_version 3 (it was at 1): the state's writer had not published it");
  });

  test("leaves a bundle as new as the state alone, and reads it only when the state moved past it", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    const cache = new StateCache(b.store, b, { log: () => {} });
    await cache.refresh();
    await customerSecret(cell, "sp_a");
    const bundleReads = () => fakeS3().requests.filter((q) => q.method === "GET" && q.key.endsWith(bundleKey)).length;
    const [puts, reads] = [bundlePuts().length, bundleReads()];
    await cache.refresh(); // the state moved: one look at the bundle, which is current
    await cache.refresh(); // 304: no look
    expect([bundlePuts().length, bundleReads(), cache.publishedVersion]).toEqual([puts, reads + 1, 3]);
  });

  test("tries a republish that failed again at the next revalidation, though the state is unchanged", async () => {
    const { cell, cache, log } = await stoppedWriter();
    fakeS3().denyWrites = true;
    await cache.refresh();
    expect(readBundle(cell.state).keys.feed_version).toBe(1);
    expect(log.at(-1)).toMatch(/^republishing the bundle at feed_version 3 failed: PUT s3:\/\/.*feeds-bundle.json answered 403 AccessDenied$/);
    fakeS3().denyWrites = false;
    await cache.refresh();
    expect(stateReads().at(-1)?.status).toBe(304);
    expect(readBundle(cell.state).keys.feed_version).toBe(3);
  });
});

describe("the issuer's own changes", () => {
  test("take effect in the copy at once, cost no state read, and the next revalidation is a 304", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    const cache = new StateCache(b.store, b, { log: () => {} });
    const reads = stateReads().length;
    await cache.mutate((s) => {
      s.journal.push({ at: "t", command: "mine", detail: "" });
    });
    expect([stateReads().length, cache.state.journal.at(-1)?.command]).toEqual([reads, "mine"]);
    expect(readState(cell.state).journal.at(-1)?.command).toBe("mine");
    await cache.refresh();
    expect(stateReads().at(-1)?.status).toBe(304);
  });

  test("a stored state behind the copy is not taken: the revalidation fails and the copy ages", async () => {
    const cell = await issuerCell();
    const b = await boot(readConfig(cell.env));
    let t = 1_000_000;
    const log: string[] = [];
    const cache = new StateCache(b.store, b, { now: () => t, log: (l) => log.push(l) });
    const old = readStateText(cell.state);
    await customerSecret(cell, "sp_a");
    await cache.refresh();
    expect(cache.state.feed_version).toBe(3);
    writeStateText(cell.state, old);
    t += 1_000;
    await cache.refresh();
    expect([cache.state.feed_version, cache.ageMs(), cache.revalidationFailures]).toEqual([3, 1_000, 1]);
    expect(log.at(-1)).toBe(
      "state revalidation failed (copy 1 s old): the stored state is at feed_version 1, behind this copy's 3: " +
        "was it restored from a stale copy?",
    );
  });
});
