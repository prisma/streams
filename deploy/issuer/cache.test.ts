// The issuer's copy of the state (public-beta issuer triage B2.4).
// Run with `bun test ./deploy/issuer`.
import { afterEach, describe, expect, test } from "bun:test";
import { cleanup } from "../cell-admin/test-rig";
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
