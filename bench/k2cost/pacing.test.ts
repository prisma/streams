// k2gen's pacing flags against the product-surface fake (fake-cell.ts).
// A project admitted at the beta's free tier (requests_per_sec 10) holds
// one second of that rate in its request bucket and in its store budget,
// and a create weighs 10 of the budget's 10 a second (src/quota/bucket.rs,
// src/quota/parked.rs, src/store_timing.rs): setup --stagger spaces a
// project's creates so none meets the budget in debt. Run with
// `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { HeaderSource, Http, Meter, Opts } from "./common.ts";
import { FakeCell } from "./fake-cell.ts";
import { setup, type Ctx } from "./produce.ts";
import { type Manifest, checkManifest } from "./tenant-plan.ts";

let cell: FakeCell;
beforeAll(() => {
  cell = new FakeCell();
  cell.tokens.set("tok-p", "proj_p");
});
afterAll(() => cell.stop());

function ctx(flags: Record<string, string>, streams: number): Ctx {
  const meter = new Meter();
  const hdrs = new HeaderSource(undefined, { authorization: "Bearer tok-p", "prisma-encryption-key": "k" });
  return { http: new Http(cell.base, hdrs, meter), meter, o: new Opts(new Map(Object.entries(flags))), prefix: flags.prefix!, streams, seed: 5, duration: 10 };
}

/** When the traced requests with this method and path arrived (ms). */
const arrivals = (method: string, path: RegExp): number[] =>
  cell.trace!.filter(([, m, p]) => m === method && path.test(p)).map(([, , , t]) => t);
const gaps = (ts: number[]): number[] => ts.slice(1).map((t, i) => Math.round(t - ts[i]!));

test("setup --stagger creates a project's streams one at a time, each at least SECS after the last", async () => {
  cell.trace = [];
  const r = await setup(ctx({ prefix: "s/", stagger: "0.25" }, 4));
  const puts = arrivals("PUT", /^\/v1\/streams\/s\/\d+$/);
  cell.trace = null;
  expect([r.created, r.failed, r.names]).toEqual([4, 0, ["s/00000", "s/00001", "s/00002", "s/00003"]]);
  expect(puts).toHaveLength(4);
  // 250 ms apart at the client; the fake, in this process, sees each
  // arrival as late as the shared event loop lets it (unpaced: under 5 ms).
  for (const g of gaps(puts)) expect(g).toBeGreaterThanOrEqual(200);
  const m: Manifest = {
    name: "pace", seed: 1, duration: 10,
    classes: [{ class: "free", count: 1, role: "compliant", streams: 4, phases: [{ mode: "setup", at: 0, args: { stagger: 1 } }] }],
  };
  expect(() => checkManifest(m)).not.toThrow();
}, 20_000);
