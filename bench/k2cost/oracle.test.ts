// The leak oracle (oracle.ts) on every read path k2gen's modules have:
// pages (walk), long-polls and SSE (tail), consumer pulls (group), churn's
// read-back and the audit. Each path reads a stream holding the reader's
// own oracle records plus one record of another project (a leak) and one
// of its own with a changed byte (corruption), and must count exactly
// those. Run with `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { HeaderSource, Http, Meter, Opts, Reporter, deadlineSignal } from "./common.ts";
import { PayloadGen } from "./corpus.ts";
import { FakeCell } from "./fake-cell.ts";
import { ownerOf, verifyOracle } from "./oracle.ts";
import { audit } from "./audit.ts";
import { churn } from "./churn.ts";
import { group, tail, walk } from "./consume.ts";
import { produce, setup, type Ctx } from "./produce.ts";

const A = ownerOf("run-secret", "proj_a");
const B = ownerOf("run-secret", "proj_b");
let cell: FakeCell;
const dir = mkdtempSync(join(tmpdir(), "k2-oracle-"));

beforeAll(() => {
  cell = new FakeCell();
  cell.tokens.set("tok-a", "proj_a");
  cell.tokens.set("tok-b", "proj_b");
});
afterAll(() => {
  cell.stop();
  rmSync(dir, { recursive: true, force: true });
});

function ctx(token: string, owner: typeof A | undefined, flags: Record<string, string>, streams = 2): Ctx {
  const meter = new Meter();
  const hdrs = new HeaderSource(undefined, { authorization: `Bearer ${token}`, "streams-encryption-key": "k" });
  return {
    http: new Http(cell.base, hdrs, meter), meter, o: new Opts(new Map(Object.entries(flags))),
    prefix: flags.prefix ?? "o/", streams, seed: 7, duration: 5, owner,
  };
}

const counts = (m: Meter) =>
  Object.fromEntries(["ok", "foreign", "bad_mac", "unmarked"].map((k) => [k, m.tot.get(`oracle_${k}`)]));

test("an oracle record verifies only for its own project and only intact", () => {
  const g1 = new PayloadGen("oracle", 300, 1, 3, A);
  const g2 = new PayloadGen("oracle", 300, 1, 3, A);
  const recs = Array.from({ length: 50 }, () => g1.json());
  expect(Array.from({ length: 50 }, () => g2.json())).toEqual(recs);
  for (const r of recs) {
    expect(Math.abs(r.length - 300)).toBeLessThanOrEqual(3);
    expect(verifyOracle(A, JSON.parse(r))).toBe("ok");
    expect(verifyOracle(B, JSON.parse(r))).toBe("foreign");
  }
  const changed = JSON.parse(recs[0]!);
  changed.data = (changed.data[0] === "A" ? "B" : "A") + changed.data.slice(1);
  expect(verifyOracle(A, changed)).toBe("bad_mac");
  const forged = { ...JSON.parse(recs[1]!), o: "proj_b" };
  expect(verifyOracle(B, forged)).toBe("bad_mac");
  expect(verifyOracle(A, JSON.parse(new PayloadGen("b64rand", 300, 1, 3).json()))).toBe("unmarked");
  expect(() => new PayloadGen("oracle", 300, 1, 3)).toThrow("needs an owning project");
});

test("every read path counts a leaked and a corrupted record, and audit flags both", async () => {
  // Proj A writes 40 oracle records over two streams; B writes into its own
  // stream of the same name; then one of B's records and one tampered copy
  // of A's land in A's stream o/00000.
  const prod = ctx("tok-a", A, { rate: "200", payload: "oracle", "record-bytes": "256", prefix: "o/" });
  expect(await setup(ctx("tok-a", A, { prefix: "o/" }))).toMatchObject({ created: 2, failed: 0 });
  await setup(ctx("tok-b", B, { prefix: "o/" }, 1));
  const rep = new Reporter("produce", prod.meter, undefined, undefined, 10, prod.o.asObject(), undefined);
  const deadline = new AbortController();
  const running = produce(prod, deadline.signal);
  while (prod.meter.tot.get("acked_records") < 40) await Bun.sleep(5);
  deadline.abort();
  const result = await running;
  const ledger = { ...rep.totals(true), prefix: "o/", streams: 2, seed: 7, ...result };
  const ledgerFile = join(dir, "a.ledger.json");
  writeFileSync(ledgerFile, JSON.stringify(ledger));
  const own = cell.ns("proj_a").get("o/00000")!;
  const acked = cell.ns("proj_a").get("o/00000")!.length + cell.ns("proj_a").get("o/00001")!.length;
  expect(acked).toBe(Number(ledger.acked_records));
  const tampered = JSON.parse(own[0]!);
  tampered.data = (tampered.data[0] === "A" ? "B" : "A") + tampered.data.slice(1);
  cell.plant("proj_a", "o/00000", new PayloadGen("oracle", 256, 99, 3, B).json());
  cell.plant("proj_a", "o/00000", JSON.stringify(tampered));
  const want = { ok: acked, foreign: 1, bad_mac: 1, unmarked: 0 };

  const w = ctx("tok-a", A, { prefix: "o/" });
  await walk(w, deadlineSignal(10));
  expect(counts(w.meter)).toEqual(want);

  for (const via of ["long-poll", "sse"]) {
    const t = ctx("tok-a", A, { prefix: "o/", via, from: "beginning", "wait-ms": "200" });
    const stopAt = new AbortController();
    const done = tail(t, stopAt.signal);
    const until = Date.now() + 5000;
    while (t.meter.tot.get("delivered_records") < acked + 2 && Date.now() < until) await Bun.sleep(10);
    stopAt.abort();
    await done;
    expect([via, counts(t.meter)]).toEqual([via, want]);
  }

  const g = ctx("tok-a", A, { prefix: "o/", pull: "10", "until-empty": "1", "wait-ms": "50" });
  await group(g, deadlineSignal(10));
  expect(counts(g.meter)).toEqual(want);

  const c = ctx("tok-a", A, { prefix: "c/", count: "3", "size-kib": "2", "record-bytes": "512", payload: "oracle" });
  expect(await churn(c, deadlineSignal(10))).toMatchObject({ churn: { completed: 3, failed: 0 } });
  expect(counts(c.meter)).toEqual({ ok: 12, foreign: 0, bad_mac: 0, unmarked: 0 });

  const au = ctx("tok-a", A, { prefix: "o/", expect: ledgerFile });
  const v = (await audit(au, deadlineSignal(10))).verify as Record<string, unknown>;
  expect([v.ok, v.unexpected_records, v.read_records, v.expected_records]).toEqual([false, 2, acked + 2, acked]);
  expect(counts(au.meter)).toEqual(want);
  await expect(audit(ctx("tok-b", B, { prefix: "o/", expect: ledgerFile }), deadlineSignal(10)))
    .rejects.toThrow("written by owner proj_a, audited as proj_b");
});

test("a page or a pull whose body does not parse counts as a corrupt delivery, never as nothing", async () => {
  const C = ownerOf("run-secret", "proj_c");
  cell.tokens.set("tok-c", "proj_c");
  expect(await setup(ctx("tok-c", C, { prefix: "u/" }, 1))).toMatchObject({ created: 1, failed: 0 });
  // A record cut short makes the whole page `[{"o":"proj_c","id":` invalid JSON.
  cell.plant("proj_c", "u/00000", '{"o":"proj_c","id":');
  const w = ctx("tok-c", C, { prefix: "u/" }, 1);
  await walk(w, deadlineSignal(10));
  expect([counts(w.meter), w.meter.tot.get("bad_json")]).toEqual([{ ok: 0, foreign: 0, bad_mac: 0, unmarked: 1 }, 1]);
});
