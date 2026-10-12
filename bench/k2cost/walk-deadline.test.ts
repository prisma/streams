// A walk phase whose deadline falls while pages are in flight (Layer B's
// h3 and h3k walk the beginning again and again until the run ends): every
// page the fake cell metered must be counted in the project's ledger, so
// usage still equals the ledger. Run with `bun test ./bench/k2cost`.
import { afterAll, beforeAll, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { FakeCell } from "./fake-cell.ts";
import { type Manifest, expand } from "./tenant-plan.ts";
import { run } from "./tenants.ts";

let cell: FakeCell;
const dir = mkdtempSync(join(tmpdir(), "k2-walk-deadline-"));
beforeAll(() => {
  cell = new FakeCell();
});
afterAll(() => {
  cell.stop();
  rmSync(dir, { recursive: true, force: true });
});

test("a walk whose phase ends mid-page still counts every page the cell metered", async () => {
  const m: Manifest = {
    name: "walkend", seed: 9, duration: 3.2,
    classes: [
      { class: "scan", count: 2, role: "hostile", streams: 2, phases: [
        { mode: "setup", at: 0 },
        { mode: "produce", at: 0.1, secs: 0.6, args: { rate: 100, records: 4, "record-bytes": 512 } },
        // Pages of 4 KiB, each sent 150 ms after the cell metered it: a walk
        // of the ~240 records takes far longer than its 1.6 s phase.
        { mode: "walk", at: 1.2, secs: 1.6, repeat: true, args: { parallel: 2, readers: 2, "max-bytes": 4096 } },
      ] },
    ],
  };
  const plan = expand(m, "sha");
  const map = join(dir, "tokens.json");
  writeFileSync(map, JSON.stringify(plan.projects.map((p) => ({ project: p.id, token: `w-${p.id}` }))));
  for (const p of plan.projects) cell.tokens.set(`w-${p.id}`, p.id);
  cell.pageDelayMs = 150;
  const out = join(dir, "run");
  try {
    await run(plan, cell.base, map, out, 1);
  } finally {
    cell.pageDelayMs = 0;
  }
  for (const p of plan.projects) {
    const t = JSON.parse(readFileSync(join(out, "projects", p.id, "ledger.json"), "utf8")).totals;
    const metered = cell.meters.get(p.id)!;
    expect(t.delivered_records).toBeGreaterThan(0);
    expect([p.id, metered.readRecords, metered.readPayloadBytes]).toEqual([p.id, t.delivered_records, t.delivered_payload_bytes]);
  }
}, 30_000);
