import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteDurableStore } from "../src/db/db";
import { Result } from "better-result";

describe("wal append", () => {
  test("append assigns contiguous offsets and updates counters", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-wal-"));
    try {
      const db = new SqliteDurableStore(`${root}/wal.sqlite`);
      db.ensureStream("s", null);
      const res = db.appendWalRows({
        stream: "s",
        startOffset: 0n,
        baseAppendMs: 1n,
        rows: [
          { routingKey: null, contentType: null, payload: new Uint8Array([1]), appendMs: 1n },
          { routingKey: null, contentType: null, payload: new Uint8Array([2, 3]), appendMs: 2n },
        ],
      });
      expect(Result.isOk(res)).toBe(true);
      const s = db.getStream("s")!;
      expect(s.next_offset).toBe(2n);
      expect(s.pending_rows).toBe(2n);
      expect(s.pending_bytes).toBe(3n);
      expect(s.logical_size_bytes).toBe(3n);
      expect(s.wal_rows).toBe(2n);
      expect(s.wal_bytes).toBe(3n);
      db.close();
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
