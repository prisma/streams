import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SqliteDurableStore } from "../src/db/db";
import { SchemaRegistryStore } from "../src/schema/registry";

describe("schema registry", () => {
  test("install schema then upgrade with lens", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-schema-"));
    try {
      const db = new SqliteDurableStore(`${root}/wal.sqlite`);
      db.ensureStream("s", null);
      const regStore = new SchemaRegistryStore(db);

      const v1 = {
        type: "object",
        properties: { a: { type: "string" } },
        required: ["a"],
        additionalProperties: false,
      };

      let reg = regStore.updateRegistry("s", db.getStream("s")!, { schema: v1 });
      expect(reg.currentVersion).toBe(1);
      expect(reg.boundaries[0].offset).toBe(0);

      const v2 = {
        type: "object",
        properties: { a: { type: "string" }, b: { type: "object", additionalProperties: false } },
        required: ["a", "b"],
        additionalProperties: false,
      };

      const lens = {
        apiVersion: "durable.lens/v1",
        schema: "s",
        from: 1,
        to: 2,
        ops: [{ op: "add", path: "/b", schema: { type: "object" } }],
      };

      reg = regStore.updateRegistry("s", db.getStream("s")!, { schema: v2, lens });
      expect(reg.currentVersion).toBe(2);
      const storedLens = reg.lenses["1"];
      expect(storedLens.ops[0].default).toEqual({});
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
