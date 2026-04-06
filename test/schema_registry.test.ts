import { describe, test, expect } from "bun:test";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Result } from "better-result";
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

      reg = regStore.updateSearch("s", {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            exact: true,
            column: true,
            exists: true,
            sortable: true,
          },
          service: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/service" }],
            normalizer: "lowercase_v1",
            exact: true,
            prefix: true,
            exists: true,
          },
          status: {
            kind: "integer",
            bindings: [{ version: 1, jsonPointer: "/status" }],
            exact: true,
            column: true,
            exists: true,
          },
        },
      });
      expect(reg.search).toEqual({
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            exact: true,
            column: true,
            exists: true,
            sortable: true,
          },
          service: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/service" }],
            normalizer: "lowercase_v1",
            exact: true,
            prefix: true,
            exists: true,
          },
          status: {
            kind: "integer",
            bindings: [{ version: 1, jsonPointer: "/status" }],
            exact: true,
            column: true,
            exists: true,
          },
        },
      });

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
      expect(reg.search).toEqual({
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            exact: true,
            column: true,
            exists: true,
            sortable: true,
          },
          service: {
            kind: "keyword",
            bindings: [{ version: 1, jsonPointer: "/service" }],
            normalizer: "lowercase_v1",
            exact: true,
            prefix: true,
            exists: true,
          },
          status: {
            kind: "integer",
            bindings: [{ version: 1, jsonPointer: "/status" }],
            exact: true,
            column: true,
            exists: true,
          },
        },
      });
      const storedLens = reg.lenses["1"];
      expect(storedLens.ops[0].default).toEqual({});
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });

  test("rejects search-only updates before a schema version exists", () => {
    const root = mkdtempSync(join(tmpdir(), "ds-schema-search-"));
    try {
      const db = new SqliteDurableStore(`${root}/wal.sqlite`);
      db.ensureStream("s", null);
      const regStore = new SchemaRegistryStore(db);

      const res = regStore.updateSearchResult("s", {
        primaryTimestampField: "eventTime",
        fields: {
          eventTime: {
            kind: "date",
            bindings: [{ version: 1, jsonPointer: "/eventTime" }],
            column: true,
            exists: true,
            sortable: true,
          },
        },
      });
      expect(Result.isError(res)).toBe(true);
      if (Result.isOk(res)) return;
      expect(res.error.message).toContain("installed schema version");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
});
