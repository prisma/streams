import { describe, test, expect } from "bun:test";
import { validateLensAgainstSchemas } from "../src/schema/proof";

function schema(json: string): any {
  return JSON.parse(json);
}

function expectPass(oldSchema: any, newSchema: any, lens: any): void {
  expect(() => validateLensAgainstSchemas(oldSchema, newSchema, lens)).not.toThrow();
}

function expectFail(oldSchema: any, newSchema: any, lens: any): void {
  expect(() => validateLensAgainstSchemas(oldSchema, newSchema, lens)).toThrow();
}

describe("schema proof", () => {
  test("rename + wrap passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignee"],
      "properties":{
        "id":{"type":"integer"},
        "assignee":{"type":"string"}
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignees"],
      "properties":{
        "id":{"type":"integer"},
        "assignees":{"type":"array","items":{"type":"string"}}
      }
    }`);
    const lens = {
      ops: [
        { op: "rename", from: "/assignee", to: "/assignees" },
        { op: "wrap", path: "/assignees", mode: "singleton" },
      ],
    };
    expectPass(v1, v2, lens);
  });

  test("missing wrap is rejected", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignee"],
      "properties":{
        "id":{"type":"integer"},
        "assignee":{"type":"string"}
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignees"],
      "properties":{
        "id":{"type":"integer"},
        "assignees":{"type":"array","items":{"type":"string"}}
      }
    }`);
    const lens = { ops: [{ op: "rename", from: "/assignee", to: "/assignees" }] };
    expectFail(v1, v2, lens);
  });

  test("add default checked against enum", () => {
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignees"],
      "properties":{
        "id":{"type":"integer"},
        "assignees":{"type":"array","items":{"type":"string"}}
      }
    }`);
    const v3 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","assignees","status"],
      "properties":{
        "id":{"type":"integer"},
        "assignees":{"type":"array","items":{"type":"string"}},
        "status":{"type":"string","enum":["todo","done"]}
      }
    }`);
    expectPass(v2, v3, { ops: [{ op: "add", path: "/status", schema: { type: "string" }, default: "todo" }] });
    expectFail(v2, v3, { ops: [{ op: "add", path: "/status", schema: { type: "string" }, default: "oops" }] });
  });

  test("hoist passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","meta"],
      "properties":{
        "id":{"type":"integer"},
        "meta":{
          "type":"object",
          "additionalProperties": false,
          "required":["owner"],
          "properties":{"owner":{"type":"string"}}
        }
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id","meta","owner"],
      "properties":{
        "id":{"type":"integer"},
        "owner":{"type":"string"},
        "meta":{"type":"object","additionalProperties": false,"properties":{}}
      }
    }`);
    const lens = { ops: [{ op: "hoist", host: "/meta", name: "owner", to: "/owner" }] };
    expectPass(v1, v2, lens);
  });

  test("plunge createHost passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["id"],
      "properties":{
        "id":{"type":"integer"},
        "meta":{"type":"object","additionalProperties": false}
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["meta"],
      "properties":{
        "meta":{
          "type":"object",
          "additionalProperties": false,
          "required":["id"],
          "properties":{"id":{"type":"integer"}}
        }
      }
    }`);
    const lens = { ops: [{ op: "plunge", from: "/id", host: "/meta", name: "id", createHost: true }] };
    expectPass(v1, v2, lens);
  });

  test("head passes and rejects missing minItems", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["labels"],
      "properties":{
        "labels":{"type":"array","items":{"type":"string"},"minItems":1}
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["labels"],
      "properties":{"labels":{"type":"string"}}
    }`);
    const okLens = { ops: [{ op: "head", path: "/labels" }] };
    expectPass(v1, v2, okLens);

    const badV1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["labels"],
      "properties":{
        "labels":{"type":"array","items":{"type":"string"}}
      }
    }`);
    expectFail(badV1, v2, okLens);
  });

  test("convert cases", () => {
    const cases = [
      {
        name: "builtin_total",
        oldSchema: schema(`{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string"}}}`),
        newSchema: schema(`{"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string"}}}`),
        lens: {
          ops: [
            {
              op: "convert",
              path: "/name",
              fromType: "string",
              toType: "string",
              forward: { builtin: "lowercase" },
              backward: { builtin: "uppercase" },
            },
          ],
        },
        shouldFail: false,
      },
      {
        name: "builtin_non_total_rejected",
        oldSchema: schema(`{"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"string"}}}`),
        newSchema: schema(`{"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"integer"}}}`),
        lens: {
          ops: [
            {
              op: "convert",
              path: "/code",
              fromType: "string",
              toType: "integer",
              forward: { builtin: "string_to_int" },
              backward: { builtin: "int_to_string" },
            },
          ],
        },
        shouldFail: true,
      },
      {
        name: "builtin_enum_ok",
        oldSchema: schema(`{"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"string","enum":["1","2"]}}}`),
        newSchema: schema(`{"type":"object","additionalProperties":false,"required":["code"],"properties":{"code":{"type":"integer"}}}`),
        lens: {
          ops: [
            {
              op: "convert",
              path: "/code",
              fromType: "string",
              toType: "integer",
              forward: { builtin: "string_to_int" },
              backward: { builtin: "int_to_string" },
            },
          ],
        },
        shouldFail: false,
      },
      {
        name: "map_default_ok",
        oldSchema: schema(`{"type":"object","additionalProperties":false,"required":["status"],"properties":{"status":{"type":"string"}}}`),
        newSchema: schema(`{"type":"object","additionalProperties":false,"required":["status"],"properties":{"status":{"type":"string"}}}`),
        lens: {
          ops: [
            {
              op: "convert",
              path: "/status",
              fromType: "string",
              toType: "string",
              forward: { map: { A: "active", B: "blocked" }, default: "unknown" },
              backward: { builtin: "uppercase" },
            },
          ],
        },
        shouldFail: false,
      },
      {
        name: "map_no_default_rejected",
        oldSchema: schema(`{"type":"object","additionalProperties":false,"required":["status"],"properties":{"status":{"type":"string"}}}`),
        newSchema: schema(`{"type":"object","additionalProperties":false,"required":["status"],"properties":{"status":{"type":"string"}}}`),
        lens: {
          ops: [
            {
              op: "convert",
              path: "/status",
              fromType: "string",
              toType: "string",
              forward: { map: { A: "active", B: "blocked" } },
              backward: { builtin: "uppercase" },
            },
          ],
        },
        shouldFail: true,
      },
    ];

    for (const tc of cases) {
      if (tc.shouldFail) expectFail(tc.oldSchema, tc.newSchema, tc.lens);
      else expectPass(tc.oldSchema, tc.newSchema, tc.lens);
    }
  });

  test("in + rename/add passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["profile"],
      "properties":{
        "profile":{
          "type":"object",
          "additionalProperties": false,
          "required":["name"],
          "properties":{"name":{"type":"string"}}
        }
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["profile"],
      "properties":{
        "profile":{
          "type":"object",
          "additionalProperties": false,
          "required":["full_name","age"],
          "properties":{"full_name":{"type":"string"},"age":{"type":"integer"}}
        }
      }
    }`);
    const lens = {
      ops: [
        {
          op: "in",
          path: "/profile",
          ops: [
            { op: "rename", from: "/name", to: "/full_name" },
            { op: "add", path: "/age", schema: { type: "integer" }, default: 0 },
          ],
        },
      ],
    };
    expectPass(v1, v2, lens);
  });

  test("map + convert items passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["items"],
      "properties":{
        "items":{
          "type":"array",
          "items":{
            "type":"object",
            "additionalProperties": false,
            "required":["id"],
            "properties":{"id":{"type":"integer"}}
          }
        }
      }
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["items"],
      "properties":{
        "items":{
          "type":"array",
          "items":{
            "type":"object",
            "additionalProperties": false,
            "required":["id"],
            "properties":{"id":{"type":"string"}}
          }
        }
      }
    }`);
    const lens = {
      ops: [
        {
          op: "map",
          path: "/items",
          ops: [
            {
              op: "convert",
              path: "/id",
              fromType: "integer",
              toType: "string",
              forward: { builtin: "int_to_string" },
              backward: { builtin: "string_to_int" },
            },
          ],
        },
      ],
    };
    expectPass(v1, v2, lens);
  });

  test("add to open object passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": true,
      "required":["id"],
      "properties":{"id":{"type":"integer"}}
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": true,
      "required":["id","status"],
      "properties":{"id":{"type":"integer"},"status":{"type":"string"}}
    }`);
    const lens = { ops: [{ op: "add", path: "/status", schema: { type: "string" }, default: "new" }] };
    expectPass(v1, v2, lens);
  });

  test("rename overrides existing property passes", () => {
    const v1 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["a","x"],
      "properties":{"a":{"type":"integer"},"x":{"type":"string"}}
    }`);
    const v2 = schema(`{
      "type":"object",
      "additionalProperties": false,
      "required":["x"],
      "properties":{"x":{"type":"integer"}}
    }`);
    const lens = { ops: [{ op: "rename", from: "/a", to: "/x" }] };
    expectPass(v1, v2, lens);
  });
});
