import { describe, test, expect } from "bun:test";
import { applyCompiledLens, applyLensChain, compileLens, type Lens } from "../src/lens/lens";

function apply(lens: Lens, input: any): any {
  const compiled = compileLens(lens);
  const copy = JSON.parse(JSON.stringify(input));
  return applyCompiledLens(compiled, copy);
}

describe("lens ops", () => {
  test("rename", () => {
    const lens: Lens = { apiVersion: "durable.lens/v1", schema: "S", from: 1, to: 2, ops: [{ op: "rename", from: "/a", to: "/b" }] };
    const out = apply(lens, { a: 1 });
    expect(out).toEqual({ b: 1 });
  });

  test("copy", () => {
    const lens: Lens = { apiVersion: "durable.lens/v1", schema: "S", from: 1, to: 2, ops: [{ op: "copy", from: "/a", to: "/b" }] };
    const out = apply(lens, { a: 1 });
    expect(out).toEqual({ a: 1, b: 1 });
  });

  test("add/remove", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        { op: "add", path: "/b", schema: { type: "number" }, default: 2 },
        { op: "remove", path: "/a", schema: { type: "number" } },
      ],
    };
    const out = apply(lens, { a: 1 });
    expect(out).toEqual({ b: 2 });
  });

  test("hoist/plunge", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        { op: "hoist", host: "/obj", name: "a", to: "/a" },
        { op: "plunge", from: "/a", host: "/nested", name: "a" },
      ],
    };
    const out = apply(lens, { obj: { a: 1 } });
    expect(out).toEqual({ nested: { a: 1 }, obj: {} });
  });

  test("wrap/head", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        { op: "wrap", path: "/a", mode: "singleton" },
        { op: "head", path: "/a" },
      ],
    };
    const out = apply(lens, { a: "x" });
    expect(out).toEqual({ a: "x" });
  });

  test("convert map/builtin", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        {
          op: "convert",
          path: "/status",
          fromType: "string",
          toType: "string",
          forward: { map: { open: "todo" }, default: "todo" },
          backward: { map: { todo: "open" }, default: "open" },
        },
        {
          op: "convert",
          path: "/name",
          fromType: "string",
          toType: "string",
          forward: { builtin: "lowercase" },
          backward: { builtin: "uppercase" },
        },
      ],
    };
    const out = apply(lens, { status: "open", name: "ALICE" });
    expect(out).toEqual({ status: "todo", name: "alice" });
  });

  test("in/map", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        { op: "in", path: "/obj", ops: [{ op: "rename", from: "/a", to: "/b" }] },
        { op: "map", path: "/arr", ops: [{ op: "rename", from: "/x", to: "/y" }] },
      ],
    };
    const out = apply(lens, { obj: { a: 1 }, arr: [{ x: 1 }, { x: 2 }] });
    expect(out).toEqual({ obj: { b: 1 }, arr: [{ y: 1 }, { y: 2 }] });
  });

  test("destination overwrite", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "S",
      from: 1,
      to: 2,
      ops: [
        { op: "rename", from: "/a", to: "/x" },
        { op: "copy", from: "/b", to: "/y" },
        { op: "add", path: "/tags", schema: { type: "array", items: { type: "string" } }, default: [] },
        { op: "hoist", host: "/user", name: "login", to: "/userLogin" },
        { op: "plunge", from: "/height", host: "/properties", name: "height", createHost: true },
      ],
    };
    const input = {
      a: 1,
      x: 9,
      b: 2,
      y: 9,
      tags: ["keep"],
      user: { login: "alice", id: 1 },
      userLogin: "pre",
      height: 180,
      properties: { height: 160 },
    };
    const out = apply(lens, input);
    expect(out).toEqual({
      x: 1,
      b: 2,
      y: 2,
      tags: [],
      user: { id: 1 },
      userLogin: "alice",
      properties: { height: 180 },
    });
  });

  test("lens chain applies sequentially", () => {
    const l1: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "T",
      from: 1,
      to: 2,
      ops: [
        { op: "rename", from: "/a", to: "/x" },
        { op: "add", path: "/tags", schema: { type: "array", items: { type: "string" } }, default: [] },
      ],
    };
    const l2: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "T",
      from: 2,
      to: 3,
      ops: [
        { op: "copy", from: "/x", to: "/y" },
        { op: "remove", path: "/x", schema: { type: "number" } },
      ],
    };

    const input = { a: 1 };
    const seq = apply(l2, apply(l1, input));
    const compiled = [compileLens(l1), compileLens(l2)];
    const chained = applyLensChain(compiled, JSON.parse(JSON.stringify(input)));
    expect(chained).toEqual(seq);
  });

  test("chain of 10 ops across 10 events", () => {
    const lens: Lens = {
      apiVersion: "durable.lens/v1",
      schema: "Task",
      from: 1,
      to: 2,
      ops: [
        { op: "rename", from: "/assignee", to: "/assignees" },
        { op: "wrap", path: "/assignees", mode: "singleton" },
        { op: "add", path: "/tags", schema: { type: "array", items: { type: "string" } }, default: [] },
        {
          op: "convert",
          path: "/status",
          fromType: "string",
          toType: "string",
          forward: { map: { open: "todo", closed: "done" }, default: "todo" },
          backward: { map: { todo: "open", done: "closed" }, default: "open" },
        },
        { op: "map", path: "/labels", ops: [{ op: "rename", from: "/name", to: "/category" }] },
        { op: "hoist", host: "/user", name: "login", to: "/userLogin" },
        { op: "remove", path: "/user", schema: { type: "object" }, default: {} },
        {
          op: "in",
          path: "/invoice",
          ops: [
            { op: "remove", path: "/object", schema: { type: "string" }, default: "invoice" },
            { op: "rename", from: "/id", to: "/invoiceId" },
          ],
        },
        { op: "copy", from: "/id", to: "/legacyId" },
        { op: "plunge", from: "/height", host: "/properties", name: "height", createHost: true },
      ],
    };

    for (let i = 0; i < 10; i++) {
      const status = i % 2 === 0 ? "open" : "closed";
      const event = {
        id: `t${i}`,
        assignee: `user${i}`,
        status,
        labels: [{ name: "bug" }, { name: `p${i}` }],
        user: { login: `user${i}`, id: i },
        invoice: { id: `inv${i}`, object: "invoice" },
        height: 170 + i,
      };

      const out = apply(lens, event) as Record<string, any>;
      expect(out.assignee).toBeUndefined();
      expect(Array.isArray(out.assignees)).toBe(true);
      expect(out.assignees[0]).toBe(`user${i}`);
      expect(out.tags).toEqual([]);
      expect(out.status).toBe(status === "open" ? "todo" : "done");

      const labels = out.labels as Array<Record<string, any>>;
      expect(labels.length).toBe(2);
      expect(labels[0].name).toBeUndefined();
      expect(typeof labels[0].category).toBe("string");

      expect(out.user).toBeUndefined();
      expect(out.userLogin).toBe(`user${i}`);

      const invoice = out.invoice as Record<string, any>;
      expect(invoice.object).toBeUndefined();
      expect(invoice.id).toBeUndefined();
      expect(invoice.invoiceId).toBe(`inv${i}`);
      expect(out.legacyId).toBe(`t${i}`);

      expect(out.height).toBeUndefined();
      expect(out.properties.height).toBe(170 + i);
    }
  });
});
