import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { describe, expect, test } from "bun:test";
import { xxh3Hex, xxh32, xxh64Hex } from "../src/runtime/hash";

const repoRoot = resolve(import.meta.dir, "..");

type NodeHashResult = {
  xxh3Hex: string;
  xxh64Hex: string;
  xxh32: number;
};

function runNodeHashScript(inputExpression: string): NodeHashResult {
  const script = `
import { xxh3Hex, xxh32, xxh64Hex } from "./src/runtime/hash.ts";
const input = ${inputExpression};
console.log(JSON.stringify({
  xxh3Hex: xxh3Hex(input),
  xxh64Hex: xxh64Hex(input),
  xxh32: xxh32(input),
}));
`;

  const result = spawnSync("node", ["--input-type=module", "--eval", script], {
    cwd: repoRoot,
    encoding: "utf8",
    env: process.env,
  });

  if (result.status !== 0) {
    throw new Error(result.stderr || result.stdout || `node exited with ${result.status}`);
  }

  return JSON.parse(result.stdout.trim()) as NodeHashResult;
}

describe("runtime hash compatibility", () => {
  test("Node and Bun agree on string inputs", () => {
    const input = "public.todos\0status\0tenantId\0userId";
    const node = runNodeHashScript(JSON.stringify(input));

    expect(node).toEqual({
      xxh3Hex: xxh3Hex(input),
      xxh64Hex: xxh64Hex(input),
      xxh32: xxh32(input),
    });
  });

  test("Node and Bun agree on Uint8Array inputs", () => {
    const bytes = Uint8Array.from([0, 1, 2, 3, 255, 128, 64, 32, 16]);
    const node = runNodeHashScript(`Uint8Array.from(${JSON.stringify(Array.from(bytes))})`);

    expect(node).toEqual({
      xxh3Hex: xxh3Hex(bytes),
      xxh64Hex: xxh64Hex(bytes),
      xxh32: xxh32(bytes),
    });
  });
});
