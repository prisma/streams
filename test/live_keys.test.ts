import { describe, expect, test } from "bun:test";
import { tableKeyFor, templateIdFor, watchKeyFor } from "../src/touch/live_keys";
import { xxh3Hex } from "../src/runtime/hash";

function utf8(s: string): Uint8Array {
  return new TextEncoder().encode(s);
}

function concat(parts: Uint8Array[]): Uint8Array {
  let total = 0;
  for (const p of parts) total += p.byteLength;
  const out = new Uint8Array(total);
  let off = 0;
  for (const p of parts) {
    out.set(p, off);
    off += p.byteLength;
  }
  return out;
}

function encodeU64Le(v: bigint): Uint8Array {
  const out = new Uint8Array(8);
  let x = v;
  for (let i = 0; i < 8; i++) {
    out[i] = Number(x & 0xffn);
    x >>= 8n;
  }
  return out;
}

function watchKeyForLe(templateIdHex16: string, encodedArgs: string[]): string {
  const tplBytes = encodeU64Le(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("key\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Hex(concat(parts));
}

describe("live key hashing (golden vectors)", () => {
  test("tableKey/templateId/watchKey are stable, and watchKey uses BE templateId bytes", () => {
    const entity = "public.todos";
    const fieldsSorted = ["status", "tenantId", "userId"];

    const tableKey = tableKeyFor(entity);
    const templateId = templateIdFor(entity, fieldsSorted);
    const watchKey = watchKeyFor(templateId, ["open", "t1", "u1"]);

    // Golden vectors. If these change, cross-language implementations will drift.
    expect(tableKey).toBe("feadeb84d447fd63");
    expect(templateId).toBe("ddd2330fdab379da");
    expect(watchKey).toBe("39accd125778dc36");

    // Endianness check: LE encoding yields a different watch key.
    const le = watchKeyForLe(templateId, ["open", "t1", "u1"]);
    expect(le).not.toBe(watchKey);
  });
});
