import { describe, expect, test } from "bun:test";
import { membershipKeyFor, projectedFieldKeyFor, tableKeyFor, templateIdFor, watchKeyFor } from "../src/touch/live_keys";
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

function membershipKeyForLe(templateIdHex16: string, encodedArgs: string[]): string {
  const tplBytes = encodeU64Le(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("mem\0"), tplBytes];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Hex(concat(parts));
}

function projectedFieldKeyForLe(templateIdHex16: string, fieldName: string, encodedArgs: string[]): string {
  const tplBytes = encodeU64Le(BigInt(`0x${templateIdHex16}`));
  const parts: Uint8Array[] = [utf8("fld\0"), tplBytes, utf8("\0"), utf8(fieldName)];
  for (const a of encodedArgs) {
    parts.push(utf8("\0"));
    parts.push(utf8(a));
  }
  return xxh3Hex(concat(parts));
}

describe("live key hashing (golden vectors)", () => {
  test("tableKey/templateId/watchKey/membershipKey/projectedFieldKey are stable, and fine keys use BE templateId bytes", () => {
    const entity = "public.todos";
    const fieldsSorted = ["status", "tenantId", "userId"];

    const tableKey = tableKeyFor(entity);
    const templateId = templateIdFor(entity, fieldsSorted);
    const watchKey = watchKeyFor(templateId, ["open", "t1", "u1"]);
    const membershipKey = membershipKeyFor(templateId, ["open", "t1", "u1"]);
    const projectedFieldKey = projectedFieldKeyFor(templateId, "title", ["open", "t1", "u1"]);

    // Golden vectors. If these change, cross-language implementations will drift.
    expect(tableKey).toBe("feadeb84d447fd63");
    expect(templateId).toBe("ddd2330fdab379da");
    expect(watchKey).toBe("39accd125778dc36");
    expect(membershipKey).toBe("eae633979a7ee466");
    expect(projectedFieldKey).toBe("1ecc53e76a89f0d5");

    // Endianness check: LE encoding yields different fine keys.
    const le = watchKeyForLe(templateId, ["open", "t1", "u1"]);
    expect(le).not.toBe(watchKey);
    const membershipLe = membershipKeyForLe(templateId, ["open", "t1", "u1"]);
    expect(membershipLe).not.toBe(membershipKey);
    const projectedLe = projectedFieldKeyForLe(templateId, "title", ["open", "t1", "u1"]);
    expect(projectedLe).not.toBe(projectedFieldKey);
  });
});
