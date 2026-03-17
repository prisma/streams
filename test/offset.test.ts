import { describe, test, expect } from "bun:test";
import fc from "fast-check";
import { encodeOffset, parseOffset, canonicalizeOffset } from "../src/offset";

describe("offset encoding", () => {
  test("encode -> parse roundtrip", () => {
    fc.assert(
      fc.property(fc.integer({ min: 0, max: 1000 }), fc.integer({ min: 0, max: 1_000_000 }), (epoch, seq) => {
        const enc = encodeOffset(epoch, BigInt(seq));
        const parsed = parseOffset(enc);
        expect(parsed.kind).toBe("seq");
        if (parsed.kind === "seq") {
          expect(parsed.epoch).toBe(epoch);
          expect(parsed.seq).toBe(BigInt(seq));
        }
      })
    );
  });

  test("decimal alias is rejected", () => {
    expect(() => canonicalizeOffset("5")).toThrow();
  });

  test("start offset -1", () => {
    const p = parseOffset("-1");
    expect(p.kind).toBe("start");
  });

  test("encoded length is always 26", () => {
    const cases: Array<[number, bigint, number]> = [
      [0, 0n, 0],
      [1, 0n, 0],
      [0, 1n, 0],
      [0, 123n, 9],
      [0xffffffff, 0xffffffffn, 0xffffffff],
    ];
    for (const [epoch, seq, inBlock] of cases) {
      const enc = encodeOffset(epoch, seq, inBlock);
      expect(enc.length).toBe(26);
    }
  });

  test("case-insensitive decode", () => {
    const enc = encodeOffset(12345, 6789n, 5);
    const parsedUpper = parseOffset(enc);
    const parsedLower = parseOffset(enc.toLowerCase());
    expect(parsedUpper).toEqual(parsedLower);
  });

  test("lexicographic ordering matches numeric ordering", () => {
    const offsets = [
      encodeOffset(1, 0n, 0),
      encodeOffset(1, 0n, 1),
      encodeOffset(1, 1n, 0),
      encodeOffset(2, 0n, 0),
    ];
    const sorted = [...offsets].sort();
    expect(sorted).toEqual(offsets);
  });

  test("crockford alphabet does not include I/L/O/U", () => {
    const enc = encodeOffset(0xffffffff, 0xffffffffn, 0xffffffff);
    for (const ch of enc) {
      expect("ILOU".includes(ch)).toBe(false);
    }
  });
});
