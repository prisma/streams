import { describe, expect, test } from "bun:test";
import { Bloom256, bloom256MaskForKey } from "../src/util/bloom256";

describe("Bloom256", () => {
  test("never returns a false negative for inserted keys", () => {
    const bloom = new Bloom256();
    const encoder = new TextEncoder();
    const keys = Array.from({ length: 128 }, (_, i) => encoder.encode(`routing-key-${i}`));

    for (const key of keys) bloom.add(key);

    for (const key of keys) {
      expect(bloom.maybeHas(key)).toBe(true);
    }
  });

  test("precomputed masks produce the same membership as direct add", () => {
    const bloom = new Bloom256();
    const encoder = new TextEncoder();
    const keys = Array.from({ length: 128 }, (_, i) => encoder.encode(`routing-key-${i % 17}`));

    for (const key of keys) {
      bloom.orMask(bloom256MaskForKey(key));
    }

    for (const key of keys) {
      expect(bloom.maybeHas(key)).toBe(true);
    }
  });
});
