import { describe, expect, test } from "bun:test";
import { MockR2Store } from "../src/objectstore/mock_r2";
import { retryAbortable } from "../src/util/retry";

describe("retryAbortable", () => {
  test("aborts slow PUTs instead of letting them complete in the background", async () => {
    const store = new MockR2Store({ putDelayMs: 100 });
    const payload = new Uint8Array(16);

    await expect(
      retryAbortable((signal) => store.put("streams/a/segments/0000000000000001.bin", payload, { signal }), {
        retries: 0,
        baseDelayMs: 1,
        maxDelayMs: 1,
        timeoutMs: 10,
      })
    ).rejects.toThrow("timeout");

    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(store.size()).toBe(0);
    expect(await store.head("streams/a/segments/0000000000000001.bin")).toBeNull();
  });
});
