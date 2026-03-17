import { describe, expect, test } from "bun:test";
import { StreamNotifier } from "../src/notifier";

describe("StreamNotifier", () => {
  test("waitFor resolves immediately when stream already advanced", async () => {
    const notifier = new StreamNotifier();
    notifier.notify("s", 7n);

    const start = performance.now();
    await notifier.waitFor("s", 3n, 5_000);
    const elapsed = performance.now() - start;

    expect(elapsed).toBeLessThan(25);
  });
});
