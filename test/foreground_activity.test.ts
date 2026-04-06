import { describe, expect, test } from "bun:test";
import { ForegroundActivityTracker } from "../src/foreground_activity";

describe("ForegroundActivityTracker", () => {
  test("tracks active foreground work", () => {
    const tracker = new ForegroundActivityTracker();
    expect(tracker.isActive()).toBe(false);
    const leave = tracker.enter();
    expect(tracker.isActive()).toBe(true);
    expect(tracker.getActive()).toBe(1);
    leave();
    expect(tracker.isActive()).toBe(false);
    expect(tracker.getActive()).toBe(0);
  });

  test("background yield waits for foreground to drain", async () => {
    const tracker = new ForegroundActivityTracker();
    const leave = tracker.enter();
    let resolved = false;
    const wait = tracker.yieldBackgroundWork(1000).then(() => {
      resolved = true;
    });
    await Bun.sleep(10);
    expect(resolved).toBe(false);
    leave();
    await wait;
    expect(resolved).toBe(true);
  });

  test("records recent foreground activity", async () => {
    const tracker = new ForegroundActivityTracker();
    expect(tracker.wasActiveWithin(10)).toBe(false);
    const leave = tracker.enter();
    expect(tracker.wasActiveWithin(10)).toBe(true);
    leave();
    expect(tracker.wasActiveWithin(10)).toBe(true);
    await Bun.sleep(20);
    expect(tracker.wasActiveWithin(10)).toBe(false);
  });
});
