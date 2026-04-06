import { describe, expect, test } from "bun:test";
import { ConcurrencyGate } from "../src/concurrency_gate";

describe("concurrency gate", () => {
  test("queues until a permit is released", async () => {
    const gate = new ConcurrencyGate(1);
    const events: string[] = [];
    const release1 = await gate.acquire();
    const second = gate.run(async () => {
      events.push("second-start");
      return "ok";
    });

    await Promise.resolve();
    expect(events).toEqual([]);
    release1();

    await expect(second).resolves.toBe("ok");
    expect(events).toEqual(["second-start"]);
  });

  test("aborts queued waiters", async () => {
    const gate = new ConcurrencyGate(1);
    const release1 = await gate.acquire();
    const controller = new AbortController();
    const queued = gate.acquire(controller.signal);
    controller.abort();

    await expect(queued).rejects.toMatchObject({ name: "AbortError" });
    release1();
    expect(gate.getQueued()).toBe(0);
    expect(gate.getActive()).toBe(0);
  });

  test("setLimit drains queued work", async () => {
    const gate = new ConcurrencyGate(1);
    const release1 = await gate.acquire();
    const started: string[] = [];
    const second = gate.run(async () => {
      started.push("second");
    });
    const third = gate.run(async () => {
      started.push("third");
    });

    await Promise.resolve();
    expect(started).toEqual([]);

    gate.setLimit(2);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(started).toContain("second");

    release1();
    await Promise.all([second, third]);
    expect(started).toEqual(["second", "third"]);
  });
});
