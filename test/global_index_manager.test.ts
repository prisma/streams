import { describe, expect, test } from "bun:test";
import { GlobalIndexManager } from "../src/index/global_index_manager";

type StubFamily = {
  enqueued: string[];
  ops: string[];
  start: () => void;
  stop: () => void;
  enqueue: (stream: string) => void;
  runOneBuildTask: () => Promise<boolean>;
  runOneCompactionTask: () => Promise<boolean>;
};

function makeStubFamily(ops: string[], buildName: string, compactName: string): StubFamily {
  return {
    enqueued: [],
    ops,
    start() {
      // no-op
    },
    stop() {
      // no-op
    },
    enqueue(stream: string) {
      this.enqueued.push(stream);
    },
    async runOneBuildTask() {
      ops.push(buildName);
      return true;
    },
    async runOneCompactionTask() {
      ops.push(compactName);
      return true;
    },
  };
}

describe("GlobalIndexManager", () => {
  test("round-robins indexing work kinds across ticks", async () => {
    const ops: string[] = [];
    const workers = {
      start() {
        ops.push("workers:start");
      },
      stop() {
        ops.push("workers:stop");
      },
    };
    const routing = makeStubFamily(ops, "routing:build", "routing:compact");
    const secondary = makeStubFamily(ops, "secondary:build", "secondary:compact");
    const companion = {
      enqueued: [] as string[],
      start() {
        // no-op
      },
      stop() {
        // no-op
      },
      enqueue(stream: string) {
        this.enqueued.push(stream);
      },
      async runOneBuildTask() {
        ops.push("companion:build");
        return true;
      },
    };
    const lexicon = makeStubFamily(ops, "lexicon:build", "lexicon:compact");

    const manager = new GlobalIndexManager(
      { indexCheckIntervalMs: 10 } as any,
      routing as any,
      secondary as any,
      companion as any,
      lexicon as any,
      workers as any
    );

    manager.start();
    manager.enqueue("stream-a");
    await manager.tick();
    const firstTickOps = ops.slice();
    expect(routing.enqueued).toEqual(["stream-a"]);
    expect(secondary.enqueued).toEqual(["stream-a"]);
    expect(companion.enqueued).toEqual(["stream-a"]);
    expect(lexicon.enqueued).toEqual(["stream-a"]);
    expect(firstTickOps.slice(1)).toEqual([
      "routing:build",
      "lexicon:build",
      "secondary:build",
      "companion:build",
      "routing:compact",
      "lexicon:compact",
      "secondary:compact",
    ]);

    ops.length = 0;
    await manager.tick();
    expect(ops).toEqual([
      "lexicon:build",
      "secondary:build",
      "companion:build",
      "routing:compact",
      "lexicon:compact",
      "secondary:compact",
      "routing:build",
    ]);

    manager.stop();
    expect(ops.at(-1)).toBe("workers:stop");
  });
});
