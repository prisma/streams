import { describe, expect, test } from "bun:test";
import { GlobalIndexManager } from "../src/index/global_index_manager";

type StubFamily = {
  enqueued: string[];
  ops: string[];
  buildResults: boolean[];
  compactResults: boolean[];
  start: () => void;
  stop: () => void;
  enqueue: (stream: string) => void;
  runOneBuildTask: () => Promise<boolean>;
  runOneCompactionTask: () => Promise<boolean>;
};

function makeStubFamily(ops: string[], buildName: string, compactName: string, buildResults: boolean[], compactResults: boolean[]): StubFamily {
  return {
    enqueued: [],
    ops,
    buildResults,
    compactResults,
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
      return this.buildResults.shift() ?? false;
    },
    async runOneCompactionTask() {
      ops.push(compactName);
      return this.compactResults.shift() ?? false;
    },
  };
}

describe("GlobalIndexManager", () => {
  test("round-robins indexing work kinds across rounds and drains until idle", async () => {
    const ops: string[] = [];
    const workers = {
      start() {
        ops.push("workers:start");
      },
      stop() {
        ops.push("workers:stop");
      },
    };
    const routing = makeStubFamily(ops, "routing:build", "routing:compact", [true, false], [true, false]);
    const secondary = makeStubFamily(ops, "secondary:build", "secondary:compact", [true, false], [true, false]);
    const companion = {
      enqueued: [] as string[],
      buildResults: [true, false],
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
        return this.buildResults.shift() ?? false;
      },
    };
    const lexicon = makeStubFamily(ops, "lexicon:build", "lexicon:compact", [true, false], [true, false]);

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
    expect(routing.enqueued).toEqual(["stream-a"]);
    expect(secondary.enqueued).toEqual(["stream-a"]);
    expect(companion.enqueued).toEqual(["stream-a"]);
    expect(lexicon.enqueued).toEqual(["stream-a"]);
    expect(ops.slice(1)).toEqual([
      "routing:build",
      "lexicon:build",
      "secondary:build",
      "companion:build",
      "routing:compact",
      "lexicon:compact",
      "secondary:compact",
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
