import { describe, expect, test } from "bun:test";
import { Result } from "better-result";
import { RoutingLexiconL0BuildCoordinator } from "../src/index/routing_lexicon_l0_build_coordinator";

describe("RoutingLexiconL0BuildCoordinator", () => {
  test("deduplicates in-flight and cached combined window builds", async () => {
    let resolveBuild: ((value: any) => void) | null = null;
    let buildCalls = 0;
    const workers = {
      buildResult() {
        buildCalls += 1;
        return new Promise((resolve) => {
          resolveBuild = resolve;
        });
      },
    };
    const coordinator = new RoutingLexiconL0BuildCoordinator(workers as any, 60_000, 8);
    const input = {
      stream: "stream-a",
      sourceKind: "routing_key",
      sourceName: "",
      cacheToken: "secret-1",
      startSegment: 0,
      span: 16,
      secret: new Uint8Array([1, 2, 3, 4]),
      segments: [{ segmentIndex: 0, localPath: "/tmp/segment-0" }],
    };

    const first = coordinator.buildWindowResult(input);
    const second = coordinator.buildWindowResult(input);
    expect(buildCalls).toBe(1);

    resolveBuild?.(
      Result.ok({
        kind: "routing_lexicon_l0_build",
        output: {
          routing: {
            meta: {
              runId: "routing-run",
              level: 0,
              startSegment: 0,
              endSegment: 15,
              objectKey: "routing-key",
              filterLen: 4,
              recordCount: 1,
            },
            payload: new Uint8Array([1]),
          },
          lexicon: {
            meta: {
              runId: "lexicon-run",
              level: 0,
              startSegment: 0,
              endSegment: 15,
              objectKey: "lexicon-key",
              recordCount: 1,
            },
            payload: new Uint8Array([2]),
          },
        },
      })
    );

    const firstRes = await first;
    const secondRes = await second;
    expect(Result.isOk(firstRes)).toBe(true);
    expect(Result.isOk(secondRes)).toBe(true);
    if (Result.isError(firstRes) || Result.isError(secondRes)) return;
    expect(firstRes.value.cacheStatus).toBe("miss");
    expect(secondRes.value.cacheStatus).toBe("shared_inflight");

    const thirdRes = await coordinator.buildWindowResult(input);
    expect(Result.isOk(thirdRes)).toBe(true);
    if (Result.isError(thirdRes)) return;
    expect(thirdRes.value.cacheStatus).toBe("cache_hit");
    expect(buildCalls).toBe(1);
  });
});
