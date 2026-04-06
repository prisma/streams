import { describe, expect, test } from "bun:test";
import { zstdDecompressSync } from "node:zlib";
import { Result } from "better-result";
import { decodeAggSegmentCompanionResult, encodeAggSegmentCompanion, type AggSectionInput } from "../src/search/agg_format";
import { readU32 } from "../src/search/binary/codec";
import type { SearchCompanionPlan } from "../src/search/companion_plan";

const plan: SearchCompanionPlan = {
  families: { col: false, fts: false, agg: true, mblk: false },
  fields: [
    field(0, "eventType", "keyword"),
    field(1, "repoOwner", "keyword"),
    field(2, "eventTime", "date"),
    field(3, "public", "bool"),
    field(4, "payloadBytes", "integer"),
    field(5, "commitCount", "integer"),
  ],
  rollups: [
    {
      ordinal: 0,
      name: "events",
      timestamp_field_ordinal: 2,
      dimension_ordinals: [0, 1, 3],
      intervals: [
        { ordinal: 0, name: "1m", ms: 60_000 },
        { ordinal: 1, name: "1h", ms: 3_600_000 },
      ],
      measures: [
        { ordinal: 0, name: "events", kind: "count", field_ordinal: null, histogram: null },
        { ordinal: 1, name: "payloadBytes", kind: "summary", field_ordinal: 4, histogram: "log2_v1" },
        { ordinal: 2, name: "commitCount", kind: "summary", field_ordinal: 5, histogram: "log2_v1" },
      ],
    },
  ],
  summary: {},
};

describe("agg2 interval-local compression", () => {
  test("compresses large interval payloads and decodes them lazily", () => {
    const input = buildCompressibleInput();
    const bytes = encodeAggSegmentCompanion(input, plan);
    const intervalEntries = readIntervalEntries(bytes, 0);
    expect(intervalEntries).toHaveLength(2);
    expect(intervalEntries[0]!.compression).toBe(1);
    expect(intervalEntries[1]!.compression).toBe(1);

    for (const entry of intervalEntries) {
      const compressed = bytes.subarray(entry.payloadOffset, entry.payloadOffset + entry.payloadLength);
      const inflated = new Uint8Array(zstdDecompressSync(compressed));
      expect(inflated.byteLength).toBeGreaterThan(compressed.byteLength);
    }

    const decoded = decodeAggSegmentCompanionResult(bytes, plan);
    if (Result.isError(decoded)) throw new Error(decoded.error.message);
    const oneMinute = decoded.value.getInterval("events", 60_000);
    expect(oneMinute).not.toBeNull();
    const visited: Array<{ windowStartMs: number; group: ReturnType<typeof buildGroup> }> = [];
    oneMinute!.forEachGroupInRange(0, 4 * 60_000, (windowStartMs, group) => {
      visited.push({ windowStartMs, group: group as ReturnType<typeof buildGroup> });
    });
    expect(visited.length).toBe(16);
    expect(visited[0]!.windowStartMs).toBe(0);
    expect(visited[0]!.group.dimensions.eventType).toBe("PushEvent");
    expect(visited[0]!.group.measures.events).toEqual({ kind: "count", value: 10 });
    expect(visited[0]!.group.measures.payloadBytes).toEqual({
      kind: "summary",
      summary: { count: 10, sum: 1280, min: 128, max: 128, histogram: { "128": 10 } },
    });
  });

  test("round-trips small interval payloads", () => {
    const input: AggSectionInput = {
      rollups: {
        events: {
          intervals: {
            "1m": {
              interval_ms: 60_000,
              windows: [
                {
                  start_ms: 0,
                  groups: [
                    {
                      dimensions: { eventType: "PushEvent", repoOwner: null, public: "true" },
                      measures: { events: { kind: "count", value: 1 } },
                    },
                  ],
                },
              ],
            },
          },
        },
      },
    };
    const bytes = encodeAggSegmentCompanion(input, plan);
    const intervalEntries = readIntervalEntries(bytes, 0);
    expect(intervalEntries).toHaveLength(1);
    expect([0, 1]).toContain(intervalEntries[0]!.compression);

    const decoded = decodeAggSegmentCompanionResult(bytes, plan);
    if (Result.isError(decoded)) throw new Error(decoded.error.message);
    const oneMinute = decoded.value.getInterval("events", 60_000);
    expect(oneMinute).not.toBeNull();
    const groups: string[] = [];
    oneMinute!.forEachGroupInRange(0, 60_000, (_windowStartMs, group) => {
      groups.push(group.dimensions.eventType ?? "missing");
    });
    expect(groups).toEqual(["PushEvent"]);
  });
});

function buildCompressibleInput(): AggSectionInput {
  const oneMinuteWindows = [];
  const oneHourWindows = [];
  for (let windowIndex = 0; windowIndex < 8; windowIndex++) {
    oneMinuteWindows.push({
      start_ms: windowIndex * 60_000,
      groups: Array.from({ length: 4 }, (_value, groupIndex) => buildGroup(windowIndex, groupIndex)),
    });
  }
  for (let windowIndex = 0; windowIndex < 6; windowIndex++) {
    oneHourWindows.push({
      start_ms: windowIndex * 3_600_000,
      groups: Array.from({ length: 4 }, (_value, groupIndex) => buildGroup(windowIndex + 10, groupIndex)),
    });
  }
  return {
    rollups: {
      events: {
        intervals: {
          "1m": { interval_ms: 60_000, windows: oneMinuteWindows },
          "1h": { interval_ms: 3_600_000, windows: oneHourWindows },
        },
      },
    },
  };
}

function buildGroup(windowIndex: number, groupIndex: number) {
  const repoOwner = groupIndex % 2 === 0 ? "prisma" : "octokit";
  const commitCount = windowIndex + groupIndex + 1;
  const payloadBytes = 128 * (groupIndex + 1);
  return {
    dimensions: {
      eventType: groupIndex % 2 === 0 ? "PushEvent" : "PullRequestEvent",
      repoOwner,
      public: groupIndex % 2 === 0 ? "true" : "false",
    },
    measures: {
      events: { kind: "count" as const, value: 10 + groupIndex },
      payloadBytes: {
        kind: "summary" as const,
        summary: {
          count: 10 + groupIndex,
          sum: payloadBytes * (10 + groupIndex),
          min: payloadBytes,
          max: payloadBytes,
          histogram: { [String(payloadBytes)]: 10 + groupIndex },
        },
      },
      commitCount: {
        kind: "summary" as const,
        summary: {
          count: 10 + groupIndex,
          sum: commitCount * (10 + groupIndex),
          min: commitCount,
          max: commitCount,
          histogram: { [String(2 ** Math.floor(Math.log2(commitCount)))]: 10 + groupIndex },
        },
      },
    },
  };
}

function readIntervalEntries(
  bytes: Uint8Array,
  rollupIndex: number
): Array<{ compression: number; payloadOffset: number; payloadLength: number }> {
  const rollupDirOffset = 4;
  const rollupEntryOffset = rollupDirOffset + rollupIndex * 12;
  const intervalCount = new DataView(bytes.buffer, bytes.byteOffset + rollupEntryOffset + 2, 2).getUint16(0, true);
  const intervalsOffset = readU32(bytes, rollupEntryOffset + 4);
  const entries = [];
  for (let intervalIndex = 0; intervalIndex < intervalCount; intervalIndex++) {
    const intervalEntryOffset = intervalsOffset + intervalIndex * 12;
    entries.push({
      compression: bytes[intervalEntryOffset + 2] ?? 0,
      payloadOffset: readU32(bytes, intervalEntryOffset + 4),
      payloadLength: readU32(bytes, intervalEntryOffset + 8),
    });
  }
  return entries;
}

function field(
  ordinal: number,
  name: string,
  kind: SearchCompanionPlan["fields"][number]["kind"]
): SearchCompanionPlan["fields"][number] {
  return {
    ordinal,
    name,
    kind,
    bindings: [],
    normalizer: null,
    analyzer: null,
    exact: false,
    prefix: false,
    column: false,
    exists: false,
    sortable: false,
    aggregatable: false,
    contains: false,
    positions: false,
  };
}
