import { describe, expect, test } from "bun:test";
import { performance } from "node:perf_hooks";
import { buildEvlogEvent } from "../experiments/demo/evlog_ingester";
import { buildEvlogDefaultRegistry } from "../src/profiles/evlog/schema";
import { buildDesiredSearchCompanionPlan } from "../src/search/companion_plan";
import {
  compileSearchFieldAccessorsResult,
  extractRawSearchValuesForFieldsResult,
  extractRawSearchValuesWithCompiledAccessorsResult,
} from "../src/search/schema";
import { Result } from "better-result";

function buildParsedEvents(count: number): unknown[] {
  const events: unknown[] = [];
  for (let id = 0; id < count; id += 1) {
    const timestamp = new Date(Date.UTC(2026, 3, 7, 0, 0, id % 60, id % 1000)).toISOString();
    events.push(buildEvlogEvent(id, timestamp));
  }
  return events;
}

describe("companion field extraction performance", () => {
  test("compiled accessors are at least 25% faster on evlog companion fields", () => {
    const registry = buildEvlogDefaultRegistry("evlog-1");
    const plan = buildDesiredSearchCompanionPlan(registry);
    const fieldNames = plan.fields.map((field) => field.name).sort((a, b) => a.localeCompare(b));
    const records = buildParsedEvents(20_000);
    const compiledRes = compileSearchFieldAccessorsResult(registry, fieldNames, 1);
    expect(Result.isOk(compiledRes)).toBe(true);
    if (Result.isError(compiledRes)) return;

    let legacyValueCount = 0;
    for (const record of records.slice(0, 512)) {
      const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, 0n, record, fieldNames);
      expect(Result.isOk(rawValuesRes)).toBe(true);
      if (Result.isOk(rawValuesRes)) {
        for (const values of rawValuesRes.value.values()) legacyValueCount += values.length;
      }
    }

    let compiledValueCount = 0;
    for (const record of records.slice(0, 512)) {
      const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(record, compiledRes.value);
      expect(Result.isOk(rawValuesRes)).toBe(true);
      if (Result.isOk(rawValuesRes)) {
        for (const values of rawValuesRes.value.values()) compiledValueCount += values.length;
      }
    }
    expect(compiledValueCount).toBe(legacyValueCount);

    const legacyStarted = performance.now();
    let legacyCount = 0;
    for (const record of records) {
      const rawValuesRes = extractRawSearchValuesForFieldsResult(registry, 0n, record, fieldNames);
      if (Result.isError(rawValuesRes)) throw new Error(rawValuesRes.error.message);
      for (const values of rawValuesRes.value.values()) legacyCount += values.length;
    }
    const legacyElapsedMs = performance.now() - legacyStarted;

    const compiledStarted = performance.now();
    let compiledCount = 0;
    for (const record of records) {
      const rawValuesRes = extractRawSearchValuesWithCompiledAccessorsResult(record, compiledRes.value);
      if (Result.isError(rawValuesRes)) throw new Error(rawValuesRes.error.message);
      for (const values of rawValuesRes.value.values()) compiledCount += values.length;
    }
    const compiledElapsedMs = performance.now() - compiledStarted;

    expect(compiledCount).toBe(legacyCount);
    expect(compiledElapsedMs).toBeLessThan(legacyElapsedMs * 0.75);
  }, 30_000);
});
