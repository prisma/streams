import { describe, expect, test } from "bun:test";
import { searchDocIdCandidatesTestHelpers } from "../src/reader";

describe("search doc id candidates", () => {
  test("supports point membership from unsorted doc ids", () => {
    const candidates = searchDocIdCandidatesTestHelpers.build([42, 7, 99, 7, 12, 3]);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 3)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 7)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 13)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 100)).toBe(false);
  });

  test("supports fast range checks for sparse candidates", () => {
    const candidates = searchDocIdCandidatesTestHelpers.build([3, 11, 17, 25, 42, 100, 255]);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 0, 2)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 0, 3)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 12, 16)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 12, 17)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 101, 254)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 200, 300)).toBe(true);
  });

  test("handles large candidate sets without requiring contiguous doc ids", () => {
    const docIds: number[] = [];
    for (let index = 0; index < 4096; index += 2) docIds.push(index);
    const candidates = searchDocIdCandidatesTestHelpers.build(docIds);

    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 3990, 3998)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 3991, 3991)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.intersectsRange(candidates, 4097, 5000)).toBe(false);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 2048)).toBe(true);
    expect(searchDocIdCandidatesTestHelpers.has(candidates, 2049)).toBe(false);
  });
});
