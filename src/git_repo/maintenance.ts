import type { GitRefCheckpoint, GitRepoRecord } from "./types";
import { buildRefs } from "./refs";

export function buildRefCheckpoint(args: {
  repoId: string;
  records: GitRepoRecord[];
  streamOffset: number;
  generation: number;
  defaultBranch: string;
  createdAt?: string;
}): GitRefCheckpoint {
  return {
    repoId: args.repoId,
    generation: args.generation,
    streamOffset: args.streamOffset,
    refs: buildRefs(args.records),
    head: {
      symbolicRef: args.defaultBranch,
    },
    createdAt: args.createdAt ?? new Date().toISOString(),
  };
}

export function latestInlineRefCheckpoint(records: GitRepoRecord[]): GitRefCheckpoint | null {
  for (let i = records.length - 1; i >= 0; i--) {
    const record = records[i]!;
    if (record.type === "maintenance-published" && record.refCheckpoint) return record.refCheckpoint;
  }
  return null;
}
