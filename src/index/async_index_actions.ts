import type { SqliteDurableStore } from "../db/db";

export type AsyncIndexActionKind =
  | "routing_l0_build"
  | "routing_compaction_build"
  | "lexicon_l0_build"
  | "lexicon_compaction_build"
  | "secondary_l0_build"
  | "secondary_compaction_build"
  | "companion_build";

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type AsyncIndexActionDetail = { [key: string]: JsonValue };

export type AsyncIndexActionStart = {
  stream: string;
  actionKind: AsyncIndexActionKind;
  targetKind?: string | null;
  targetName?: string | null;
  inputKind: string;
  inputCount: number;
  inputSizeBytes: bigint;
  startSegment?: number | null;
  endSegment?: number | null;
  detail?: AsyncIndexActionDetail;
};

export type AsyncIndexActionFinish = {
  outputCount?: number;
  outputSizeBytes?: bigint;
  detail?: AsyncIndexActionDetail;
};

function mergeDetail(base: AsyncIndexActionDetail | undefined, extra: AsyncIndexActionDetail | undefined): string {
  return JSON.stringify({
    ...(base ?? {}),
    ...(extra ?? {}),
  });
}

export class AsyncIndexActionTracker {
  private finished = false;

  constructor(
    private readonly db: SqliteDurableStore,
    private readonly seq: bigint,
    private readonly startDetail: AsyncIndexActionDetail | undefined
  ) {}

  succeed(finish: AsyncIndexActionFinish = {}): void {
    if (this.finished) return;
    this.finished = true;
    this.db.completeAsyncIndexAction({
      seq: this.seq,
      status: "succeeded",
      outputCount: finish.outputCount ?? 1,
      outputSizeBytes: finish.outputSizeBytes ?? 0n,
      detailJson: mergeDetail(this.startDetail, finish.detail),
    });
  }

  fail(errorMessage: string, finish: AsyncIndexActionFinish = {}): void {
    if (this.finished) return;
    this.finished = true;
    this.db.completeAsyncIndexAction({
      seq: this.seq,
      status: "failed",
      outputCount: finish.outputCount ?? 0,
      outputSizeBytes: finish.outputSizeBytes ?? 0n,
      errorMessage,
      detailJson: mergeDetail(this.startDetail, finish.detail),
    });
  }
}

export function beginAsyncIndexAction(db: SqliteDurableStore, input: AsyncIndexActionStart): AsyncIndexActionTracker {
  const seq = db.beginAsyncIndexAction({
    stream: input.stream,
    actionKind: input.actionKind,
    targetKind: input.targetKind ?? null,
    targetName: input.targetName ?? null,
    inputKind: input.inputKind,
    inputCount: input.inputCount,
    inputSizeBytes: input.inputSizeBytes,
    startSegment: input.startSegment ?? null,
    endSegment: input.endSegment ?? null,
    detailJson: mergeDetail(input.detail, undefined),
  });
  return new AsyncIndexActionTracker(db, seq, input.detail);
}
