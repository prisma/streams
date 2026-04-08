import type { SqliteDurableStore } from "../db/db";

type JsonPrimitive = string | number | boolean | null;
type JsonValue = JsonPrimitive | JsonValue[] | { [key: string]: JsonValue };

export type SegmentBuildActionDetail = { [key: string]: JsonValue };

export type SegmentBuildActionStart = {
  stream: string;
  actionKind: "segment_build";
  inputKind: "wal";
  segmentIndex?: number | null;
  startOffset?: bigint | null;
  detail?: SegmentBuildActionDetail;
};

export type SegmentBuildActionFinish = {
  inputCount?: number;
  inputSizeBytes?: bigint;
  outputCount?: number;
  outputSizeBytes?: bigint;
  endOffset?: bigint | null;
  detail?: SegmentBuildActionDetail;
};

function mergeDetail(base: SegmentBuildActionDetail | undefined, extra: SegmentBuildActionDetail | undefined): string {
  return JSON.stringify({
    ...(base ?? {}),
    ...(extra ?? {}),
  });
}

function shouldIgnoreSegmentBuildTelemetryError(error: unknown): boolean {
  const message = String((error as { message?: unknown })?.message ?? error ?? "").toLowerCase();
  const code = String((error as { code?: unknown })?.code ?? "");
  return (
    code === "SQLITE_IOERR_VNODE" ||
    message.includes("disk i/o error") ||
    message.includes("database has closed") ||
    message.includes("closed database") ||
    message.includes("statement has finalized")
  );
}

export class SegmentBuildActionTracker {
  private finished = false;

  constructor(
    private readonly db: SqliteDurableStore,
    private readonly seq: bigint | null,
    private readonly startDetail: SegmentBuildActionDetail | undefined
  ) {}

  succeed(finish: SegmentBuildActionFinish = {}): void {
    if (this.finished) return;
    this.finished = true;
    if (this.seq == null) return;
    try {
      this.db.completeSegmentBuildAction({
        seq: this.seq,
        status: "succeeded",
        inputCount: finish.inputCount ?? 0,
        inputSizeBytes: finish.inputSizeBytes ?? 0n,
        outputCount: finish.outputCount ?? 1,
        outputSizeBytes: finish.outputSizeBytes ?? 0n,
        endOffset: finish.endOffset ?? null,
        detailJson: mergeDetail(this.startDetail, finish.detail),
      });
    } catch (error) {
      if (shouldIgnoreSegmentBuildTelemetryError(error)) return;
      console.warn("[segment-build-actions] failed to complete succeeded telemetry row", error);
    }
  }

  fail(errorMessage: string, finish: SegmentBuildActionFinish = {}): void {
    if (this.finished) return;
    this.finished = true;
    if (this.seq == null) return;
    try {
      this.db.completeSegmentBuildAction({
        seq: this.seq,
        status: "failed",
        inputCount: finish.inputCount ?? 0,
        inputSizeBytes: finish.inputSizeBytes ?? 0n,
        outputCount: finish.outputCount ?? 0,
        outputSizeBytes: finish.outputSizeBytes ?? 0n,
        endOffset: finish.endOffset ?? null,
        errorMessage,
        detailJson: mergeDetail(this.startDetail, finish.detail),
      });
    } catch (error) {
      if (shouldIgnoreSegmentBuildTelemetryError(error)) return;
      console.warn("[segment-build-actions] failed to complete failed telemetry row", error);
    }
  }
}

export function beginSegmentBuildAction(db: SqliteDurableStore, input: SegmentBuildActionStart): SegmentBuildActionTracker {
  try {
    const seq = db.beginSegmentBuildAction({
      stream: input.stream,
      actionKind: input.actionKind,
      inputKind: input.inputKind,
      segmentIndex: input.segmentIndex ?? null,
      startOffset: input.startOffset ?? null,
      detailJson: mergeDetail(input.detail, undefined),
    });
    return new SegmentBuildActionTracker(db, seq, input.detail);
  } catch (error) {
    if (shouldIgnoreSegmentBuildTelemetryError(error)) return new SegmentBuildActionTracker(db, null, input.detail);
    console.warn("[segment-build-actions] failed to begin telemetry row", error);
    return new SegmentBuildActionTracker(db, null, input.detail);
  }
}
