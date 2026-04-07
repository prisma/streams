import type { Config } from "../config";
import type { SqliteDurableStore } from "../db/db";

export type SegmentCommitArgs = {
  segmentId: string;
  stream: string;
  segmentIndex: number;
  startOffset: bigint;
  endOffset: bigint;
  blockCount: number;
  lastAppendMs: bigint;
  payloadBytes: bigint;
  sizeBytes: number;
  localPath: string;
  rowsSealed: bigint;
};

export type SegmenterControl = {
  tryClaimSegment: (stream: string) => boolean | Promise<boolean>;
  commitSealedSegment: (row: SegmentCommitArgs) => void | Promise<void>;
  releaseSegmentClaim: (stream: string) => void | Promise<void>;
};

export type SegmenterControlRequest =
  | { type: "segmenter-control-request"; requestId: number; op: "claim"; stream: string }
  | { type: "segmenter-control-request"; requestId: number; op: "release"; stream: string }
  | { type: "segmenter-control-request"; requestId: number; op: "commit"; row: SegmentCommitArgs };

export type SegmenterControlResponse =
  | { type: "segmenter-control-response"; requestId: number; ok: true; claimed?: boolean }
  | { type: "segmenter-control-response"; requestId: number; ok: false; errorMessage: string; errorCode?: string; errorErrno?: number };

function isSqliteBusy(err: any): boolean {
  const code = String(err?.code ?? "");
  const errno = Number(err?.errno ?? -1);
  return code === "SQLITE_BUSY" || code === "SQLITE_BUSY_SNAPSHOT" || errno === 5 || errno === 517;
}

export class SegmenterControlCoordinator implements SegmenterControl {
  private readonly config: Config;
  private readonly db: SqliteDurableStore;

  constructor(config: Config, db: SqliteDurableStore) {
    this.config = config;
    this.db = db;
  }

  private async runWithBusyRetry<T>(fn: () => T): Promise<T> {
    const maxBusyMs = Math.max(0, this.config.ingestBusyTimeoutMs);
    if (maxBusyMs <= 0) return fn();
    const startedAtMs = Date.now();
    let attempt = 0;
    for (;;) {
      try {
        return fn();
      } catch (error) {
        if (!isSqliteBusy(error)) throw error;
        const elapsed = Date.now() - startedAtMs;
        if (elapsed >= maxBusyMs) throw error;
        const delay = Math.min(200, 5 * 2 ** attempt);
        attempt += 1;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }

  async tryClaimSegment(stream: string): Promise<boolean> {
    return this.runWithBusyRetry(() => this.db.tryClaimSegment(stream));
  }

  async commitSealedSegment(row: SegmentCommitArgs): Promise<void> {
    await this.runWithBusyRetry(() => this.db.commitSealedSegment(row));
  }

  async releaseSegmentClaim(stream: string): Promise<void> {
    await this.runWithBusyRetry(() => this.db.setSegmentInProgress(stream, 0));
  }

  async handleRequest(msg: SegmenterControlRequest): Promise<SegmenterControlResponse> {
    try {
      if (msg.op === "claim") {
        const claimed = await this.tryClaimSegment(msg.stream);
        return { type: "segmenter-control-response", requestId: msg.requestId, ok: true, claimed };
      }
      if (msg.op === "release") {
        await this.releaseSegmentClaim(msg.stream);
        return { type: "segmenter-control-response", requestId: msg.requestId, ok: true };
      }
      await this.commitSealedSegment(msg.row);
      return { type: "segmenter-control-response", requestId: msg.requestId, ok: true };
    } catch (error: any) {
      return {
        type: "segmenter-control-response",
        requestId: msg.requestId,
        ok: false,
        errorMessage: String(error?.message ?? error),
        errorCode: error?.code == null ? undefined : String(error.code),
        errorErrno: error?.errno == null ? undefined : Number(error.errno),
      };
    }
  }
}
