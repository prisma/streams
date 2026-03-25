import type { StreamProfileSpec } from "../profiles";

export type TouchRow = {
  keyId: number;
  watermark: string; // source stream offset (base-10 string)
  entity: string;
  kind: "table" | "template";
  templateId?: string;
};

export type ProcessRequest = {
  type: "process";
  id: number;
  stream: string;
  fromOffset: bigint;
  toOffset: bigint;
  profile: StreamProfileSpec;
  maxRows: number;
  maxBytes: number;
  emitFineTouches?: boolean;
  fineTouchBudget?: number | null;
  fineGranularity?: "key" | "template";
  processingMode?: "full" | "hotTemplatesOnly";
  filterHotTemplates?: boolean;
  hotTemplateIds?: string[] | null;
};

export type ProcessResult = {
  type: "result";
  id: number;
  stream: string;
  processedThrough: bigint;
  touches: TouchRow[];
  stats: {
    rowsRead: number;
    bytesRead: number;
    changes: number;
    touchesEmitted: number;
    tableTouchesEmitted: number;
    templateTouchesEmitted: number;
    maxSourceTsMs?: number;
    fineTouchesDroppedDueToBudget?: number;
    fineTouchesSuppressedDueToBudget?: boolean;
    fineTouchesSkippedColdTemplate?: number;
  };
};

export type ProcessError = {
  type: "error";
  id: number;
  stream: string;
  message: string;
  stack?: string;
};

export type WorkerMessage = ProcessResult | ProcessError;
