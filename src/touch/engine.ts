import type { StreamInterpreterConfig } from "./spec.ts";

export type CanonicalChange = {
  entity: string;
  key?: string;
  op: "insert" | "update" | "delete";
  before?: unknown;
  after?: unknown;
};

export function interpretRecordToChanges(record: any, _cfg: StreamInterpreterConfig): CanonicalChange[] {
  return interpretStateProtocolRecord(record);
}

function interpretStateProtocolRecord(record: any): CanonicalChange[] {
  if (!record || typeof record !== "object" || Array.isArray(record)) return [];
  const headers = (record as any).headers;
  if (!headers || typeof headers !== "object" || Array.isArray(headers)) return [];

  // Control messages are ignored by touch derivation.
  if (typeof (headers as any).control === "string") return [];

  const opRaw = (headers as any).operation;
  if (typeof opRaw !== "string") return [];
  const op = opRaw;
  if (op !== "insert" && op !== "update" && op !== "delete") return [];

  const type = (record as any).type;
  const key = (record as any).key;
  if (typeof type !== "string" || type.trim() === "") return [];
  if (typeof key !== "string" || key.trim() === "") return [];

  const before = Object.prototype.hasOwnProperty.call(record, "oldValue")
    ? (record as any).oldValue
    : Object.prototype.hasOwnProperty.call(record, "old_value")
      ? (record as any).old_value
      : undefined;
  const after = Object.prototype.hasOwnProperty.call(record, "value") ? (record as any).value : undefined;

  return [{ entity: type, key, op, before, after }];
}
