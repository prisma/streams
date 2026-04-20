import { Result } from "better-result";
import { parseOffsetResult } from "../../offset";
import type { PreparedJsonRecord } from "../profile";
import { expectPlainObjectResult, rejectUnknownKeysResult } from "../profile";

const CHANGE_KEYS = ["type", "key", "value", "old_value", "headers"] as const;
const CHANGE_HEADER_KEYS = ["operation", "txid", "timestamp"] as const;
const CONTROL_KEYS = ["headers"] as const;
const CONTROL_HEADER_KEYS = ["control", "offset"] as const;

function isDateTimeString(value: string): boolean {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) {
    return false;
  }
  return !Number.isNaN(Date.parse(value));
}

function nonEmptyStringFieldResult(
  value: unknown,
  path: string
): Result<string, { message: string }> {
  if (typeof value !== "string" || value.trim() === "") {
    return Result.err({ message: `${path} must be a non-empty string` });
  }
  return Result.ok(value);
}

function validateChangeRecordResult(
  record: Record<string, unknown>,
  headers: Record<string, unknown>
): Result<PreparedJsonRecord, { message: string }> {
  const keyCheck = rejectUnknownKeysResult(record, CHANGE_KEYS, "state-protocol record");
  if (Result.isError(keyCheck)) return keyCheck;

  const headerKeyCheck = rejectUnknownKeysResult(headers, CHANGE_HEADER_KEYS, "state-protocol record.headers");
  if (Result.isError(headerKeyCheck)) return headerKeyCheck;

  const typeRes = nonEmptyStringFieldResult(record.type, "state-protocol record.type");
  if (Result.isError(typeRes)) return typeRes;

  const keyRes = nonEmptyStringFieldResult(record.key, "state-protocol record.key");
  if (Result.isError(keyRes)) return keyRes;

  const operation = headers.operation;
  if (operation !== "insert" && operation !== "update" && operation !== "delete") {
    return Result.err({ message: "state-protocol record.headers.operation must be insert, update, or delete" });
  }

  if ((operation === "insert" || operation === "update") && !Object.prototype.hasOwnProperty.call(record, "value")) {
    return Result.err({ message: `state-protocol ${operation} records must include value` });
  }

  if (Object.prototype.hasOwnProperty.call(headers, "txid")) {
    const txidRes = nonEmptyStringFieldResult(headers.txid, "state-protocol record.headers.txid");
    if (Result.isError(txidRes)) return txidRes;
  }

  if (Object.prototype.hasOwnProperty.call(headers, "timestamp")) {
    if (typeof headers.timestamp !== "string" || !isDateTimeString(headers.timestamp)) {
      return Result.err({ message: "state-protocol record.headers.timestamp must be a valid RFC 3339 timestamp" });
    }
  }

  return Result.ok({ value: record, routingKey: null });
}

function validateControlRecordResult(
  record: Record<string, unknown>,
  headers: Record<string, unknown>
): Result<PreparedJsonRecord, { message: string }> {
  const keyCheck = rejectUnknownKeysResult(record, CONTROL_KEYS, "state-protocol record");
  if (Result.isError(keyCheck)) return keyCheck;

  const headerKeyCheck = rejectUnknownKeysResult(headers, CONTROL_HEADER_KEYS, "state-protocol record.headers");
  if (Result.isError(headerKeyCheck)) return headerKeyCheck;

  const control = headers.control;
  if (control !== "snapshot-start" && control !== "snapshot-end" && control !== "reset") {
    return Result.err({ message: "state-protocol record.headers.control must be snapshot-start, snapshot-end, or reset" });
  }

  if (Object.prototype.hasOwnProperty.call(headers, "offset")) {
    if (typeof headers.offset !== "string") {
      return Result.err({ message: "state-protocol record.headers.offset must be a valid stream offset string" });
    }
    const offsetRes = parseOffsetResult(headers.offset);
    if (Result.isError(offsetRes)) {
      return Result.err({ message: "state-protocol record.headers.offset must be a valid stream offset string" });
    }
  }

  return Result.ok({ value: record, routingKey: null });
}

export function validateStateProtocolRecordResult(value: unknown): Result<PreparedJsonRecord, { message: string }> {
  const recordRes = expectPlainObjectResult(value, "state-protocol record");
  if (Result.isError(recordRes)) {
    return Result.err({ message: "state-protocol records must be JSON objects" });
  }

  const headersRes = expectPlainObjectResult(recordRes.value.headers, "state-protocol record.headers");
  if (Result.isError(headersRes)) {
    return Result.err({ message: "state-protocol record.headers must be an object" });
  }

  const hasControl = Object.prototype.hasOwnProperty.call(headersRes.value, "control");
  const hasOperation = Object.prototype.hasOwnProperty.call(headersRes.value, "operation");

  if (hasControl && hasOperation) {
    return Result.err({ message: "state-protocol record.headers cannot mix control and operation" });
  }
  if (hasControl) return validateControlRecordResult(recordRes.value, headersRes.value);
  if (hasOperation) return validateChangeRecordResult(recordRes.value, headersRes.value);
  return Result.err({ message: "state-protocol record.headers must contain operation or control" });
}
