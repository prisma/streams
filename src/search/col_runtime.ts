import { Result } from "better-result";
import type { ColFieldData, ColSegmentCompanion } from "./col_format";
import { decodeSortableBool, decodeSortableFloat64, decodeSortableInt64 } from "./column_encoding";
import { bitsetGet } from "./bitset";

type ColValue = bigint | number | boolean;

function decodeValues(field: ColFieldData): ColValue[] {
  const bytes = Buffer.from(field.values_b64, "base64");
  const out: ColValue[] = [];
  if (field.kind === "integer" || field.kind === "date") {
    for (let off = 0; off + 8 <= bytes.length; off += 8) out.push(decodeSortableInt64(bytes, off));
    return out;
  }
  if (field.kind === "float") {
    for (let off = 0; off + 8 <= bytes.length; off += 8) out.push(decodeSortableFloat64(bytes, off));
    return out;
  }
  if (field.kind === "bool") {
    for (let off = 0; off < bytes.length; off++) out.push(decodeSortableBool(bytes, off));
    return out;
  }
  return out;
}

function compareValue(left: ColValue, right: ColValue): number {
  if (typeof left === "bigint" && typeof right === "bigint") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "number" && typeof right === "number") return left < right ? -1 : left > right ? 1 : 0;
  if (typeof left === "boolean" && typeof right === "boolean") return left === right ? 0 : left ? 1 : -1;
  const l = String(left);
  const r = String(right);
  return l < r ? -1 : l > r ? 1 : 0;
}

export function decodeMinMax(field: ColFieldData): { min: ColValue | null; max: ColValue | null } {
  const minBytes = field.min_b64 ? Buffer.from(field.min_b64, "base64") : null;
  const maxBytes = field.max_b64 ? Buffer.from(field.max_b64, "base64") : null;
  if (!minBytes || !maxBytes) return { min: null, max: null };
  if (field.kind === "integer" || field.kind === "date") {
    return { min: decodeSortableInt64(minBytes), max: decodeSortableInt64(maxBytes) };
  }
  if (field.kind === "float") {
    return { min: decodeSortableFloat64(minBytes), max: decodeSortableFloat64(maxBytes) };
  }
  if (field.kind === "bool") {
    return { min: decodeSortableBool(minBytes), max: decodeSortableBool(maxBytes) };
  }
  return { min: null, max: null };
}

export function filterDocIdsByColumnResult(args: {
  companion: ColSegmentCompanion;
  field: string;
  op: "eq" | "gt" | "gte" | "lt" | "lte" | "has";
  value?: ColValue;
}): Result<Set<number>, { message: string }> {
  const fieldData = args.companion.fields[args.field];
  if (!fieldData) return Result.err({ message: `missing .col field ${args.field}` });
  const exists = Buffer.from(fieldData.exists_b64, "base64");
  if (args.op === "has") {
    const docs = new Set<number>();
    for (let i = 0; i < args.companion.doc_count; i++) if (bitsetGet(exists, i)) docs.add(i);
    return Result.ok(docs);
  }
  const values = decodeValues(fieldData);
  let valueIndex = 0;
  const matches = new Set<number>();
  const target = args.value!;
  const { min, max } = decodeMinMax(fieldData);
  if (min != null && max != null) {
    if ((args.op === "gt" || args.op === "gte") && compareValue(max, target) < 0) return Result.ok(matches);
    if ((args.op === "lt" || args.op === "lte") && compareValue(min, target) > 0) return Result.ok(matches);
  }
  for (let docId = 0; docId < args.companion.doc_count; docId++) {
    if (!bitsetGet(exists, docId)) continue;
    const current = values[valueIndex++];
    const cmp = compareValue(current, target);
    if (args.op === "eq" && cmp === 0) matches.add(docId);
    if (args.op === "gt" && cmp > 0) matches.add(docId);
    if (args.op === "gte" && cmp >= 0) matches.add(docId);
    if (args.op === "lt" && cmp < 0) matches.add(docId);
    if (args.op === "lte" && cmp <= 0) matches.add(docId);
  }
  return Result.ok(matches);
}
