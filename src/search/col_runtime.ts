import { Result } from "better-result";
import { ColSectionView, compareColScalars, type ColScalar } from "./col_format";

function pageMayMatch(page: { min: ColScalar; max: ColScalar }, op: "eq" | "gt" | "gte" | "lt" | "lte", target: ColScalar): boolean {
  if (op === "eq") return compareColScalars(page.min, target) <= 0 && compareColScalars(page.max, target) >= 0;
  if (op === "gt") return compareColScalars(page.max, target) > 0;
  if (op === "gte") return compareColScalars(page.max, target) >= 0;
  if (op === "lt") return compareColScalars(page.min, target) < 0;
  return compareColScalars(page.min, target) <= 0;
}

function compareCurrent(op: "eq" | "gt" | "gte" | "lt" | "lte", current: ColScalar, target: ColScalar): boolean {
  const cmp = compareColScalars(current, target);
  if (op === "eq") return cmp === 0;
  if (op === "gt") return cmp > 0;
  if (op === "gte") return cmp >= 0;
  if (op === "lt") return cmp < 0;
  return cmp <= 0;
}

export function filterDocIdsByColumnResult(args: {
  companion: ColSectionView;
  field: string;
  op: "eq" | "gt" | "gte" | "lt" | "lte" | "has";
  value?: ColScalar;
}): Result<Set<number>, { message: string }> {
  const field = args.companion.getField(args.field);
  if (!field) return Result.err({ message: `missing .col2 field ${args.field}` });
  if (args.op === "has") return Result.ok(new Set(field.docIds()));
  const target = args.value!;
  const op = args.op;
  const min = field.minValue();
  const max = field.maxValue();
  if (min != null && max != null) {
    if ((op === "gt" || op === "gte") && compareColScalars(max, target) < (op === "gt" ? 1 : 0)) return Result.ok(new Set());
    if ((op === "lt" || op === "lte") && compareColScalars(min, target) > (op === "lt" ? -1 : 0)) return Result.ok(new Set());
    if (op === "eq" && (compareColScalars(min, target) > 0 || compareColScalars(max, target) < 0)) return Result.ok(new Set());
  }

  const matches = new Set<number>();
  if (!field.hasPageIndex()) {
    field.forEachValue((docId, value) => {
      if (compareCurrent(op, value, target)) matches.add(docId);
    });
    return Result.ok(matches);
  }

  const pages = field.pageEntries();
  for (let pageIndex = 0; pageIndex < pages.length; pageIndex++) {
    const page = pages[pageIndex]!;
    if (!pageMayMatch(page, op, target)) continue;
    const start = page.valueStartIndex;
    const end = pageIndex === pages.length - 1 ? field.docIds().length : pages[pageIndex + 1]!.valueStartIndex;
    field.forEachValueRange(start, end, (docId, value) => {
      if (compareCurrent(op, value, target)) matches.add(docId);
    });
  }
  return Result.ok(matches);
}
