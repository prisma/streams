import { Result } from "better-result";
import { ExactSectionView } from "./exact_format";
import type { SearchExactClause } from "./query";

type CandidateDocIds = ReadonlySet<number> | null;

function intersectInto(target: Set<number> | null, next: Set<number>): Set<number> {
  if (target == null) return next;
  for (const docId of Array.from(target)) {
    if (!next.has(docId)) target.delete(docId);
  }
  return target;
}

function docsForClauseResult(
  companion: ExactSectionView,
  clause: SearchExactClause,
  candidateDocIds: CandidateDocIds = null
): Result<Set<number>, { message: string; docFreq: number }> {
  const field = companion.getField(clause.field);
  if (!field) return Result.err({ message: `missing .exact2 field ${clause.field}`, docFreq: Number.MAX_SAFE_INTEGER });
  const termOrdinal = field.lookupTerm(clause.canonicalValue);
  if (termOrdinal == null) return Result.ok(new Set());
  const docs = new Set<number>();
  for (const docId of field.docIds(termOrdinal)) {
    if (!candidateDocIds || candidateDocIds.has(docId)) docs.add(docId);
  }
  return Result.ok(docs);
}

export function filterDocIdsByExactClausesResult(args: {
  companion: ExactSectionView;
  clauses: SearchExactClause[];
}): Result<Set<number>, { message: string }> {
  if (args.clauses.length === 0) return Result.ok(new Set());

  const planned: Array<{ clause: SearchExactClause; docFreq: number }> = [];
  for (const clause of args.clauses) {
    const field = args.companion.getField(clause.field);
    if (!field) return Result.err({ message: `missing .exact2 field ${clause.field}` });
    const termOrdinal = field.lookupTerm(clause.canonicalValue);
    planned.push({ clause, docFreq: termOrdinal == null ? 0 : field.docFreq(termOrdinal) });
  }

  planned.sort((left, right) => left.docFreq - right.docFreq);
  let intersection: Set<number> | null = null;
  for (const plan of planned) {
    const clauseRes = docsForClauseResult(args.companion, plan.clause, intersection);
    if (Result.isError(clauseRes)) return Result.err({ message: clauseRes.error.message });
    intersection = intersectInto(intersection, clauseRes.value);
    if (intersection.size === 0) break;
  }

  return Result.ok(intersection ?? new Set());
}
