import { Result } from "better-result";
import type { SearchFtsClause, SearchTextTarget } from "./query";
import { FtsFieldView, FtsSectionView, type FtsPostingBlock } from "./fts_format";

export const SEARCH_PREFIX_TERM_LIMIT = 1024;

type PostingDoc = {
  docId: number;
  positions?: number[];
};

type ResolvedTermGroup = {
  ordinals: number[];
  docFreq: number;
};

type CandidateDocIds = ReadonlySet<number> | null;

function intersectInto(target: Set<number> | null, next: Set<number>): Set<number> {
  if (target == null) return next;
  for (const docId of Array.from(target)) {
    if (!next.has(docId)) target.delete(docId);
  }
  return target;
}

function unionInto(target: Set<number>, next: Iterable<number>): void {
  for (const docId of next) target.add(docId);
}

function unionDocIdsInto(target: Set<number>, docIds: Uint32Array): void {
  for (let index = 0; index < docIds.length; index++) target.add(docIds[index]!);
}

function postingDocs(field: FtsFieldView, termOrdinal: number): Map<number, PostingDoc> {
  const docs = new Map<number, PostingDoc>();
  const iterator = field.postings(termOrdinal);
  for (;;) {
    const block = iterator.nextBlock();
    if (!block) break;
    for (let index = 0; index < block.docIds.length; index++) {
      const docId = block.docIds[index]!;
      if (!block.positions || !block.posOffsets) {
        docs.set(docId, { docId });
        continue;
      }
      const start = block.posOffsets[index]!;
      const end = block.posOffsets[index + 1]!;
      docs.set(docId, {
        docId,
        positions: Array.from(block.positions.subarray(start, end)),
      });
    }
  }
  return docs;
}

function docsForTerm(field: FtsFieldView, termOrdinal: number, candidateDocIds: CandidateDocIds = null): Set<number> {
  if (candidateDocIds && candidateDocIds.size === 0) return new Set();
  const docs = new Set<number>();
  const iterator = field.postings(termOrdinal);
  for (;;) {
    const block = iterator.nextBlock();
    if (!block) break;
    if (!candidateDocIds) {
      unionDocIdsInto(docs, block.docIds);
      continue;
    }
    for (let index = 0; index < block.docIds.length; index++) {
      const docId = block.docIds[index]!;
      if (candidateDocIds.has(docId)) docs.add(docId);
    }
    if (docs.size === candidateDocIds.size) break;
  }
  return docs;
}

function docsForOrdinals(field: FtsFieldView, ordinals: number[], candidateDocIds: CandidateDocIds = null): Set<number> {
  if (candidateDocIds && candidateDocIds.size === 0) return new Set();
  const docs = new Set<number>();
  for (const ordinal of ordinals) {
    unionInto(docs, docsForTerm(field, ordinal, candidateDocIds));
    if (candidateDocIds && docs.size === candidateDocIds.size) break;
  }
  return docs;
}

function totalDocFreq(field: FtsFieldView, ordinals: number[]): number {
  let total = 0;
  for (const ordinal of ordinals) total += field.docFreq(ordinal);
  return total;
}

function sortBySelectivity(groups: ResolvedTermGroup[]): ResolvedTermGroup[] {
  return [...groups].sort((left, right) => left.docFreq - right.docFreq);
}

function phraseDocsForFieldResult(
  field: FtsFieldView,
  tokens: string[],
  prefix: boolean,
  candidateDocIds: CandidateDocIds = null
): Result<Set<number>, { message: string }> {
  if (!field.positions) return Result.err({ message: "field does not support phrase queries" });
  if (tokens.length === 0) return Result.ok(new Set());

  const expandedTokens: ResolvedTermGroup[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const isLast = index === tokens.length - 1;
    if (prefix && isLast) {
      const expansionRes = field.expandPrefixResult(token, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      expandedTokens.push({ ordinals: expansionRes.value, docFreq: totalDocFreq(field, expansionRes.value) });
      continue;
    }
    const termOrdinal = field.lookupTerm(token);
    const ordinals = termOrdinal == null ? [] : [termOrdinal];
    expandedTokens.push({ ordinals, docFreq: totalDocFreq(field, ordinals) });
  }

  let candidateDocs: Set<number> | null = candidateDocIds ? new Set(candidateDocIds) : null;
  for (const group of sortBySelectivity(expandedTokens)) {
    const docs = docsForOrdinals(field, group.ordinals, candidateDocs);
    candidateDocs = intersectInto(candidateDocs, docs);
    if (candidateDocs.size === 0) return Result.ok(candidateDocs);
  }

  const positionsByToken = expandedTokens.map((group) => group.ordinals.map((ordinal) => postingDocs(field, ordinal)));
  const matches = new Set<number>();
  for (const docId of candidateDocs ?? []) {
    const positionSets: Array<Set<number>[]> = [];
    for (const tokenMaps of positionsByToken) {
      const variants: Set<number>[] = [];
      for (const tokenMap of tokenMaps) {
        const posting = tokenMap.get(docId);
        if (posting?.positions) variants.push(new Set(posting.positions));
      }
      positionSets.push(variants);
    }
    let found = false;
    for (const startPositions of positionSets[0] ?? []) {
      for (const start of startPositions) {
        let ok = true;
        for (let tokenIndex = 1; tokenIndex < positionSets.length; tokenIndex++) {
          const needed = start + tokenIndex;
          const any = positionSets[tokenIndex]!.some((positions) => positions.has(needed));
          if (!any) {
            ok = false;
            break;
          }
        }
        if (ok) {
          found = true;
          break;
        }
      }
      if (found) break;
    }
    if (found) matches.add(docId);
  }
  return Result.ok(matches);
}

function docsForTextFieldResult(
  field: FtsFieldView,
  tokens: string[],
  phrase: boolean,
  prefix: boolean,
  candidateDocIds: CandidateDocIds = null
): Result<Set<number>, { message: string }> {
  if (phrase) return phraseDocsForFieldResult(field, tokens, prefix, candidateDocIds);
  if (tokens.length === 0) return Result.ok(new Set());

  const groups: ResolvedTermGroup[] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const isLast = index === tokens.length - 1;
    if (prefix && isLast) {
      const expansionRes = field.expandPrefixResult(token, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      groups.push({ ordinals: expansionRes.value, docFreq: totalDocFreq(field, expansionRes.value) });
    } else {
      const termOrdinal = field.lookupTerm(token);
      const ordinals = termOrdinal == null ? [] : [termOrdinal];
      groups.push({ ordinals, docFreq: totalDocFreq(field, ordinals) });
    }
  }

  let intersection: Set<number> | null = candidateDocIds ? new Set(candidateDocIds) : null;
  for (const group of sortBySelectivity(groups)) {
    const docs = docsForOrdinals(field, group.ordinals, intersection);
    intersection = intersectInto(intersection, docs);
    if (intersection.size === 0) return Result.ok(intersection);
  }
  return Result.ok(intersection ?? new Set());
}

function docsForTargetFieldResult(
  field: FtsFieldView,
  target: SearchTextTarget,
  tokens: string[],
  phrase: boolean,
  prefix: boolean,
  candidateDocIds: CandidateDocIds = null
): Result<Set<number>, { message: string }> {
  if (target.config.kind === "keyword") {
    const docs = new Set<number>();
    if (tokens.length === 0) return Result.ok(docs);
    if (prefix) {
      const expansionRes = field.expandPrefixResult(tokens[0]!, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      for (const termOrdinal of expansionRes.value) {
        unionInto(docs, docsForTerm(field, termOrdinal, candidateDocIds));
        if (candidateDocIds && docs.size === candidateDocIds.size) break;
      }
      return Result.ok(docs);
    }
    const termOrdinal = field.lookupTerm(tokens[0]!);
    if (termOrdinal != null) unionInto(docs, docsForTerm(field, termOrdinal, candidateDocIds));
    return Result.ok(docs);
  }
  return docsForTextFieldResult(field, tokens, phrase, prefix, candidateDocIds);
}

export function filterDocIdsByFtsClauseResult(args: {
  companion: FtsSectionView;
  clause: SearchFtsClause;
  candidateDocIds?: CandidateDocIds;
}): Result<Set<number>, { message: string }> {
  const candidateDocIds = args.candidateDocIds ?? null;
  if (args.clause.kind === "has") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    const docs = new Set<number>();
    for (const docId of field.existsDocIds()) {
      if (!candidateDocIds || candidateDocIds.has(docId)) docs.add(docId);
    }
    return Result.ok(docs);
  }

  if (args.clause.kind === "keyword") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    if (args.clause.prefix) {
      const expansionRes = field.expandPrefixResult(args.clause.canonicalValue, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      const docs = new Set<number>();
      for (const termOrdinal of expansionRes.value) {
        unionInto(docs, docsForTerm(field, termOrdinal, candidateDocIds));
        if (candidateDocIds && docs.size === candidateDocIds.size) break;
      }
      return Result.ok(docs);
    }
    const termOrdinal = field.lookupTerm(args.clause.canonicalValue);
    return Result.ok(termOrdinal == null ? new Set() : docsForTerm(field, termOrdinal, candidateDocIds));
  }

  const docs = new Set<number>();
  for (const target of args.clause.fields) {
    const field = args.companion.getField(target.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${target.field}` });
    const fieldDocsRes = docsForTargetFieldResult(
      field,
      target,
      args.clause.tokens,
      args.clause.phrase,
      args.clause.prefix,
      candidateDocIds
    );
    if (Result.isError(fieldDocsRes)) return fieldDocsRes;
    unionInto(docs, fieldDocsRes.value);
    if (candidateDocIds && docs.size === candidateDocIds.size) break;
  }
  return Result.ok(docs);
}

function estimateDocFreqForFtsClauseResult(args: {
  companion: FtsSectionView;
  clause: SearchFtsClause;
}): Result<number, { message: string }> {
  if (args.clause.kind === "has") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    return Result.ok(field.existsDocIds().length);
  }

  if (args.clause.kind === "keyword") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    if (args.clause.prefix) {
      const expansionRes = field.expandPrefixResult(args.clause.canonicalValue, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      return Result.ok(totalDocFreq(field, expansionRes.value));
    }
    const termOrdinal = field.lookupTerm(args.clause.canonicalValue);
    return Result.ok(termOrdinal == null ? 0 : field.docFreq(termOrdinal));
  }

  return Result.ok(Number.MAX_SAFE_INTEGER);
}

export function filterDocIdsByFtsClausesResult(args: {
  companion: FtsSectionView;
  clauses: SearchFtsClause[];
  onEstimateMs?: (deltaMs: number) => void;
}): Result<Set<number>, { message: string }> {
  if (args.clauses.length === 0) return Result.ok(new Set());

  const planned: Array<{ clause: SearchFtsClause; docFreq: number }> = [];
  const estimateStartedAt = Date.now();
  for (const clause of args.clauses) {
    const docFreqRes = estimateDocFreqForFtsClauseResult({ companion: args.companion, clause });
    if (Result.isError(docFreqRes)) return docFreqRes;
    planned.push({ clause, docFreq: docFreqRes.value });
  }
  args.onEstimateMs?.(Date.now() - estimateStartedAt);

  planned.sort((left, right) => left.docFreq - right.docFreq);
  let intersection: Set<number> | null = null;
  for (const plan of planned) {
    const clauseRes = filterDocIdsByFtsClauseResult({
      companion: args.companion,
      clause: plan.clause,
      candidateDocIds: intersection,
    });
    if (Result.isError(clauseRes)) return clauseRes;
    intersection = intersection == null ? clauseRes.value : intersectInto(intersection, clauseRes.value);
    if (intersection.size === 0) break;
  }

  return Result.ok(intersection ?? new Set());
}
