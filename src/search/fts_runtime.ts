import { Result } from "better-result";
import type { SearchFtsClause, SearchTextTarget } from "./query";
import { FtsFieldView, FtsSectionView, type FtsPostingBlock } from "./fts_format";

export const SEARCH_PREFIX_TERM_LIMIT = 1024;

type PostingDoc = {
  docId: number;
  positions?: number[];
};

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

function blockDocSet(block: FtsPostingBlock): Set<number> {
  return new Set(Array.from(block.docIds));
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

function docsForTerm(field: FtsFieldView, termOrdinal: number): Set<number> {
  const docs = new Set<number>();
  const iterator = field.postings(termOrdinal);
  for (;;) {
    const block = iterator.nextBlock();
    if (!block) break;
    unionInto(docs, blockDocSet(block));
  }
  return docs;
}

function phraseDocsForFieldResult(
  field: FtsFieldView,
  tokens: string[],
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (!field.positions) return Result.err({ message: "field does not support phrase queries" });
  if (tokens.length === 0) return Result.ok(new Set());

  const expandedTokens: number[][] = [];
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const isLast = index === tokens.length - 1;
    if (prefix && isLast) {
      const expansionRes = field.expandPrefixResult(token, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      expandedTokens.push(expansionRes.value);
      continue;
    }
    const termOrdinal = field.lookupTerm(token);
    expandedTokens.push(termOrdinal == null ? [] : [termOrdinal]);
  }

  let candidateDocs: Set<number> | null = null;
  const postingsByToken = expandedTokens.map((ordinals) => {
    const docs = new Set<number>();
    for (const ordinal of ordinals) unionInto(docs, docsForTerm(field, ordinal));
    return docs;
  });
  for (const docs of postingsByToken) {
    candidateDocs = intersectInto(candidateDocs, docs);
    if (candidateDocs.size === 0) return Result.ok(candidateDocs);
  }

  const positionsByToken = expandedTokens.map((ordinals) => ordinals.map((ordinal) => postingDocs(field, ordinal)));
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
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (phrase) return phraseDocsForFieldResult(field, tokens, prefix);
  if (tokens.length === 0) return Result.ok(new Set());

  let intersection: Set<number> | null = null;
  for (let index = 0; index < tokens.length; index++) {
    const token = tokens[index]!;
    const isLast = index === tokens.length - 1;
    let docs = new Set<number>();
    if (prefix && isLast) {
      const expansionRes = field.expandPrefixResult(token, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      for (const termOrdinal of expansionRes.value) unionInto(docs, docsForTerm(field, termOrdinal));
    } else {
      const termOrdinal = field.lookupTerm(token);
      if (termOrdinal != null) docs = docsForTerm(field, termOrdinal);
    }
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
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (target.config.kind === "keyword") {
    const docs = new Set<number>();
    if (tokens.length === 0) return Result.ok(docs);
    if (prefix) {
      const expansionRes = field.expandPrefixResult(tokens[0]!, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      for (const termOrdinal of expansionRes.value) unionInto(docs, docsForTerm(field, termOrdinal));
      return Result.ok(docs);
    }
    const termOrdinal = field.lookupTerm(tokens[0]!);
    if (termOrdinal != null) unionInto(docs, docsForTerm(field, termOrdinal));
    return Result.ok(docs);
  }
  return docsForTextFieldResult(field, tokens, phrase, prefix);
}

export function filterDocIdsByFtsClauseResult(args: {
  companion: FtsSectionView;
  clause: SearchFtsClause;
}): Result<Set<number>, { message: string }> {
  if (args.clause.kind === "has") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    return Result.ok(new Set(field.existsDocIds()));
  }

  if (args.clause.kind === "keyword") {
    const field = args.companion.getField(args.clause.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${args.clause.field}` });
    if (args.clause.prefix) {
      const expansionRes = field.expandPrefixResult(args.clause.canonicalValue, SEARCH_PREFIX_TERM_LIMIT);
      if (Result.isError(expansionRes)) return expansionRes;
      const docs = new Set<number>();
      for (const termOrdinal of expansionRes.value) unionInto(docs, docsForTerm(field, termOrdinal));
      return Result.ok(docs);
    }
    const termOrdinal = field.lookupTerm(args.clause.canonicalValue);
    return Result.ok(termOrdinal == null ? new Set() : docsForTerm(field, termOrdinal));
  }

  const docs = new Set<number>();
  for (const target of args.clause.fields) {
    const field = args.companion.getField(target.field);
    if (!field) return Result.err({ message: `missing .fts2 field ${target.field}` });
    const fieldDocsRes = docsForTargetFieldResult(field, target, args.clause.tokens, args.clause.phrase, args.clause.prefix);
    if (Result.isError(fieldDocsRes)) return fieldDocsRes;
    unionInto(docs, fieldDocsRes.value);
  }
  return Result.ok(docs);
}
