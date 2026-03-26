import { Result } from "better-result";
import type { FtsFieldCompanion, FtsPosting, FtsSegmentCompanion } from "./fts_format";
import type { SearchFtsClause, SearchTextTarget } from "./query";

export const SEARCH_PREFIX_TERM_LIMIT = 1024;

function postingsToDocSet(postings: FtsPosting[] | undefined): Set<number> {
  const out = new Set<number>();
  for (const posting of postings ?? []) out.add(posting.d);
  return out;
}

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

function expandPrefixTermsResult(field: FtsFieldCompanion, prefix: string): Result<string[], { message: string }> {
  const out: string[] = [];
  for (const term of Object.keys(field.terms)) {
    if (!term.startsWith(prefix)) continue;
    out.push(term);
    if (out.length > SEARCH_PREFIX_TERM_LIMIT) {
      return Result.err({ message: `prefix expansion exceeds limit (${SEARCH_PREFIX_TERM_LIMIT})` });
    }
  }
  return Result.ok(out);
}

function phraseDocsForFieldResult(
  field: FtsFieldCompanion,
  tokens: string[],
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (!field.positions) return Result.err({ message: "field does not support phrase queries" });
  if (tokens.length === 0) return Result.ok(new Set());

  const expandedTokens: string[][] = [];
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    const isLast = i === tokens.length - 1;
    if (prefix && isLast) {
      const expansionRes = expandPrefixTermsResult(field, token);
      if (Result.isError(expansionRes)) return expansionRes;
      expandedTokens.push(expansionRes.value);
    } else {
      expandedTokens.push([token]);
    }
  }

  let candidateDocs: Set<number> | null = null;
  const postingsByToken = expandedTokens.map((tokenList) =>
    tokenList.flatMap((token) => field.terms[token] ?? []).filter((posting, index, arr) => arr.findIndex((item) => item.d === posting.d) === index)
  );
  for (const postings of postingsByToken) {
    candidateDocs = intersectInto(candidateDocs, postingsToDocSet(postings));
    if (candidateDocs.size === 0) return Result.ok(candidateDocs);
  }

  const matches = new Set<number>();
  for (const docId of candidateDocs ?? []) {
    const positionSets: Array<Set<number>[]> = [];
    for (let i = 0; i < expandedTokens.length; i++) {
      const tokenVariants = expandedTokens[i];
      const variants: Set<number>[] = [];
      for (const token of tokenVariants) {
        const posting = (field.terms[token] ?? []).find((item) => item.d === docId);
        if (posting?.p) variants.push(new Set(posting.p));
      }
      positionSets.push(variants);
    }
    let found = false;
    for (const startPositions of positionSets[0] ?? []) {
      for (const start of startPositions) {
        let ok = true;
        for (let i = 1; i < positionSets.length; i++) {
          const needed = start + i;
          const any = positionSets[i].some((positions) => positions.has(needed));
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
  field: FtsFieldCompanion,
  tokens: string[],
  phrase: boolean,
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (phrase) return phraseDocsForFieldResult(field, tokens, prefix);
  if (tokens.length === 0) return Result.ok(new Set());

  let intersection: Set<number> | null = null;
  for (let i = 0; i < tokens.length; i++) {
    const token = tokens[i];
    const isLast = i === tokens.length - 1;
    let docs = new Set<number>();
    if (prefix && isLast) {
      const expansionRes = expandPrefixTermsResult(field, token);
      if (Result.isError(expansionRes)) return expansionRes;
      for (const term of expansionRes.value) unionInto(docs, postingsToDocSet(field.terms[term]));
    } else {
      docs = postingsToDocSet(field.terms[token]);
    }
    intersection = intersectInto(intersection, docs);
    if (intersection.size === 0) return Result.ok(intersection);
  }
  return Result.ok(intersection ?? new Set());
}

function docsForTargetFieldResult(
  field: FtsFieldCompanion,
  target: SearchTextTarget,
  tokens: string[],
  phrase: boolean,
  prefix: boolean
): Result<Set<number>, { message: string }> {
  if (target.config.kind === "keyword") {
    const docs = new Set<number>();
    if (tokens.length === 0) return Result.ok(docs);
    if (prefix) {
      const expansionRes = expandPrefixTermsResult(field, tokens[0]);
      if (Result.isError(expansionRes)) return expansionRes;
      for (const term of expansionRes.value) unionInto(docs, postingsToDocSet(field.terms[term]));
      return Result.ok(docs);
    }
    return Result.ok(postingsToDocSet(field.terms[tokens[0]]));
  }
  return docsForTextFieldResult(field, tokens, phrase, prefix);
}

export function filterDocIdsByFtsClauseResult(args: {
  companion: FtsSegmentCompanion;
  clause: SearchFtsClause;
}): Result<Set<number>, { message: string }> {
  if (args.clause.kind === "has") {
    const field = args.companion.fields[args.clause.field];
    if (!field) return Result.err({ message: `missing .fts field ${args.clause.field}` });
    return Result.ok(new Set(field.exists_docs));
  }

  if (args.clause.kind === "keyword") {
    const field = args.companion.fields[args.clause.field];
    if (!field) return Result.err({ message: `missing .fts field ${args.clause.field}` });
    if (args.clause.prefix) {
      const expansionRes = expandPrefixTermsResult(field, args.clause.canonicalValue);
      if (Result.isError(expansionRes)) return expansionRes;
      const docs = new Set<number>();
      for (const term of expansionRes.value) unionInto(docs, postingsToDocSet(field.terms[term]));
      return Result.ok(docs);
    }
    return Result.ok(postingsToDocSet(field.terms[args.clause.canonicalValue]));
  }

  const docs = new Set<number>();
  for (const target of args.clause.fields) {
    const field = args.companion.fields[target.field];
    if (!field) return Result.err({ message: `missing .fts field ${target.field}` });
    const fieldDocsRes = docsForTargetFieldResult(field, target, args.clause.tokens, args.clause.phrase, args.clause.prefix);
    if (Result.isError(fieldDocsRes)) return fieldDocsRes;
    unionInto(docs, fieldDocsRes.value);
  }
  return Result.ok(docs);
}
