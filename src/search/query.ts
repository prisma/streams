import { Result } from "better-result";
import type { SchemaRegistry, SearchConfig, SearchFieldConfig } from "../schema/registry";
import {
  analyzeTextValue,
  canonicalizeColumnValue,
  canonicalizeExactValue,
  extractRawSearchValuesResult,
  extractSearchExactValuesResult,
  extractSearchTextValuesResult,
  resolveSearchAlias,
} from "./schema";

type Token =
  | { kind: "word"; value: string }
  | { kind: "string"; value: string }
  | { kind: "lparen" }
  | { kind: "rparen" }
  | { kind: "colon" }
  | { kind: "op"; value: "=" | ">" | ">=" | "<" | "<=" }
  | { kind: "minus" };

type ParsedQuery =
  | { kind: "and"; left: ParsedQuery; right: ParsedQuery }
  | { kind: "or"; left: ParsedQuery; right: ParsedQuery }
  | { kind: "not"; expr: ParsedQuery }
  | { kind: "has"; field: string }
  | { kind: "field"; field: string; op: SearchComparisonOp; value: string; quoted: boolean }
  | { kind: "bare"; value: string; quoted: boolean };

export type SearchComparisonOp = "eq" | "gt" | "gte" | "lt" | "lte";
export type SearchSortDirection = "asc" | "desc";
export type SearchSortSpec =
  | { kind: "score"; direction: SearchSortDirection }
  | { kind: "offset"; direction: SearchSortDirection }
  | { kind: "field"; direction: SearchSortDirection; field: string; config: SearchFieldConfig };

export type SearchTextTarget = {
  field: string;
  config: SearchFieldConfig;
  boost: number;
};

export type CompiledSearchQuery =
  | { kind: "and"; left: CompiledSearchQuery; right: CompiledSearchQuery }
  | { kind: "or"; left: CompiledSearchQuery; right: CompiledSearchQuery }
  | { kind: "not"; expr: CompiledSearchQuery }
  | { kind: "has"; field: string; config: SearchFieldConfig }
  | {
      kind: "compare";
      field: string;
      config: SearchFieldConfig;
      op: SearchComparisonOp;
      canonicalValue?: string;
      compareValue?: bigint | number | boolean;
    }
  | {
      kind: "keyword";
      field: string;
      config: SearchFieldConfig;
      canonicalValue: string;
      prefix: boolean;
    }
  | {
      kind: "text";
      fields: SearchTextTarget[];
      tokens: string[];
      phrase: boolean;
      prefix: boolean;
      rawText: string;
    };

export type SearchRequest = {
  q: CompiledSearchQuery;
  size: number;
  trackTotalHits: boolean;
  timeoutMs: number | null;
  sort: SearchSortSpec[];
  searchAfter: unknown[] | null;
};

export type SearchExactClause = {
  field: string;
  canonicalValue: string;
};

export type SearchColumnClause = {
  field: string;
  op: SearchComparisonOp | "has";
  compareValue?: bigint | number | boolean;
};

export type SearchFtsClause =
  | { kind: "has"; field: string }
  | { kind: "keyword"; field: string; canonicalValue: string; prefix: boolean }
  | { kind: "text"; fields: SearchTextTarget[]; tokens: string[]; phrase: boolean; prefix: boolean };

type SearchDocument = {
  exactValues: Map<string, string[]>;
  textValues: Map<string, string[]>;
  rawValues: Map<string, unknown[]>;
};

export type SearchEvaluation = {
  matched: boolean;
  score: number;
  matchedText: boolean;
};

export type SearchHitFieldMap = Record<string, unknown>;

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function tokenizeResult(input: string): Result<Token[], { message: string }> {
  const tokens: Token[] = [];
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (/\s/.test(ch)) {
      i += 1;
      continue;
    }
    if (ch === "(") {
      tokens.push({ kind: "lparen" });
      i += 1;
      continue;
    }
    if (ch === ")") {
      tokens.push({ kind: "rparen" });
      i += 1;
      continue;
    }
    if (ch === ":") {
      tokens.push({ kind: "colon" });
      i += 1;
      continue;
    }
    if (ch === "-") {
      tokens.push({ kind: "minus" });
      i += 1;
      continue;
    }
    if (ch === ">" || ch === "<" || ch === "=") {
      if ((ch === ">" || ch === "<") && input[i + 1] === "=") {
        tokens.push({ kind: "op", value: `${ch}=` as ">=" | "<=" });
        i += 2;
        continue;
      }
      tokens.push({ kind: "op", value: ch as "=" | ">" | "<" });
      i += 1;
      continue;
    }
    if (ch === "\"") {
      let out = "";
      i += 1;
      while (i < input.length) {
        const cur = input[i];
        if (cur === "\\") {
          if (i + 1 >= input.length) return Result.err({ message: "unterminated escape in query string" });
          out += input[i + 1];
          i += 2;
          continue;
        }
        if (cur === "\"") break;
        out += cur;
        i += 1;
      }
      if (i >= input.length || input[i] !== "\"") return Result.err({ message: "unterminated quoted string in search query" });
      i += 1;
      tokens.push({ kind: "string", value: out });
      continue;
    }
    let j = i;
    while (j < input.length) {
      const cur = input[j];
      if (/\s/.test(cur) || cur === "(" || cur === ")" || cur === ":" || cur === ">" || cur === "<" || cur === "=") break;
      j += 1;
    }
    const word = input.slice(i, j);
    if (word === "") return Result.err({ message: "invalid search query syntax" });
    tokens.push({ kind: "word", value: word });
    i = j;
  }
  return Result.ok(tokens);
}

class Parser {
  constructor(private readonly tokens: Token[], private pos = 0) {}

  parseResult(): Result<ParsedQuery, { message: string }> {
    const exprRes = this.parseOrResult();
    if (Result.isError(exprRes)) return exprRes;
    if (!this.isAtEnd()) return Result.err({ message: "unexpected token in search query" });
    return exprRes;
  }

  private parseOrResult(): Result<ParsedQuery, { message: string }> {
    let leftRes = this.parseAndResult();
    if (Result.isError(leftRes)) return leftRes;
    let left = leftRes.value;
    while (this.peekWord("OR")) {
      this.pos += 1;
      const rightRes = this.parseAndResult();
      if (Result.isError(rightRes)) return rightRes;
      left = { kind: "or", left, right: rightRes.value };
    }
    return Result.ok(left);
  }

  private parseAndResult(): Result<ParsedQuery, { message: string }> {
    let leftRes = this.parseUnaryResult();
    if (Result.isError(leftRes)) return leftRes;
    let left = leftRes.value;
    while (!this.isAtEnd() && !this.peekKind("rparen") && !this.peekWord("OR")) {
      if (this.peekWord("AND")) this.pos += 1;
      const rightRes = this.parseUnaryResult();
      if (Result.isError(rightRes)) return rightRes;
      left = { kind: "and", left, right: rightRes.value };
    }
    return Result.ok(left);
  }

  private parseUnaryResult(): Result<ParsedQuery, { message: string }> {
    if (this.peekWord("NOT")) {
      this.pos += 1;
      const innerRes = this.parseUnaryResult();
      if (Result.isError(innerRes)) return innerRes;
      return Result.ok({ kind: "not", expr: innerRes.value });
    }
    if (this.peekKind("minus")) {
      this.pos += 1;
      const innerRes = this.parseUnaryResult();
      if (Result.isError(innerRes)) return innerRes;
      return Result.ok({ kind: "not", expr: innerRes.value });
    }
    return this.parsePrimaryResult();
  }

  private parsePrimaryResult(): Result<ParsedQuery, { message: string }> {
    if (this.peekKind("lparen")) {
      this.pos += 1;
      const exprRes = this.parseOrResult();
      if (Result.isError(exprRes)) return exprRes;
      if (!this.peekKind("rparen")) return Result.err({ message: "missing ')' in search query" });
      this.pos += 1;
      return exprRes;
    }

    const token = this.consumeWordOrString();
    if (!token) return Result.err({ message: "expected clause in search query" });
    const quoted = token.kind === "string";

    if (token.kind === "word" && this.peekKind("colon")) {
      this.pos += 1;
      if (token.value === "has") {
        const fieldToken = this.consumeWordOrString();
        if (!fieldToken || fieldToken.kind !== "word") return Result.err({ message: "has: requires a field name" });
        return Result.ok({ kind: "has", field: fieldToken.value });
      }
      if (token.value === "contains") {
        return Result.err({ message: "contains: is not supported on _search yet" });
      }
      let op: SearchComparisonOp = "eq";
      if (this.peekKind("op")) {
        const raw = (this.tokens[this.pos] as Extract<Token, { kind: "op" }>).value;
        this.pos += 1;
        op = raw === "=" ? "eq" : raw === ">" ? "gt" : raw === ">=" ? "gte" : raw === "<" ? "lt" : "lte";
      }
      const valueToken = this.consumeWordOrString();
      if (!valueToken) return Result.err({ message: "expected value in fielded search clause" });
      return Result.ok({
        kind: "field",
        field: token.value,
        op,
        value: valueToken.value,
        quoted: valueToken.kind === "string",
      });
    }

    return Result.ok({ kind: "bare", value: token.value, quoted });
  }

  private consumeWordOrString(): Extract<Token, { kind: "word" | "string" }> | null {
    const token = this.tokens[this.pos];
    if (!token || (token.kind !== "word" && token.kind !== "string")) return null;
    this.pos += 1;
    return token;
  }

  private peekKind(kind: Token["kind"]): boolean {
    return this.tokens[this.pos]?.kind === kind;
  }

  private peekWord(value: string): boolean {
    const token = this.tokens[this.pos];
    return token?.kind === "word" && token.value.toUpperCase() === value;
  }

  private isAtEnd(): boolean {
    return this.pos >= this.tokens.length;
  }
}

function resolveDefaultFieldsResult(search: SearchConfig | undefined): Result<SearchTextTarget[], { message: string }> {
  if (!search) return Result.err({ message: "search is not configured for this stream" });
  const out: SearchTextTarget[] = [];
  if (Array.isArray(search.defaultFields) && search.defaultFields.length > 0) {
    for (const entry of search.defaultFields) {
      const resolved = resolveSearchAlias(search, entry.field);
      const config = search.fields[resolved];
      if (!config) return Result.err({ message: `search default field ${entry.field} is not defined` });
      if (config.kind !== "text" && config.kind !== "keyword") {
        return Result.err({ message: `search default field ${entry.field} must be text or keyword` });
      }
      out.push({ field: resolved, config, boost: entry.boost ?? 1 });
    }
    return Result.ok(out);
  }
  for (const [field, config] of Object.entries(search.fields)) {
    if (config.kind === "text") out.push({ field, config, boost: 1 });
  }
  if (out.length === 0) return Result.err({ message: "search.defaultFields must include at least one text or keyword field" });
  return Result.ok(out);
}

function compileCompareValueResult(
  config: SearchFieldConfig,
  rawValue: string,
  op: SearchComparisonOp
): Result<{ canonicalValue?: string; compareValue?: bigint | number | boolean }, { message: string }> {
  const canonical = canonicalizeExactValue(config, rawValue);
  if (canonical == null) return Result.err({ message: "invalid value for search field" });
  if (op === "eq") {
    const compareValue = canonicalizeColumnValue(config, rawValue);
    return Result.ok({ canonicalValue: canonical, compareValue: compareValue ?? undefined });
  }
  const compareValue = canonicalizeColumnValue(config, rawValue);
  if (compareValue == null) return Result.err({ message: "comparison operator is only supported on typed column fields" });
  return Result.ok({ canonicalValue: canonical, compareValue });
}

function compileTextClauseResult(
  fields: SearchTextTarget[],
  rawValue: string,
  quoted: boolean
): Result<CompiledSearchQuery, { message: string }> {
  const prefix = rawValue.endsWith("*") && rawValue.length > 1;
  const text = prefix ? rawValue.slice(0, -1) : rawValue;
  const normalized = text.trim();
  if (normalized === "") return Result.err({ message: "search text clause must not be empty" });
  let tokens: string[] = [];
  for (const field of fields) {
    if (field.config.kind === "keyword") {
      const canonical = canonicalizeExactValue(field.config, normalized);
      if (canonical) tokens = [canonical];
      break;
    }
    tokens = analyzeTextValue(normalized, field.config.analyzer);
    if (tokens.length > 0) break;
  }
  if (tokens.length === 0) return Result.err({ message: "search text clause did not produce any searchable tokens" });
  return Result.ok({
    kind: "text",
    fields,
    tokens,
    phrase: quoted,
    prefix,
    rawText: normalized,
  });
}

function compileFieldClauseResult(
  search: SearchConfig,
  fieldName: string,
  op: SearchComparisonOp,
  rawValue: string,
  quoted: boolean
): Result<CompiledSearchQuery, { message: string }> {
  const resolvedField = resolveSearchAlias(search, fieldName);
  const config = search.fields[resolvedField];
  if (!config) return Result.err({ message: `unknown search field ${fieldName}` });

  if (config.kind === "integer" || config.kind === "float" || config.kind === "date" || config.kind === "bool") {
    if (!config.column && op !== "eq") {
      return Result.err({ message: `search field ${fieldName} does not support comparisons` });
    }
    if (!config.column && !config.exact) {
      return Result.err({ message: `search field ${fieldName} does not support equality` });
    }
    const valueRes = compileCompareValueResult(config, rawValue, op);
    if (Result.isError(valueRes)) return valueRes;
    return Result.ok({
      kind: "compare",
      field: resolvedField,
      config,
      op,
      canonicalValue: valueRes.value.canonicalValue,
      compareValue: valueRes.value.compareValue,
    });
  }

  if (config.kind === "keyword") {
    if (op !== "eq") return Result.err({ message: `search field ${fieldName} does not support comparisons` });
    const prefix = rawValue.endsWith("*") && rawValue.length > 1;
    if (prefix && !config.prefix) return Result.err({ message: `search field ${fieldName} does not support prefix queries` });
    if (!prefix && !config.exact) return Result.err({ message: `search field ${fieldName} does not support exact queries` });
    const canonical = canonicalizeExactValue(config, prefix ? rawValue.slice(0, -1) : rawValue);
    if (canonical == null) return Result.err({ message: `invalid keyword value for ${fieldName}` });
    return Result.ok({
      kind: "keyword",
      field: resolvedField,
      config,
      canonicalValue: canonical,
      prefix,
    });
  }

  if (config.kind === "text") {
    if (op !== "eq") return Result.err({ message: `search field ${fieldName} does not support comparisons` });
    return compileTextClauseResult([{ field: resolvedField, config, boost: 1 }], rawValue, quoted);
  }

  return Result.err({ message: `unsupported search field ${fieldName}` });
}

function compileQueryResult(search: SearchConfig, node: ParsedQuery): Result<CompiledSearchQuery, { message: string }> {
  if (node.kind === "and") {
    const leftRes = compileQueryResult(search, node.left);
    if (Result.isError(leftRes)) return leftRes;
    const rightRes = compileQueryResult(search, node.right);
    if (Result.isError(rightRes)) return rightRes;
    return Result.ok({ kind: "and", left: leftRes.value, right: rightRes.value });
  }
  if (node.kind === "or") {
    const leftRes = compileQueryResult(search, node.left);
    if (Result.isError(leftRes)) return leftRes;
    const rightRes = compileQueryResult(search, node.right);
    if (Result.isError(rightRes)) return rightRes;
    return Result.ok({ kind: "or", left: leftRes.value, right: rightRes.value });
  }
  if (node.kind === "not") {
    const innerRes = compileQueryResult(search, node.expr);
    if (Result.isError(innerRes)) return innerRes;
    return Result.ok({ kind: "not", expr: innerRes.value });
  }
  if (node.kind === "has") {
    const resolvedField = resolveSearchAlias(search, node.field);
    const config = search.fields[resolvedField];
    if (!config) return Result.err({ message: `unknown search field ${node.field}` });
    if (!config.exists && !config.exact && !config.column && config.kind !== "text") {
      return Result.err({ message: `search field ${node.field} does not support has:` });
    }
    return Result.ok({ kind: "has", field: resolvedField, config });
  }
  if (node.kind === "field") {
    return compileFieldClauseResult(search, node.field, node.op, node.value, node.quoted);
  }
  const defaultsRes = resolveDefaultFieldsResult(search);
  if (Result.isError(defaultsRes)) return defaultsRes;
  return compileTextClauseResult(defaultsRes.value, node.value, node.quoted);
}

function parseSortItemResult(search: SearchConfig, raw: string): Result<SearchSortSpec, { message: string }> {
  const trimmed = raw.trim();
  if (trimmed === "") return Result.err({ message: "sort entries must not be empty" });
  const idx = trimmed.indexOf(":");
  const fieldName = idx >= 0 ? trimmed.slice(0, idx) : trimmed;
  const directionRaw = idx >= 0 ? trimmed.slice(idx + 1) : "desc";
  const direction = directionRaw === "asc" || directionRaw === "desc" ? directionRaw : null;
  if (!direction) return Result.err({ message: `invalid sort direction for ${fieldName}` });
  if (fieldName === "_score") return Result.ok({ kind: "score", direction });
  if (fieldName === "offset") return Result.ok({ kind: "offset", direction });

  const resolvedField = resolveSearchAlias(search, fieldName);
  const config = search.fields[resolvedField];
  if (!config) return Result.err({ message: `unknown sort field ${fieldName}` });
  if (!config.sortable && resolvedField !== search.primaryTimestampField) {
    return Result.err({ message: `search field ${fieldName} is not sortable` });
  }
  return Result.ok({ kind: "field", direction, field: resolvedField, config });
}

function hasScoringTextClause(query: CompiledSearchQuery): boolean {
  if (query.kind === "and" || query.kind === "or") return hasScoringTextClause(query.left) || hasScoringTextClause(query.right);
  if (query.kind === "not") return hasScoringTextClause(query.expr);
  return query.kind === "text";
}

function normalizeSortWithTieBreaker(search: SearchConfig, query: CompiledSearchQuery, explicit: SearchSortSpec[]): SearchSortSpec[] {
  const timestampSort: SearchSortSpec = {
    kind: "field",
    direction: "desc",
    field: search.primaryTimestampField,
    config: search.fields[search.primaryTimestampField]!,
  };
  const sorts: SearchSortSpec[] =
    explicit.length > 0
      ? [...explicit]
      : hasScoringTextClause(query)
        ? [{ kind: "score", direction: "desc" }, timestampSort, { kind: "offset", direction: "desc" }]
        : [timestampSort, { kind: "offset", direction: "desc" }];
  if (!sorts.some((sort) => sort.kind === "offset")) {
    sorts.push({ kind: "offset", direction: "desc" });
  }
  return sorts;
}

function parseSearchAfterResult(raw: unknown): Result<unknown[] | null, { message: string }> {
  if (raw == null) return Result.ok(null);
  if (!Array.isArray(raw)) return Result.err({ message: "search_after must be an array" });
  return Result.ok(raw.map((value) => structuredClone(value)));
}

export function parseSearchQueryResult(registry: SchemaRegistry, input: string): Result<CompiledSearchQuery, { message: string }> {
  const search = registry.search;
  if (!search) return Result.err({ message: "search is not configured for this stream" });
  const trimmed = input.trim();
  if (trimmed === "") return Result.err({ message: "q must not be empty" });
  const tokensRes = tokenizeResult(trimmed);
  if (Result.isError(tokensRes)) return tokensRes;
  const parser = new Parser(tokensRes.value);
  const parsedRes = parser.parseResult();
  if (Result.isError(parsedRes)) return parsedRes;
  return compileQueryResult(search, parsedRes.value);
}

export function parseSearchRequestBodyResult(registry: SchemaRegistry, raw: unknown): Result<SearchRequest, { message: string }> {
  if (!isPlainObject(raw)) return Result.err({ message: "search request must be an object" });
  if (typeof raw.q !== "string") return Result.err({ message: "q must be a string" });
  const queryRes = parseSearchQueryResult(registry, raw.q);
  if (Result.isError(queryRes)) return queryRes;
  const size = raw.size === undefined ? 50 : Number(raw.size);
  if (!Number.isFinite(size) || size <= 0 || !Number.isInteger(size) || size > 500) {
    return Result.err({ message: "size must be an integer between 1 and 500" });
  }
  const trackTotalHits = raw.track_total_hits === undefined ? true : raw.track_total_hits === true;
  if (raw.track_total_hits !== undefined && typeof raw.track_total_hits !== "boolean") {
    return Result.err({ message: "track_total_hits must be boolean" });
  }
  const timeoutMs =
    raw.timeout_ms === undefined || raw.timeout_ms === null
      ? null
      : typeof raw.timeout_ms === "number" && Number.isFinite(raw.timeout_ms) && raw.timeout_ms >= 0
        ? Math.trunc(raw.timeout_ms)
        : null;
  if (raw.timeout_ms !== undefined && raw.timeout_ms !== null && timeoutMs == null) {
    return Result.err({ message: "timeout_ms must be a non-negative number" });
  }
  const search = registry.search!;
  const sortItems = raw.sort === undefined ? [] : Array.isArray(raw.sort) ? raw.sort : null;
  if (raw.sort !== undefined && !sortItems) return Result.err({ message: "sort must be an array of strings" });
  const parsedSorts: SearchSortSpec[] = [];
  for (const entry of sortItems ?? []) {
    if (typeof entry !== "string") return Result.err({ message: "sort entries must be strings" });
    const sortRes = parseSortItemResult(search, entry);
    if (Result.isError(sortRes)) return sortRes;
    parsedSorts.push(sortRes.value);
  }
  const searchAfterRes = parseSearchAfterResult(raw.search_after);
  if (Result.isError(searchAfterRes)) return searchAfterRes;
  const sort = normalizeSortWithTieBreaker(search, queryRes.value, parsedSorts);
  return Result.ok({
    q: queryRes.value,
    size,
    trackTotalHits,
    timeoutMs,
    sort,
    searchAfter: searchAfterRes.value,
  });
}

export function parseSearchRequestQueryResult(
  registry: SchemaRegistry,
  params: URLSearchParams
): Result<SearchRequest, { message: string }> {
  const q = params.get("q");
  if (!q) return Result.err({ message: "missing q" });
  const sortValues = params.getAll("sort");
  const splitSorts = sortValues.flatMap((value) => value.split(",")).map((value) => value.trim()).filter((value) => value !== "");
  let searchAfter: unknown[] | null = null;
  const searchAfterParam = params.get("search_after");
  if (searchAfterParam) {
    try {
      const parsed = JSON.parse(searchAfterParam);
      if (!Array.isArray(parsed)) return Result.err({ message: "search_after must be a JSON array" });
      searchAfter = parsed;
    } catch {
      return Result.err({ message: "search_after must be a JSON array" });
    }
  }
  return parseSearchRequestBodyResult(registry, {
    q,
    size: params.get("size") ? Number(params.get("size")) : undefined,
    track_total_hits: params.get("track_total_hits") == null ? undefined : params.get("track_total_hits") === "true",
    timeout_ms: params.get("timeout_ms") ? Number(params.get("timeout_ms")) : undefined,
    sort: splitSorts.length > 0 ? splitSorts : undefined,
    search_after: searchAfter,
  });
}

export function collectPositiveSearchExactClauses(query: CompiledSearchQuery): SearchExactClause[] {
  const out: SearchExactClause[] = [];
  const visit = (node: CompiledSearchQuery, negated: boolean): void => {
    if (node.kind === "and") {
      visit(node.left, negated);
      visit(node.right, negated);
      return;
    }
    if (node.kind === "not") {
      visit(node.expr, !negated);
      return;
    }
    if (negated) return;
    if (node.kind === "keyword" && !node.prefix && node.config.exact) {
      out.push({ field: node.field, canonicalValue: node.canonicalValue });
      return;
    }
    if (node.kind === "compare" && node.op === "eq" && node.config.exact && node.canonicalValue) {
      out.push({ field: node.field, canonicalValue: node.canonicalValue });
    }
  };
  visit(query, false);
  return out;
}

export function collectPositiveSearchColumnClauses(query: CompiledSearchQuery): SearchColumnClause[] {
  const out: SearchColumnClause[] = [];
  const supportsColumn = (config: SearchFieldConfig): boolean =>
    config.column === true && (config.kind === "integer" || config.kind === "float" || config.kind === "date" || config.kind === "bool");
  const visit = (node: CompiledSearchQuery, negated: boolean): void => {
    if (node.kind === "and") {
      visit(node.left, negated);
      visit(node.right, negated);
      return;
    }
    if (node.kind === "not") {
      visit(node.expr, !negated);
      return;
    }
    if (negated) return;
    if (node.kind === "has") {
      if (supportsColumn(node.config)) out.push({ field: node.field, op: "has" });
      return;
    }
    if (node.kind === "compare" && supportsColumn(node.config)) {
      out.push({ field: node.field, op: node.op, compareValue: node.compareValue });
    }
  };
  visit(query, false);
  return out;
}

export function collectPositiveSearchFtsClauses(query: CompiledSearchQuery): SearchFtsClause[] {
  const out: SearchFtsClause[] = [];
  const visit = (node: CompiledSearchQuery, negated: boolean): void => {
    if (node.kind === "and") {
      visit(node.left, negated);
      visit(node.right, negated);
      return;
    }
    if (node.kind === "not") {
      visit(node.expr, !negated);
      return;
    }
    if (negated) return;
    if (node.kind === "has") {
      if (node.config.kind === "text" || (node.config.kind === "keyword" && node.config.prefix === true)) {
        out.push({ kind: "has", field: node.field });
      }
      return;
    }
    if (node.kind === "keyword") {
      if (node.config.kind === "keyword" && node.config.prefix === true) {
        out.push({ kind: "keyword", field: node.field, canonicalValue: node.canonicalValue, prefix: node.prefix });
      }
      return;
    }
    if (node.kind === "text") {
      out.push({ kind: "text", fields: node.fields, tokens: node.tokens, phrase: node.phrase, prefix: node.prefix });
    }
  };
  visit(query, false);
  return out;
}

export function buildSearchDocumentResult(
  registry: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<SearchDocument, { message: string }> {
  const exactRes = extractSearchExactValuesResult(registry, offset, value);
  if (Result.isError(exactRes)) return exactRes;
  const textRes = extractSearchTextValuesResult(registry, offset, value);
  if (Result.isError(textRes)) return textRes;
  const rawRes = extractRawSearchValuesResult(registry, offset, value);
  if (Result.isError(rawRes)) return rawRes;
  return Result.ok({
    exactValues: exactRes.value,
    textValues: textRes.value,
    rawValues: rawRes.value,
  });
}

function compareCanonical(left: string, right: bigint | number | boolean | string, kind: SearchFieldConfig["kind"]): number {
  if (kind === "integer" || kind === "date") {
    const l = BigInt(left);
    const r = right as bigint;
    return l < r ? -1 : l > r ? 1 : 0;
  }
  if (kind === "float") {
    const l = Number(left);
    const r = right as number;
    return l < r ? -1 : l > r ? 1 : 0;
  }
  if (kind === "bool") {
    const l = left === "true";
    const r = right === true || right === "true";
    return l === r ? 0 : l ? 1 : -1;
  }
  const r = String(right);
  return left < r ? -1 : left > r ? 1 : 0;
}

function matchPhraseTokens(docTokens: string[], queryTokens: string[], prefix: boolean): boolean {
  if (queryTokens.length === 0) return false;
  for (let start = 0; start < docTokens.length; start++) {
    let matched = true;
    for (let i = 0; i < queryTokens.length; i++) {
      const docToken = docTokens[start + i];
      if (docToken == null) {
        matched = false;
        break;
      }
      const queryToken = queryTokens[i];
      const isLast = i === queryTokens.length - 1;
      if (prefix && isLast) {
        if (!docToken.startsWith(queryToken)) {
          matched = false;
          break;
        }
      } else if (docToken !== queryToken) {
        matched = false;
        break;
      }
    }
    if (matched) return true;
  }
  return false;
}

function scoreTextTokens(docTokens: string[], queryTokens: string[], boost: number, phrase: boolean, prefix: boolean): number {
  if (queryTokens.length === 0) return 0;
  let matches = 0;
  for (let i = 0; i < queryTokens.length; i++) {
    const token = queryTokens[i];
    const isLast = i === queryTokens.length - 1;
    matches += docTokens.filter((docToken) => (prefix && isLast ? docToken.startsWith(token) : docToken === token)).length;
  }
  if (matches === 0) return 0;
  const phraseBonus = phrase ? queryTokens.length * 2 : 0;
  return (matches + phraseBonus) * boost;
}

function evaluateTextClause(node: Extract<CompiledSearchQuery, { kind: "text" }>, doc: SearchDocument): { matched: boolean; score: number } {
  let matched = false;
  let score = 0;
  for (const target of node.fields) {
    const values = doc.textValues.get(target.field) ?? [];
    if (values.length === 0) continue;
    for (const value of values) {
      if (target.config.kind === "keyword") {
        if (node.phrase) {
          const ok = node.prefix ? value.startsWith(node.tokens[0]) : value === node.tokens[0];
          if (!ok) continue;
          matched = true;
          score = Math.max(score, target.boost);
          continue;
        }
        const ok = node.prefix ? value.startsWith(node.tokens[0]) : value === node.tokens[0];
        if (!ok) continue;
        matched = true;
        score = Math.max(score, target.boost);
        continue;
      }
      const docTokens = analyzeTextValue(value, target.config.analyzer);
      const ok = node.phrase
        ? matchPhraseTokens(docTokens, node.tokens, node.prefix)
        : node.prefix && node.tokens.length === 1
          ? docTokens.some((token) => token.startsWith(node.tokens[0]))
          : node.tokens.every((token) => docTokens.includes(token));
      if (!ok) continue;
      matched = true;
      score = Math.max(score, scoreTextTokens(docTokens, node.tokens, target.boost, node.phrase, node.prefix));
    }
  }
  return { matched, score };
}

export function evaluateSearchQueryResult(
  registry: SchemaRegistry,
  offset: bigint,
  query: CompiledSearchQuery,
  value: unknown
): Result<SearchEvaluation, { message: string }> {
  const docRes = buildSearchDocumentResult(registry, offset, value);
  if (Result.isError(docRes)) return docRes;
  const doc = docRes.value;
  const evalNode = (node: CompiledSearchQuery): SearchEvaluation => {
    if (node.kind === "and") {
      const left = evalNode(node.left);
      if (!left.matched) return { matched: false, score: 0, matchedText: false };
      const right = evalNode(node.right);
      if (!right.matched) return { matched: false, score: 0, matchedText: false };
      return { matched: true, score: left.score + right.score, matchedText: left.matchedText || right.matchedText };
    }
    if (node.kind === "or") {
      const left = evalNode(node.left);
      const right = evalNode(node.right);
      if (!left.matched && !right.matched) return { matched: false, score: 0, matchedText: false };
      return {
        matched: true,
        score: left.score + right.score,
        matchedText: left.matchedText || right.matchedText,
      };
    }
    if (node.kind === "not") {
      const inner = evalNode(node.expr);
      return { matched: !inner.matched, score: 0, matchedText: false };
    }
    if (node.kind === "has") {
      const values = doc.rawValues.get(node.field);
      return { matched: !!values && values.length > 0, score: 0, matchedText: false };
    }
    if (node.kind === "compare") {
      const values = doc.exactValues.get(node.field) ?? [];
      if (values.length === 0) return { matched: false, score: 0, matchedText: false };
      if (node.op === "eq") {
        return { matched: !!node.canonicalValue && values.includes(node.canonicalValue), score: 0, matchedText: false };
      }
      for (const value of values) {
        const cmp = compareCanonical(value, node.compareValue!, node.config.kind);
        if (node.op === "gt" && cmp > 0) return { matched: true, score: 0, matchedText: false };
        if (node.op === "gte" && cmp >= 0) return { matched: true, score: 0, matchedText: false };
        if (node.op === "lt" && cmp < 0) return { matched: true, score: 0, matchedText: false };
        if (node.op === "lte" && cmp <= 0) return { matched: true, score: 0, matchedText: false };
      }
      return { matched: false, score: 0, matchedText: false };
    }
    if (node.kind === "keyword") {
      const values = doc.exactValues.get(node.field) ?? [];
      const matched = node.prefix
        ? values.some((value) => value.startsWith(node.canonicalValue))
        : values.includes(node.canonicalValue);
      return { matched, score: 0, matchedText: false };
    }
    const textEval = evaluateTextClause(node, doc);
    return { matched: textEval.matched, score: textEval.score, matchedText: textEval.matched };
  };
  return Result.ok(evalNode(query));
}

export function extractSearchHitFieldsResult(
  registry: SchemaRegistry,
  offset: bigint,
  value: unknown
): Result<SearchHitFieldMap, { message: string }> {
  const rawRes = extractRawSearchValuesResult(registry, offset, value);
  if (Result.isError(rawRes)) return rawRes;
  const out: SearchHitFieldMap = {};
  for (const [field, values] of rawRes.value) {
    if (values.length === 1) out[field] = structuredClone(values[0]);
    else if (values.length > 1) out[field] = structuredClone(values);
  }
  return Result.ok(out);
}
