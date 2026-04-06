import { Result } from "better-result";
import type { SchemaRegistry, SearchFieldConfig } from "./schema/registry";
import { extractSearchExactValuesResult, resolveSearchAlias } from "./search/schema";

type Token =
  | { kind: "word"; value: string }
  | { kind: "string"; value: string }
  | { kind: "lparen" }
  | { kind: "rparen" }
  | { kind: "colon" }
  | { kind: "op"; value: "=" | ">" | ">=" | "<" | "<=" }
  | { kind: "minus" };

export type ReadFilterComparisonOp = "eq" | "gt" | "gte" | "lt" | "lte";

type FilterExpr =
  | { kind: "and"; left: FilterExpr; right: FilterExpr }
  | { kind: "or"; left: FilterExpr; right: FilterExpr }
  | { kind: "not"; expr: FilterExpr }
  | { kind: "has"; field: string }
  | { kind: "compare"; field: string; op: ReadFilterComparisonOp; rawValue: string };

export type CompiledReadFilterClause = {
  kind: "has" | "compare";
  field: string;
  index: SearchFieldConfig;
  op?: ReadFilterComparisonOp;
  canonicalValue?: string;
  compareValue?: bigint | number | boolean | string;
};

export type CompiledReadFilter =
  | { kind: "and"; left: CompiledReadFilter; right: CompiledReadFilter }
  | { kind: "or"; left: CompiledReadFilter; right: CompiledReadFilter }
  | { kind: "not"; expr: CompiledReadFilter }
  | ({ kind: "has" } & CompiledReadFilterClause)
  | ({ kind: "compare" } & CompiledReadFilterClause);

export type ReadFilterExactClause = {
  field: string;
  canonicalValue: string;
};

export type ReadFilterColumnClause = {
  field: string;
  op: ReadFilterComparisonOp | "has";
  compareValue?: bigint | number | boolean;
};

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
          if (i + 1 >= input.length) return Result.err({ message: "unterminated escape in filter string" });
          out += input[i + 1];
          i += 2;
          continue;
        }
        if (cur === "\"") break;
        out += cur;
        i += 1;
      }
      if (i >= input.length || input[i] !== "\"") return Result.err({ message: "unterminated quoted string in filter" });
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
    if (word === "") return Result.err({ message: "invalid filter syntax" });
    tokens.push({ kind: "word", value: word });
    i = j;
  }
  return Result.ok(tokens);
}

class Parser {
  constructor(private readonly tokens: Token[], private pos = 0) {}

  parseResult(): Result<FilterExpr, { message: string }> {
    const exprRes = this.parseOrResult();
    if (Result.isError(exprRes)) return exprRes;
    if (!this.isAtEnd()) return Result.err({ message: "unexpected token in filter" });
    return exprRes;
  }

  private parseOrResult(): Result<FilterExpr, { message: string }> {
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

  private parseAndResult(): Result<FilterExpr, { message: string }> {
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

  private parseUnaryResult(): Result<FilterExpr, { message: string }> {
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

  private parsePrimaryResult(): Result<FilterExpr, { message: string }> {
    if (this.peekKind("lparen")) {
      this.pos += 1;
      const exprRes = this.parseOrResult();
      if (Result.isError(exprRes)) return exprRes;
      if (!this.peekKind("rparen")) return Result.err({ message: "missing ')' in filter" });
      this.pos += 1;
      return exprRes;
    }

    const token = this.consumeWordOrString();
    if (!token) return Result.err({ message: "expected clause in filter" });
    if (token.kind !== "word") return Result.err({ message: "expected field name in filter" });
    const fieldOrKeyword = token.value;

    if (!this.peekKind("colon")) return Result.err({ message: "expected ':' in filter clause" });
    this.pos += 1;

    if (fieldOrKeyword === "has") {
      const fieldToken = this.consumeWordOrString();
      if (!fieldToken || fieldToken.kind !== "word") return Result.err({ message: "has: requires a field name" });
      return Result.ok({ kind: "has", field: fieldToken.value });
    }

    let op: ReadFilterComparisonOp = "eq";
    if (this.peekKind("op")) {
      const raw = (this.tokens[this.pos] as Extract<Token, { kind: "op" }>).value;
      this.pos += 1;
      op = raw === "=" ? "eq" : raw === ">" ? "gt" : raw === ">=" ? "gte" : raw === "<" ? "lt" : "lte";
    }
    const valueToken = this.consumeWordOrString();
    if (!valueToken) return Result.err({ message: "expected value in filter clause" });
    return Result.ok({
      kind: "compare",
      field: fieldOrKeyword,
      op,
      rawValue: valueToken.value,
    });
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

function canonicalizeFilterValue(index: SearchFieldConfig, rawValue: string): string | null {
  switch (index.kind) {
    case "keyword":
      return index.normalizer === "lowercase_v1" ? rawValue.toLowerCase() : rawValue;
    case "integer":
      return /^-?(0|[1-9][0-9]*)$/.test(rawValue.trim()) ? String(BigInt(rawValue.trim())) : null;
    case "float": {
      const parsed = Number(rawValue);
      return Number.isFinite(parsed) ? String(parsed) : null;
    }
    case "date": {
      if (rawValue.trim() === "") return null;
      const parsed = Date.parse(rawValue);
      if (Number.isFinite(parsed)) return String(Math.trunc(parsed));
      return /^-?(0|[1-9][0-9]*)$/.test(rawValue.trim()) ? String(BigInt(rawValue.trim())) : null;
    }
    case "bool": {
      const lowered = rawValue.trim().toLowerCase();
      return lowered === "true" || lowered === "false" ? lowered : null;
    }
    default:
      return null;
  }
}

function compileCompareValueResult(index: SearchFieldConfig, rawValue: string, op: ReadFilterComparisonOp): Result<{
  canonicalValue: string;
  compareValue: bigint | number | boolean | string;
}, { message: string }> {
  const canonical = canonicalizeFilterValue(index, rawValue);
  if (canonical == null) return Result.err({ message: "invalid value for filter field" });
  if (op === "eq") {
    if (index.kind === "integer" || index.kind === "date") {
      return Result.ok({ canonicalValue: canonical, compareValue: BigInt(canonical) });
    }
    if (index.kind === "float") {
      const parsed = Number(canonical);
      if (!Number.isFinite(parsed)) return Result.err({ message: "invalid numeric value for filter field" });
      return Result.ok({ canonicalValue: canonical, compareValue: parsed });
    }
    if (index.kind === "bool") {
      return Result.ok({ canonicalValue: canonical, compareValue: canonical === "true" });
    }
    return Result.ok({ canonicalValue: canonical, compareValue: canonical });
  }
  if (index.kind === "integer" || index.kind === "date") {
    return Result.ok({ canonicalValue: canonical, compareValue: BigInt(canonical) });
  }
  if (index.kind === "float") {
    const parsed = Number(canonical);
    if (!Number.isFinite(parsed)) return Result.err({ message: "invalid numeric value for filter field" });
    return Result.ok({ canonicalValue: canonical, compareValue: parsed });
  }
  return Result.err({ message: "comparison operator not supported for filter field" });
}

function compileExprResult(
  expr: FilterExpr,
  indexByName: Map<string, { field: string; config: SearchFieldConfig }>
): Result<CompiledReadFilter, { message: string }> {
  if (expr.kind === "and") {
    const leftRes = compileExprResult(expr.left, indexByName);
    if (Result.isError(leftRes)) return leftRes;
    const rightRes = compileExprResult(expr.right, indexByName);
    if (Result.isError(rightRes)) return rightRes;
    return Result.ok({ kind: "and", left: leftRes.value, right: rightRes.value });
  }
  if (expr.kind === "or") {
    const leftRes = compileExprResult(expr.left, indexByName);
    if (Result.isError(leftRes)) return leftRes;
    const rightRes = compileExprResult(expr.right, indexByName);
    if (Result.isError(rightRes)) return rightRes;
    return Result.ok({ kind: "or", left: leftRes.value, right: rightRes.value });
  }
  if (expr.kind === "not") {
    const innerRes = compileExprResult(expr.expr, indexByName);
    if (Result.isError(innerRes)) return innerRes;
    return Result.ok({ kind: "not", expr: innerRes.value });
  }
  const resolved = indexByName.get(expr.field);
  if (!resolved) return Result.err({ message: `filter field ${expr.field} is not indexed` });
  const index = resolved.config;
  if (expr.kind === "has" && !index.exists && !index.exact && !index.column) {
    return Result.err({ message: `filter field ${expr.field} does not support has:` });
  }
  if (expr.kind === "has") {
    return Result.ok({ kind: "has", field: resolved.field, index });
  }
  const compareRes = compileCompareValueResult(index, expr.rawValue, expr.op);
  if (Result.isError(compareRes)) return compareRes;
  if (expr.op !== "eq" && !index.column) {
    return Result.err({ message: `filter field ${expr.field} does not support comparisons` });
  }
  if (expr.op === "eq" && !index.exact && !index.column) {
    return Result.err({ message: `filter field ${expr.field} does not support equality filters` });
  }
  return Result.ok({
    kind: "compare",
    field: resolved.field,
    index,
    op: expr.op,
    canonicalValue: compareRes.value.canonicalValue,
    compareValue: compareRes.value.compareValue,
  });
}

export function parseReadFilterResult(
  registry: SchemaRegistry,
  input: string
): Result<CompiledReadFilter, { message: string }> {
  const trimmed = input.trim();
  if (trimmed === "") return Result.err({ message: "filter must not be empty" });
  const tokensRes = tokenizeResult(trimmed);
  if (Result.isError(tokensRes)) return tokensRes;
  const parser = new Parser(tokensRes.value);
  const exprRes = parser.parseResult();
  if (Result.isError(exprRes)) return exprRes;
  const fields = registry.search?.fields ?? {};
  const indexByName = new Map<string, { field: string; config: SearchFieldConfig }>();
  for (const [fieldName, config] of Object.entries(fields)) indexByName.set(fieldName, { field: fieldName, config });
  for (const alias of Object.keys(registry.search?.aliases ?? {})) {
    const resolved = resolveSearchAlias(registry.search, alias);
    const config = fields[resolved];
    if (config) indexByName.set(alias, { field: resolved, config });
  }
  return compileExprResult(exprRes.value, indexByName);
}

function compareCanonicals(left: string, right: bigint | number | boolean | string, kind: SearchFieldConfig["kind"]): number {
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

function evaluateClause(filter: CompiledReadFilterClause, values: string[] | undefined): boolean {
  if (filter.kind === "has") return !!values && values.length > 0;
  if (!values || values.length === 0) return false;
  if (filter.op === "eq") return values.includes(filter.canonicalValue!);
  for (const value of values) {
    const cmp = compareCanonicals(value, filter.compareValue!, filter.index.kind);
    if (filter.op === "gt" && cmp > 0) return true;
    if (filter.op === "gte" && cmp >= 0) return true;
    if (filter.op === "lt" && cmp < 0) return true;
    if (filter.op === "lte" && cmp <= 0) return true;
  }
  return false;
}

export function evaluateReadFilterResult(
  registry: SchemaRegistry,
  offset: bigint,
  filter: CompiledReadFilter,
  value: unknown
): Result<boolean, { message: string }> {
  const valuesRes = extractSearchExactValuesResult(registry, offset, value);
  if (Result.isError(valuesRes)) return valuesRes;
  const values = valuesRes.value;
  const evalNode = (node: CompiledReadFilter): boolean => {
    if (node.kind === "and") return evalNode(node.left) && evalNode(node.right);
    if (node.kind === "or") return evalNode(node.left) || evalNode(node.right);
    if (node.kind === "not") return !evalNode(node.expr);
    return evaluateClause(node, values.get(node.field));
  };
  return Result.ok(evalNode(filter));
}

export function collectPositiveExactFilterClauses(filter: CompiledReadFilter): ReadFilterExactClause[] {
  const out: ReadFilterExactClause[] = [];
  const visit = (node: CompiledReadFilter, negated: boolean): void => {
    if (node.kind === "and") {
      visit(node.left, negated);
      visit(node.right, negated);
      return;
    }
    if (node.kind === "not") {
      visit(node.expr, !negated);
      return;
    }
    if (negated || node.kind !== "compare" || node.op !== "eq" || !node.canonicalValue) return;
    out.push({ field: node.field, canonicalValue: node.canonicalValue });
  };
  visit(filter, false);
  return out;
}

export function collectPositiveColumnFilterClauses(filter: CompiledReadFilter): ReadFilterColumnClause[] {
  const out: ReadFilterColumnClause[] = [];
  const supportsColumn = (node: CompiledReadFilterClause): boolean =>
    node.index.column === true &&
    (node.index.kind === "integer" || node.index.kind === "float" || node.index.kind === "date" || node.index.kind === "bool");
  const visit = (node: CompiledReadFilter, negated: boolean): void => {
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
      if (supportsColumn(node)) out.push({ field: node.field, op: "has" });
      return;
    }
    if (node.kind === "compare" && supportsColumn(node)) {
      out.push({
        field: node.field,
        op: node.op!,
        compareValue:
          typeof node.compareValue === "bigint" || typeof node.compareValue === "number" || typeof node.compareValue === "boolean"
            ? node.compareValue
            : undefined,
      });
    }
  };
  visit(filter, false);
  return out;
}
