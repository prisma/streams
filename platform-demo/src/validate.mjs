// Minimal JSON-Schema validator for the streams-platform v1 contract
// (docs/CONTROL-PLANE-INTEGRATION.md §14.3: "The demo validates its
// output against the language-neutral schemas before publication").
//
// Deliberately a SUBSET: exactly the keywords the five v1 schemas use
// (type, required, properties, additionalProperties:false, items,
// enum, const, pattern, minimum, minLength, maxLength, minItems,
// maxItems, $ref → #/$defs/*). `format` is annotation-only, as in
// JSON Schema itself. If a future schema revision uses a keyword this
// file does not implement, add it HERE — silent non-enforcement is
// exactly the bug class this validator exists to prevent, so unknown
// CONSTRAINT keywords fail loudly instead.
const ANNOTATIONS = new Set(["$schema", "$id", "$defs", "title", "description", "format", "default", "examples"]);
const IMPLEMENTED = new Set([
  "type", "required", "properties", "additionalProperties", "items",
  "enum", "const", "pattern", "minimum", "minLength", "maxLength",
  "minItems", "maxItems", "$ref",
]);

function typeOf(v) {
  if (v === null) return "null";
  if (Array.isArray(v)) return "array";
  if (typeof v === "number") return Number.isInteger(v) ? "integer" : "number";
  return typeof v;
}

function walk(doc, schema, defs, path, errs) {
  let s = schema;
  if (s.$ref) {
    const m = /^#\/\$defs\/(.+)$/.exec(s.$ref);
    const target = m && defs[m[1]];
    if (!target) {
      errs.push(`${path}: unresolvable $ref ${s.$ref}`);
      return;
    }
    s = target;
  }
  for (const k of Object.keys(s)) {
    if (!IMPLEMENTED.has(k) && !ANNOTATIONS.has(k)) errs.push(`${path}: schema uses unimplemented keyword "${k}"`);
  }
  const t = typeOf(doc);
  if (s.type) {
    const types = Array.isArray(s.type) ? s.type : [s.type];
    if (!types.includes(t) && !(t === "integer" && types.includes("number"))) {
      errs.push(`${path}: expected ${types.join("|")}, got ${t}`);
      return; // further keyword checks would only cascade
    }
  }
  if (s.enum && !s.enum.some((v) => JSON.stringify(v) === JSON.stringify(doc)))
    errs.push(`${path}: ${JSON.stringify(doc)} not in enum`);
  if (s.const !== undefined && JSON.stringify(s.const) !== JSON.stringify(doc))
    errs.push(`${path}: expected const ${JSON.stringify(s.const)}`);
  if (t === "string") {
    if (s.minLength !== undefined && doc.length < s.minLength) errs.push(`${path}: shorter than minLength ${s.minLength}`);
    if (s.maxLength !== undefined && doc.length > s.maxLength) errs.push(`${path}: longer than maxLength ${s.maxLength}`);
    if (s.pattern !== undefined && !new RegExp(s.pattern).test(doc)) errs.push(`${path}: does not match ${s.pattern}`);
  }
  if ((t === "integer" || t === "number") && s.minimum !== undefined && doc < s.minimum)
    errs.push(`${path}: below minimum ${s.minimum}`);
  if (t === "array") {
    if (s.minItems !== undefined && doc.length < s.minItems) errs.push(`${path}: fewer than minItems ${s.minItems}`);
    if (s.maxItems !== undefined && doc.length > s.maxItems) errs.push(`${path}: more than maxItems ${s.maxItems}`);
    if (s.items) doc.forEach((v, i) => walk(v, s.items, defs, `${path}[${i}]`, errs));
  }
  if (t === "object") {
    for (const r of s.required ?? []) if (!(r in doc)) errs.push(`${path}: missing required "${r}"`);
    for (const [k, v] of Object.entries(doc)) {
      if (s.properties && k in s.properties) walk(v, s.properties[k], defs, `${path}.${k}`, errs);
      else if (s.additionalProperties === false) errs.push(`${path}: unexpected property "${k}"`);
    }
  }
}

/** Returns [] when valid, else human-readable errors. */
export function validateDocument(doc, rootSchema) {
  const errs = [];
  walk(doc, rootSchema, rootSchema.$defs ?? {}, "$", errs);
  return errs;
}
