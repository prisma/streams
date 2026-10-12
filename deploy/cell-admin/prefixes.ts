// A credential's stream prefixes, as the cell's grants parser takes them
// (src/tenant.rs: CanonicalPrefix::normalize, normalize_prefix_set,
// PREFIX_MAX_BYTES, PREFIX_MAX_COUNT, RESERVED_ROOT). The cell parses its
// grants feed strictly (src/auth_feed.rs, parse_grants): one prefix it
// refuses fails the whole snapshot, the previous one ages to the 300 s
// staleness refusal, and every project on the cell goes dark. So every
// writer checks a prefix here before it takes one, and bundleOf refuses a
// state that holds one the cell would refuse.

export const PREFIX_MAX_BYTES = 256;
export const PREFIX_MAX_COUNT = 64;
const RESERVED_ROOT = "__ds";
/// Rust's char::is_control (Unicode Cc: U+0000-U+001F, U+007F-U+009F), and
/// the space, as normalize refuses them.
const FORBIDDEN = /[\u0000-\u001f\u007f-\u009f ]/;

/// Why the cell would refuse `raw` as a stream prefix, or null.
export function prefixRefusal(raw: string): string | null {
  if (raw === "") return "is empty";
  // A lone surrogate has no UTF-8 form: the cell cannot parse its JSON.
  if (!raw.isWellFormed()) return "is not well-formed Unicode";
  const bytes = Buffer.byteLength(raw, "utf8");
  if (bytes > PREFIX_MAX_BYTES) return `is ${bytes} bytes, more than ${PREFIX_MAX_BYTES}`;
  if (FORBIDDEN.test(raw)) return "holds a control character or a space";
  const components = raw.split("/");
  if (components.some((c) => c === "")) return "has an empty component (a leading, trailing or double slash)";
  if (components.some((c) => c === "." || c === "..")) return 'has a "." or ".." component';
  if (components[0] === RESERVED_ROOT) return `is in the reserved ${RESERVED_ROOT} namespace`;
  return null;
}

/// Why the cell would refuse `prefixes` as a credential's grant, or null:
/// absent (every stream), or 1 to PREFIX_MAX_COUNT prefixes it takes.
export function prefixesRefusal(prefixes: readonly string[] | undefined): string | null {
  if (prefixes === undefined) return null;
  if (prefixes.length === 0 || prefixes.length > PREFIX_MAX_COUNT) {
    return `holds ${prefixes.length} prefixes, not 1 to ${PREFIX_MAX_COUNT}`;
  }
  for (const p of prefixes) {
    const why = prefixRefusal(p);
    if (why !== null) return `${JSON.stringify(p)} ${why}`;
  }
  return null;
}
