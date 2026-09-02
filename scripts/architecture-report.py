#!/usr/bin/env python3
"""Architecture report (WP-00, docs/refactor/BASELINE.md).

Warning-only structural scanner. Reports, never fails (unless --fail is
passed; reserved for WP-17 hard gates):

  - Rust files over the file budget (default 1,000 lines)
  - functions over the function budget (default 200 lines)
  - forbidden module edges (crate::http references outside transport)
  - direct std::env reads outside the configuration allowlist
  - Axum transport types outside transport adapters
  - raw storage key-tag construction outside storage::keyspace
  - mutable process statics (static mut / atomic / Mutex / OnceLock ...)

Function spans are computed with a small Rust lexer (line comments,
nested block comments, strings, raw strings, char literals are stripped
before brace matching), so spans track the source faithfully enough to
reproduce the review's cited spans (e.g. commit_group ~2,106 lines).

Usage:
  python3 scripts/architecture-report.py            # human report
  python3 scripts/architecture-report.py --json     # machine-readable
Exit status is always 0 unless --fail is given.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SRC = REPO / "src"

FILE_BUDGET = 1000
FUNCTION_BUDGET = 200

# WP-01 allowlist: modules permitted to read the process environment once
# the config boundary exists. Today this is aspirational; every hit is
# reported either way, classified as allowed vs violation.
ENV_ALLOWED_PREFIXES = (
    "src/config/",
    "src/bin/",
)

# Forbidden-edge rules: (importer file predicate, imported module, why).
# The target architecture (codereview1.md section 5.1) forbids anything
# outside transport adapters from depending on the HTTP transport.
TRANSPORT_FILES = ("src/http.rs",)
FORBIDDEN_EDGE_MODULES = ("crate::http",)

AXUM_RE = re.compile(
    r"\b(axum::(?:response::)?(?:Response|Json|body::Body)|HeaderMap|StatusCode|http::StatusCode)\b"
)

ENV_RE = re.compile(r"\b(?:std::)?env::var(?:_os)?\s*\(")

# Raw key-tag assembly heuristics: byte-literal tag pushes / constants.
KEYTAG_RE = re.compile(
    r"(\.push\(\s*b'[A-Za-z]'\s*\)|vec!\[\s*b'[A-Za-z]'|=\s*b'[A-Za-z]'\s*;)"
)
KEYSPACE_PREFIXES = ("src/storage/keyspace",)  # does not exist yet

STATIC_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?static(?:\s+mut)?\s+[A-Z_][A-Z0-9_]*\s*:\s*"
    r"([^=;]+)",
    re.MULTILINE,
)
MUTABLE_STATIC_KINDS = (
    "Atomic",
    "Mutex",
    "RwLock",
    "OnceLock",
    "LazyLock",
    "OnceCell",
    "Lazy",
)

FN_RE = re.compile(
    r"\b(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)"
)


RAW_STR_RE = re.compile(r"r(#*)\"")
CHAR_RE = re.compile(r"'(\\.|[^\\'])'")


def strip_noncode(src: str) -> str:
    """Replace comments/strings/chars with spaces, preserving newlines.

    Handles Rust's nested block comments and raw strings so brace
    matching in the cleaned output is not fooled by text content.
    All scans index in place (no tail slicing), so this stays O(n).
    """
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        c = src[i]
        if src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j == -1 else j
            for k in range(i, j):
                out[k] = " "
            i = j
        elif src.startswith("/*", i):
            depth = 1
            j = i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            for k in range(i, min(j, n)):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        elif c == '"':
            j = i + 1
            while j < n:
                if src[j] == "\\":
                    j += 2
                    continue
                if src[j] == '"':
                    j += 1
                    break
                j += 1
            for k in range(i, min(j, n)):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        elif c == "r" and (m := RAW_STR_RE.match(src, i)):
            hashes = len(m.group(1))
            start = m.end()
            closer = '"' + "#" * hashes
            j = src.find(closer, start)
            j = n if j == -1 else j + len(closer)
            for k in range(i, min(j, n)):
                if out[k] != "\n":
                    out[k] = " "
            i = j
        elif c == "'" and (m := CHAR_RE.match(src, i)):
            j = m.end()
            for k in range(i, j):
                out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def find_functions(clean: str) -> list[tuple[str, int, int]]:
    """Return (name, start_line, end_line) 1-based spans for fn bodies."""
    spans: list[tuple[str, int, int]] = []
    line_starts = [0]
    for m in re.finditer("\n", clean):
        line_starts.append(m.end())

    def line_of(pos: int) -> int:
        lo, hi = 0, len(line_starts)
        while lo + 1 < hi:
            mid = (lo + hi) // 2
            if line_starts[mid] <= pos:
                lo = mid
            else:
                hi = mid
        return lo + 1

    for m in FN_RE.finditer(clean):
        name = m.group(1)
        i = m.end()
        depth_paren = depth_angle = depth_bracket = 0
        body = -1
        while i < len(clean):
            ch = clean[i]
            if ch == ";" and depth_paren == depth_angle == depth_bracket == 0:
                break  # declaration without body (trait method)
            if ch == "{" and depth_paren == depth_angle == depth_bracket == 0:
                body = i
                break
            if ch == "(":
                depth_paren += 1
            elif ch == ")":
                depth_paren -= 1
            elif ch == "[":
                depth_bracket += 1
            elif ch == "]":
                depth_bracket -= 1
            elif ch == "<":
                depth_angle += 1
            elif ch == ">":
                depth_angle = max(0, depth_angle - 1)
            i += 1
        if body == -1:
            continue
        depth = 1
        j = body + 1
        while j < len(clean) and depth:
            if clean[j] == "{":
                depth += 1
            elif clean[j] == "}":
                depth -= 1
            j += 1
        spans.append((name, line_of(m.start()), line_of(j - 1)))
    return spans


def rel(p: Path) -> str:
    return str(p.relative_to(REPO))


def is_test_only(path: Path, text: str) -> bool:
    """Whole-file test code: dst/ modules, or any file gated by an inner
    `#![cfg(test)]` attribute (the file-split form of `#[cfg(test)] mod`,
    e.g. src/golden_tests.rs) anywhere in its leading attribute block."""
    r = rel(path)
    if r.startswith("src/dst/") or r == "src/dst.rs":
        return True
    for line in text.splitlines()[:12]:
        stripped = line.strip()
        if stripped.startswith("#![cfg(test)]"):
            return True
        # Leading doc comments and inner attributes may precede it.
        if stripped and not (
            stripped.startswith("//!")
            or stripped.startswith("///")
            or stripped.startswith("#![")
            or stripped.startswith("//")
        ):
            break
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--json", action="store_true", help="machine-readable output")
    ap.add_argument("--fail", action="store_true", help="exit 1 on any violation (WP-17)")
    args = ap.parse_args()

    files = sorted(p for p in SRC.rglob("*.rs") if p.is_file())
    report: dict = {
        "budgets": {"file_lines": FILE_BUDGET, "function_lines": FUNCTION_BUDGET},
        "files_over_budget": [],
        "functions_over_budget": [],
        "forbidden_edges": [],
        "env_reads": [],
        "axum_outside_transport": [],
        "keytag_sites": [],
        "mutable_statics": [],
        "totals": {},
    }

    total_lines = 0
    for path in files:
        r = rel(path)
        text = path.read_text(encoding="utf-8", errors="replace")
        lines = text.splitlines()
        n_lines = len(lines)
        total_lines += n_lines
        test_only = is_test_only(path, text)

        if n_lines > FILE_BUDGET:
            report["files_over_budget"].append(
                {"file": r, "lines": n_lines, "test_only": test_only}
            )

        clean = strip_noncode(text)
        for name, start, end in find_functions(clean):
            span = end - start + 1
            if span > FUNCTION_BUDGET:
                report["functions_over_budget"].append(
                    {"file": r, "function": name, "start": start, "end": end, "lines": span}
                )

        if r not in TRANSPORT_FILES:
            for mod in FORBIDDEN_EDGE_MODULES:
                hits = [i + 1 for i, ln in enumerate(lines) if mod in ln]
                if hits:
                    report["forbidden_edges"].append(
                        {
                            "file": r,
                            "references": mod,
                            "count": len(hits),
                            "test_only": test_only,
                        }
                    )

        for i, ln in enumerate(lines):
            if ENV_RE.search(ln):
                report["env_reads"].append(
                    {
                        "file": r,
                        "line": i + 1,
                        "src": ln.strip(),
                        "test_only": test_only,
                        # WP-01 target rule: env reads belong in the config
                        # loader, binaries, or marked config-parser tests.
                        "allowed": r.startswith(ENV_ALLOWED_PREFIXES) or r == "src/main.rs",
                    }
                )

        if r != "src/http.rs" and not r.startswith("src/transport/"):
            count = sum(len(AXUM_RE.findall(ln)) for ln in lines)
            if count:
                report["axum_outside_transport"].append(
                    {"file": r, "count": count, "test_only": test_only}
                )

        if not r.startswith(KEYSPACE_PREFIXES):
            for i, ln in enumerate(lines):
                if KEYTAG_RE.search(ln):
                    report["keytag_sites"].append({"file": r, "line": i + 1, "src": ln.strip()})

        for m in STATIC_RE.finditer(text):
            ty = m.group(1)
            if any(k in ty for k in MUTABLE_STATIC_KINDS):
                ln_no = text[: m.start()].count("\n") + 1
                report["mutable_statics"].append(
                    {"file": r, "line": ln_no, "type": ty.strip(), "test_only": test_only}
                )

    report["totals"] = {
        "rust_files": len(files),
        "rust_lines": total_lines,
        "files_over_budget": len(report["files_over_budget"]),
        "production_files_over_budget": sum(
            1 for f in report["files_over_budget"] if not f["test_only"]
        ),
        "functions_over_budget": len(report["functions_over_budget"]),
        "env_reads": len(report["env_reads"]),
        "env_reads_disallowed": sum(1 for e in report["env_reads"] if not e["allowed"]),
        "forbidden_edge_refs": sum(e["count"] for e in report["forbidden_edges"]),
        "axum_refs_outside_transport": sum(
            e["count"] for e in report["axum_outside_transport"]
        ),
        "keytag_sites": len(report["keytag_sites"]),
        "mutable_statics": len(report["mutable_statics"]),
    }

    if args.json:
        json.dump(report, sys.stdout, indent=2)
        print()
    else:
        t = report["totals"]
        print("== architecture report (warning-only until WP-17) ==")
        print(f"rust files: {t['rust_files']}  lines: {t['rust_lines']}")
        print(
            f"files over {FILE_BUDGET} lines: {t['files_over_budget']} "
            f"(production: {t['production_files_over_budget']})"
        )
        for f in report["files_over_budget"]:
            tag = " [test]" if f["test_only"] else ""
            print(f"  {f['file']}: {f['lines']}{tag}")
        print(f"functions over {FUNCTION_BUDGET} lines: {t['functions_over_budget']}")
        for f in sorted(report["functions_over_budget"], key=lambda x: -x["lines"]):
            print(f"  {f['file']}:{f['start']}-{f['end']} {f['function']} ({f['lines']} lines)")
        print(f"crate::http references outside http.rs: {t['forbidden_edge_refs']}")
        for e in report["forbidden_edges"]:
            tag = " [test]" if e["test_only"] else ""
            print(f"  {e['file']}: {e['count']} refs{tag}")
        print(
            f"direct env reads: {t['env_reads']} "
            f"(outside config/binaries: {t['env_reads_disallowed']})"
        )
        by_file: dict[str, int] = {}
        for e in report["env_reads"]:
            if not e["allowed"]:
                by_file[e["file"]] = by_file.get(e["file"], 0) + 1
        for f, c in sorted(by_file.items(), key=lambda kv: -kv[1]):
            print(f"  {f}: {c}")
        print(f"axum type refs outside transport: {t['axum_refs_outside_transport']}")
        for e in report["axum_outside_transport"]:
            tag = " [test]" if e["test_only"] else ""
            print(f"  {e['file']}: {e['count']}{tag}")
        print(f"raw key-tag construction sites: {t['keytag_sites']}")
        by_file = {}
        for e in report["keytag_sites"]:
            by_file[e["file"]] = by_file.get(e["file"], 0) + 1
        for f, c in sorted(by_file.items(), key=lambda kv: -kv[1]):
            print(f"  {f}: {c}")
        print(f"mutable process statics: {t['mutable_statics']}")
        by_file = {}
        for e in report["mutable_statics"]:
            by_file[e["file"]] = by_file.get(e["file"], 0) + 1
        for f, c in sorted(by_file.items(), key=lambda kv: -kv[1]):
            print(f"  {f}: {c}")

    violations = (
        report["totals"]["functions_over_budget"]
        + report["totals"]["files_over_budget"]
        + report["totals"]["env_reads_disallowed"]
        + report["totals"]["forbidden_edge_refs"]
        + sum(1 for e in report["axum_outside_transport"] if not e["test_only"])
        + report["totals"]["keytag_sites"]
        + sum(1 for e in report["mutable_statics"] if not e["test_only"])
    )
    if args.fail and violations:
        return 1
    return 0


def _norm_ws(s: str) -> str:
    return " ".join(s.split())


def item_keys(report: dict) -> dict[str, set]:
    """Stable per-category PRESENCE keys for baseline diffing (line
    numbers deliberately absent — they move under any edit).

    PR 4.1: NEW means absent before and present now; RESOLVED the
    reverse. Counts are therefore NOT part of the identity (a
    forbidden-reference count going 47 -> 46 is an improvement, not a
    new offender and a resolved one) — count movement is reported
    separately as SHRINK/GROWTH by `baseline_diff`."""
    from collections import Counter

    fn_names = Counter(
        f"{f['file']}::{f['function']}" for f in report["functions_over_budget"]
    )
    static_types = Counter(
        f"{e['file']}|{e['type']}" for e in report["mutable_statics"]
    )
    return {
        "files_over_budget": {f["file"] for f in report["files_over_budget"]},
        "functions_over_budget": {f"{k}|x{n}" for k, n in fn_names.items()},
        "forbidden_edges": {e["file"] for e in report["forbidden_edges"]},
        "env_reads": {f"{e['file']}|{_norm_ws(e['src'])}" for e in report["env_reads"]},
        "axum_outside_transport": {e["file"] for e in report["axum_outside_transport"]},
        "keytag_sites": {f"{e['file']}|{_norm_ws(e['src'])}" for e in report["keytag_sites"]},
        "mutable_statics": {f"{k}|x{n}" for k, n in static_types.items()},
    }


def count_keys(report: dict) -> dict[str, dict[str, int]]:
    """Per-category COUNT metrics for the SHRINK/GROWTH report: how many
    references/lines an already-present item carries."""
    return {
        "forbidden_edges": {e["file"]: e["count"] for e in report["forbidden_edges"]},
        "axum_outside_transport": {
            e["file"]: e["count"] for e in report["axum_outside_transport"]
        },
        "files_over_budget": {f["file"]: f["lines"] for f in report["files_over_budget"]},
        "functions_over_budget": {
            f"{f['file']}::{f['function']}": f["lines"]
            for f in report["functions_over_budget"]
        },
    }


def baseline_diff(current: dict, baseline_path: Path) -> int:
    """Compare the current report against the checked-in baseline JSON and
    print per-category NEW regressions and RESOLVED debt. This is the
    warning-only signal CI should surface: not 'how big is the debt',
    but 'did THIS change add to it'."""
    try:
        baseline = json.loads(baseline_path.read_text())
    except OSError:
        print(f"baseline-diff: cannot read {baseline_path}", file=sys.stderr)
        return 1
    cur = item_keys(current)
    base = item_keys(baseline)
    any_new = False
    for cat in cur:
        new = sorted(cur[cat] - base[cat])
        resolved = sorted(base[cat] - cur[cat])
        if new:
            any_new = True
        print(f"[{cat}] new: {len(new)}  resolved: {len(resolved)}")
        for k in new:
            print(f"  NEW      {k}")
        for k in resolved:
            print(f"  RESOLVED {k}")
    # PR 3.2.1/4.1: movement INSIDE already-present items — line counts
    # of over-budget files/functions, reference counts of forbidden
    # edges and Axum leaks — is reported as SHRINK/GROWTH, never as a
    # NEW+RESOLVED pair.
    growth: list[str] = []
    shrink: list[str] = []
    cur_c = count_keys(current)
    base_c = count_keys(baseline)
    units = {
        "files_over_budget": "lines",
        "functions_over_budget": "lines",
        "forbidden_edges": "refs",
        "axum_outside_transport": "refs",
    }
    for cat, unit in units.items():
        for k in sorted(set(cur_c[cat]) & set(base_c[cat])):
            d = cur_c[cat][k] - base_c[cat][k]
            line = f"  {cat:<24} {k:<44} {base_c[cat][k]} -> {cur_c[cat][k]} {unit} ({d:+})"
            if d > 0:
                growth.append(line)
            elif d < 0:
                shrink.append(line)
    if growth:
        print("GROWTH (inside already-present items):")
        for g in growth:
            print(g)
    else:
        print("GROWTH: none inside already-present items")
    if shrink:
        print("SHRINK (inside already-present items):")
        for s in shrink:
            print(s)
    print("baseline-diff:", "REGRESSIONS PRESENT" if any_new else "no new regressions")
    return 0


def self_test() -> int:
    """Scanner self-tests: the lexer and classifier on tricky inputs.
    Runs in the commit gate (same role as verify-rc-evidence's self-test)."""
    failures = []

    # 1. Braces inside strings/raw strings/comments must not break spans.
    tricky = '''
fn a() {
    let s = "}";
    let r = r#"}{"#;
    /* nested /* comment with { and } */ */
    let c = '}';
}
fn b() { let x = 1; }
'''
    clean = strip_noncode(tricky)
    spans = find_functions(clean)
    if [n for n, _, _ in spans] != ["a", "b"]:
        failures.append(f"string/comment braces broke fn detection: {spans}")

    # 2. A trait method declaration without a body is not a body.
    decl = "trait T { fn decl(&self) -> u64; fn has_body(&self) -> u64 { 1 } }"
    spans = find_functions(strip_noncode(decl))
    if [n for n, _, _ in spans] != ["has_body"]:
        failures.append(f"trait declaration misdetected: {spans}")

    # 3. cfg(test)-gated file classification.
    gated = "#![cfg(test)]\n\nfn helper() {}\n"
    if not is_test_only(Path(REPO / "src" / "golden_tests.rs"), gated):
        failures.append("#![cfg(test)] inner attribute not classified as test-only")
    prod = "//! production module\n\npub fn f() {}\n"
    if is_test_only(Path(REPO / "src" / "anything.rs"), prod):
        failures.append("production file misclassified as test-only")

    # 4. Span math: a 5-line function measures 5 lines.
    five = "fn five() {\n\n\n\n}\n"
    spans = find_functions(strip_noncode(five))
    if len(spans) != 1 or spans[0][2] - spans[0][1] + 1 != 5:
        failures.append(f"span math wrong: {spans}")

    if failures:
        for f in failures:
            print(f"self-test FAIL: {f}")
        return 1
    print("architecture-report self-test: OK (4 checks)")
    return 0


if __name__ == "__main__":
    import sys as _sys

    if "--self-test" in _sys.argv:
        _sys.exit(self_test())
    if "--baseline-diff" in _sys.argv:
        # Reuse main()'s scan, then diff against the checked-in baseline.
        class _Args:
            json = False
            fail = False

        _sys.argv = [a for a in _sys.argv if a != "--baseline-diff"]
        import io, contextlib

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            # main() prints; we need the structured report — call it via json.
            _sys.argv.append("--json")
            main_rc = main()
        report = json.loads(buf.getvalue())
        _sys.exit(baseline_diff(report, REPO / "docs" / "refactor" / "architecture-baseline.json"))
    _sys.exit(main())
