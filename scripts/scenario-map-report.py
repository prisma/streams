#!/usr/bin/env python3
"""Scenario-map validator + summary generator (WP-00/PR 3.1 B).

`docs/refactor/test-scenario-map.json` is the AUTHORITATIVE inventory
mapping DST scenario IDs to concrete tests. This script:

  1. validates the map against `docs/dst/SCENARIO-CATALOG.md`:
     - exactly the same scenario IDs as the catalogue (no missing, no extras);
     - unique IDs;
     - each entry's `status` EQUALS the catalogue's parsed status label
       (PR 3.2: the summary previously counted the JSON's own claim);
     - `mapped` is true iff the entry lists at least one test;
     - coverage is one of full/partial/external (mapped) — unmapped
       entries carry no coverage claim;
     - every referenced test symbol exists in the referenced file as an
       exact lexical `fn NAME` word match (PR 3.2: substring matching
       could be satisfied by a comment or a longer name);
  2. regenerates `docs/refactor/SCENARIO-MAP.md` from the JSON so the
     human-readable summary can never drift from the machine-readable
     map again.

Usage:
  python3 scripts/scenario-map-report.py            # validate + rewrite SCENARIO-MAP.md
  python3 scripts/scenario-map-report.py --check    # validate only, exit 1 on any finding
  python3 scripts/scenario-map-report.py --self-test  # masker/matcher self-tests
"""

from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
CATALOG = REPO / "docs/dst/SCENARIO-CATALOG.md"
MAP_JSON = REPO / "docs/refactor/test-scenario-map.json"
OUT_MD = REPO / "docs/refactor/SCENARIO-MAP.md"

ID_RE = re.compile(r"^###\s+([A-Z]{2,4}-\d{3})\b.*?$")
STATUS_RE = re.compile(r"\*\*Status:\*\*\s*(.+?)\s*$")


def mask_noncode(src: str) -> str:
    """Blank line comments, block comments (nested), strings, raw
    strings and char literals so symbol matching is lexical (PR 3.2.1:
    a commented-out `fn name` or a name inside a string must not
    satisfy a scenario's test reference). Same masking discipline as
    architecture-report's strip_noncode, scoped to what fn-matching
    needs."""
    out = list(src)
    i, n = 0, len(src)
    while i < n:
        if src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j == -1 else j
            for k in range(i, j):
                out[k] = " "
            i = j
        elif src.startswith("/*", i):
            depth, j = 1, i + 2
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
        elif src[i] == '"' or (src[i] == "r" and re.match(r'r#*"', src[i:])):
            if src[i] == "r":
                m = re.match(r'r(#*)"', src[i:])
                closer = '"' + "#" * len(m.group(1))
                j = src.find(closer, i + m.end())
                j = n if j == -1 else j + len(closer)
            else:
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
        elif src[i] == "'" and (m := re.match(r"'(\\.|[^\\'])'", src[i:])):
            j = i + m.end()
            for k in range(i, j):
                out[k] = " "
            i = j
        else:
            i += 1
    return "".join(out)


def symbol_defined(body_masked: str, name: str) -> bool:
    """Exact lexical `fn NAME` word match over MASKED source."""
    return re.search(rf"\bfn\s+{re.escape(name)}\b", body_masked) is not None


def self_test() -> int:
    cases = [
        ("// fn commented_out() {}", "commented_out", False,
         "commented-out function must not satisfy"),
        ('let s = "fn in_a_string()";', "in_a_string", False,
         "name inside a string must not satisfy"),
        ("fn prefix_name_longer() {}", "prefix_name", False,
         "prefix collision must not satisfy"),
        ("async fn real_async_case() {}", "real_async_case", True,
         "real async fn must satisfy"),
        ("#[tokio::test]\nasync fn attributed_test() {}", "attributed_test", True,
         "attributed test must satisfy"),
        ("/* fn in_block_comment() */ fn real_one() {}", "in_block_comment", False,
         "block-commented fn must not satisfy"),
        ("/* fn masked() */ fn real_one() {}", "real_one", True,
         "code after a block comment must still match"),
    ]
    failures = []
    for src, name, want, why in cases:
        got = symbol_defined(mask_noncode(src), name)
        if got != want:
            failures.append(f"{why}: {src!r} -> {got}")
    if failures:
        print("scenario-map self-test: FAILED")
        for f in failures:
            print(f"  - {f}")
        return 1
    print(f"scenario-map self-test: OK ({len(cases)} cases)")
    return 0


def catalog_scenarios() -> dict[str, str]:
    """Scenario ID -> status label, from the catalogue."""
    out: dict[str, str] = {}
    lines = CATALOG.read_text().splitlines()
    for i, line in enumerate(lines):
        m = ID_RE.match(line)
        if m:
            sid = m.group(1)
            status = ""
            for j in range(i + 1, min(i + 6, len(lines))):
                sm = STATUS_RE.search(lines[j])
                if sm:
                    status = sm.group(1)
                    break
            out[sid] = status
    return out


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    check_only = "--check" in sys.argv
    problems: list[str] = []

    catalog = catalog_scenarios()
    data = json.loads(MAP_JSON.read_text())

    ids = [s["id"] for s in data]
    dupes = [k for k, c in Counter(ids).items() if c > 1]
    if dupes:
        problems.append(f"duplicate scenario IDs in map: {sorted(dupes)}")
    missing = sorted(set(catalog) - set(ids))
    extra = sorted(set(ids) - set(catalog))
    if missing:
        problems.append(f"map is missing catalogue scenarios: {missing}")
    if extra:
        problems.append(f"map has scenarios not in the catalogue: {extra}")

    valid_cov = {"full", "partial", "external"}
    symbol_cache: dict[str, str] = {}

    def file_text(p: Path) -> str:
        key = str(p)
        if key not in symbol_cache:
            try:
                symbol_cache[key] = mask_noncode(p.read_text(errors="replace"))
            except OSError:
                symbol_cache[key] = ""
        return symbol_cache[key]

    for s in data:
        tests = s.get("tests", [])
        mapped = bool(s.get("mapped"))
        cov = s.get("coverage")
        if s["id"] in catalog and s.get("status", "") != catalog[s["id"]]:
            problems.append(
                f"{s['id']}: status {s.get('status')!r} != catalogue "
                f"{catalog[s['id']]!r} (the catalogue is authoritative)"
            )
        if mapped != bool(tests):
            problems.append(
                f"{s['id']}: mapped={mapped} but {len(tests)} test reference(s)"
            )
        if mapped and cov not in valid_cov:
            problems.append(f"{s['id']}: invalid coverage {cov!r}")
        if not mapped and cov:
            problems.append(f"{s['id']}: unmapped but carries coverage {cov!r}")
        for t in tests:
            f = REPO / t["file"]
            if not f.exists():
                problems.append(f"{s['id']}: referenced file does not exist: {t['file']}")
                continue
            if t["file"].startswith("src/"):
                body = file_text(f)
                name = t["name"]
                # Lexical match over MASKED source (PR 3.2.1): comments
                # and strings are blanked first, then `fn` + the whole
                # identifier must appear.
                if not symbol_defined(body, name):
                    problems.append(
                        f"{s['id']}: test symbol not found: {t['file']}::{name}"
                    )

    mapped_n = sum(1 for s in data if s.get("mapped"))
    unmapped = [s for s in data if not s.get("mapped")]
    by_cov = Counter(s.get("coverage", "none") for s in data if s.get("mapped"))
    by_status = Counter(s.get("status", "?") for s in data)

    report = []
    report.append("# Scenario Map Summary (WP-00 deliverable 6)")
    report.append("")
    report.append("**GENERATED by `scripts/scenario-map-report.py` from")
    report.append("`test-scenario-map.json` — do not hand-edit counts.** The JSON is")
    report.append("authoritative; this file is a view. Validation (same IDs as")
    report.append("`docs/dst/SCENARIO-CATALOG.md`, unique, mapped-iff-tests,")
    report.append("coverage values, test-symbol existence) runs with `--check`.")
    report.append("")
    report.append(f"Catalogue source: `docs/dst/SCENARIO-CATALOG.md` "
                  f"({len(catalog)} scenarios).")
    report.append("")
    report.append("## Counts")
    report.append("")
    report.append(f"- scenarios inventoried: **{len(data)}**")
    report.append(f"- mapped to concrete tests: **{mapped_n}** "
                  f"(full {by_cov['full']}, partial {by_cov['partial']}, "
                  f"external {by_cov['external']})")
    report.append(f"- unmapped (no concrete test): **{len(unmapped)}**")
    report.append("")
    report.append("Status labels as recorded in the catalogue:")
    for st, n in sorted(by_status.items(), key=lambda kv: -kv[1]):
        report.append(f"- {st}: {n}")
    report.append("")
    report.append("## Unmapped scenarios")
    report.append("")
    report.append("These have no concrete test in the tree. L2-sim/L3-field")
    report.append("entries await the simulator/fleet harnesses by design;")
    report.append("Existing/Strengthen/L1-now entries are the gaps that matter")
    report.append("(see notes in the JSON).")
    report.append("")
    for s in sorted(unmapped, key=lambda x: x["id"]):
        note = s.get("note", "")
        suffix = f" — {note}" if note else ""
        report.append(f"- **{s['id']}** ({s.get('status', '?')}): {s.get('title', '')}{suffix}")
    report.append("")

    if problems:
        print("scenario-map: VALIDATION FAILED")
        for p in problems:
            print(f"  - {p}")
        return 1

    if not check_only:
        OUT_MD.write_text("\n".join(report) + "\n")
    print(
        f"scenario-map: OK — {len(data)} inventoried, {mapped_n} mapped "
        f"(full {by_cov['full']}, partial {by_cov['partial']}, external "
        f"{by_cov['external']}), {len(unmapped)} unmapped"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
