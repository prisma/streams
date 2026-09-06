#!/usr/bin/env python3
"""Ratchet stable DST test names, attributes, bodies, and scenario ownership.

File moves change ownership paths, not obligations. The saved manifest records
each test's function-token hash (including literals), its executable
attributes, scenario IDs, and explicit mechanism/configuration references.
Run --write only for a reviewed test change; --check rejects missing, new, moved,
or changed entries. --compare compares a prior manifest while allowing moves.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "docs/refactor/test-inventory.json"
spec = importlib.util.spec_from_file_location("scenario_map", ROOT / "scripts/scenario-map-report.py")
scenario_map = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scenario_map)


def canonical_tokens(source: str) -> str:
    """Ignore comments/formatting, preserving exact string and char literals."""
    output, index = [], 0
    while index < len(source):
        if source.startswith("//", index):
            newline = source.find("\n", index)
            index = len(source) if newline < 0 else newline
        elif source.startswith("/*", index):
            depth, index = 1, index + 2
            while depth and index < len(source):
                if source.startswith("/*", index):
                    depth, index = depth + 1, index + 2
                elif source.startswith("*/", index):
                    depth, index = depth - 1, index + 2
                else:
                    index += 1
        elif match := re.match(r'r(#*)"', source[index:]):
            closing = '"' + match[1]
            end = source.find(closing, index + match.end())
            if end < 0:
                raise ValueError("unterminated raw string")
            end += len(closing)
            output.append(source[index:end])
            index = end
        elif source[index] == '"':
            end = index + 1
            while end < len(source):
                if source[end] == "\\":
                    end += 2
                elif source[end] == '"':
                    end += 1
                    break
                else:
                    end += 1
            output.append(source[index:end])
            index = end
        elif source[index] == "'" and (match := re.match(r"'(\\.|[^\\'])'", source[index:])):
            output.append(match[0])
            index += len(match[0])
        else:
            if not source[index].isspace():
                output.append(source[index])
            index += 1
    return "".join(output)


def function_hash(code: str, path: Path | None = None) -> str:
    # rustfmt orders grouped imports. Preserve the imported names while
    # treating their order as formatting; strings cannot match this mask.
    mask = scenario_map.mask_noncode(code)
    for match in reversed(list(re.finditer(r"\buse\s+[^;{}]+::\{([^{}]+)\}\s*;", mask))):
        names = sorted(name.strip() for name in code[match.start(1):match.end(1)].split(",") if name.strip())
        code = code[:match.start(1)] + ",".join(names) + code[match.end(1):]
    # A module move adjusts relative fixture paths. Hash the resolved
    # repository target, so a changed fixture is still a changed test.
    if path is not None:
        def fixture(match):
            target = (path.parent / match[2]).resolve()
            digest = hashlib.sha256(target.read_bytes()).hexdigest()
            return f'{match[1]}("{target.relative_to(ROOT)}",sha256="{digest}")'
        code = re.sub(r'\b(include_(?:str|bytes)!)\("([^"]+)"\)', fixture, code)
    return hashlib.sha256(canonical_tokens(code).encode()).hexdigest()


def functions(source: str, path: Path | None = None) -> list[dict]:
    """Lex functions with their adjacent attributes, preserving original text."""
    masked = scenario_map.mask_noncode(source)
    found = []
    for match in re.finditer(r"^(?P<indent> *)(?:pub(?:\([^\n]*\))? )?(?:async )?fn (?P<name>\w+)\s*\(", masked, re.M):
        start = match.start()
        # Attributes/doc comments sit immediately before a test declaration.
        prefix_start = start
        lines = source[:start].splitlines(keepends=True)
        while lines:
            line = lines[-1].strip()
            if not line or line.startswith(("#[", "///", "//")):
                prefix_start -= len(lines.pop())
            else:
                break
        prefix = source[prefix_start:start]
        if not re.search(r"#\[(?:tokio::)?test\b", scenario_map.mask_noncode(prefix)):
            continue
        opening = masked.find("{", match.end())
        if opening < 0:
            raise ValueError(f"test {match['name']} has no body")
        depth, end = 1, opening + 1
        while depth and end < len(masked):
            depth += (masked[end] == "{") - (masked[end] == "}")
            end += 1
        if depth:
            raise ValueError(f"test {match['name']} has an unterminated body")
        code = source[start:end]
        attributes = [attribute.strip() for attribute in re.findall(r"^\s*#\[[^\n]+\]", prefix, re.M)]
        found.append({
            "name": match["name"],
            "attributes": attributes,
            "function_sha256": function_hash(code, path),
            "mechanisms": sorted(set(re.findall(r"\bmech::([A-Z_]+)", code))),
            "configuration": [line.strip() for line in code.splitlines()
                              if re.search(r"FaultPlan::|failpoint|\.require\(|\bseed\b|start_paused|worker_threads", line)],
        })
    return found


def inventory() -> list[dict]:
    scenario_ids: dict[str, list[str]] = {}
    for scenario in json.loads((ROOT / "docs/refactor/test-scenario-map.json").read_text()):
        for test in scenario.get("tests", []):
            scenario_ids.setdefault(test["name"], []).append(scenario["id"])
    tests = []
    for path in sorted((ROOT / "src/dst").rglob("*.rs")):
        for test in functions(path.read_text(), path):
            test["file"] = str(path.relative_to(ROOT))
            test["scenarios"] = sorted(set(scenario_ids.get(test["name"], [])))
            tests.append(test)
    names = [test["name"] for test in tests]
    if len(set(names)) != len(names):
        raise ValueError("DST stable test names must be unique")
    return sorted(tests, key=lambda test: test["name"])


def differences(expected: list[dict], actual: list[dict], allow_moves: bool, adaptations: list[dict] = ()) -> list[str]:
    before = {test["name"]: test for test in expected}
    after = {test["name"]: test for test in actual}
    problems = [f"missing test: {name}" for name in sorted(before.keys() - after.keys())]
    problems += [f"new test requires inventory: {name}" for name in sorted(after.keys() - before.keys())]
    for name in sorted(before.keys() & after.keys()):
        for field in before[name].keys() | after[name].keys():
            if allow_moves and field == "file":
                continue
            if field == "function_sha256" and any(
                adaptation["name"] == name
                and adaptation["before_sha256"] == before[name][field]
                and adaptation["after_sha256"] == after[name][field]
                and adaptation.get("finding") and adaptation.get("reason")
                for adaptation in adaptations
            ):
                continue
            if before[name].get(field) != after[name].get(field):
                problems.append(f"{name}: changed {field}")
    return problems


def self_test() -> None:
    source = '''// #[test]\n// fn fake() {}\n#[tokio::test(start_paused = true)]\n#[ignore = "capacity owns machine"]\nasync fn actual() { let text = "}"; assert_eq!(text, "}"); }\n'''
    entries = functions(source)
    assert [entry["name"] for entry in entries] == ["actual"]
    assert entries[0]["attributes"] == ["#[tokio::test(start_paused = true)]", '#[ignore = "capacity owns machine"]']
    original = [{**entries[0], "file": "old.rs"}]
    moved = [{**entries[0], "file": "owner.rs"}]
    assert not differences(original, moved, True)
    assert differences(original, moved, False)
    changed = [{**moved[0], "function_sha256": "changed"}]
    assert differences(original, changed, True)
    assert differences(original, [], True)
    assert function_hash('fn t() { assert_eq!(1, " a "); }') == function_hash('fn t(){\nassert_eq!(1, " a "); // unchanged\n}')
    assert function_hash('fn t() { assert_eq!(1, " a "); }') != function_hash('fn t() { assert_eq!(1, "a"); }')
    adaptation = [{"name": "actual", "before_sha256": original[0]["function_sha256"], "after_sha256": "changed", "finding": "R06", "reason": "explicit API migration"}]
    assert not differences(original, changed, True, adaptation)
    assert differences(original, [{**changed[0], "function_sha256": "unexpected"}], True, adaptation)
    print("test-inventory self-test: OK (9 controls)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--count", action="store_true", help="print the recursive total for provenance")
    parser.add_argument("--compare", type=Path)
    parser.add_argument("--adaptations", type=Path, help="exact reviewed old/new hashes allowed during --compare")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    actual = inventory()
    if args.count:
        print(len(actual))
        return 0
    if args.write:
        MANIFEST.write_text(json.dumps(actual, indent=2) + "\n")
        print(f"test-inventory: wrote {len(actual)} tests")
        return 0
    expected_path = args.compare or MANIFEST
    if args.adaptations and not args.compare:
        parser.error("--adaptations requires --compare; the current inventory gate has no exceptions")
    adaptations = json.loads(args.adaptations.read_text()) if args.adaptations else []
    problems = differences(json.loads(expected_path.read_text()), actual, bool(args.compare), adaptations)
    for problem in problems:
        print(problem)
    if problems:
        return 1
    print(f"test-inventory: OK ({len(actual)} tests, {sum(any('ignore' in a for a in t['attributes']) for t in actual)} ignored)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
