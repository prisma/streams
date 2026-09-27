#!/usr/bin/env python3
"""Change impact: what to run and what applies before and after an edit.

Read-only developer tool. For the changed files (by default everything that
differs from merge-base(HEAD, ${QUALITY_BASE_REF:-origin/slate}), committed or
not, plus untracked files) or for explicit paths (which may not exist yet), it
prints one short summary:

  test (dev)   inner-loop `cargo test` commands in the dev profile, plus the
               gate-profile (--release) form for the final evidence run;
  mutation     the owners CI's mutation leg runs, or the rows to register first;
  CI legs      miri / properties_fuzz / mutants, and the formal obligations
               CI re-runs (or EVERYTHING);
  receipts     formal receipts these paths make stale, their recorded cost, the
               receipts stale right now and the re-record command;
  guards       line-ceiling headroom, by-path harness includes, reasoned
               exception scopes, pinned tests and fingerprints, immutable
               files, hard-owner rules and client-visible edge changes.

Rules are imported, never copied: selection comes from scripts/quality/
verification_plan.py, mutation_owners.py and formal.py, budgets from
scripts/architecture-gate.py, test pins from scripts/test-inventory.py. The one
rule a gate keeps inline, the source ratchet's line ceiling
(source_rules.violations), is restated in line_ceiling() and pinned to the
gate by test_impact.py.

  scripts/dev/impact.py                     working changes vs the merge base
  scripts/dev/impact.py src/shard.rs NEW.rs explicit, possibly planned, paths
  scripts/dev/impact.py --commits 3         the last 3 commits + uncommitted work
  scripts/dev/impact.py --base origin/main  another merge-base target
  scripts/dev/impact.py -v | --json         per-file detail | machine output
  scripts/dev/impact.py --codemap           the top-level module map
"""
import sys

import os  # noqa: E402
import shutil  # noqa: E402

if sys.version_info < (3, 11):
    # macOS's /usr/bin/python3 is 3.9: re-run under the newest 3.11+ installed.
    if not os.environ.get('STREAMS_PYTHON_REEXEC'):
        os.environ['STREAMS_PYTHON_REEXEC'] = '1'
        for candidate in ('python3.14', 'python3.13', 'python3.12', 'python3.11',
                          '/opt/homebrew/bin/python3.13', '/opt/homebrew/bin/python3.12',
                          '/opt/homebrew/bin/python3.11'):
            found = shutil.which(candidate)
            if found:
                os.execv(found, [found, *sys.argv])
    sys.exit('impact.py: Python >= 3.11 is required (the gate modules import tomllib); '
             'install one (brew install python@3.12) or `. scripts/dev/env.sh` first')
sys.dont_write_bytecode = True  # read-only: no __pycache__ beside the gate modules

import argparse  # noqa: E402
from collections import Counter, defaultdict  # noqa: E402
from dataclasses import asdict, dataclass, field  # noqa: E402
from functools import cache  # noqa: E402
import hashlib  # noqa: E402
import importlib.util  # noqa: E402
import json  # noqa: E402
from pathlib import Path  # noqa: E402
import re  # noqa: E402
import subprocess  # noqa: E402
import time  # noqa: E402
import tomllib  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts/quality'))
import common  # noqa: E402
import formal  # noqa: E402
import mutation_owners  # noqa: E402
import verification_plan as vp  # noqa: E402


def load_script(name, relative):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


architecture_gate = load_script('architecture_gate', 'scripts/architecture-gate.py')
architecture = architecture_gate.architecture  # scripts/architecture-report.py


@cache
def test_inventory():
    return load_script('test_inventory', 'scripts/test-inventory.py')


WARN_HEADROOM = 25


def pin_hash(spec):
    """`FILE::FN` -> the function_sha256 review-evidence.py compares for a
    pinned test (tests first, then support functions, as its manifest has
    both kinds)."""
    file, _, name = spec.rpartition('::')
    path = ROOT / file
    if not file or not name or not path.is_file():
        raise ImpactError(f'--pin-hash wants FILE::FN with an existing file, got {spec!r}')
    source = path.read_text()
    for helpers in (False, True):
        found = [f for f in test_inventory().functions(source, path, include_helpers=helpers)
                 if f['name'] == name]
        if len(found) == 1:
            return f"{found[0]['function_sha256']}  {file}::{name}"
        if len(found) > 1:
            raise ImpactError(f'{name} is defined {len(found)} times in {file}')
    raise ImpactError(f'no function {name} in {file}')
RERECORD = 'python3 scripts/dev/formal_batch.py rerecord --stale'
REGISTER = ('add an owner row to scripts/quality/mutation_owners.py; '
            'CI refuses unregistered critical files')
EDGE_PREFIXES = ('src/http', 'src/product', 'src/application/', 'src/sse/', 'src/auth', 'sdk/')
EDGE_HINT = ('edge change? record in docs/reviews/2026-09-hardening/edge-changes.md + '
             'docs/refactor/WIRE-MATRIX.md and ask the owner')
GROWTH_HINT = ('any code growth there is exception growth: needs an owner-approved '
               'docs/quality/exception-growth.json row (propose it, never add it)')
HARNESS_CRATES = ('streams-quality-invariants', 'streams-quality-fuzz')
LOCAL_LEGS = {'mutants': 'scripts/quality/mutations.sh', 'miri': 'scripts/quality/nightly.sh miri',
              'properties_fuzz': 'scripts/quality/nightly.sh corpus'}
SELECTORS = {  # mutation owner target -> `cargo test` target selection
    'service-lib': ('--lib',),
    'harness-lib': ('-p', 'streams-quality-invariants', '--lib'),
    'pilot-benchmark': ('--bin', 'pilot'),
    'pilot-generator': ('--bin', 'pilot', '--test', 'pilot_membership'),
}
GATE_INPUTS = {
    'verification/manifest.json': 'formal CI selects EVERY obligation; receipts whose entry changes go stale',
    'quality-tools.toml': ('tool pins: formal CI selects EVERY obligation; '
                           'a [formal]/[slatedb] change stales every receipt'),
    'scripts/quality/formal.py': 'formal driver: CI selects EVERY obligation and every receipt digests it',
    'Cargo.lock': 'every formal receipt digests it; selects the miri and properties_fuzz legs',
    'Cargo.toml': 'every formal receipt digests it; selects the miri and properties_fuzz legs',
    'build.rs': 'every Kani receipt digests it',
    'rust-toolchain.toml': 'every Kani receipt digests it; selects the miri and properties_fuzz legs',
    'verification/assumptions.md': 'obligations naming a changed ASM entry are selected and go stale',
    'docs/refactor/test-inventory.json': 'written only by python3 scripts/test-inventory.py --write, after review',
    'docs/quality/exception-growth.json': 'owner-approved rows only: propose a row, never add it',
    'docs/quality/source-allowances.json': 'only shrinks (scripts/quality/gate.py --prune); never grow it',
    'docs/quality/owners.json': 'reviewed ledger: never grow it to absorb debt',
    'docs/refactor/architecture-policy.json': 'budget exceptions are owner decisions, not agent edits',
    'scripts/mt-audit-baseline.txt': 'regenerated only in the commit that converts or moves a site (reviewed)',
}


class ImpactError(Exception):
    """An environment problem the caller must fix (bad path, missing base)."""


# --------------------------------------------------------------------------
# Git
# --------------------------------------------------------------------------

def git(*args, check=True):
    result = subprocess.run(['git', *args], cwd=ROOT, capture_output=True, text=True)
    if check and result.returncode:
        raise ImpactError(f'git {" ".join(args)}: {result.stderr.strip() or result.returncode}')
    return result.stdout


def show_many(revision, paths):
    """{path: text or None} at `revision`, in one `git cat-file --batch`."""
    paths = sorted(set(paths))
    if not paths or not revision:
        return {path: None for path in paths}
    request = ''.join(f'{revision}:{path}\n' for path in paths).encode()
    output = subprocess.run(['git', 'cat-file', '--batch'], cwd=ROOT, input=request,
                            capture_output=True, check=True).stdout
    result, offset = {}, 0
    for path in paths:
        end = output.index(b'\n', offset)
        header = output[offset:end].split()
        offset = end + 1
        if header[-1] == b'missing':
            result[path] = None
            continue
        size = int(header[2])
        result[path] = output[offset:offset + size].decode(errors='replace')
        offset += size + 1
    return result


# --------------------------------------------------------------------------
# Module tree: which crate targets compile each Rust file, at which module path
# --------------------------------------------------------------------------

MOD_DECL = re.compile(
    r'^(?P<lead>[ \t]*(?:#\[[^\n]*?\][ \t]*)*)(?:pub(?:\([^)\n]*\))?[ \t]+)?mod[ \t]+(?:r#)?'
    r'(?P<name>[A-Za-z_]\w*)[ \t]*(?P<kind>[;{])', re.M)
NESTING = re.compile(r'(?P<brace>[{}])|' + MOD_DECL.pattern, re.M)
ATTRIBUTE_LINE = re.compile(r'(?:#\[[^\n]*?\]\s*)+(?://.*)?')
TEST_ATTR = re.compile(r'#\[(?:tokio::)?test\b')
INCLUDE = re.compile(r'\binclude_(?:str|bytes)!\(\s*"([^"]+)"')


@dataclass(frozen=True)
class Placement:
    crate: str
    target: str        # 'lib', 'bin pilot', 'test pilot_membership', 'fuzz postings'
    selector: tuple    # `cargo test` target selection; () when cargo test cannot run it
    modpath: tuple     # module path below the crate root; () is the root itself
    cfgs: tuple        # cfg predicates along the declaration chain

    @property
    def test_only(self):
        return any(re.search(r'\b(?:test|kani|loom)\b', cfg) and 'not(' not in cfg
                   for cfg in self.cfgs)

    @property
    def runnable(self):
        """Compiled by a plain `cargo test` (not a kani/loom-only module)."""
        return not any(re.search(r'\b(?:kani|loom)\b', cfg) and 'not(' not in cfg for cfg in self.cfgs)

    def describe(self):
        cfg = f'  cfg({" & ".join(self.cfgs)})' if self.cfgs else ''
        return f'{self.crate} {self.target}: {"::".join(("crate",) + self.modpath)}{cfg}'

    def short(self):
        if self.modpath:
            return '::'.join(self.modpath)
        return 'crate root' if (self.crate, self.target) == ('streams-slate', 'lib') else f'{self.target} root'


def attributes_above(text, start):
    """Outer attributes on the lines directly above offset `start`, bottom-up."""
    found, pending = [], []
    lines = text[:start].split('\n')[:-1]
    for raw in reversed(lines[-80:]):
        line = raw.strip()
        if pending:  # inside a multi-line attribute, collecting up to its `#[`
            pending.append(line)
            if line.startswith('#['):
                found.append(' '.join(reversed(pending)))
                pending = []
            elif not line or line.endswith((';', '{', '}')) or len(pending) > 40:
                break
            continue
        if not line or line.startswith('//'):
            continue
        if ATTRIBUTE_LINE.fullmatch(line):  # attributes only: `#[cfg(test)] mod c;` is an item
            found.append(line)
        elif line.endswith(']') and not line.startswith('#['):
            pending = [line]
        else:
            break
    return found


def declared_attributes(text, match):
    lead = text[match.start('lead'):match.end('lead')]  # raw: `match` may be on masked text
    attributes = re.findall(r'#\[[^\n]*?\]', lead) + attributes_above(text, match.start())
    path, cfgs = None, []
    for attribute in attributes:
        if found := re.match(r'#\[\s*path\s*=\s*"([^"]*)"', attribute):
            path = path or found[1]
        elif found := re.match(r'#\[\s*cfg\s*\((.*)\)\s*\]', attribute, re.S):
            cfgs.append(re.sub(r'\s+', ' ', found[1]).strip())
    return path, tuple(cfgs)


def module_decls(text):
    """(name, enclosing inline modules, #[path], cfgs) of each file module."""
    matches = list(MOD_DECL.finditer(text))
    inline = next((m.start() for m in matches if m['kind'] == '{'), None)
    if inline is None or not any(m['kind'] == ';' and m.start() > inline for m in matches):
        return [(m['name'], (), *declared_attributes(text, m)) for m in matches if m['kind'] == ';']
    # A file module declared after an inline block may sit inside it: track
    # braces on masked source (same offsets), read attributes from the raw text.
    decls, stack, depth = [], [], 0
    for token in NESTING.finditer(architecture.strip_noncode(text)):
        if token['brace']:
            depth += 1 if token['brace'] == '{' else -1
            while stack and stack[-1][1] > depth:
                stack.pop()
            continue
        path, cfgs = declared_attributes(text, token)
        if token['kind'] == '{':
            depth += 1
            stack.append((token['name'], depth, cfgs))
        else:
            decls.append((token['name'], tuple(name for name, _, _ in stack), path,
                          tuple(c for _, _, inherited in stack for c in inherited) + cfgs))
    return decls


def crate_roots():
    cargo = tomllib.loads((ROOT / 'Cargo.toml').read_text())
    roots = [('streams-slate', 'lib', ('--lib',), 'src/lib.rs')]
    bins = {entry['path']: entry['name'] for entry in cargo.get('bin', [])}
    if cargo.get('package', {}).get('autobins', True):
        for path in sorted((ROOT / 'src/bin').glob('*.rs')):
            bins.setdefault(path.relative_to(ROOT).as_posix(), path.stem)
    for path, name in sorted(bins.items()):
        roots.append(('streams-slate', f'bin {name}', ('--bin', name), path))
    for path in sorted((ROOT / 'tests').glob('*.rs')):
        roots.append(('streams-slate', f'test {path.stem}', ('--test', path.stem),
                      path.relative_to(ROOT).as_posix()))
    examples = {entry['path']: entry['name'] for entry in cargo.get('example', [])}
    if cargo.get('package', {}).get('autoexamples', True):
        for path in sorted((ROOT / 'examples').glob('*.rs')):
            examples.setdefault(path.relative_to(ROOT).as_posix(), path.stem)
    for path, name in sorted(examples.items()):
        roots.append(('streams-slate', f'example {name}', ('--example', name), path))
    roots.append(('streams-quality-invariants', 'lib', SELECTORS['harness-lib'],
                  'tools/quality-invariants/src/lib.rs'))
    roots.append(('streams-quality-syntax', 'bin', ('-p', 'streams-quality-syntax'),
                  'tools/quality-syntax/src/main.rs'))
    for path in sorted((ROOT / 'fuzz/fuzz_targets').glob('*.rs')):
        roots.append(('streams-quality-fuzz', f'fuzz {path.stem}', (), path.relative_to(ROOT).as_posix()))
    return roots


class Tree:
    """Every workspace Rust file reachable from a crate root, per target."""

    def __init__(self):
        self.texts = {}
        self.placements = defaultdict(list)
        for crate, target, selector, root in crate_roots():
            self._walk(ROOT / root, Placement(crate, target, selector, (), ()), True, False, set())
        self.by_target = defaultdict(list)
        for rel, placements in self.placements.items():
            for placement in placements:
                self.by_target[(placement.crate, placement.target)].append(
                    (placement.modpath, rel, placement.runnable))

    def text(self, rel):
        if rel not in self.texts:
            path = ROOT / rel
            self.texts[rel] = path.read_text(errors='replace') if path.is_file() else None
        return self.texts[rel]

    def _walk(self, file, placement, is_root, via_path, seen):
        file = Path(os.path.normpath(file))
        if file in seen or not file.is_file() or not file.is_relative_to(ROOT):
            return
        seen.add(file)
        rel = file.relative_to(ROOT).as_posix()
        self.placements[rel].append(placement)
        # Crate roots, mod.rs and #[path] files own their directory; any other
        # file's children live in the directory named after it.
        owned = file.parent if is_root or via_path or file.name == 'mod.rs' else file.parent / file.stem
        for name, inline, path, cfgs in module_decls(self.text(rel)):
            child = Placement(placement.crate, placement.target, placement.selector,
                              placement.modpath + inline + (name,), placement.cfgs + cfgs)
            if path:
                target = (owned.joinpath(*inline) if inline else file.parent) / path
                self._walk(target, child, False, True, seen)
            else:
                directory = owned.joinpath(*inline)
                target = directory / f'{name}.rs'
                self._walk(target if target.is_file() else directory / name / 'mod.rs',
                           child, False, False, seen)

    def tests_in(self, rel):
        text = self.text(rel)
        return len(TEST_ATTR.findall(text)) if text else 0

    def subtree_tests(self, placement, prefix):
        entries = self.by_target[(placement.crate, placement.target)]
        return sum(self.tests_in(rel) for modpath, rel, runnable in entries
                   if runnable and modpath[:len(prefix)] == prefix)

    def lib(self, rel):
        return next((p for p in self.placements.get(rel, ()) if p.crate == 'streams-slate' and p.target == 'lib'),
                    None)

    @cache
    def includes(self):
        """{non-Rust file: [Rust files that include_str!/include_bytes! it]}."""
        found = defaultdict(list)
        for rel in list(self.placements):
            for target in INCLUDE.findall(self.text(rel) or ''):
                resolved = os.path.normpath(os.path.join(os.path.dirname(rel), target))
                found[resolved].append(rel)
        return found


@cache
def module_tree():
    """One walk per process: the tool never writes, so the tree cannot move under it."""
    return Tree()


def whole_test(rel, text):
    """The syntax scanner's whole-file test code: `*/proofs.rs` or `#![cfg(test)]`."""
    return rel.endswith('/proofs.rs') or bool(text and re.search(r'^#!\[cfg\(test\)\]', text, re.M))


def test_filter(tree, placement):
    """The narrowest module filter that still runs a test; '' is the whole
    target, None means no test anywhere above this module."""
    prefix = placement.modpath
    while prefix:
        if tree.subtree_tests(placement, prefix):
            return '::'.join(prefix) + '::'
        prefix = prefix[:-1]
    return '' if not placement.modpath else None


def first_sentence(text, limit=150):
    lines = []
    for line in (text or '').splitlines():
        if line.startswith('//!'):
            body = line[3:].strip()
            if not body and lines:
                break
            lines.append(body)
        elif lines or line.strip() and not line.startswith(('#!', '//')):
            break
    paragraph = ' '.join(x for x in lines if x)
    match = re.match(r'(.+?[.!?])(?:\s|$)', paragraph)
    sentence = match[1] if match else paragraph
    return sentence if len(sentence) <= limit else sentence[:limit - 1] + '…'


# --------------------------------------------------------------------------
# Ledgers the gates read (loaded once, read-only)
# --------------------------------------------------------------------------

class Ledgers:
    def __init__(self):
        def load(relative):
            try:
                return json.loads((ROOT / relative).read_text())
            except (OSError, ValueError) as error:  # e.g. a ledger mid-edit
                raise ImpactError(f'cannot read {relative}: {error}')
        self.legacy = load('docs/quality/legacy-source.json')
        self.policy = load('docs/quality/policy.json')
        self.allowances = load('docs/quality/source-allowances.json')['occurrences']
        self.owners = load('docs/quality/owners.json')['occurrences']
        self.growth = load('docs/quality/exception-growth.json')['rows']
        self.fragments = load('docs/quality/syntax-fragments.json')
        self.arch_policy = load('docs/refactor/architecture-policy.json')
        self.arch_baseline = load('docs/refactor/architecture-review-baseline.json')
        mechanisms = load('docs/refactor/review-mechanisms.json')
        self.inventory = defaultdict(list)
        for test in load('docs/refactor/test-inventory.json'):
            self.inventory[test['file']].append(test)
        self.scenarios = defaultdict(list)
        for scenario in load('docs/refactor/test-scenario-map.json'):
            for test in scenario.get('tests', []):
                self.scenarios[test['file']].append(test['name'])
        # review-evidence.py --check re-hashes each of these function bodies.
        self.pins = defaultdict(list)
        for entry in mechanisms['mechanisms']:
            for test in entry.get('tests', []):
                self.pins[test['file']].append(('test', test['name'], test.get('sha256')))
            for helper in entry.get('support_functions', []):
                self.pins[helper['file']].append(('support', helper['name'], helper.get('sha256')))
        for section in ('fixture_changes', 'source_adaptations'):
            for change in mechanisms.get(section, []):
                self.pins[change['file']].append((section, change['name'], change.get('after_sha256')))
        for moved in load('docs/refactor/review-unit-relocations.json'):
            self.pins[moved['to_file']].append(('relocation', moved['name'], moved['function_sha256']))
        self.immutable = {path: ('text', sha) for path, sha in self.policy['immutable_sha256'].items()}
        self.immutable.update({path: ('text', entry['sha256']) for path, entry in self.fragments.items()})
        self.immutable['docs/refactor/architecture-review-baseline.json'] = (
            'bytes', self.arch_policy.get('baseline_sha256'))
        clippy = mechanisms['clippy_baseline']
        self.immutable[clippy['file']] = ('bytes', clippy['sha256'])
        self.mt_audit = defaultdict(list)
        for line in (ROOT / 'scripts/mt-audit-baseline.txt').read_text().splitlines():
            parts = line.split('\t', 2)
            if len(parts) == 3:
                self.mt_audit[parts[1]].append(parts[2])


@cache
def ledgers_once():
    return Ledgers()


def line_ceiling(path, legacy_lines, adoption, base_lines):
    """The source ratchet's file-growth ceiling for one path.

    Restates the inline rule of scripts/quality/source_rules.violations: the
    adoption ceiling (legacy lines plus a bounded addition, at least 1,000),
    lowered to the merge base's length once that is within it.
    test_impact.py pins this restatement to the gate itself."""
    limit = max(1000, legacy_lines.get(path, 0) + adoption.get(path, {}).get('extra_lines', 0))
    if base_lines is not None:
        limit = min(limit, max(1000, base_lines))
    return limit


def architecture_budgets(path, text, ledgers):
    """(file limit or None, [(function, length, limit)]) under architecture-gate.violations."""
    if not path.startswith('src/') or architecture.is_test_only(ROOT / path, text):
        return None, []
    before = ledgers.arch_baseline['metrics'].get(path, {'lines': 1000, 'functions': {}})
    exceptions = ledgers.arch_policy.get('budget_exceptions', {})
    file_limit = exceptions.get(f'file:{path}', {}).get('limit', max(1000, before.get('lines', 0)))
    lengths = {}
    for name, start, end in architecture.find_functions(architecture.strip_noncode(text)):
        lengths[name] = max(lengths.get(name, 0), end - start + 1)
    functions = []
    for name, length in lengths.items():
        default = max(architecture.FUNCTION_BUDGET, before.get('functions', {}).get(name, 0))
        functions.append((name, length, exceptions.get(f'function:{path}::{name}', {}).get('limit', default)))
    return file_limit, functions


# --------------------------------------------------------------------------
# Per-file analysis
# --------------------------------------------------------------------------

@dataclass
class FileImpact:
    path: str
    status: str                 # M A D R?? git status; 'planned' for an explicit path that does not exist
    previous: str = ''          # rename source
    exists: bool = True
    modules: list = field(default_factory=list)
    module: str = ''            # the short module path shown on the file's summary line
    doc: str = ''
    owner: dict = None
    critical: bool = False
    unregistered: bool = False  # CI's plan selects it for mutation and no owner row exists
    test_only: bool = False
    filters: list = field(default_factory=list)    # [[selector, filter], ...] inner-loop contributions
    checks: list = field(default_factory=list)
    formal: list = field(default_factory=list)
    gate_input: str = ''
    lines: int = None
    ceiling: int = None
    arch_ceiling: int = None
    headroom: int = None
    tight_functions: list = field(default_factory=list)
    harness: list = field(default_factory=list)
    included_by: list = field(default_factory=list)
    exception_scopes: list = field(default_factory=list)
    ledgers: dict = field(default_factory=dict)
    pins: dict = field(default_factory=dict)
    immutable: str = ''
    edge: bool = False
    hard_owner: bool = False
    details: dict = field(default_factory=dict)    # verbose-only facts


def analyze_file(change, status, tree, ledgers, base_texts, verbose):
    path = change.after or change.before
    report = FileImpact(path, status, change.before if status.startswith('R') else '')
    report.exists = (ROOT / path).is_file()
    text = (tree.text(path) if path.endswith('.rs') else read_text(path)) if report.exists else None
    report.gate_input = GATE_INPUTS.get(path, '')
    report.formal = formal_obligations().get(path, [])
    if path.startswith('verification/receipts/'):
        report.gate_input = 'written only by formal.py run --record (or formal_batch.py rerecord)'
    immutable_status(report, text, ledgers)
    report.included_by = sorted(tree.includes().get(path, []))
    for includer in report.included_by:  # e.g. RUNBOOK.md -> src/operator.rs
        for placement in tree.placements.get(includer, ()):
            if placement.selector and (chosen := test_filter(tree, placement)) is not None:
                report.filters.append([list(placement.selector), chosen])
    if path.endswith('.rs'):
        analyze_rust(report, text, tree, ledgers, base_texts.get(path), verbose)
    report.edge = path.startswith(EDGE_PREFIXES) and not report.test_only
    analyze_pins(report, text, ledgers)
    report.checks = checks_for(report, text)
    return report


def read_text(path):
    try:
        return (ROOT / path).read_text(errors='replace')
    except OSError:
        return None


def immutable_status(report, text, ledgers):
    if report.path not in ledgers.immutable:
        return
    kind, expected = ledgers.immutable[report.path]
    raw = (ROOT / report.path).read_bytes() if report.exists else b''
    actual = common.digest(text or '') if kind == 'text' else hashlib.sha256(raw).hexdigest()
    report.immutable = 'hash-pinned: never edit' + ('' if actual == expected else ' (CHANGED: revert it)')


def analyze_rust(report, text, tree, ledgers, base_text, verbose):
    path = report.path
    placements = tree.placements.get(path, [])
    lib = tree.lib(path)
    report.modules = [p.describe() for p in placements] or [
        'deleted' if report.status == 'D'
        else f'not declared yet: planned {guess_module(path)}{declare_hint(path)}' if not report.exists
        else 'build script' if path.endswith('build.rs')
        else 'not reached from any crate root (script, template or undeclared)']
    report.module = placements[0].short() if placements else \
        report.modules[0].replace('not declared yet: ', '').split(' (')[0]
    report.doc = first_sentence(text)
    report.test_only = whole_test(path, text) or bool(placements) and all(
        p.test_only or p.modpath[:1] == ('dst',) for p in placements)
    owner = mutation_owners.source_map().get(path)
    report.critical = path.startswith(vp.CRITICAL_PREFIXES)
    if owner:
        report.owner = {'name': owner.name, 'package': owner.package, 'target': owner.target,
                        'filters': list(owner.test_filters)}
    # Inner loop: the narrowest module filter per target, plus the owner's gate filters.
    for placement in placements:
        if placement.selector and (chosen := test_filter(tree, placement)) is not None:
            report.filters.append([list(placement.selector), chosen])
    if not placements and path.startswith('src/') and not path.startswith('src/bin/'):
        chosen = planned_filter(tree, guess_module(path))
        if chosen is not None:
            report.filters.append([['--lib'], chosen])
    if owner:
        for value in owner.test_filters or ('',):
            report.filters.append([list(SELECTORS[owner.target]), value])
    if path.startswith('src/'):  # service source a harness crate also compiles by #[path]
        report.harness = sorted({p.crate for p in placements if p.crate in HARNESS_CRATES})
    report.hard_owner = path.startswith('src/application/') or path in ledgers.arch_policy['sse_core_files']
    if report.status == 'D':
        return
    if not report.exists:
        report.lines, report.ceiling, report.headroom = 0, 1000, 1000
        return
    report.lines = len(text.splitlines())
    report.ceiling = line_ceiling(path, ledgers.legacy['lines'], ledgers.policy['adoption_line_additions'],
                                  None if base_text is None else len(base_text.splitlines()))
    report.arch_ceiling, functions = architecture_budgets(path, text, ledgers)
    report.headroom = min(x for x in (report.ceiling, report.arch_ceiling) if x is not None) - report.lines
    report.tight_functions = [f'{name} {length}/{limit}' for name, length, limit in sorted(functions)
                              if limit - length <= WARN_HEADROOM]
    for match in re.finditer(r'#\[(expect|allow)\(([^\]]*?)\)\]', text, re.S):
        if 'reason' in match[2]:
            lint = re.sub(r'\s+', ' ', match[2]).split(',')[0].strip()
            report.exception_scopes.append(f'L{text.count(chr(10), 0, match.start()) + 1} {match[1]}({lint})')
    counts = defaultdict(Counter)
    for name, rows in (('source-allowances', ledgers.allowances), ('owners.json', ledgers.owners)):
        for row in rows:
            if row['path'] == path:
                counts[name][row['category']] += row['count']
    report.ledgers = {name: dict(value) for name, value in counts.items()}
    growth = [f"{row['owner']}:{row['lint']}" for row in ledgers.growth if row['path'] == path]
    if growth:
        report.ledgers['exception-growth rows'] = growth
    if verbose:
        report.details = rust_details(path, text, tree, None if report.test_only else lib)


def planned_filter(tree, module):
    """For a module not declared yet: the nearest existing ancestor with tests."""
    parts = tuple(module.split('::'))
    lib = Placement('streams-slate', 'lib', ('--lib',), (), ())
    for size in range(len(parts) - 1, 0, -1):
        if tree.subtree_tests(lib, parts[:size]):
            return '::'.join(parts[:size]) + '::'
    return None


def declare_hint(path):
    """Where a planned `src/a/b.rs` gets its `mod b;` (the existing parent module file)."""
    parent = Path(path).parent
    for candidate in (parent.with_suffix('.rs'), parent / 'mod.rs'):
        if (ROOT / candidate).is_file():
            return f' (declare `mod {Path(path).stem};` in {candidate.as_posix()})'
    return ''


def guess_module(path):
    """`src/a/b.rs` -> `a::b`, for a module that is planned but not declared."""
    parts = list(Path(path).with_suffix('').parts)
    if parts[:2] == ['src', 'bin']:
        return '::'.join(parts[2:])
    parts = parts[1:] if parts[:1] == ['src'] else parts
    if parts and parts[-1] == 'mod':
        parts.pop()
    return '::'.join(parts)


def rust_details(path, text, tree, lib):
    """Verbose-only: where this production file's tests live, including DST
    scenarios that import its module or name its public items (a heuristic)."""
    details = {'tests in file': tree.tests_in(path)}
    if lib is None:
        return details
    entries = tree.by_target[('streams-slate', 'lib')]
    children = [f'{rel}({tree.tests_in(rel)})' for modpath, rel, _ in entries
                if rel != path and modpath[:len(lib.modpath)] == lib.modpath and tree.tests_in(rel)]
    details['tests in module subtree'] = tree.subtree_tests(lib, lib.modpath) if lib.modpath else 'crate root'
    if children:
        details['child test files'] = children
    dst = {rel: tree.text(rel) for modpath, rel, _ in entries if modpath[:1] == ('dst',)}
    top = lib.modpath[0] if lib.modpath else None
    if top and top != 'dst':
        pattern = re.compile(rf'\bcrate::{top}\b|\buse\s+crate::\{{[^}}]*\b{top}\b')
        importers = sorted(Path(rel).stem for rel, body in dst.items() if pattern.search(body))
        if importers:
            details['DST files importing crate::' + top] = f'{len(importers)}: {brief(importers, 12)}'
    names = set(re.findall(r'\bpub(?:\([^)]*\))?\s+(?:async\s+)?(?:fn|struct|enum|trait|type|const)\s+'
                           r'([A-Za-z_]\w{3,})', text))
    names -= {'new', 'from', 'into', 'default', 'build', 'start', 'open', 'close', 'read', 'write',
              'next', 'with', 'from_bytes'}
    if names and not path.startswith('src/dst/'):
        pattern = re.compile(r'\b(' + '|'.join(sorted(map(re.escape, names))) + r')\b')
        scored = sorted(((len(set(pattern.findall(body))), Path(rel).stem) for rel, body in dst.items()
                         if '/tests/' in rel and pattern.search(body)), reverse=True)
        if scored:
            details['DST tests naming its items'] = ', '.join(f'{name}({n})' for n, name in scored[:8])
            details['DST filter hint'] = 'cargo test --locked --lib -- ' + ' '.join(
                f'dst_tests::{name}::' for _, name in scored[:3])
    return details


def analyze_pins(report, text, ledgers):
    """Pinned tests and fingerprints in this file, and which ones it no longer matches."""
    path, pins = report.path, {}
    exists = report.exists and text is not None
    if ledgers.inventory.get(path):
        entry = {'tests': len(ledgers.inventory[path])}
        if exists and path.endswith('.rs'):
            entry.update(inventory_drift(path, text, ledgers.inventory[path]))
        pins['dst-inventory'] = entry
    elif exists and path.startswith('src/dst/') and path.endswith('.rs'):
        drift = inventory_drift(path, text, [])
        if drift:
            pins['dst-inventory'] = {'tests': 0, **drift}
    if ledgers.pins.get(path):
        entry = {'pins': len(ledgers.pins[path])}
        broken = pin_drift(path, text, ledgers.pins[path]) if exists else [n for _, n, _ in ledgers.pins[path]]
        if broken:
            entry['changed'] = broken
        pins['review-mechanisms'] = entry
    if ledgers.scenarios.get(path):
        names = sorted(set(ledgers.scenarios[path]))
        entry = {'mapped tests': len(names)}
        scenario_map = test_inventory().scenario_map
        masked = scenario_map.mask_noncode(text) if exists else ''
        missing = [name for name in names if not scenario_map.symbol_defined(masked, name)]
        if missing:
            entry['missing'] = missing
        pins['scenario-map'] = entry
    if ledgers.mt_audit.get(path):
        entry = {'fingerprints': len(ledgers.mt_audit[path])}
        current = {' '.join(line.split()) for line in (text or '').splitlines()}
        gone = [fp for fp in ledgers.mt_audit[path] if fp not in current]
        if gone:
            entry['gone'] = gone
        pins['mt-audit'] = entry
    report.pins = pins


def inventory_drift(path, text, pinned):
    try:
        current = {t['name']: t for t in test_inventory().functions(text, ROOT / path)}
    except ValueError as error:  # a body mid-edit that does not lex
        return {'unreadable': str(error)}
    before = {t['name']: t for t in pinned}
    fields = ('function_sha256', 'attributes', 'mechanisms', 'configuration')
    drift = {
        'changed': sorted(n for n in set(current) & set(before)
                          if any(current[n].get(k) != before[n].get(k) for k in fields)),
        'new': sorted(set(current) - set(before)),
        'gone': sorted(set(before) - set(current)),
    }
    return {key: value for key, value in drift.items() if value}


def pin_drift(path, text, pins):
    """The pins review-evidence.py --check would find changed, hashed its way."""
    inventory = test_inventory()
    if not path.endswith('.rs'):
        digest = hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
        return [name for _, name, sha in pins if digest != sha or name not in text]
    try:
        tests = inventory.functions(text, ROOT / path)
        helpers = inventory.functions(text, ROOT / path, include_helpers=True)
        unanchored = inventory.functions(text, include_helpers=True)  # fixture/adaptation hashing
    except ValueError:
        return [name for _, name, _ in pins]
    broken = []
    for kind, name, sha in pins:
        pool = tests if kind in ('test', 'relocation') else helpers if kind == 'support' else unanchored
        found = [f for f in pool if f['name'] == name]
        if len(found) != 1 or found[0]['function_sha256'] != sha:
            broken.append(name)
    return broken


def checks_for(report, text):
    path, checks = report.path, []
    if path.startswith('scripts/quality/'):
        checks.append('python3 -m unittest discover -s scripts/quality')
    elif path.startswith('scripts/dev/') and path.endswith('.py'):
        checks.append("python3 -m unittest discover -s scripts/dev -p 'test_*.py'")
    elif path.startswith('scripts/effective-config/'):
        checks.append('python3 -m unittest discover -s scripts/effective-config')
    elif re.fullmatch(r'scripts/[\w-]+\.py', path) and text and 'def self_test' in text:
        checks.append(f'python3 {path} --self-test')
    if path.startswith('verification/') or report.formal or path in GATE_INPUTS and 'formal' in GATE_INPUTS[path]:
        checks.append('python3 scripts/quality/formal.py check')
    if 'dst-inventory' in report.pins or path in ('docs/refactor/test-inventory.json',):
        checks.append('python3 scripts/test-inventory.py --check')
    if 'scenario-map' in report.pins or path in ('docs/refactor/test-scenario-map.json',
                                                'docs/dst/SCENARIO-CATALOG.md'):
        checks.append('python3 scripts/scenario-map-report.py --check')
    if 'review-mechanisms' in report.pins or path.startswith('docs/refactor/review-'):
        checks.append('python3 scripts/review-evidence.py --check')
    if 'mt-audit' in report.pins or path == 'scripts/mt-audit-baseline.txt':
        checks.append('bash scripts/multitenancy-audit.sh')
    if path.startswith('docs/refactor/architecture'):
        checks.append('python3 scripts/architecture-gate.py --check')
    if path.startswith('.github/workflows/'):
        checks.append('actionlint')
    if path.startswith('sdk/'):
        checks.append('(cd sdk && npm test)')
    return checks


@cache
def formal_obligations():
    """{path: [obligation ids whose receipt digests it]} (formal.digested_files)."""
    result = defaultdict(list)
    for obligation in formal.load()['obligations']:
        for path in formal.digested_files(obligation):
            result[path].append(obligation['id'])
    return result


# --------------------------------------------------------------------------
# Whole-change analysis: CI plan, formal selection and staleness, commands
# --------------------------------------------------------------------------

@dataclass
class Impact:
    mode: str
    base: str
    base_label: str
    ratchet_base: str
    files: list
    plan: dict
    formal: dict
    tests: list
    release: str
    checks: list
    notes: list
    seconds: float = 0.0


def resolve_paths(values):
    """Repository-relative paths for cwd- or repo-relative arguments; a
    directory expands to its tracked and untracked files."""
    result = []
    for value in values:
        candidates = [Path(value)] if Path(value).is_absolute() else [Path.cwd() / value, ROOT / value]
        chosen = next((c for c in candidates if c.exists()), None)
        if chosen is None:
            chosen = next((c for c in candidates if Path(os.path.normpath(c)).is_relative_to(ROOT)), None)
        if chosen is None:
            raise ImpactError(f'{value}: not inside the repository {ROOT}')
        chosen = Path(os.path.normpath(chosen.absolute()))
        if not chosen.is_relative_to(ROOT):
            raise ImpactError(f'{value}: not inside the repository {ROOT}')
        rel = chosen.relative_to(ROOT).as_posix()
        if chosen.is_dir():
            listed = git('ls-files', '-z', '--cached', '--others', '--exclude-standard', '--', rel or '.')
            result.extend(p for p in listed.split('\0') if p)
        else:
            result.append(rel)
    return list(dict.fromkeys(result))


def resolve_bases(base, commits):
    target = base or os.environ.get('QUALITY_BASE_REF') or 'origin/slate'
    try:
        ratchet = git('merge-base', 'HEAD', target).strip()
    except ImpactError as error:
        raise ImpactError(f'no merge base with {target} ({error}); pass --base REV or set QUALITY_BASE_REF')
    if commits:
        try:
            return git('rev-parse', f'HEAD~{commits}').strip(), f'HEAD~{commits}', ratchet
        except ImpactError:
            raise ImpactError(f'HEAD~{commits} does not exist (shallow clone or too few commits)')
    return ratchet, f'merge-base with {target}', ratchet


def working_changes(base):
    """Rename-aware changes from `base` to the working tree, plus untracked
    files (status '??'; CI will see them as additions once committed)."""
    try:
        changes = vp.discover_changes(base, ROOT)
    except ValueError as error:
        raise ImpactError(f'could not read the git diff against {base[:10]}: {error}') from error
    known = {c.after or c.before for c in changes}
    untracked = git('ls-files', '-z', '--others', '--exclude-standard').split('\0')
    changes += [vp.SourceChange('??', '', p) for p in untracked if p and p not in known]
    return changes


def syntax_binary_note():
    binary = common.syntax_binary()
    if not binary.is_file():
        return ('syntax scanner missing (cargo build --locked -p streams-quality-syntax): mutation '
                'selection assumes every changed .rs file changes production code')
    newest = max((p.stat().st_mtime for p in (ROOT / 'tools/quality-syntax').rglob('*.rs')), default=0)
    if binary.stat().st_mtime < newest:
        return 'syntax scanner is older than tools/quality-syntax: rebuild it for CI-exact selection'
    return None


def ci_plan(changes, base, explicit, tree, notes):
    """verification_plan's own selection. A diff is classified by the syntax
    scanner exactly as CI does; explicit paths are hypothetical production
    edits, except whole-file test code, which CI never mutates."""
    for_plan = list(changes)
    visibility = production = formatted = ()
    if explicit:
        production = [c.after for c in for_plan
                      if c.after.endswith('.rs') and whole_test(c.after, tree.text(c.after))]
    elif any((c.after or c.before).endswith('.rs') for c in for_plan):
        note = syntax_binary_note()
        if note:
            notes.append(note)
        if note is None or 'older' in note:
            try:
                visibility, production, formatted = vp.source_changes(base, for_plan)
            except (subprocess.CalledProcessError, ValueError, OSError) as error:
                notes.append(f'syntax classification failed ({str(error).splitlines()[0]}): '
                             'assuming production edits')
    try:
        mutation_owners.validate_sources(ROOT)
    except ValueError as error:
        notes.append(f'mutation owner table: {str(error).splitlines()[0]}')
    return vp.plan_changes(for_plan, visibility, production, formatted, vp.previous_registered_sources(base))


def receipt_minutes():
    minutes = {}
    for path in (ROOT / 'verification/receipts').glob('*.json'):
        try:
            receipt = json.loads(path.read_text())
            minutes[receipt['id']] = sum(c.get('seconds', 0) for c in receipt.get('checks', [])) / 60
        except (ValueError, KeyError, TypeError):
            continue
    return minutes


def ci_formal_wall(selected, ids, minutes):
    """(wall minutes, shards) of CI's formal job for `selected`, sharded the way
    the workflow does it: by scripts/quality/formal_shards.py (LPT over the
    selection) once the workflow calls it, else by sorted position modulo N."""
    workflow = ROOT / '.github/workflows/rust-quality.yml'
    text = workflow.read_text() if workflow.is_file() else ''
    found = re.search(r'FORMAL_SHARDS:\s*(\d+)', text)
    shards = int(found[1]) if found else 1
    if 'formal_shards.py' in text:
        try:
            import formal_shards
            work = formal_shards.weights(selected)
            return formal_shards.makespan(formal_shards.lpt(work, shards), work) / 60, shards
        except (ImportError, AttributeError, ValueError):
            pass
    load = Counter()
    for position, oid in enumerate(ids):
        if oid in selected:
            load[position % shards] += minutes.get(oid, 0)
    return max(load.values(), default=0), shards


def json_at(revision, path):
    text = show_many(revision, [path])[path]
    return json.loads(text) if text else None


def predicted_stale(manifest, changed, base):
    """Obligations whose receipt digest these paths move (formal.input_snapshot's inputs)."""
    base_manifest = json_at(base, 'verification/manifest.json') or {'obligations': []}
    base_entries = {o['id']: o for o in base_manifest['obligations']}
    pins_moved = False
    if 'quality-tools.toml' in changed:
        before = tomllib.loads(show_many(base, ['quality-tools.toml'])['quality-tools.toml'] or '')
        pins_moved = {'formal': before.get('formal'), 'slatedb': before.get('slatedb')} != formal.pinned()
    moved = set()
    if formal.ledger_path() in changed:
        now, before = formal.assumption_entries(), formal.ledger_at(base)
        moved = {asm for asm in set(now) | set(before) if now.get(asm) != before.get(asm)}
    entry_moved = 'verification/manifest.json' in changed
    return sorted(o['id'] for o in manifest['obligations']
                  if formal.digested_files(o) & changed or pins_moved
                  or entry_moved and base_entries.get(o['id']) != o
                  or set(o.get('assumptions', ())) & moved)


def formal_impact(changed, base, explicit, notes):
    try:
        manifest = formal.load()
        selected = formal.select(manifest, changed, formal.ledger_at(base))
        everything = sorted(changed & set(formal.SELECTION_INPUTS))
        predicted = predicted_stale(manifest, changed, base)
        problems, stale = formal.receipt_report(manifest)
    except (AttributeError, KeyError, TypeError, ValueError, OSError) as error:
        notes.append(f'formal section skipped ({error!r}): run python3 scripts/quality/formal.py select '
                     f'--base {base} and formal.py check')
        return {}
    minutes = receipt_minutes()
    ids = sorted(o['id'] for o in manifest['obligations'])
    wall, shards = ci_formal_wall(selected, ids, minutes)
    reasons = Counter()
    for entry in stale:
        for item in re.findall(r'\(([^)]*)\)', entry)[-1:]:
            reasons.update(x.strip() for x in item.split(','))
    return {
        'total': len(ids),
        'selected': selected,
        'everything': everything,
        'ci_wall_minutes': round(wall, 1),
        'shards': shards,
        'stale_from_paths': predicted,
        'stale_from_paths_minutes': round(sum(minutes.get(i, 0) for i in predicted), 1),
        'stale_now': sorted(entry.split(':')[0] for entry in stale),
        'stale_now_inputs': [name for name, _ in reasons.most_common(3)],
        'stale_now_minutes': round(sum(minutes.get(entry.split(':')[0], 0) for entry in stale), 1),
        'problems': problems,
        'unchanged_but_named': explicit and bool(changed & {'verification/manifest.json', 'quality-tools.toml',
                                                             formal.ledger_path()}),
    }


def minimal_filters(filters):
    """Drop a filter another one already covers (libtest filters are substrings)."""
    values = sorted(set(filters))
    return [f for f in values if not any(g != f and g in f for g in values)]


def cargo_command(selector, filters, release=False):
    words = ['cargo', 'test', '--locked'] + (['--release'] if release else []) + list(selector)
    if '' in filters:
        return ' '.join(words)
    # Every filter after `--`: libtest ORs positional filters.
    return ' '.join(words + ['--', *minimal_filters(filters)])


def test_commands(files, plan):
    groups = defaultdict(set)
    for report in files:
        for selector, value in report.filters:
            groups[tuple(selector)].add(value)
    for name in plan.get('selected_mutation_owners', []):
        owner = next(o for o in mutation_owners.OWNERS if o.name == name)
        groups[SELECTORS[owner.target]].update(owner.test_filters or ('',))
    order = sorted(groups, key=lambda s: (s != ('--lib',), s))
    tests = [cargo_command(s, groups[s]) for s in order]
    release = cargo_command(('--lib',), groups[('--lib',)], release=True) if ('--lib',) in groups else ''
    return tests, release


def explicit_changes(paths, base):
    """(display status, SourceChange) for explicit paths: an edit of an
    existing file, a new file, a planned file or a deletion."""
    wanted = resolve_paths(paths)
    at_base = show_many(base, wanted)
    result = []
    for path in wanted:
        tracked = at_base[path] is not None
        if (ROOT / path).exists():
            result.append(('edit', vp.SourceChange('M', path, path)) if tracked
                          else ('new', vp.SourceChange('A', '', path)))
        elif tracked:
            result.append(('D', vp.SourceChange('D', path, '')))
        else:
            result.append(('planned', vp.SourceChange('A', '', path)))
    return result


def analyze(paths=(), base=None, commits=0, verbose=False):
    started = time.monotonic()
    notes = []
    diff_base, label, ratchet = resolve_bases(base, commits)
    if paths:
        mode = 'paths'
        statused = explicit_changes(paths, ratchet)
        changed = {c.after or c.before for _, c in statused}
    else:
        mode = 'commits' if commits else 'diff'
        statused = [(c.status, vp.SourceChange('A', '', c.after) if c.status == '??' else c)
                    for c in working_changes(diff_base)]
        changed = formal.changed_since(diff_base)
    changes = [c for _, c in statused]
    tree = module_tree()
    ledgers = ledgers_once()
    base_texts = show_many(ratchet, [c.after for c in changes if c.after.endswith('.rs')])
    files = [analyze_file(c, status, tree, ledgers, base_texts, verbose) for status, c in statused]
    plan = ci_plan(changes, diff_base, mode == 'paths', tree, notes)
    for report in files:
        report.unregistered = report.path in plan.get('unregistered_mutation_source_files', ())
    formal_result = formal_impact(changed, diff_base, mode == 'paths', notes)
    tests, release = test_commands(files, plan)
    checks = list(dict.fromkeys(check for report in files for check in report.checks))
    return Impact(mode, diff_base, label, ratchet, files, plan, formal_result, tests, release, checks, notes,
                  round(time.monotonic() - started, 2))


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def brief(items, limit=8):
    items = list(items)
    return ' '.join(items[:limit]) + (f' (+{len(items) - limit} more)' if len(items) > limit else '')


def minutes(value):
    return '<1 min' if value < 1 else f'~{value:.0f} min'


def file_line(report):
    status = report.status if report.status in ('??', 'new', 'edit', 'planned') else report.status[:1]
    facts = [report.module] if report.module else []
    if report.included_by:
        facts.append(f'include_str! in {", ".join(report.included_by)}')
    if report.owner:
        facts.append(f'owner {report.owner["name"]}')
    elif report.unregistered:
        facts.append('UNREGISTERED')
    if report.headroom is not None and report.headroom <= WARN_HEADROOM:
        facts.append(f'headroom {report.headroom}')
    if report.formal:
        facts.append(f'formal {len(report.formal)}')
    if report.test_only and report.path.endswith('.rs'):
        facts.append('test code')
    if report.gate_input:
        facts.append('gate input')
    arrow = f'{report.previous} -> ' if report.previous else ''
    return f'  {status:<7} {arrow}{report.path}' + (f'  [{"; ".join(facts)}]' if facts else '')


def summary_lines(impact, verbose):
    limit = 1000 if verbose else 8
    plan, formal_result, files = impact.plan, impact.formal, impact.files
    out = []

    def add(label, value):
        if value:
            out.append(f'{label:<14}{value}')

    for index, command in enumerate(impact.tests):
        add('test (dev)' if index == 0 else '', command)
    if not impact.tests and any(f.path.endswith('.rs') for f in files):
        add('test (dev)', 'no module test reaches these files; see --verbose for DST tests naming their items')
    add('evidence', impact.release and f'{impact.release}   [gate profile, once before commit]')
    for index, check in enumerate(impact.checks):
        add('checks' if index == 0 else '', check)
    owners = plan.get('selected_mutation_owners', [])
    unregistered = plan.get('unregistered_mutation_source_files', [])
    mutation = []
    if owners:
        mutation.append('CI runs owner' + ('s ' if len(owners) > 1 else ' ') + brief(owners, limit))
    if unregistered:
        mutation.append(f'REGISTER FIRST: {brief(unregistered, limit)} ({REGISTER})')
    if plan.get('deleted_critical_files'):
        mutation.append(f'deleted critical: {brief(plan["deleted_critical_files"], limit)}')
    add('mutation', '; '.join(mutation) or 'no mutation leg')
    legs = [name for name in ('mutants', 'miri', 'properties_fuzz') if plan.get(name)]
    local = [LOCAL_LEGS[name] for name in legs]
    add('CI legs', ' '.join(legs) + f'   [locally: {"; ".join(local)}]' if legs
        else 'compiler + clippy + quality_ tests only')
    if formal_result:
        add('formal CI', formal_line(formal_result, limit))
        for index, line in enumerate(receipt_lines(formal_result, limit)):
            add('receipts' if index == 0 else '', line)
    tight = [f'{f.path} {f.headroom} lines left ({f.lines}/{min(x for x in (f.ceiling, f.arch_ceiling) if x)})'
             + (f', fn {", ".join(f.tight_functions)}' if f.tight_functions else '')
             for f in files if f.headroom is not None and (f.headroom <= WARN_HEADROOM or f.tight_functions)]
    for index, line in enumerate(tight):
        add('headroom' if index == 0 else '', line)
    harness = [f'{f.path} ({", ".join(f.harness)})' for f in files if f.harness]
    if harness:
        add('by-path', f'{brief(harness, limit)} compiled into a harness crate: {GROWTH_HINT}')
    scopes = [f'{f.path}({len(f.exception_scopes)})' for f in files if f.exception_scopes]
    if scopes:
        add('exceptions', f'{brief(scopes, limit)} reasoned expect/allow scopes: growth inside one '
                          'needs an owner-approved exception-growth row (propose, never add)')
    for index, line in enumerate(pin_lines(files, limit)):
        add('pinned' if index == 0 else '', line)
    immutable = [f'{f.path}: {f.immutable}' for f in files if f.immutable]
    for index, line in enumerate(immutable):
        add('immutable' if index == 0 else '', line)
    hard = [f.path for f in files if f.hard_owner]
    add('hard owner', hard and f'{brief(hard, limit)}: no crate::http/crate::product imports, '
                               'no AppState/axum/HeaderMap/Response')
    edge = [f.path for f in files if f.edge]
    add('edge', edge and f'{brief(edge, limit)}: {EDGE_HINT}')
    for index, note in enumerate(impact.notes):
        add('note' if index == 0 else '', note)
    return out


def formal_line(result, limit):
    if result['everything']:
        return f'EVERYTHING: all {result["total"]} obligations ({", ".join(result["everything"])} changed)' \
               f', ~{result["ci_wall_minutes"]:.0f} min wall on {result["shards"]} shards (local receipt timings; CI ~0.6x for TLC)'
    if not result['selected']:
        return 'nothing selected'
    return (f'{len(result["selected"])} obligation(s): {brief(result["selected"], limit)}'
            f', ~{result["ci_wall_minutes"]:.0f} min wall on {result["shards"]} shards (local receipt timings; CI ~0.6x for TLC)')


def receipt_lines(result, limit):
    lines = []
    predicted, now = result['stale_from_paths'], result['stale_now']
    if predicted:
        lines.append(f'stale from these paths: {brief(predicted, limit)} '
                     f'({minutes(result["stale_from_paths_minutes"])} serial)')
    if result['unchanged_but_named']:
        lines.append('also stale: every obligation whose manifest entry, ASM text or [formal]/[slatedb] pin '
                     'you change')
    if now:
        inputs = f'; inputs: {", ".join(result["stale_now_inputs"])}' if result['stale_now_inputs'] else ''
        lines.append(f'stale now: {len(now)} ({minutes(result["stale_now_minutes"])} serial{inputs}) '
                     f'-> {RERECORD}')
    elif predicted:
        lines.append(f'after the edit: {RERECORD}')
    if result['problems']:
        lines.append(f'INVALID receipts/manifest: {len(result["problems"])} '
                     '-> python3 scripts/quality/formal.py check')
    return lines


def pin_lines(files, limit):
    lines = []
    for f in files:
        pins = f.pins
        if 'dst-inventory' in pins:
            entry = pins['dst-inventory']
            drift = ', '.join(f'{k} {brief(v, 4) if isinstance(v, list) else v}' for k, v in entry.items()
                              if k != 'tests')
            action = ('after review: python3 scripts/test-inventory.py --write' if drift
                      else 'a body/attribute edit needs a reviewed test-inventory --write')
            lines.append(f'{f.path}: DST inventory {entry["tests"]} tests' + (f' [{drift}]' if drift else '')
                         + f' -> {action}')
        if 'review-mechanisms' in pins:
            entry = pins['review-mechanisms']
            changed = f' [CHANGED: {brief(entry["changed"], 4)}]' if entry.get('changed') else ''
            lines.append(f'{f.path}: {entry["pins"]} function(s) pinned by docs/refactor/review-mechanisms.json'
                         f'{changed} -> never edit without the owner')
        if 'scenario-map' in pins and (pins['scenario-map'].get('missing') or len(lines) < limit):
            entry = pins['scenario-map']
            missing = f' [MISSING: {brief(entry["missing"], 4)}]' if entry.get('missing') else ''
            lines.append(f'{f.path}: {entry["mapped tests"]} scenario-mapped test(s){missing} '
                         '-> renames/moves update docs/refactor/test-scenario-map.json')
        if 'mt-audit' in pins:
            entry = pins['mt-audit']
            gone = f' [{len(entry["gone"])} gone]' if entry.get('gone') else ''
            lines.append(f'{f.path}: {entry["fingerprints"]} scripts/mt-audit-baseline.txt fingerprint(s){gone}'
                         ' -> edit/move: bash scripts/multitenancy-audit.sh, --regen in the reviewed commit')
    return lines


def detail_lines(report):
    out = [f'-- {report.path}  [{report.status}]' + (f' from {report.previous}' if report.previous else '')]

    def add(label, value):
        if value not in (None, '', [], {}):
            out.append(f'   {label}: {value}')

    add('module', '; '.join(report.modules))
    add('doc', report.doc)
    add('gate input', report.gate_input)
    add('included by', ', '.join(report.included_by))
    if report.owner:
        add('mutation owner', f'{report.owner["name"]} ({report.owner["package"]} {report.owner["target"]}; '
                              f'filters {" ".join(report.owner["filters"]) or "-"})')
    elif report.critical:
        add('mutation owner', 'none (critical prefix: CI refuses a production edit until registered)'
            if not report.test_only else 'none needed (whole-file test code)')
    add('test filters', ', '.join(dict.fromkeys(f'{" ".join(s)} {v or "(all)"}' for s, v in report.filters)))
    for key, value in report.details.items():
        add(key, value if not isinstance(value, list) else ', '.join(value))
    add('formal', ' '.join(report.formal))
    if report.lines is not None:
        add('lines', f'{report.lines}; source-gate ceiling {report.ceiling}'
                     + (f'; architecture ceiling {report.arch_ceiling}' if report.arch_ceiling else '')
                     + f'; headroom {report.headroom}')
    add('tight functions', ', '.join(report.tight_functions))
    add('harness crates', ', '.join(report.harness))
    add('exception scopes', '; '.join(report.exception_scopes))
    for name, value in report.ledgers.items():
        add(name, value)
    for name, value in report.pins.items():
        add(name, value)
    add('immutable', report.immutable)
    add('checks', '; '.join(report.checks))
    return out


def render_text(impact, verbose=False):
    head = {'paths': 'explicit paths (hypothetical production edits)',
            'diff': 'working tree incl. uncommitted and untracked',
            'commits': 'those commits + working tree'}[impact.mode]
    lines = [f'impact: {len(impact.files)} file(s), {head}; base {impact.base[:10]} ({impact.base_label})']
    shown = impact.files if verbose else impact.files[:12]
    lines += [file_line(f) for f in shown]
    if len(shown) < len(impact.files):
        lines.append(f'  ... +{len(impact.files) - len(shown)} more (--verbose)')
    if verbose:
        for report in impact.files:
            lines += detail_lines(report)
    lines.append('== SUMMARY')
    lines += summary_lines(impact, verbose)
    lines.append(f'({impact.seconds:.1f} s; -v per-file detail, --json machine output)')
    return '\n'.join(lines)


def render_json(impact):
    return json.dumps(asdict(impact), indent=2, sort_keys=True, default=list)


def codemap(tree=None):
    """Top-level library modules by size, with their first doc sentence."""
    tree = tree or module_tree()
    rows = defaultdict(lambda: {'lines': 0, 'files': 0, 'test_lines': 0, 'doc': ''})
    others = defaultdict(lambda: [0, 0])
    for rel, placements in sorted(tree.placements.items()):
        text = tree.text(rel) or ''
        count = len(text.splitlines())
        lib = next((p for p in placements if p.crate == 'streams-slate' and p.target == 'lib'), None)
        if lib is None or not lib.modpath:
            first = placements[0]
            if not (first.crate == 'streams-slate' and first.target == 'lib'):
                others[f'{first.crate} {first.target}'][0] += count
                others[f'{first.crate} {first.target}'][1] += 1
            continue
        row = rows[lib.modpath[0]]
        row['lines'] += count
        row['files'] += 1
        if lib.test_only or whole_test(rel, text) or lib.modpath[0] == 'dst':
            row['test_lines'] += count
        if len(lib.modpath) == 1:
            row['doc'] = first_sentence(text, 120)
    lines = ['| module | lines | files | test lines | purpose (first doc sentence) |', '|---|---:|---:|---:|---|']
    for name, row in sorted(rows.items(), key=lambda item: -item[1]['lines']):
        lines.append(f'| `{name}` | {row["lines"]} | {row["files"]} | {row["test_lines"]} | {row["doc"]} |')
    lines.append('')
    lines.append('Other targets (files not in the library): ' + ', '.join(
        f'{name} {n} lines/{k} files' for name, (n, k) in sorted(others.items(), key=lambda i: -i[1][0])))
    return '\n'.join(lines)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('paths', nargs='*', help='explicit files or directories (may not exist yet)')
    parser.add_argument('--base', help='merge-base target (default ${QUALITY_BASE_REF:-origin/slate})')
    parser.add_argument('--commits', type=int, default=0, help='compare with HEAD~N (N commits + working tree)')
    parser.add_argument('-v', '--verbose', action='store_true', help='per-file detail and untruncated lists')
    parser.add_argument('--json', action='store_true', help='machine-readable output')
    parser.add_argument('--codemap', action='store_true', help='print the top-level module map and exit')
    parser.add_argument('--pin-hash', metavar='FILE::FN',
                        help='print the sha256 docs/refactor/review-mechanisms.json pins for a Rust test '
                             '(or support function) and exit; the same hash review-evidence.py checks')
    args = parser.parse_args(argv)
    if args.commits < 0:
        parser.error('--commits must be positive')
    if args.commits and args.paths:
        parser.error('--commits and explicit paths are exclusive')
    try:
        if args.codemap:
            print(codemap())
            return 0
        if args.pin_hash:
            print(pin_hash(args.pin_hash))
            return 0
        impact = analyze(args.paths, args.base, args.commits, args.verbose or args.json)
    except (ImpactError, subprocess.CalledProcessError) as error:
        print(f'impact.py: {error}', file=sys.stderr)
        return 2
    if not impact.files:
        print(f'impact: no changes against {impact.base[:10]} ({impact.base_label}); '
              'pass paths to ask about a planned edit')
        return 0
    print(render_json(impact) if args.json else render_text(impact, args.verbose))
    return 0


if __name__ == '__main__':
    sys.exit(main())
