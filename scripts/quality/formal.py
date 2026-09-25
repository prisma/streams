#!/usr/bin/env python3
"""Run the implemented formal-verification obligations and reconcile what ran.

`verification/manifest.json` lists implemented obligations only; the whole plan
stays in docs/PRISMA-STREAMS-FORMAL-VERIFICATION-ROADMAP.md. Every check names
a role and an expected verdict, and nothing is green unless each selected check
ran to completion with exactly that verdict:

- baseline: the unmodified model or harness passes. TLC must finish its whole
  search with no error and no states left; Kani must hold every property and
  satisfy every cover.
- negative-control: a deliberately broken variant fails on the named property.
  A TLC control is its own model variant; a Kani control is a source patch
  applied to a scratch copy of the tree, never a production switch.
- witness: the unmodified TLC model reaches the named behaviour, reported as
  its `Witness_*` invariant being violated.
- known-defect: the unmodified TLC model still reproduces a confirmed, open
  production defect, reported as its named property being violated. Only an
  obligation whose status is `counterexample` may carry one; when the defect
  is fixed the check becomes a baseline.

A missing harness, zero discovery, a parse or configuration error, a timeout,
an unwinding failure, an unsatisfied cover, or a control that fails for any
reason other than its named property is never a pass.

A receipt is evidence only while it is structurally valid (its obligation's
exact check set, each with the expected verdict on the named property) and
fresh (recorded for the current input digest). `check` fails on an invalid or
missing receipt and reports a stale one; `check --fresh` fails on both.
"""
import argparse
from collections import Counter
import contextlib
from dataclasses import dataclass, field
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / 'verification/manifest.json'
RECEIPTS = ROOT / 'verification/receipts'
ASSUMPTIONS = ROOT / 'verification/assumptions.md'
TOOLS = ROOT / 'target/quality-tools'
STATUSES = {'implemented-unchecked', 'pass-with-recorded-scope', 'counterexample',
            'incomplete', 'unsupported'}
ROLES = {'baseline', 'negative-control', 'witness', 'known-defect'}
# Statuses that claim a result: each needs a valid receipt. The other three
# claim nothing, so a receipt is optional, but one that exists must be valid.
CLAIMS = {'pass-with-recorded-scope', 'counterexample'}
# Schema 1 receipts predate the input map, the log hashes and the assumption
# and dependency digest: they still validate structurally but are always stale.
RECEIPT_SCHEMA = 2
RECEIPT_SCHEMAS = {1, RECEIPT_SCHEMA}
# Files whose change can alter every verdict: this driver, the tool pins and
# the locked dependencies. The TLA+ models abstract the pinned SlateDB and
# object_store durability, fencing and checkpoint contracts, and the Kani
# proofs compile against them, so both digest the lockfile and the Cargo
# manifest; Kani also digests its build inputs. Selection treats any manifest
# or pin edit as affecting every obligation; a receipt digests only the
# obligation's own manifest entry, its assumption-ledger entries and the
# [formal] and [slatedb] pins, so adding one obligation does not stale every
# other receipt.
DRIVER = 'scripts/quality/formal.py'
SELECTION_INPUTS = ('verification/manifest.json', 'quality-tools.toml', DRIVER)
DEPENDENCY_INPUTS = ('Cargo.lock', 'Cargo.toml')
KANI_INPUTS = ('build.rs', 'rust-toolchain.toml')
TLC_DONE = 'Model checking completed. No error has been found.'
TLC_STATES = re.compile(r'([\d,]+) states generated, ([\d,]+) distinct states found, '
                        r'([\d,]+) states left on queue')
TLC_DEPTH = re.compile(r'The depth of the complete state graph search is (\d+)')
TLC_VIOLATION = re.compile(r'^Error: (?:Invariant|Action property) (\S+) is violated',
                           re.MULTILINE)
KANI_HARNESS = re.compile(r'^Checking harness (\S+)\.\.\.$', re.MULTILINE)
KANI_COVERS = re.compile(r'\*\* (\d+) of (\d+) cover properties satisfied')
KANI_TOTAL = re.compile(r'Complete - (\d+) successfully verified harnesses, (\d+) failures, '
                        r'(\d+) total')
KANI_CHECK = re.compile(r'^Check \d+: .*\n\t - Status: (\w+)\n\t - Description: "(.*)"$',
                        re.MULTILINE)


def sha256(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def load(path=MANIFEST):
    return json.loads(Path(path).read_text())


def pins():
    return tomllib.loads((ROOT / 'quality-tools.toml').read_text())['formal']


def pinned():
    """The pins a verdict rests on: the verifiers and the SlateDB source revision."""
    config = tomllib.loads((ROOT / 'quality-tools.toml').read_text())
    return {'formal': config['formal'], 'slatedb': config.get('slatedb')}


def canonical_sha256(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


# --------------------------------------------------------------------------
# Static validation: the manifest names real, discoverable, attributable checks.
# --------------------------------------------------------------------------

def module_path(source):
    """`src/a/b.rs` -> `a::b`; `src/a/mod.rs` -> `a`; lib/main roots are ''."""
    parts = Path(source).with_suffix('').parts
    if parts[:1] != ('src',):
        raise ValueError(f'Kani harnesses live under src/: {source}')
    parts = list(parts[1:])
    if parts[-1] == 'mod':
        parts.pop()
    if parts in (['lib'], ['main']):
        return ''
    return '::'.join(parts)


def declared_harnesses(source):
    text = (ROOT / source).read_text()
    names = re.findall(r'#\[kani::proof\]\s*(?:#\[[^\]]*\]\s*)*fn\s+(\w+)', text)
    prefix = module_path(source)
    return {f'{prefix}::{name}' if prefix else name for name in names}


TLC_KEYWORDS = {'SPECIFICATION', 'INIT', 'NEXT', 'INVARIANT', 'INVARIANTS', 'PROPERTY',
                'PROPERTIES', 'CONSTANT', 'CONSTANTS', 'SYMMETRY', 'VIEW', 'CONSTRAINT',
                'CONSTRAINTS', 'ACTION_CONSTRAINT', 'ACTION_CONSTRAINTS', 'CHECK_DEADLOCK',
                'POSTCONDITION', 'ALIAS'}


def tla_config_properties(config):
    """Names under INVARIANT(S)/PROPERTY(IES) in a TLC configuration."""
    names, section = set(), None
    for raw in (ROOT / config).read_text().splitlines():
        line = raw.split('\\*')[0].strip()
        if not line:
            continue
        head, _, rest = line.partition(' ')
        if head in TLC_KEYWORDS:
            section = head if head.startswith(('INVARIANT', 'PROPERT')) else None
            if section:
                names.update(rest.split())
        elif section:
            names.update(line.split())
    return names


def ledger_entries(text):
    """`### ASM-...` entries in ledger order: (ID, exact text from its heading
    up to the next heading of level 1 to 3). Level-4 subheadings stay inside."""
    entries, current = [], None
    for line in text.splitlines(keepends=True):
        if re.match(r'#{1,3} ', line):
            match = re.match(r'### (ASM-[A-Z0-9-]+)', line)
            current = [match[1], ''] if match else None
            if current:
                entries.append(current)
        if current:
            current[1] += line
    return [tuple(entry) for entry in entries]


def assumption_entries(text=None):
    if text is None:
        text = ASSUMPTIONS.read_text() if ASSUMPTIONS.exists() else ''
    return dict(ledger_entries(text))


def ledger_path():
    return ASSUMPTIONS.relative_to(ROOT).as_posix()


def check_files(obligation):
    files = set(obligation.get('source_paths', [])) | set(obligation.get('verification_paths', []))
    for check in obligation.get('checks', []):
        for key in ('spec', 'config', 'patch'):
            if check.get(key):
                files.add(check[key])
    return files


def validate(manifest):
    problems = []
    if manifest.get('schema') != 1:
        problems.append('manifest schema must be 1')
    obligations = manifest.get('obligations')
    if not isinstance(obligations, list):
        return problems + ['manifest obligations must be a list']
    ledger = ledger_entries(ASSUMPTIONS.read_text()) if ASSUMPTIONS.exists() else []
    known_assumptions = {asm for asm, _ in ledger}
    for asm in sorted(asm for asm, count in Counter(a for a, _ in ledger).items() if count > 1):
        problems.append(f'{ledger_path()}: {asm} has more than one entry')
    seen_ids, seen_checks = set(), set()
    for obligation in obligations:
        oid = obligation.get('id', '<missing id>')
        where = f'{oid}:'
        if oid in seen_ids:
            problems.append(f'{where} duplicate obligation id')
        seen_ids.add(oid)
        for key in ('id', 'kind', 'title', 'status', 'owner', 'source_paths',
                    'verification_paths', 'requirements', 'input_scope', 'checks'):
            if key not in obligation:
                problems.append(f'{where} missing field {key}')
        kind = obligation.get('kind')
        if kind not in ('tla', 'kani'):
            problems.append(f'{where} unknown kind {kind!r}')
            continue
        if obligation.get('status') not in STATUSES:
            problems.append(f'{where} unknown status {obligation.get("status")!r}')
        for path in sorted(check_files(obligation)):
            if not (ROOT / path).is_file():
                problems.append(f'{where} missing file {path}')
        for assumption in obligation.get('assumptions', []):
            if assumption not in known_assumptions:
                problems.append(f'{where} assumption {assumption} is not in {ASSUMPTIONS.relative_to(ROOT)}')
        harnesses = set()
        if kind == 'kani':
            for path in obligation.get('verification_paths', []):
                if (ROOT / path).is_file():
                    harnesses |= declared_harnesses(path)
        roles = set()
        baselines = {}
        for check in obligation.get('checks', []):
            cid = check.get('id', '<missing check id>')
            if cid in seen_checks:
                problems.append(f'{cid}: duplicate check id')
            seen_checks.add(cid)
            role = check.get('role')
            roles.add(role)
            if role not in ROLES:
                problems.append(f'{cid}: unknown role {role!r}')
            expect = check.get('expect', {})
            if kind == 'tla':
                problems.extend(validate_tla_check(cid, role, check, expect))
                if role == 'baseline' and check.get('spec') and check.get('config'):
                    baselines[(check['spec'], check['config'])] = cid
            else:
                problems.extend(validate_kani_check(cid, role, check, expect, harnesses))
        for check in obligation.get('checks', []):
            if check.get('role') == 'negative-control' and kind == 'tla' \
                    and (check.get('spec'), check.get('config')) in baselines:
                problems.append(f'{check.get("id")}: a negative control reuses a baseline model unchanged')
        if obligation.get('status') == 'pass-with-recorded-scope':
            for role in ('baseline', 'negative-control'):
                if role not in roles:
                    problems.append(f'{where} a passing obligation needs a {role} check')
        if 'known-defect' in roles and obligation.get('status') != 'counterexample':
            problems.append(f'{where} an open known defect makes the status counterexample')
        if obligation.get('status') == 'counterexample' and 'known-defect' not in roles:
            problems.append(f'{where} a counterexample needs a known-defect check that reproduces it')
    return problems


def validate_tla_check(cid, role, check, expect):
    problems = []
    for key in ('spec', 'config'):
        if not check.get(key):
            problems.append(f'{cid}: TLA check needs {key}')
    if problems or not (ROOT / check['config']).is_file():
        return problems
    wanted = 'pass' if role == 'baseline' else 'violation'
    if expect.get('result') != wanted:
        problems.append(f'{cid}: a {role} must expect {wanted}')
    declared = tla_config_properties(check['config'])
    if not declared:
        problems.append(f'{cid}: {check["config"]} checks no invariant or property')
    if wanted == 'violation':
        prop = expect.get('property')
        if prop not in declared:
            problems.append(f'{cid}: expected property {prop!r} is not checked by {check["config"]}')
        elif len(declared) != 1:
            problems.append(f'{cid}: a {role} config must check only its target property')
        if role == 'witness' and not str(prop).startswith('Witness_'):
            problems.append(f'{cid}: witness properties are named Witness_*')
    return problems


def validate_kani_check(cid, role, check, expect, harnesses):
    problems = []
    harness = check.get('harness')
    if harness not in harnesses:
        problems.append(f'{cid}: harness {harness!r} is not declared in the obligation\'s verification_paths')
    if role in ('witness', 'known-defect'):
        problems.append(f'{cid}: the {role} role is for TLA models; a Kani counterexample becomes a regression')
    wanted = 'pass' if role == 'baseline' else 'fail'
    if expect.get('result') != wanted:
        problems.append(f'{cid}: a {role} must expect {wanted}')
    if role == 'negative-control':
        if not check.get('patch'):
            problems.append(f'{cid}: a Kani negative control needs a source patch')
        if not expect.get('property'):
            problems.append(f'{cid}: a negative control names the assertion it must break')
    elif check.get('patch'):
        problems.append(f'{cid}: only a negative control may patch the source')
    return problems


# --------------------------------------------------------------------------
# Selection: which implemented obligations a change can affect.
# --------------------------------------------------------------------------

def digested_files(obligation):
    files = check_files(obligation) | {DRIVER} | set(DEPENDENCY_INPUTS)
    if obligation['kind'] == 'kani':
        files |= set(KANI_INPUTS)
    return files


def select(manifest, changed, previous_ledger=None):
    """Obligations whose inputs the changed paths can affect. A ledger change
    selects the obligations that name an entry whose text differs from
    `previous_ledger` (ID -> text at the base); without it, every obligation
    that names any assumption."""
    changed = set(changed)
    everything = bool(changed & set(SELECTION_INPUTS))
    moved = None
    if ledger_path() in changed and previous_ledger is not None:
        current = assumption_entries()
        moved = {asm for asm in set(current) | set(previous_ledger)
                 if current.get(asm) != previous_ledger.get(asm)}
    selected = []
    for obligation in manifest['obligations']:
        named = set(obligation.get('assumptions', []))
        ledger = ledger_path() in changed and bool(named) and (moved is None or bool(named & moved))
        if everything or ledger or digested_files(obligation) & changed:
            selected.append(obligation['id'])
    return sorted(selected)


def changed_since(base):
    output = subprocess.check_output(['git', 'diff', '--name-only', '-z', base, '--'], cwd=ROOT)
    untracked = subprocess.check_output(['git', 'ls-files', '-z', '--others', '--exclude-standard'],
                                        cwd=ROOT)
    return {p.decode() for p in (output + untracked).split(b'\0') if p}


def ledger_at(base):
    """The assumption entries at `base`; none when the ledger did not exist there."""
    shown = subprocess.run(['git', 'show', f'{base}:{ledger_path()}'], cwd=ROOT,
                           capture_output=True, text=True)
    return assumption_entries(shown.stdout if shown.returncode == 0 else '')


def selection(manifest, base):
    return select(manifest, changed_since(base), ledger_at(base))


def input_snapshot(obligation):
    """Everything a verdict of this obligation rests on, hashed per input."""
    entries = assumption_entries()
    return {
        'files': {path: sha256(ROOT / path) if (ROOT / path).is_file() else '<missing>'
                  for path in sorted(digested_files(obligation))},
        'assumptions': {asm: hashlib.sha256(entries[asm].encode()).hexdigest()
                        if asm in entries else '<missing>'
                        for asm in obligation.get('assumptions', [])},
        'manifest_entry': canonical_sha256(obligation),
        'pins': pinned(),
    }


def input_digest(obligation):
    return canonical_sha256(input_snapshot(obligation))


def changed_inputs(before, after):
    """The inputs that differ between two snapshots, named for a reader."""
    changed = []
    for section in sorted(set(before) | set(after)):
        old, new = before.get(section), after.get(section)
        if section in ('files', 'assumptions') and isinstance(old, dict) and isinstance(new, dict):
            changed += [f'{section}:{key}' for key in sorted(set(old) | set(new))
                        if old.get(key) != new.get(key)]
        elif old != new:
            changed.append(section)
    return changed


# --------------------------------------------------------------------------
# Execution and verdict parsing.
# --------------------------------------------------------------------------

@dataclass
class Result:
    check: str
    role: str
    expected: str
    verdict: str
    matched: bool
    seconds: float
    detail: dict = field(default_factory=dict)


def tlc_verdict(output, returncode, timed_out):
    """Classify one TLC run: pass, violation, deadlock, error or incomplete."""
    detail = {}
    if match := TLC_STATES.findall(output):
        generated, distinct, left = (int(v.replace(',', '')) for v in match[-1])
        detail.update(states_generated=generated, distinct_states=distinct, states_left=left)
    if match := TLC_DEPTH.search(output):
        detail['depth'] = int(match[1])
    if timed_out:
        return 'incomplete', detail
    if violation := TLC_VIOLATION.search(output):
        detail['property'] = violation[1]
        return 'violation', detail
    if 'Error: Temporal properties were violated.' in output:
        detail['property'] = '<temporal>'
        return 'violation', detail
    if 'Error: Deadlock reached.' in output:
        return 'deadlock', detail
    if returncode == 0 and TLC_DONE in output and detail.get('states_left') == 0:
        return 'pass', detail
    if returncode == 0 and TLC_DONE in output:
        return 'incomplete', detail
    return 'error', detail


def kani_verdict(output, returncode, timed_out, harness):
    """Classify one Kani harness run: pass, fail, error or incomplete."""
    detail = {}
    ran = KANI_HARNESS.findall(output)
    detail['harnesses_run'] = ran
    checks = KANI_CHECK.findall(output)
    failed = [description for status, description in checks if status == 'FAILURE']
    detail['failed_checks'] = failed
    if covers := KANI_COVERS.search(output):
        detail['covers_satisfied'], detail['covers'] = int(covers[1]), int(covers[2])
    if timed_out:
        return 'incomplete', detail
    total = KANI_TOTAL.search(output)
    if ran != [harness] or not total or int(total[3]) != 1:
        return 'error', detail
    if any('unwinding assertion' in description for description in failed):
        return 'incomplete', detail
    if 'VERIFICATION:- FAILED' in output and failed:
        return 'fail', detail
    if returncode == 0 and 'VERIFICATION:- SUCCESSFUL' in output and not failed:
        if detail.get('covers_satisfied', 0) != detail.get('covers', 0):
            return 'incomplete', detail
        return 'pass', detail
    return 'error', detail


def judge(role, expect, verdict, detail):
    if role == 'baseline':
        return verdict == 'pass'
    wanted = 'violation' if expect['result'] == 'violation' else 'fail'
    if verdict != wanted:
        return False
    prop = expect.get('property')
    if wanted == 'violation':
        return detail.get('property') in (prop, '<temporal>')
    return any(prop in description for description in detail.get('failed_checks', []))


def expected_string(expect):
    return expect['result'] + (f":{expect['property']}" if expect.get('property') else '')


def stop_group(process):
    """Kill the check's whole process group and wait until no member is left.

    The group is signalled only while its leader is unreaped, so its ID
    cannot yet belong to anyone else."""
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
    process.wait()
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            os.killpg(process.pid, 0)
        except ProcessLookupError:
            return
        time.sleep(0.05)


def run_process(command, cwd, timeout, env=None):
    """Run one check in its own process group. A timeout or a cancellation
    (any exception, SIGTERM included once main() maps it to one) kills the
    whole group, so no verifier child outlives the check."""
    started = time.monotonic()
    with subprocess.Popen(command, cwd=cwd, env=env, text=True, errors='replace',
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          start_new_session=True) as process:
        try:
            output, _ = process.communicate(timeout=timeout)
            return output, process.returncode, False, time.monotonic() - started
        except subprocess.TimeoutExpired:
            stop_group(process)
            output, _ = process.communicate()
            return output or '', None, True, time.monotonic() - started
        except BaseException:
            stop_group(process)
            raise


def tlc_scratch_prefix():
    """Every TLC run of this process names its metadir with this prefix; the
    self-test requires no such directory or process to survive a timeout or
    a cancellation."""
    return f'formal-tlc-{os.getpid()}-'


def tlc_command(check, metadir):
    """TLC extracts its standard modules into java.io.tmpdir, so each run gets
    its own inside its metadir: concurrent runs never share one."""
    jar = TOOLS / 'tla2tools.jar'
    command = ['java', '-XX:+UseParallelGC', f'-Xmx{check.get("heap", "3g")}',
               f'-Djava.io.tmpdir={Path(metadir) / "tmp"}', '-cp', str(jar),
               'tlc2.TLC', '-workers', str(check.get('workers', 2)), '-metadir', str(metadir),
               '-config', str(ROOT / check['config'])]
    if not check.get('deadlock_check', True):
        command.append('-deadlock')
    return command + [str(ROOT / check['spec'])]


def write_log(logs, check, output):
    log = logs / (check['id'].replace('/', '__') + '.log')
    log.write_text(output)
    return {'log': str(log), 'log_sha256': sha256(log)}


def run_tlc(check, logs):
    with tempfile.TemporaryDirectory(prefix=tlc_scratch_prefix()) as metadir:
        (Path(metadir) / 'tmp').mkdir()
        output, code, timed_out, seconds = run_process(
            tlc_command(check, metadir), cwd=(ROOT / check['spec']).parent,
            timeout=check.get('timeout_seconds', 1800))
    verdict, detail = tlc_verdict(output, code, timed_out)
    detail.update(write_log(logs, check, output))
    detail['inputs'] = {path: sha256(ROOT / path) for path in (check['spec'], check['config'])}
    return verdict, detail, seconds


def kani_command(check):
    return ['cargo', 'kani', '--lib', '--output-format', 'regular', '--exact', '--harness',
            check['harness']]


def scratch_tree(patch):
    """Copy the tracked and untracked-unignored tree and apply `patch` to it."""
    listed = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=ROOT)
    destination = Path(tempfile.mkdtemp(prefix='formal-control-'))
    for name in (p.decode() for p in listed.split(b'\0') if p):
        source = ROOT / name
        if source.is_file():
            target = destination / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
    subprocess.run(['git', 'apply', '--check', str(ROOT / patch)], cwd=destination, check=True,
                   capture_output=True)
    subprocess.run(['git', 'apply', str(ROOT / patch)], cwd=destination, check=True,
                   capture_output=True)
    return destination


def patched_files(patch):
    return sorted(set(re.findall(r'^\+\+\+ b/(\S+)$', (ROOT / patch).read_text(), re.MULTILINE)))


def run_kani(check, logs):
    env = dict(os.environ)
    env['PATH'] = f'{TOOLS / "bin"}{os.pathsep}{env.get("PATH", "")}'
    cwd, detail = ROOT, {}
    if check.get('patch'):
        env['CARGO_TARGET_DIR'] = str(ROOT / 'target/formal/controls')
        try:
            cwd = scratch_tree(check['patch'])
        except subprocess.CalledProcessError as error:
            return 'error', {'patch': f'does not apply: {error.stderr}'}, 0.0
        detail['mutated'] = {path: {'baseline': sha256(ROOT / path), 'control': sha256(cwd / path)}
                             for path in patched_files(check['patch'])}
        if not detail['mutated'] or any(v['baseline'] == v['control'] for v in detail['mutated'].values()):
            shutil.rmtree(cwd)
            return 'error', detail | {'patch': 'did not change the source'}, 0.0
    try:
        output, code, timed_out, seconds = run_process(
            kani_command(check), cwd=cwd, env=env, timeout=check.get('timeout_seconds', 3600))
    finally:
        if cwd != ROOT:
            shutil.rmtree(cwd)
    verdict, parsed = kani_verdict(output, code, timed_out, check['harness'])
    return verdict, detail | parsed | write_log(logs, check, output), seconds


def run_check(obligation, check, logs):
    runner = run_tlc if obligation['kind'] == 'tla' else run_kani
    verdict, detail, seconds = runner(check, logs)
    expect = check['expect']
    return Result(check['id'], check['role'], expected_string(expect), verdict,
                  judge(check['role'], expect, verdict, detail), round(seconds, 1), detail)


def tool_versions():
    host = subprocess.run(['rustc', '-vV'], text=True, capture_output=True).stdout
    versions = {'platform': next((line.removeprefix('host: ') for line in host.splitlines()
                                  if line.startswith('host: ')), 'unknown')}
    probes = {
        'java': (['java', '-version'], r'version "([^"]+)"'),
        'tlc': (['java', '-cp', str(TOOLS / 'tla2tools.jar'), 'tlc2.TLC', '-h'], r'Version (\S+)'),
        'kani': ([str(TOOLS / 'bin/cargo-kani'), '--version'], r'Kani Rust Verifier (\S+)'),
    }
    for name, (command, pattern) in probes.items():
        try:
            probe = subprocess.run(command, text=True, capture_output=True)
        except FileNotFoundError:
            continue
        output = probe.stdout + probe.stderr
        if match := re.search(pattern, output):
            versions[name] = match[1]
    if (TOOLS / 'tla2tools.jar').is_file():
        versions['tla2tools_sha256'] = sha256(TOOLS / 'tla2tools.jar')
    return versions


def tool_problems(kinds):
    problems = []
    expected = pins()
    versions = tool_versions()
    if 'tla' in kinds:
        if versions.get('tla2tools_sha256') != expected['tla2tools-sha256']:
            problems.append('tla2tools.jar missing or not the pinned build; run scripts/install-formal-tools.py')
        elif versions.get('tlc') != expected['tlc']:
            problems.append(f'TLC {versions.get("tlc")} is not the pinned {expected["tlc"]}')
    if 'kani' in kinds and versions.get('kani') != expected['kani']:
        problems.append(f'Kani {versions.get("kani")} is not the pinned {expected["kani"]}; '
                        'run scripts/install-formal-tools.py')
    return problems, versions


def git_identity():
    head = subprocess.run(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True,
                          capture_output=True).stdout.strip()
    dirty = subprocess.run(['git', 'status', '--porcelain'], cwd=ROOT, text=True,
                           capture_output=True).stdout.strip() != ''
    return head, dirty


def run(manifest, ids, roles, out, record):
    obligations = [o for o in manifest['obligations'] if not ids or o['id'] in ids]
    missing = set(ids or ()) - {o['id'] for o in obligations}
    if missing:
        print(f'FORMAL_FAIL: unknown obligation id(s): {", ".join(sorted(missing))}', file=sys.stderr)
        return 1
    if not obligations:
        print('FORMAL_FAIL: nothing selected; an empty selection is not a pass', file=sys.stderr)
        return 1
    problems, versions = tool_problems({o['kind'] for o in obligations})
    if problems:
        print('\n'.join(f'FORMAL_FAIL: {p}' for p in problems), file=sys.stderr)
        return 1
    logs = out / 'logs'
    logs.mkdir(parents=True, exist_ok=True)
    head, dirty = git_identity()
    receipt = {'schema': RECEIPT_SCHEMA, 'source_revision': head, 'dirty': dirty, 'tools': versions,
               'obligations': []}
    failed = False
    for obligation in obligations:
        selected = [c for c in obligation['checks'] if not roles or c['role'] in roles]
        if not selected:
            print(f'FORMAL_FAIL: {obligation["id"]}: no check matches roles {sorted(roles)}',
                  file=sys.stderr)
            failed = True
            continue
        # The digest is taken before the first check and must still hold after
        # every one: a receipt names only inputs that every check analysed.
        before, moved, results = input_snapshot(obligation), [], []
        for check in selected:
            result = run_check(obligation, check, logs)
            results.append(result)
            mark = 'ok' if result.matched else 'MISMATCH'
            print(f'{result.check}: expected {result.expected}, got {result.verdict} '
                  f'({result.seconds}s) ... {mark}', flush=True)
            failed |= not result.matched
            moved = changed_inputs(before, input_snapshot(obligation))
            if moved:
                print(f'FORMAL_FAIL: {obligation["id"]}: inputs changed while {check["id"]} ran '
                      f'({", ".join(moved)}); the remaining checks and the receipt are refused',
                      file=sys.stderr)
                failed = True
                break
        entry = {'id': obligation['id'], 'inputs_sha256': canonical_sha256(before), 'inputs': before,
                 'inputs_changed': moved,
                 'complete': not moved and len(results) == len(obligation['checks']),
                 'checks': [vars(r) for r in results]}
        receipt['obligations'].append(entry)
        if record and entry['complete'] and all(r.matched for r in results):
            write_receipt(obligation, entry, head, dirty, versions)
    (out / 'receipt.json').write_text(json.dumps(receipt, indent=2, sort_keys=True) + '\n')
    print(f'FORMAL_{"FAIL" if failed else "OK"}: {sum(len(o["checks"]) for o in receipt["obligations"])} '
          f'check(s) across {len(receipt["obligations"])} obligation(s); receipt {out / "receipt.json"}')
    return int(failed)


# What a receipt keeps of each check: enough to re-judge its verdict against
# the manifest without the logs, plus the retained log's hash.
RECEIPT_DETAIL = {
    'tla': ('property', 'states_generated', 'distinct_states', 'states_left', 'depth'),
    'kani': ('failed_checks', 'covers', 'covers_satisfied', 'mutated'),
}
# Detail a current-schema receipt must carry for its verdict to be re-judged.
RECEIPT_REQUIRED = {'tla': ('property',), 'kani': ('failed_checks',)}


def write_receipt(obligation, entry, head, dirty, versions):
    RECEIPTS.mkdir(parents=True, exist_ok=True)
    kind = obligation['kind']
    compact = {
        'id': obligation['id'], 'schema': RECEIPT_SCHEMA, 'complete': True,
        'inputs_sha256': entry['inputs_sha256'], 'inputs': entry['inputs'],
        'run_on': {'revision': head, 'dirty_tree': dirty}, 'tools': versions,
        'checks': [{key: check[key] for key in ('check', 'role', 'expected', 'verdict', 'seconds')}
                   | {'log_sha256': check['detail']['log_sha256']}
                   | {key: check['detail'].get(key) for key in RECEIPT_REQUIRED[kind]}
                   | {key: check['detail'][key] for key in RECEIPT_DETAIL[kind]
                      if key in check['detail']}
                   for check in entry['checks']],
    }
    (RECEIPTS / f'{obligation["id"]}.json').write_text(json.dumps(compact, indent=2, sort_keys=True) + '\n')


def receipt_check_problems(kind, check, recorded, schema):
    """Why one recorded check does not stand for the manifest's check."""
    cid, role, expect = check['id'], check['role'], check['expect']
    problems = []
    if recorded.get('role') != role:
        problems.append(f'{cid}: receipt records role {recorded.get("role")!r}, the manifest {role!r}')
    if recorded.get('expected') != expected_string(expect):
        problems.append(f'{cid}: receipt expected {recorded.get("expected")!r}, '
                        f'the manifest {expected_string(expect)!r}')
    if schema >= 2:
        for key in ('log_sha256',) + RECEIPT_REQUIRED[kind]:
            if key not in recorded:
                problems.append(f'{cid}: receipt lacks {key}')
        if not re.fullmatch(r'[0-9a-f]{64}', str(recorded.get('log_sha256'))):
            problems.append(f'{cid}: receipt log_sha256 is not a sha256')
    failed_checks = recorded.get('failed_checks', [])
    if not isinstance(failed_checks, list) or not all(isinstance(d, str) for d in failed_checks):
        return problems + [f'{cid}: receipt failed_checks is not a list of descriptions']
    if not judge(role, expect, recorded.get('verdict'), recorded):
        problems.append(f'{cid}: recorded verdict {recorded.get("verdict")!r} '
                        f'(property {recorded.get("property")!r}, failed {failed_checks!r}) '
                        f'does not meet {role} {expected_string(expect)!r}')
    if kind == 'kani' and role == 'baseline' and (
            failed_checks or recorded.get('covers_satisfied') != recorded.get('covers')):
        problems.append(f'{cid}: a passing harness has no failed check and every cover satisfied')
    if kind == 'tla' and role == 'baseline' and recorded.get('states_left', 0) != 0:
        problems.append(f'{cid}: a passing search leaves no state on the queue')
    if kind == 'kani' and role == 'negative-control':
        mutated = recorded.get('mutated')
        if not isinstance(mutated, dict) or not mutated or any(
                not isinstance(v, dict) or v.get('baseline') == v.get('control')
                for v in mutated.values()):
            problems.append(f'{cid}: a control records the source it mutated, changed')
    return problems


def receipt_problems(obligation, receipt):
    """Why a receipt is not evidence for its obligation at all (not staleness)."""
    oid = obligation['id']
    if not isinstance(receipt, dict):
        return [f'{oid}: receipt is not a JSON object']
    schema = receipt.get('schema')
    if type(schema) is not int or schema not in RECEIPT_SCHEMAS:  # `true == 1` is no schema
        return [f'{oid}: receipt schema {schema!r} is not one of {sorted(RECEIPT_SCHEMAS)}']
    problems = []
    if receipt.get('id') != oid:
        problems.append(f'{oid}: receipt names {receipt.get("id")!r}')
    if not re.fullmatch(r'[0-9a-f]{64}', str(receipt.get('inputs_sha256'))):
        problems.append(f'{oid}: receipt inputs_sha256 is not a sha256')
    if schema >= 2:
        # Schema 1 had no such field: its writer recorded complete runs only,
        # and the exact check set below is required of both.
        if receipt.get('complete') is not True:
            problems.append(f'{oid}: receipt is not of a complete run')
        if not isinstance(receipt.get('inputs'), dict) \
                or canonical_sha256(receipt['inputs']) != receipt.get('inputs_sha256'):
            problems.append(f'{oid}: receipt inputs_sha256 is not the digest of its recorded inputs')
    checks = receipt.get('checks')
    if not isinstance(checks, list) or not all(
            isinstance(c, dict) and isinstance(c.get('check'), str) for c in checks):
        return problems + [f'{oid}: receipt checks are not a list of named check objects']
    names = [c.get('check') for c in checks]
    wanted = {c['id']: c for c in obligation['checks']}
    for label, ids in (('duplicates', {n for n, count in Counter(names).items() if count > 1}),
                       ('omits', set(wanted) - set(names)),
                       ('has checks the manifest does not', set(names) - set(wanted))):
        if ids:
            problems.append(f'{oid}: receipt {label}: {", ".join(sorted(ids))}')
    reproduced = 0
    for recorded in checks:
        check = wanted.get(recorded.get('check'))
        if check is None:
            continue
        found = receipt_check_problems(obligation['kind'], check, recorded, schema)
        problems += found
        reproduced += check['role'] == 'known-defect' and not found
    if obligation['status'] == 'pass-with-recorded-scope' \
            and any(c.get('role') == 'known-defect' for c in checks):
        problems.append(f'{oid}: a passing obligation\'s receipt carries a known defect')
    if obligation['status'] == 'counterexample' and not reproduced:
        problems.append(f'{oid}: a counterexample\'s receipt reproduces no known defect')
    return problems


def receipt_staleness(obligation, receipt):
    """None when the receipt was recorded for the current inputs."""
    if receipt['schema'] != RECEIPT_SCHEMA:
        return f'receipt schema {receipt["schema"]} predates the current input digest'
    if receipt['inputs_sha256'] == input_digest(obligation):
        return None
    moved = changed_inputs(receipt['inputs'], input_snapshot(obligation))
    return f'inputs changed since its receipt ({", ".join(moved) or "digest"})'


def receipt_report(manifest):
    """(structural problems, stale receipts). Structural problems make a
    receipt no evidence at all; a stale one was valid for other inputs."""
    problems, stale = [], []
    ids = {o['id'] for o in manifest['obligations']}
    if RECEIPTS.is_dir():
        for path in sorted(RECEIPTS.glob('*.json')):
            if path.stem not in ids:
                problems.append(f'{path.name}: receipt for no implemented obligation')
    for obligation in manifest['obligations']:
        path = RECEIPTS / f'{obligation["id"]}.json'
        claims = obligation['status'] in CLAIMS
        if not path.is_file():
            if claims:
                problems.append(f'{obligation["id"]}: status {obligation["status"]} has no receipt')
            continue
        try:
            receipt = json.loads(path.read_text())
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            problems.append(f'{obligation["id"]}: receipt is not JSON ({error})')
            continue
        found = receipt_problems(obligation, receipt)
        problems += found
        if claims and not found and (reason := receipt_staleness(obligation, receipt)):
            stale.append(f'{obligation["id"]}: {reason}')
    return problems, stale


# --------------------------------------------------------------------------
# Self-test: the driver must reject bad runs with the real tools.
# --------------------------------------------------------------------------

FIXTURES = 'verification/fixtures'
SELF_TEST = [
    # (description, obligation kind, check, verdict the driver must report)
    ('a complete search passes', 'tla',
     {'id': 'fixture/baseline', 'role': 'baseline', 'spec': f'{FIXTURES}/Counter.tla',
      'config': f'{FIXTURES}/Counter.cfg', 'expect': {'result': 'pass'}}, 'pass'),
    ('a reachable witness is reported by name', 'tla',
     {'id': 'fixture/witness', 'role': 'witness', 'spec': f'{FIXTURES}/Counter.tla',
      'config': f'{FIXTURES}/CounterWitness.cfg',
      'expect': {'result': 'violation', 'property': 'Witness_ReachesTwo'}}, 'violation'),
    ('a configuration naming an undefined invariant is an error', 'tla',
     {'id': 'fixture/wrong-config', 'role': 'baseline', 'spec': f'{FIXTURES}/Counter.tla',
      'config': f'{FIXTURES}/CounterWrongConfig.cfg', 'expect': {'result': 'pass'}}, 'error'),
    ('an unfinished search is incomplete', 'tla',
     {'id': 'fixture/incomplete', 'role': 'baseline', 'spec': f'{FIXTURES}/Unbounded.tla',
      'config': f'{FIXTURES}/Unbounded.cfg', 'expect': {'result': 'pass'}, 'timeout_seconds': 5},
     'incomplete'),
    ('a stuck protocol is a deadlock, not a pass', 'tla',
     {'id': 'fixture/deadlock', 'role': 'baseline', 'spec': f'{FIXTURES}/Stuck.tla',
      'config': f'{FIXTURES}/Stuck.cfg', 'expect': {'result': 'pass'}}, 'deadlock'),
    ('a harness nothing declares is zero discovery', 'kani',
     {'id': 'fixture/zero-discovery', 'role': 'baseline',
      'harness': 'offsets::proofs::no_such_harness', 'expect': {'result': 'pass'}}, 'error'),
]


class Cancelled(BaseException):
    """The self-test's stand-in for an interrupted or terminated driver."""


def tlc_leftovers():
    """TLC processes or scratch directories of this driver process still alive."""
    prefix = tlc_scratch_prefix()
    listing = subprocess.run(['ps', '-A', '-o', 'pid=,command='], text=True,
                             capture_output=True, check=True).stdout
    processes = [f'process {line.strip()}' for line in listing.splitlines() if prefix in line]
    directories = [f'directory {path}' for path in Path(tempfile.gettempdir()).glob(prefix + '*')]
    return processes + sorted(directories)


def concurrent_tlc_problems(logs, copies=2):
    """Runs the passing and the witness fixture `copies` times each at once;
    every run must reach its own verdict (they once shared java.io.tmpdir)."""
    import threading
    baseline, witness = SELF_TEST[0][2], SELF_TEST[1][2]
    checks = [dict(check, id=f'{check["id"]}-concurrent-{n}')
              for n in range(copies) for check in (baseline, witness)]
    results = [None] * len(checks)

    def work(index):
        try:
            results[index] = run_check({'kind': 'tla'}, checks[index], logs)
        except Exception as error:  # a crashed worker is reported, never ignored
            results[index] = error

    threads = [threading.Thread(target=work, args=(i,)) for i in range(len(checks))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    return [f'{check["id"]}: concurrent run gave {getattr(result, "verdict", result)!r}'
            for check, result in zip(checks, results)
            if not isinstance(result, Result) or not result.matched]


def cancellation_problems(logs, after=3.0):
    """Cancels an endless TLC search mid-run; nothing of it may survive."""
    check = dict(SELF_TEST[3][2], id='fixture/cancelled', timeout_seconds=600)

    def cancel(signum, frame):
        raise Cancelled()

    previous = signal.signal(signal.SIGALRM, cancel)
    signal.setitimer(signal.ITIMER_REAL, after)
    try:
        run_check({'kind': 'tla'}, check, logs)
        return ['a cancelled TLC search ran to its end instead of stopping']
    except Cancelled:
        return tlc_leftovers()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def self_test(out):
    problems, _ = tool_problems({'tla', 'kani'})
    if problems:
        print('\n'.join(f'FORMAL_FAIL: {p}' for p in problems), file=sys.stderr)
        return 1
    logs = out / 'self-test'
    logs.mkdir(parents=True, exist_ok=True)
    failed = False
    for description, kind, check, wanted in SELF_TEST:
        result = run_check({'kind': kind}, check, logs)
        good = result.verdict == wanted and result.matched == (wanted in ('pass', 'violation'))
        failed |= not good
        print(f'{check["id"]}: {description}: driver said {result.verdict} '
              f'(accepted={result.matched}) ... {"ok" if good else "WRONG"}')
    for description, probe in (
            ('a timed-out search leaves no TLC process or scratch directory', tlc_leftovers),
            ('concurrent TLC runs each reach their own verdict', lambda: concurrent_tlc_problems(logs)),
            ('a cancelled search leaves no TLC process or scratch directory',
             lambda: cancellation_problems(logs))):
        found = probe()
        failed |= bool(found)
        print(f'fixture/isolation: {description} ... {"WRONG" if found else "ok"}')
        for problem in found:
            print(f'  {problem}')
    print(f'FORMAL_SELF_TEST_{"FAIL" if failed else "OK"}')
    return int(failed)


def terminate(signum, frame):
    """SIGTERM (a cancelled CI job) unwinds like an interrupt, so the running
    check's process group is killed and its scratch directories removed."""
    raise SystemExit(128 + signum)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest='command', required=True)
    check = sub.add_parser('check', help='validate the manifest without running tools')
    check.add_argument('--fresh', action='store_true',
                       help='also fail when a claimed result\'s receipt was recorded for other inputs')
    selector = sub.add_parser('select', help='list obligations a change can affect')
    selector.add_argument('--base', required=True)
    runner = sub.add_parser('run', help='run checks and reconcile expected against actual')
    runner.add_argument('--id', action='append', default=[])
    runner.add_argument('--role', action='append', default=[], choices=sorted(ROLES))
    runner.add_argument('--changed-from', help='run only obligations the diff from this revision affects')
    runner.add_argument('--out', type=Path, default=ROOT / 'target/formal')
    runner.add_argument('--record', action='store_true',
                        help='write verification/receipts/<ID>.json for fully matched obligations')
    tester = sub.add_parser('self-test', help='prove the driver rejects bad runs')
    tester.add_argument('--out', type=Path, default=ROOT / 'target/formal')
    args = parser.parse_args()
    manifest = load()
    if args.command == 'check':
        return check_command(manifest, args.fresh)
    if args.command == 'select':
        print('\n'.join(selection(manifest, args.base)))
        return 0
    signal.signal(signal.SIGTERM, terminate)
    if args.command == 'self-test':
        return self_test(args.out)
    problems = validate(manifest)
    if problems:
        print('\n'.join(f'FORMAL_FAIL: {p}' for p in problems), file=sys.stderr)
        return 1
    ids = set(args.id)
    if args.changed_from:
        affected = set(selection(manifest, args.changed_from))
        ids = (ids & affected) if ids else affected
        if not ids:
            print(f'FORMAL_OK: no implemented obligation depends on the diff from {args.changed_from}')
            return 0
    return run(manifest, ids, set(args.role), args.out, args.record)


def check_command(manifest, fresh):
    """Structural problems always fail; a stale receipt fails only with --fresh."""
    problems = validate(manifest)
    stale = []
    if not problems:  # receipts are judged against a valid manifest only
        receipt_errors, stale = receipt_report(manifest)
        problems += receipt_errors
    for problem in problems:
        print(f'FORMAL_FAIL: {problem}', file=sys.stderr)
    for entry in stale:
        print(f'FORMAL_{"FAIL" if fresh else "STALE"}: {entry}',
              file=sys.stderr if fresh else sys.stdout)
    failed = bool(problems) or (fresh and bool(stale))
    print(f'FORMAL_CHECK_{"FAIL" if failed else "OK"}: {len(manifest["obligations"])} '
          f'implemented obligation(s), {len(problems)} problem(s), {len(stale)} stale receipt(s)')
    return int(failed)


if __name__ == '__main__':
    sys.exit(main())
