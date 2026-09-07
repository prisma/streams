#!/usr/bin/env python3
"""Validate mechanism claims, unresolved dispositions and final-source receipts.

--check establishes an internally consistent source inventory, never execution
certification. --record-run executes a command in a clean checkout and records
its exact HEAD, configuration, toolchains, log hash and nonzero test counts.
Receipts must live outside the checkout to avoid self-referential source hashes.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

sys.dont_write_bytecode = True
ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / 'docs/refactor/review-mechanisms.json'
DISPOSITIONS = ROOT / 'docs/refactor/scenario-dispositions.json'
REQUIRED = {'R01', 'R09', 'R10', 'R13', 'R14', 'R15', 'R17', 'R18', 'R19', 'R20', 'R21', 'R22',
            'DUR-005', 'SEL-022', 'DUR-014'}
spec = importlib.util.spec_from_file_location('inventory', ROOT / 'scripts/test-inventory.py')
inventory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(inventory)


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def git(*args: str) -> str:
    return subprocess.check_output(['git', *args], cwd=ROOT, text=True).strip()


def historical_source(commit: str, path: str) -> tuple[str | None, str | None]:
    """Missing ancestor objects must fail the gate, with actionable provenance."""
    anchor = f'{commit}:{path}'
    result = subprocess.run(['git', 'show', anchor], cwd=ROOT, text=True, capture_output=True)
    if result.returncode:
        return None, (f'missing historical provenance: {anchor}; obtain the pinned ancestor '
                      'objects (CI actions/checkout fetch-depth: 0, or git fetch --unshallow). '
                      'Historical source verification was not performed; checks are not skipped.')
    return result.stdout, None


def validate_dispositions(scenarios: list, dispositions: list) -> list[str]:
    failures, by_id = [], {}
    for disposition in dispositions:
        sid = disposition['id']
        if sid in by_id:
            failures.append(f'duplicate disposition: {sid}')
        by_id[sid] = disposition
        for field in ('owner', 'mechanism', 'disposition', 'closure_evidence'):
            if not disposition.get(field):
                failures.append(f'{sid}: disposition missing {field}')
        if disposition.get('state') not in ('pending', 'partial', 'local_mechanism'):
            failures.append(f'{sid}: unsupported disposition state')
    catalog_ids = {s['id'] for s in scenarios}
    for sid in by_id.keys() - catalog_ids:
        failures.append(f'disposition drops/renames catalogue obligation: {sid}')
    for scenario in scenarios:
        if (not scenario['mapped'] or scenario.get('coverage') in ('partial', 'external')) and scenario['id'] not in by_id:
            failures.append(f'{scenario["id"]}: incomplete scenario lacks disposition')
    return failures


def validate_clippy(original: set[str], current: set[str], proofs: list[dict]) -> list[str]:
    failures, justified = [], set()
    for proof in proofs:
        before, after = proof['original_fingerprint'], proof['current_fingerprint']
        valid = before in original and bool(proof.get('proof'))
        if proof['kind'] == 'relocation':
            valid &= before.rsplit(' :: ', 1)[0] == after.rsplit(' :: ', 1)[0]
        elif proof['kind'] == 'strict_subset':
            old_names, new_names = set(re.findall(r'`([^`]+)`', before)), set(re.findall(r'`([^`]+)`', after))
            valid &= bool(new_names) and new_names < old_names and before.rsplit(' :: ', 1)[-1] == after.rsplit(' :: ', 1)[-1]
        else:
            valid = False
        if valid:
            justified.add(after)
        else:
            failures.append(f'invalid original-baseline lint disposition: {after}')
    failures.extend(f'new clippy baseline debt lacks original proof: {entry}' for entry in sorted(current - original - justified))
    return failures


def fixture_change_failures(change: dict, before_source: str, after_source: str) -> list[str]:
    """A fixture edit can change preserved tests without changing their bodies."""
    failures = []
    if not all(change.get(field) for field in ('finding', 'reason', 'name', 'file')):
        failures.append('fixture change requires its finding, reason and source identity')
    for label, source in [('before', before_source), ('after', after_source)]:
        found = [function for function in inventory.functions(source, include_helpers=True) if function['name'] == change['name']]
        if len(found) != 1 or found[0]['function_sha256'] != change.get(label + '_sha256'):
            failures.append(f'{change["name"]}: {label} fixture body changed or missing')
    return failures


def source_anchor_failures(changes: list[dict], required: dict) -> list[str]:
    failures, recorded = [], set()
    for change in changes:
        identity = (change.get('file'), change.get('name'))
        if identity in recorded:
            failures.append('duplicate reviewed source provenance')
        recorded.add(identity)
        if identity not in required or change.get('before_commit') != required[identity]:
            failures.append('reviewed source baseline identity/commit mismatch')
    if required.keys() - recorded:
        failures.append('required reviewed source provenance missing')
    return failures


def check() -> list[str]:
    scenarios = json.loads((ROOT / 'docs/refactor/test-scenario-map.json').read_text())
    manifest = json.loads(MANIFEST.read_text())
    failures = validate_dispositions(scenarios, json.loads(DISPOSITIONS.read_text()))
    pinned = manifest['clippy_baseline']
    original = (ROOT / pinned['file']).read_bytes()
    if pinned.get('commit') != 'a7e2070f3b4346b3e54d552069ff91c56e900130' or sha(original) != pinned['sha256']:
        failures.append('clippy original-baseline content/commit mismatch')
    failures.extend(validate_clippy(set(original.decode().splitlines()),
                    set((ROOT / 'scripts/clippy-baseline-fingerprints.txt').read_text().splitlines()),
                    json.loads((ROOT / 'docs/refactor/clippy-review-dispositions.json').read_text())))

    required_fixtures = {
        ('src/dst/tests/fixture_http.rs', 'http_rig_cold_absorb'): 'b1864fffaca3f753a34129f95b8f5734cccd0a4a',
        ('src/dst/tests/fixture_http.rs', 'default'): 'd1131213250624de7dabf79ee3e0b9abce171b23',
        ('src/dst/tests/fixture_http.rs', 'http_rig_build'): 'd1131213250624de7dabf79ee3e0b9abce171b23',
        ('src/dst/tests/fixture_http.rs', 'rig_opener'): 'd1131213250624de7dabf79ee3e0b9abce171b23',
    }
    required_units = {
        (file, name): 'd1131213250624de7dabf79ee3e0b9abce171b23'
        for file, names in [
            ('src/ops.rs', ['cancelled_ops_append_restores_batch_order_and_retry_ids', 'cancelled_ops_batch_overflow_preserves_full_gap_magnitude']),
            ('src/audit.rs', ['cancelled_audit_append_restores_batch_order_and_retry_ids', 'cancelled_audit_batch_overflow_preserves_full_gap_magnitude']),
        ] for name in names
    }
    required_units[('src/fleet/repository/document_tests.rs', 'r09_fleet_document_deadlines_leave_cas_source_retryable')] = 'e273b3a94f6de769b361f8eeef3433b308c6eb07'
    required_units[('src/history/controller_tests.rs', 'active_absorber_cancel')] = '93f77ecfdb053850773bb698b3512e3777ce3354'
    required_units[('src/application/creation.rs', 'r05_cancelled_ttl_attempt_releases_only_its_owned_slot')] = '24cfdac0dfcf6618fd1ac51eae0ef70d458e29d1'
    required_units[('src/shard/transaction_tests.rs', 'r03a_mixed_transaction_preserves_every_row_reply_and_publication')] = '89884ab114b22929aa93707361e9e8d52c762bc2'
    for section, required_changes in [('fixture_changes', required_fixtures), ('source_adaptations', required_units)]:
        changes = manifest.get(section, [])
        failures.extend(source_anchor_failures(changes, required_changes))
        for change in changes:
            path = ROOT / change['file']
            if not path.is_file():
                failures.append(f'missing reviewed source: {change["file"]}')
                continue
            if change.get('before_commit') != required_changes.get((change.get('file'), change.get('name'))):
                continue
            original_source, missing = historical_source(change['before_commit'], change['file'])
            if missing:
                failures.append(missing)
                continue
            failures.extend(fixture_change_failures(change, original_source, path.read_text()))
    obligations = set()
    for entry in manifest['mechanisms']:
        obligations.update(entry['obligations'])
        for field in ('owner', 'mechanism', 'entered_proof', 'oracle', 'configuration', 'limitations'):
            if not entry.get(field):
                failures.append(f'{entry["id"]}: missing mechanism field {field}')
        # No source-availability checker may mint a pass for an unexecuted leg.
        if entry.get('execution') != 'requires_final_head_receipt':
            failures.append(f'{entry["id"]}: source inventory is not execution evidence')
        if not entry.get('tests'):
            failures.append(f'{entry["id"]}: no concrete mechanism tests')
        for test in entry.get('tests', []):
            path = ROOT / test['file']
            if not path.is_file():
                failures.append(f'missing mechanism source: {test["file"]}'); continue
            if path.suffix == '.rs':
                matches = [t for t in inventory.functions(path.read_text(), path) if t['name'] == test['name']]
                if len(matches) != 1 or matches[0]['function_sha256'] != test.get('sha256'):
                    failures.append(f'mechanism test changed or missing: {test["file"]}::{test["name"]}')
                elif any('ignore' in a for a in matches[0]['attributes']):
                    failures.append(f'mechanism test ignored: {test["name"]}')
            else:
                if sha(path.read_bytes()) != test.get('sha256') or test['name'] not in path.read_text():
                    failures.append(f'SDK mechanism script changed or test missing: {test["file"]}::{test["name"]}')
        for helper in entry.get('support_functions', []):
            path = ROOT / helper['file']
            functions = inventory.functions(path.read_text(), path, include_helpers=True) if path.is_file() else []
            found = [function for function in functions if function['name'] == helper['name']]
            if len(found) != 1 or found[0]['function_sha256'] != helper.get('sha256'):
                failures.append(f'mechanism support function changed or missing: {helper["file"]}::{helper["name"]}')
    for obligation in REQUIRED - obligations:
        failures.append(f'missing required mechanism: {obligation}')
    relocations = json.loads((ROOT / 'docs/refactor/review-unit-relocations.json').read_text())
    for moved in relocations:
        path = ROOT / moved['to_file']
        found = [t for t in inventory.functions(path.read_text(), path) if t['name'] == moved['name']]
        if len(found) != 1 or found[0]['function_sha256'] != moved['function_sha256']:
            failures.append(f'relocated unit obligation changed or missing: {moved["name"]}')
        elif any('ignore' in a for a in found[0]['attributes']):
            failures.append(f'relocated unit obligation ignored: {moved["name"]}')
    for external in manifest.get('external_legs', []):
        if external.get('state') != 'pending' or not all(external.get(k) for k in ('owner', 'required_evidence', 'reason')):
            failures.append('external legs require explicit pending state, owner and evidence')
    if not manifest.get('external_legs'):
        failures.append('external acceptance legs must remain explicit')
    return failures


def test_counts(log: str) -> dict:
    # Count successful concrete execution summaries, not a filter count or
    # the number of referenced symbols. Failed/zero-selected runs cannot pass.
    rust = re.findall(r'^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored;', log, re.M)
    node_pass = re.findall(r'^[#ℹ] pass (\d+)\s*$', log, re.M)
    node_fail = re.findall(r'^[#ℹ] fail (\d+)\s*$', log, re.M)
    return {'passed': sum(int(p) for state, p, f, i in rust if state == 'ok') + sum(map(int, node_pass)),
            'failed': sum(int(f) for state, p, f, i in rust) + sum(map(int, node_fail)),
            'ignored': sum(int(i) for state, p, f, i in rust),
            'bad_summary': any(state != 'ok' for state, *_ in rust)}


def receipt_failures(receipt: dict, head: str, tree: str, clean: bool, log: bytes,
                     expected_config: dict | None = None, toolchains: dict | None = None) -> list[str]:
    failures = []
    if not clean or not receipt.get('clean_checkout'):
        failures.append('receipt requires a clean source checkout')
    if receipt.get('head') != head or receipt.get('tree') != tree:
        failures.append('receipt source does not match current HEAD/tree')
    if receipt.get('log_sha256') != sha(log):
        failures.append('receipt log content hash mismatch')
    if not receipt.get('configuration') or (expected_config is not None and receipt.get('configuration') != expected_config):
        failures.append('receipt configuration missing or mismatched')
    if not receipt.get('toolchains') or (toolchains is not None and receipt.get('toolchains') != toolchains):
        failures.append('receipt toolchains missing or mismatched')
    counts = test_counts(log.decode(errors='replace'))
    if counts != receipt.get('tests') or counts['bad_summary'] or counts['failed']:
        failures.append('receipt test results failed or mismatched')
    if not isinstance(receipt.get('minimum_tests'), int) or receipt['minimum_tests'] < 1 or counts['passed'] < receipt['minimum_tests']:
        failures.append('receipt selected too few tests')
    if receipt.get('exit_code') != 0 or not receipt.get('command'):
        failures.append('receipt command failed or missing')
    return failures


def toolchain_versions(commands: list[list[str]]) -> dict:
    for command in commands:
        if (len(command) != 2 or command[1] != '--version'
                or Path(command[0]).name not in ('rustc', 'cargo', 'node', 'npm', 'bun', 'deno')):
            raise ValueError('toolchain receipts only permit known runtime --version probes')
    return {' '.join(c): subprocess.check_output(c, cwd=ROOT, text=True, stderr=subprocess.STDOUT).strip() for c in commands}


def self_test() -> None:
    scenario = [{'id': 'SDK-003', 'mapped': False}]
    disposition = [{'id': 'SDK-003', 'state': 'pending', 'owner': 'SDK', 'mechanism': 'bounded map',
                    'disposition': 'one million sparse keys required', 'closure_evidence': 'pending cardinality run'}]
    assert not validate_dispositions(scenario, disposition)
    assert validate_dispositions(scenario, [])
    assert validate_dispositions(scenario, [{**disposition[0], 'owner': ''}])
    assert validate_dispositions(scenario, disposition*2)
    log = b'test result: ok. 2 passed; 0 failed; 0 ignored; 3 filtered out; finished in 0.1s\n'
    r = {'clean_checkout': True, 'head': 'head', 'tree': 'tree', 'log_sha256': sha(log), 'configuration': {'profile': 'test'},
         'toolchains': {'rustc --version': 'rustc pinned'}, 'tests': test_counts(log.decode()), 'minimum_tests': 2,
         'exit_code': 0, 'command': ['cargo', 'test']}
    assert not receipt_failures(r, 'head', 'tree', True, log, r['configuration'], r['toolchains'])
    for key, value in [('head', 'old'), ('tree', 'old'), ('clean_checkout', False), ('log_sha256', 'wrong'),
                       ('configuration', {}), ('toolchains', {}), ('tests', {}), ('minimum_tests', 0),
                       ('minimum_tests', 3), ('exit_code', 1), ('command', [])]:
        assert receipt_failures({**r, key: value}, 'head', 'tree', True, log, r['configuration'], r['toolchains']), key
    assert receipt_failures(r, 'head', 'tree', False, log)
    assert receipt_failures(r, 'head', 'tree', True, log+b'tampered')
    assert test_counts('test result: ok. 0 passed; 0 failed; 0 ignored; 805 filtered out;') ['passed'] == 0
    assert test_counts('# pass 3\n# fail 1\n')['failed'] == 1
    try:
        toolchain_versions([['sh', '--version']])
        raise AssertionError('a receipt must not select arbitrary executables')
    except ValueError:
        pass
    old = 'methods `one` and `two` are never used :: src/old.rs'
    new = 'method `two` is never used :: src/old.rs'
    proof = {'kind': 'strict_subset', 'original_fingerprint': old, 'current_fingerprint': new, 'proof': 'one now called'}
    assert not validate_clippy({old}, {new}, [proof])
    assert validate_clippy({old}, {'method `new` is never used :: src/old.rs'}, [proof])
    assert validate_clippy({old}, {new}, [{**proof, 'current_fingerprint': 'method `three` is never used :: src/old.rs'}])
    assert validate_clippy({old}, {new}, [{**proof, 'kind': 'relocation'}])
    old_fixture = 'fn fixture() -> u8 { 1 }'
    new_fixture = 'fn fixture() -> u8 { 2 }'
    fixture = {'finding': 'R09', 'reason': 'owned pause establishes the cold schedule',
               'file': 'fixture.rs', 'name': 'fixture',
               'before_sha256': inventory.functions(old_fixture, include_helpers=True)[0]['function_sha256'],
               'after_sha256': inventory.functions(new_fixture, include_helpers=True)[0]['function_sha256']}
    assert not fixture_change_failures(fixture, old_fixture, new_fixture)
    assert fixture_change_failures(fixture, new_fixture, new_fixture)
    assert fixture_change_failures(fixture, old_fixture, old_fixture)
    assert fixture_change_failures({**fixture, 'reason': ''}, old_fixture, new_fixture)
    anchors = {('fixture.rs', 'fixture'): 'fixed-commit'}
    anchored = {**fixture, 'before_commit': 'fixed-commit'}
    assert not source_anchor_failures([anchored], anchors)
    assert source_anchor_failures([{**anchored, 'before_commit': 'new-head'}], anchors)
    assert source_anchor_failures([], anchors)
    assert source_anchor_failures([anchored, anchored], anchors)
    print('review-evidence self-test: OK (33 controls)')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument('--check', action='store_true')
    action.add_argument('--self-test', action='store_true')
    action.add_argument('--record-run', type=Path)
    action.add_argument('--verify-run', type=Path)
    parser.add_argument('--minimum-tests', type=int, default=1)
    parser.add_argument('--config-json', type=json.loads)
    parser.add_argument('--toolchain-command', action='append', type=json.loads, default=[])
    parser.add_argument('command', nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.self_test:
        self_test(); return 0
    if args.check:
        failures = check()
        for failure in failures: print(failure)
        print(f'review-evidence source inventory: {"FAIL" if failures else "OK"}; execution and external acceptance require receipts')
        return bool(failures)
    head, tree = git('rev-parse', 'HEAD'), git('rev-parse', 'HEAD^{tree}')
    clean = not git('status', '--porcelain')
    if args.record_run:
        receipt_path = args.record_run.resolve()
        if receipt_path.is_relative_to(ROOT):
            parser.error('write execution receipts outside the source checkout')
        command = args.command[1:] if args.command[:1] == ['--'] else args.command
        if not clean or not command or not args.config_json or not args.toolchain_command:
            parser.error('recording requires clean checkout, command, declared configuration and toolchain commands')
        versions = toolchain_versions(args.toolchain_command)
        log_path = receipt_path.with_suffix('.log')
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open('wb') as output:
            result = subprocess.run(command, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        log = log_path.read_bytes()
        receipt = {'schema': 1, 'head': head, 'tree': tree, 'clean_checkout': clean,
                   'command': command, 'configuration': args.config_json, 'toolchain_commands': args.toolchain_command,
                   'toolchains': versions, 'log': str(log_path), 'log_sha256': sha(log), 'exit_code': result.returncode,
                   'minimum_tests': args.minimum_tests, 'tests': test_counts(log.decode(errors='replace'))}
        receipt_path.write_text(json.dumps(receipt, indent=2)+'\n')
        # Refuse a run that changed tracked or untracked source as a side effect.
        failures = receipt_failures(receipt, git('rev-parse', 'HEAD'), git('rev-parse', 'HEAD^{tree}'),
                                    not git('status', '--porcelain'), log, args.config_json, versions)
    else:
        if not args.config_json:
            parser.error('verification requires the expected --config-json declaration')
        receipt = json.loads(args.verify_run.read_text())
        versions = toolchain_versions(receipt['toolchain_commands'])
        failures = receipt_failures(receipt, head, tree, clean, Path(receipt['log']).read_bytes(), args.config_json, versions)
    for failure in failures: print(failure)
    print(f'review-evidence execution receipt: {"FAIL" if failures else "OK"} ({receipt["head"]}; {receipt["tests"]})')
    return bool(failures)


if __name__ == '__main__':
    raise SystemExit(main())
