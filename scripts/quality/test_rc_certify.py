"""rc-certify.sh certifies a release candidate only from evidence it re-checks.

Each test runs the real script, copied into a disposable Git repository
whose `scripts/release-gate.sh` is a stub (the release gate is the commit
gate's and the suite's business, not this file's), against a disposable
SOAK_HOME holding the campaign manifests the script reads.
"""
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
# The RC identity: the sha256 of the x86_64-musl server binary a campaign ran.
RC = 'c0ffee' + '0' * 58
OTHER = 'bad' + '0' * 61
GATE_STUB = '#!/bin/bash\nset -eu\nmkdir -p target\ntouch target/release-gate-ran\necho RELEASE_GATE_LOCAL_OK\n'
PROVIDER_TEST = 'bootstrap::tests::provider_contract::real_provider_meets_the_provider_contract'
# What scripts/test-leg.sh keeps from the real-provider runner (docs/PROVIDER-CONTRACT.md).
PROVIDER_LOG = f'''    Finished `test` profile [unoptimized + debuginfo] target(s) in 0.40s
     Running unittests src/lib.rs (target/debug/deps/streams_slate-0123456789abcdef)

running 1 test
qualifying endpoint https://t3.storage.dev bucket streams-provider-contract prefix Some("provider-contract-20261011") region auto
real-provider: provider contract passed; Observations {{ metadata_round_trip: true, etag_repeats_for_identical_content: false }}
test {PROVIDER_TEST} ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1439 filtered out; finished in 41.20s
'''
# The legs the lead's call 7 (2026-10-11) names for the Layer B arm and the battery.
LAYER_B_LEGS = ('H9', 'H9b', 'H11t')
BATTERY_LEGS = ('hostile-tenants', 'cost-hostile-tenants', 'parked-consumers', 'ch-probe',
                'suspension-under-load', 'revocation-under-load', 'billing-reconcile')
SINGLE_CELL_ARMS = ('layer-b', 'envelope', 'sc0-1s', 'sc1', 'stream-count', 'battery')
# Every arm manifest names its run's window in unix seconds; SC0-1s runs at
# least 72 h and SC1 at least 30 h.
STARTED = 1_790_000_000
HOUR = 3600
# The tag tree's deploy profiles (triage B9's fence): an arm ran the
# shared-cell profile layered on compute-1g, byte for byte the tag's, with
# the single-cell pins and no FLEET_PREFIX; fleet.env is a fleet-pinned file
# the tag tree also holds.
PROFILES = {
    'compute-1g.env': 'TOKIO_WORKERS=2\n',
    'shared-cell.env': '# the shared cell\nSTREAMS_RELEASE_POSTURE=1\nKEEP_AWAKE=1\nINITIAL_SHARDS=1\nPROJECT_SHARE_K=8\n',
    'fleet.env': 'FLEET_PREFIX=fleet/\n',
}
CELL_ENV = 'ce11' + '0' * 60


def sha256_text(text):
    return hashlib.sha256(text.encode()).hexdigest()


def profile(*files):
    """An arm manifest's `profile`, as bench/k2cost/shared_verdict.py's profile_fence writes it."""
    return {'files': sorted(files), 'sha256': {f: sha256_text(PROFILES.get(f, '')) for f in sorted(files)},
            'pins': [], 'pins_missing': []}


def arm(name, legs, secs=HOUR, **facts):
    return {'arm': name, 'server_sha256': 'sha256:' + RC, 'verdict': 'PASS',
            'legs': {leg: 'PASS' for leg in legs},
            'started_unix': STARTED, 'finished_unix': STARTED + secs,
            'profile': profile('compute-1g.env', 'shared-cell.env'), 'cell_env_sha256': CELL_ENV, **facts}


class RcCertify(unittest.TestCase):
    def setUp(self):
        tmp = Path(tempfile.mkdtemp(prefix='rc-certify-'))
        self.addCleanup(shutil.rmtree, tmp)
        self.repo = tmp / 'repo'
        self.soak = tmp / 'soak'
        (self.repo / 'scripts' / 'quality').mkdir(parents=True)
        shutil.copy(ROOT / 'scripts' / 'rc-certify.sh', self.repo / 'scripts' / 'rc-certify.sh')
        shutil.copy(ROOT / 'scripts' / 'quality' / 'tests_ran.py', self.repo / 'scripts' / 'quality' / 'tests_ran.py')
        gate = self.repo / 'scripts' / 'release-gate.sh'
        gate.write_text(GATE_STUB)
        gate.chmod(0o755)
        (self.repo / 'deploy' / 'profiles').mkdir(parents=True)
        for name, text in PROFILES.items():
            (self.repo / 'deploy' / 'profiles' / name).write_text(text)
        self.git('init', '-q')
        self.git('config', 'user.name', 'rc-certify fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('config', 'core.hooksPath', str(tmp / 'no-hooks'))
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        self.head = self.git('rev-parse', 'HEAD')

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], text=True,
                                       stderr=subprocess.PIPE).strip()

    def write(self, run, name, value):
        path = self.soak / 'results' / run / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value if isinstance(value, str) else json.dumps(value))
        return path

    def fleet_world(self, override=None):
        """A fleet-shaped evidence set every check accepts; `override` replaces one file."""
        files = {
            ('cap-1', 'binaries.json'): {
                'bin/streams-rc-x64': {'sha256': RC, 'bytes': 1, 'gitCommit': self.head, 'buildUnix': '1'},
                'bin/awsbench-rc-x64': {'sha256': OTHER, 'bytes': 1, 'gitCommit': self.head, 'buildUnix': '1'}},
            ('cap-1', 'capacity-verdict.json'): {
                'PASS': True, 'peak_ledger_bytes': 512 << 20, 'maintenance_shed_total': 0,
                'catchup_over_ingest': 2.0},
            ('cap-1', 'reconcile.json'): [{'verdict': 'OK'}],
            ('handoff-1', 'binary.sha'): RC + '\n',
            ('handoff-1', 'restore-verdict.json'): {'restored_over_prekill': 1.02},
            ('handoff-1', 'reconcile.json'): [{'verdict': 'OK'}],
            ('lf-fleet-1', 'livefeed-cert-manifest.json'): {
                'commit': self.head, 'verdict': 'PASS', 'legs': {'fanout': 'PASS', 'dedup': 'COVERED_INPROC'}},
            ('lf-canary-1', 'livefeed-canary-manifest.json'): {
                'server_sha256': 'sha256:' + RC, 'verdict': 'PASS', 'legs': {'breadth': 'PASS'}},
        }
        files.update(override or {})
        for (run, name), value in files.items():
            self.write(run, name, value)
        return ['--capacity-run', 'cap-1', '--handoff-run', 'handoff-1',
                '--livefeed-fleet-run', 'lf-fleet-1', '--livefeed-canary-run', 'lf-canary-1']

    def single_cell_world(self, override=None):
        """A single-cell evidence set every check accepts; `override` replaces one file."""
        files = {
            ('build-1', 'binaries.json'): {
                'bin/streams-rc-x64': {'sha256': RC, 'bytes': 1, 'gitCommit': self.head, 'buildUnix': '1'}},
            ('pc-1', 'provider-contract.log'): PROVIDER_LOG,
            ('layer-b-1', 'layer-b-manifest.json'): arm('layer-b', LAYER_B_LEGS, projects=1000),
            ('envelope-1', 'envelope-manifest.json'): arm('envelope', ['WC_PMP']),
            ('sc0-1s-1', 'sc0-1s-manifest.json'): arm('sc0-1s', ['idle-floor', 'rss-slope'], secs=72 * HOUR),
            ('sc1-1', 'sc1-manifest.json'): arm('sc1', ['wake-p99'], secs=30 * HOUR),
            ('streams-1', 'stream-count-manifest.json'): arm('stream-count', ['churn'], streams=30000),
            ('battery-1', 'battery-manifest.json'): arm('battery', BATTERY_LEGS, projects=1000),
        }
        files.update(override or {})
        for (run, name), value in files.items():
            if value is not None:
                self.write(run, name, value)
        return ['--shape', 'single-cell', '--artifact-run', 'build-1',
                '--provider-contract-log', str(self.soak / 'results' / 'pc-1' / 'provider-contract.log'),
                '--layer-b-run', 'layer-b-1', '--envelope-run', 'envelope-1', '--sc0-1s-run', 'sc0-1s-1',
                '--sc1-run', 'sc1-1', '--stream-count-run', 'streams-1',
                # A manifest path is accepted where a run id is.
                '--battery-run', str(self.soak / 'results' / 'battery-1' / 'battery-manifest.json')]

    def certify(self, args, **env):
        shutil.rmtree(self.repo / 'target', ignore_errors=True)
        return subprocess.run(['bash', 'scripts/rc-certify.sh', *args], cwd=self.repo,
                              env={**os.environ, 'SOAK_HOME': str(self.soak), **env},
                              capture_output=True, text=True)

    def assert_refused(self, result, reason):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertNotIn('RC_CERTIFY_OK', result.stdout)
        self.assertIn(reason, result.stdout + result.stderr)

    def test_a_complete_fleet_world_certifies_the_campaign_artifact(self):
        result = self.certify(self.fleet_world())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f'RC_CERTIFY_OK  binary={RC}', result.stdout)
        manifest = json.loads((self.repo / 'target' / 'rc-certify-manifest.json').read_text())
        self.assertEqual((manifest['commit'], manifest['server_sha256']), (self.head, RC))
        self.assertEqual([manifest[leg] for leg in ('capacity', 'handoff', 'livefeed_fleet', 'livefeed_canary')],
                         ['pass'] * 4)
        self.assertEqual(manifest['shape'], 'fleet')
        self.assertEqual(manifest['tag_lines'], ['capacity + handoff manifests verified via rc-certify.sh'])

    def test_each_broken_fleet_manifest_is_refused_whatever_the_python_environment(self):
        # PYTHONOPTIMIZE strips `assert`: the checks must not depend on the
        # caller's Python environment.
        broken = {
            'manifest commit': {('cap-1', 'binaries.json'): {
                'bin/streams-rc-x64': {'sha256': RC, 'gitCommit': '0' * 40}}},
            'capacity verdict not PASS': {('cap-1', 'capacity-verdict.json'): {
                'PASS': False, 'peak_ledger_bytes': 0, 'maintenance_shed_total': 0, 'catchup_over_ingest': 0}},
            'reconcile not OK': {('cap-1', 'reconcile.json'): [{'verdict': 'LOST'}]},
            'NOT the release binary': {('handoff-1', 'binary.sha'): OTHER},
            'restore ratio': {('handoff-1', 'restore-verdict.json'): {'restored_over_prekill': 0.9}},
            'fleet verdict': {('lf-fleet-1', 'livefeed-cert-manifest.json'): {
                'commit': self.head, 'verdict': 'FAIL', 'legs': {}}},
            'canary legs not green': {('lf-canary-1', 'livefeed-canary-manifest.json'): {
                'server_sha256': RC, 'verdict': 'PASS', 'legs': {'breadth': 'FAIL'}}},
        }
        for reason, override in broken.items():
            for env in ({}, {'PYTHONOPTIMIZE': '1'}):
                with self.subTest(reason=reason, env=env):
                    shutil.rmtree(self.soak, ignore_errors=True)
                    self.assert_refused(self.certify(self.fleet_world(override), **env), reason)


    def test_a_complete_single_cell_world_certifies_the_artifact_and_prints_each_waiver(self):
        result = self.certify(self.single_cell_world())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f'RC_CERTIFY_OK  binary={RC}', result.stdout)
        self.assertIn('== 1/9 local gate ==', result.stdout)
        self.assertTrue((self.repo / 'target' / 'release-gate-ran').exists())
        waived = [line.split()[1].rstrip(':') for line in result.stdout.splitlines() if line.startswith('  WAIVED ')]
        self.assertEqual(waived, ['capacity-campaign', 'handoff', 'livefeed-fleet', 'livefeed-canary'])
        manifest = json.loads((self.repo / 'target' / 'rc-certify-manifest.json').read_text())
        self.assertEqual((manifest['shape'], manifest['commit'], manifest['server_sha256']),
                         ('single-cell', self.head, RC))
        self.assertEqual(manifest['artifact_run'], 'build-1')
        self.assertEqual(sorted(manifest['waived']), ['capacity_campaign', 'handoff', 'livefeed_canary', 'livefeed_fleet'])
        self.assertEqual(list(manifest['arms']), list(SINGLE_CELL_ARMS))
        self.assertEqual(manifest['arms']['stream-count']['streams'], 30000)
        self.assertEqual(manifest['arms']['battery']['projects'], 1000)
        self.assertEqual({name: a['finished_unix'] - a['started_unix'] for name, a in manifest['arms'].items()},
                         {**{name: HOUR for name in SINGLE_CELL_ARMS}, 'sc0-1s': 72 * HOUR, 'sc1': 30 * HOUR})
        self.assertIn('  sc0-1s: PASS (2 legs, 72.0 h) on c0ffee', result.stdout)
        log = self.soak / 'results' / 'pc-1' / 'provider-contract.log'
        self.assertEqual(manifest['provider_contract']['log_sha256'],
                         hashlib.sha256(log.read_bytes()).hexdigest())
        self.assertEqual(manifest['provider_contract']['endpoint'], 'https://t3.storage.dev')
        self.assertIn('sc0-1s (72.0 h), sc1 (30.0 h), stream-count (30000 streams)', manifest['tag_lines'][0])
        self.assertTrue(manifest['tag_lines'][1].startswith('WAIVED for this single-cell, fleet-off RC'))
        for key in ('capacity', 'handoff', 'livefeed_fleet', 'livefeed_canary'):
            self.assertNotIn(key, manifest, 'a single-cell manifest claims no fleet leg')

    def test_each_broken_single_cell_manifest_is_refused(self):
        no_skip = PROVIDER_LOG.replace(
            'qualifying endpoint', 'SKIPPED: the real-provider contract runs only with '
            'STREAMS_PROVIDER_CONTRACT=1 (docs/PROVIDER-CONTRACT.md); nothing was qualified\nqualifying endpoint')
        broken = {
            'manifest commit': {('build-1', 'binaries.json'): {
                'bin/streams-rc-x64': {'sha256': RC, 'gitCommit': '0' * 40}}},
            'SKIPPED': {('pc-1', 'provider-contract.log'): no_skip},
            'is missing': {('pc-1', 'provider-contract.log'): PROVIDER_LOG.replace(f'test {PROVIDER_TEST} ... ok\n', '')},
            'provider contract passed': {('pc-1', 'provider-contract.log'): PROVIDER_LOG.replace(
                'real-provider: provider contract passed', 'real-provider: started')},
            'not a Tigris endpoint': {('pc-1', 'provider-contract.log'): PROVIDER_LOG.replace(
                'https://t3.storage.dev', 'http://127.0.0.1:9100')},
            'qualifying endpoint': {('pc-1', 'provider-contract.log'): PROVIDER_LOG.replace(
                'qualifying endpoint', 'qualified endpoint')},
            'layer-b: ran bad00000': {('layer-b-1', 'layer-b-manifest.json'): {
                **arm('layer-b', LAYER_B_LEGS, projects=1000), 'server_sha256': OTHER}},
            "envelope: verdict 'FAIL'": {('envelope-1', 'envelope-manifest.json'): {
                **arm('envelope', ['WC_PMP']), 'verdict': 'FAIL'}},
            'sc0-1s: legs not green': {('sc0-1s-1', 'sc0-1s-manifest.json'): {
                **arm('sc0-1s', ['idle-floor']), 'legs': {'idle-floor': 'PASS', 'rss-slope': 'FAIL'}}},
            'sc1: no legs': {('sc1-1', 'sc1-manifest.json'): arm('sc1', [])},
            "sc1: the manifest names arm 'sc0-1s'": {('sc1-1', 'sc1-manifest.json'): arm('sc0-1s', ['wake-p99'])},
            "layer-b: required legs missing: ['H9b']": {('layer-b-1', 'layer-b-manifest.json'): arm(
                'layer-b', ['H9', 'H11t'], projects=1000)},
            "battery: required legs missing: ['billing-reconcile']": {('battery-1', 'battery-manifest.json'): arm(
                'battery', BATTERY_LEGS[:-1], projects=1000)},
            'battery: projects 999': {('battery-1', 'battery-manifest.json'): arm('battery', BATTERY_LEGS, projects=999)},
            'layer-b: projects True': {('layer-b-1', 'layer-b-manifest.json'): arm('layer-b', LAYER_B_LEGS, projects=True)},
            'stream-count: streams None': {('streams-1', 'stream-count-manifest.json'): arm('stream-count', ['churn'])},
            'stream-count: no manifest at': {('streams-1', 'stream-count-manifest.json'): None},
            # The run's window: a short SC0-1s or SC1 that writes PASS is not the arm.
            'sc0-1s: ran 259199 s, under the arm\'s floor of 259200 s (72 h)': {
                ('sc0-1s-1', 'sc0-1s-manifest.json'): arm('sc0-1s', ['idle-floor', 'rss-slope'], secs=72 * HOUR - 1)},
            'sc1: ran 107999 s, under the arm\'s floor of 108000 s (30 h)': {
                ('sc1-1', 'sc1-manifest.json'): arm('sc1', ['wake-p99'], secs=30 * HOUR - 1)},
            'layer-b: ran 0 s': {('layer-b-1', 'layer-b-manifest.json'): arm('layer-b', LAYER_B_LEGS, secs=0,
                                                                              projects=1000)},
            'envelope: started_unix None': {('envelope-1', 'envelope-manifest.json'): arm(
                'envelope', ['WC_PMP'], started_unix=None)},
            # The profile fence (triage B9): the arm ran the tag tree's
            # single-cell profile, and every field arm one cell environment.
            'battery: no profile': {('battery-1', 'battery-manifest.json'): {
                **arm('battery', BATTERY_LEGS, projects=1000), 'profile': None}},
            f"layer-b: profile shared-cell.env has sha256 {OTHER[:16]}, the tag tree's": {
                ('layer-b-1', 'layer-b-manifest.json'): {
                    **arm('layer-b', LAYER_B_LEGS, projects=1000),
                    'profile': {**profile('compute-1g.env', 'shared-cell.env'),
                                'sha256': {'compute-1g.env': sha256_text(PROFILES['compute-1g.env']),
                                           'shared-cell.env': OTHER}}}},
            'envelope: profile deploy/profiles/staging.env is not in the tag tree': {
                ('envelope-1', 'envelope-manifest.json'): arm(
                    'envelope', ['WC_PMP'], profile=profile('shared-cell.env', 'staging.env'))},
            "sc0-1s: the profile's files lack the single-cell pins ['INITIAL_SHARDS=1', 'KEEP_AWAKE=1', "
            "'STREAMS_RELEASE_POSTURE=1', 'PROJECT_SHARE_K=8']": {
                ('sc0-1s-1', 'sc0-1s-manifest.json'): arm(
                    'sc0-1s', ['idle-floor', 'rss-slope'], secs=72 * HOUR, profile=profile('compute-1g.env'))},
            'sc1: the profile sets FLEET_PREFIX (fleet.env): a fleet-pinned run': {
                ('sc1-1', 'sc1-manifest.json'): arm(
                    'sc1', ['wake-p99'], secs=30 * HOUR,
                    profile=profile('compute-1g.env', 'shared-cell.env', 'fleet.env'))},
            'stream-count: cell_env_sha256 None': {('streams-1', 'stream-count-manifest.json'): arm(
                'stream-count', ['churn'], streams=30000, cell_env_sha256=None)},
            'the field arms ran 2 cell environments': {('streams-1', 'stream-count-manifest.json'): arm(
                'stream-count', ['churn'], streams=30000, cell_env_sha256=OTHER)},
        }
        # Under PYTHONOPTIMIZE, which strips `assert`: refused there, refused anywhere.
        for reason, override in broken.items():
            with self.subTest(reason=reason):
                shutil.rmtree(self.soak, ignore_errors=True)
                self.assert_refused(self.certify(self.single_cell_world(override), PYTHONOPTIMIZE='1'), reason)

    def test_each_shape_refuses_the_other_shapes_manifests_before_the_gate_runs(self):
        fleet, single = self.fleet_world(), self.single_cell_world()
        cases = {
            'a single-cell certification takes no --capacity-run': single + ['--capacity-run', 'cap-1'],
            'a single-cell certification takes no --handoff-run': single + ['--handoff-run', 'handoff-1'],
            'a single-cell certification takes no --livefeed-fleet-run': single + ['--livefeed-fleet-run', 'x'],
            'a single-cell certification takes no --livefeed-canary-run': single + ['--livefeed-canary-run', 'x'],
            'a fleet certification takes no --battery-run': fleet + ['--battery-run', 'battery-1'],
            'a fleet certification takes no --artifact-run': ['--shape', 'fleet', *fleet, '--artifact-run', 'build-1'],
            'unknown shape: multi-cell': ['--shape', 'multi-cell', *fleet],
        }
        for flag in ('--artifact-run', '--provider-contract-log', '--layer-b-run', '--envelope-run',
                     '--sc0-1s-run', '--sc1-run', '--stream-count-run', '--battery-run'):
            at = single.index(flag)
            cases[f'need {flag}'] = single[:at] + single[at + 2:]
        for reason, args in cases.items():
            with self.subTest(reason=reason):
                result = self.certify(args)
                self.assertEqual(result.returncode, 2, result.stdout + result.stderr)
                self.assertIn(reason, result.stdout)
                self.assertFalse((self.repo / 'target' / 'release-gate-ran').exists(), 'the gate ran first')


if __name__ == '__main__':
    unittest.main()
