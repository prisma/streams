"""rc-certify.sh certifies a release candidate only from evidence it re-checks.

Each test runs the real script, copied into a disposable Git repository
whose `scripts/release-gate.sh` is a stub (the release gate is the commit
gate's and the suite's business, not this file's), against a disposable
SOAK_HOME holding the campaign manifests the script reads.
"""
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


class RcCertify(unittest.TestCase):
    def setUp(self):
        tmp = Path(tempfile.mkdtemp(prefix='rc-certify-'))
        self.addCleanup(shutil.rmtree, tmp)
        self.repo = tmp / 'repo'
        self.soak = tmp / 'soak'
        (self.repo / 'scripts' / 'quality').mkdir(parents=True)
        shutil.copy(ROOT / 'scripts' / 'rc-certify.sh', self.repo / 'scripts' / 'rc-certify.sh')
        gate = self.repo / 'scripts' / 'release-gate.sh'
        gate.write_text(GATE_STUB)
        gate.chmod(0o755)
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


if __name__ == '__main__':
    unittest.main()
