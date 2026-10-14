"""promote-rc.sh keeps the SDK tarball it packs at the certified commit and records its digests.

Each test runs the real script, copied into a disposable Git repository
whose gates (`scripts/release-gate.sh`, `scripts/multitenancy-audit.sh`,
the noisy-neighbor campaign, `scripts/verify-rc-evidence.py`) are stubs:
they are the release gate's business, not this file's. The real
`scripts/required-checks.sh` reads the required jobs from gh_actions_stub's
`gh` on PATH (by default the push runs at the fixture's commit, every
required job green). `npm` and `node` are stubs on PATH; the `npm` stub
packs a real gzip tarball and refuses a pack destination outside the
fixture, so a tarball can only land where the test can see it. The real
`scripts/publish-sdk.sh --dry-run` then reads each tag it mints (its `npm
view` meets an empty answer: not yet published).
"""
import base64
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from gh_actions_stub import GH_STUB, ci_jobs, push_runs, required_checks

ROOT = Path(__file__).resolve().parents[2]
TAG = 'v9.9.9-beta.1-rc.1'
VERSION = '9.9.9-beta.1'
TARBALL = f'prisma-streams-{VERSION}.tgz'
STUB = '#!/bin/bash\nset -eu\necho "$(basename "$0") stub ok"\n'
GATE_STUB = ('#!/bin/bash\nset -eu\nmkdir -p target/release\n'
             'printf server-binary > target/release/streams-slate\necho RELEASE_GATE_LOCAL_OK\n')
# npm: `ci` and `run build` succeed; `pack` writes a real tarball of the
# package.json, README.md and CHANGELOG.md in its working directory and
# answers as npm pack --json does.
NPM_STUB = r'''#!/usr/bin/env python3
import base64, hashlib, io, json, os, sys, tarfile
from pathlib import Path
args = sys.argv[1:]
with open(os.environ['STUB_LOG'], 'a') as log:
    log.write('npm ' + ' '.join(args) + '\n')
if args[0] != 'pack':
    sys.exit(0)
dest = Path(args[args.index('--pack-destination') + 1]).resolve()
root = Path(os.environ['FIXTURE_ROOT']).resolve()
if root not in dest.parents:
    sys.exit(f'stub npm: pack destination {dest} is outside the fixture')
manifest = json.loads(Path('package.json').read_text())
raw = io.BytesIO()
with tarfile.open(fileobj=raw, mode='w:gz') as tar:
    for file in ('package.json', 'README.md', 'CHANGELOG.md'):
        data = Path(file).read_bytes()
        info = tarfile.TarInfo('package/' + file)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
name = manifest['name'].lstrip('@').replace('/', '-') + '-' + manifest['version'] + '.tgz'
(dest / name).write_bytes(raw.getvalue())
integrity = 'sha512-' + base64.b64encode(hashlib.sha512(raw.getvalue()).digest()).decode()
if os.environ.get('STUB_NPM_WRONG_INTEGRITY'):
    integrity = 'sha512-' + base64.b64encode(b'\0' * 64).decode()
if '--json' in args:
    print(json.dumps([{'name': manifest['name'], 'version': manifest['version'],
                       'filename': name, 'integrity': integrity}], indent=2))
else:
    print(name)
'''


def integrity(data):
    return 'sha512-' + base64.b64encode(hashlib.sha512(data).digest()).decode()


class PromoteRc(unittest.TestCase):
    def setUp(self):
        # Resolved: the script names paths from its physical working directory.
        self.tmp = Path(tempfile.mkdtemp(prefix='promote-rc-')).resolve()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = self.tmp / 'repo'
        self.bin = self.tmp / 'bin'
        self.log = self.tmp / 'calls.log'
        self.log.touch()
        scripts = self.repo / 'scripts'
        scripts.mkdir(parents=True)
        shutil.copy(ROOT / 'scripts' / 'promote-rc.sh', scripts / 'promote-rc.sh')
        shutil.copy(ROOT / 'scripts' / 'publish-sdk.sh', scripts / 'publish-sdk.sh')
        shutil.copy(ROOT / 'scripts' / 'required-checks.sh', scripts / 'required-checks.sh')
        self.executable(scripts / 'release-gate.sh', GATE_STUB)
        self.executable(scripts / 'multitenancy-audit.sh', STUB)
        (scripts / 'mt-noisy-campaign.mjs').write_text('// stub\n')
        (scripts / 'verify-rc-evidence.py').write_text('print("evidence stub ok")\n')
        (self.repo / 'sdk').mkdir()
        (self.repo / 'sdk' / 'package.json').write_text(
            json.dumps({'name': '@prisma/streams', 'version': VERSION}))
        (self.repo / 'sdk' / 'README.md').write_text('# @prisma/streams\n')
        (self.repo / 'sdk' / 'CHANGELOG.md').write_text(f'# Changelog\n\n## {VERSION}\n')
        self.executable(self.bin / 'gh', GH_STUB)
        self.executable(self.bin / 'node', '#!/bin/bash\necho "node $*" >> "$STUB_LOG"\n')
        self.executable(self.bin / 'npm', NPM_STUB)
        self.git('init', '-q')
        self.git('config', 'user.name', 'promote-rc fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('config', 'tag.gpgsign', 'false')
        self.git('config', 'core.hooksPath', str(self.tmp / 'no-hooks'))
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        self.head = self.git('rev-parse', 'HEAD')
        self.runs = self.tmp / 'runs.json'
        self.runs.write_text(json.dumps(push_runs(self.head)))

    def executable(self, path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(0o755)

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], text=True,
                                       stderr=subprocess.PIPE).strip()

    def env(self, **env):
        return {**os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}', 'STUB_LOG': str(self.log),
                'FIXTURE_ROOT': str(self.tmp), 'GH_STUB_RUNS': str(self.runs), **env}

    def promote(self, *args, **env):
        return subprocess.run(['bash', 'scripts/promote-rc.sh', 'HEAD', TAG, *args], cwd=self.repo,
                              env=self.env(**env), text=True, capture_output=True)

    def tag_message(self):
        return self.git('for-each-ref', '--format=%(contents)', f'refs/tags/{TAG}')

    def kept(self):
        return self.repo / 'target' / 'rc' / TAG / TARBALL

    def assert_records_the_kept_tarball(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('PROMOTE_RC_OK', result.stdout)
        data = self.kept().read_bytes()
        message = self.tag_message()
        self.assertIn(f'- sdk tarball sha256:   {hashlib.sha256(data).hexdigest()} ({TARBALL})\n', message)
        self.assertIn(f'- sdk tarball sha512:   {integrity(data)} (npm integrity)\n', message)
        self.assertEqual(sorted(p.name for p in self.kept().parent.iterdir()), [TARBALL])
        # The tag and the kept file are what scripts/publish-sdk.sh publishes.
        publish = subprocess.run(['bash', 'scripts/publish-sdk.sh', '--dry-run', TAG], cwd=self.repo,
                                 env=self.env(), text=True, capture_output=True)
        self.assertIn(f'PUBLISH_SDK_DRY_RUN_OK: would publish {self.kept()} as @prisma/streams@{VERSION}',
                      publish.stdout, publish.stdout + publish.stderr)

    def test_the_local_half_keeps_the_tarball_under_the_tag_and_records_both_digests(self):
        result = self.promote()
        self.assert_records_the_kept_tarball(result)
        self.assertIn('- every attempt of every required job green in the push runs at the SHA: '
                      + ' '.join(required_checks()) + '\n', self.tag_message())
        self.assertIn(f'npm pack --ignore-scripts --json --pack-destination {self.kept().parent}',
                      self.log.read_text())

    def test_a_field_legs_tag_records_both_digests_of_the_kept_tarball(self):
        manifest = {
            'server_binary_sha256': 'c0ffee' + '0' * 58,
            'exact_artifact_canary': {'log_sha256': 'a' * 64},
            'profile': {'file': 'deploy/profiles/shared-cell.env', 'sha256': 'b' * 64, 'pins': ['X=1']},
            'harness_trees': {'bench': 'c' * 40},
            'field_legs': [{'leg': 'H9', 'run_id': 'run-1', 'stage': 'stage-1', 'finding': 'PASS',
                            'stage_sha256': 'd' * 64}],
            'not_certified': ['fleet owner-movement campaign at scale'],
        }
        legs = self.tmp / 'field-legs.json'
        legs.write_text(json.dumps(manifest))
        artifact = self.tmp / 'streams-slate'
        artifact.write_text('musl binary')
        canary = self.tmp / 'canary.log'
        canary.write_text('canary ok')
        result = self.promote('--field-legs', str(legs), SOAK_HOME=str(self.tmp / 'soak'),
                              RC_ARTIFACT=str(artifact), RC_CANARY_LOG=str(canary))
        self.assert_records_the_kept_tarball(result)
        self.assertIn('- every attempt of every required job green in the push runs at the SHA\n', self.tag_message())

    def test_an_existing_tarball_directory_for_the_tag_is_refused_before_any_check(self):
        self.kept().parent.mkdir(parents=True)
        (self.kept().parent / TARBALL).write_bytes(b'an earlier promotion')
        result = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f'FAIL: {self.kept().parent} exists', result.stdout)
        self.assertEqual(self.log.read_text(), '')
        self.assertEqual(self.kept().read_bytes(), b'an earlier promotion')
        self.assertEqual(self.git('tag', '-l', TAG), '')

    def test_a_pack_whose_reported_integrity_is_not_the_tarballs_mints_no_tag(self):
        result = self.promote(STUB_NPM_WRONG_INTEGRITY='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('FAIL: npm pack reported integrity sha512-AAAA', result.stdout)
        self.assertEqual(self.git('tag', '-l', TAG), '')

    def test_a_promotion_that_mints_no_tag_leaves_no_tarball_directory_and_the_next_one_runs(self):
        failed = self.promote(STUB_NPM_WRONG_INTEGRITY='1')
        self.assertNotEqual(failed.returncode, 0)
        self.assertFalse(self.kept().parent.exists(), 'the directory of a failed promotion is removed')
        self.assert_records_the_kept_tarball(self.promote())

    def test_a_red_attempt_hidden_by_a_green_rerun_mints_no_tag(self):
        self.runs.write_text(json.dumps(push_runs(self.head, ci=ci_jobs(1, rust='failure') + ci_jobs(2))))
        result = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('FAIL: rust: attempt 1 of push run 1001 (.github/workflows/ci.yml) concluded failure', result.stdout)
        self.assertTrue(result.stdout.endswith(f'FAIL: a required job is not green at {self.head}\n'), result.stdout)
        self.assertEqual([line.split()[0] for line in self.log.read_text().splitlines()], ['gh'] * 4)
        self.assertFalse(self.kept().parent.exists())
        self.assertEqual(self.git('tag', '-l', TAG), '')

    def test_an_existing_tag_is_refused_before_any_check(self):
        self.git('tag', TAG, self.head)
        result = self.promote()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f'FAIL: tag {TAG} exists', result.stdout)
        self.assertEqual(self.log.read_text(), '')
        self.assertFalse(self.kept().parent.exists())


if __name__ == '__main__':
    unittest.main()
