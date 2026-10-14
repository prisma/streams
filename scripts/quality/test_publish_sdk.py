"""publish-sdk.sh publishes only the SDK tarball a release candidate's tag pins.

Each test runs the real script and the real `scripts/required-checks.sh`,
copied into a disposable Git repository, beside a real tarball kept in
`target/rc/<tag>/` and an annotated tag whose message records it as
promote-rc.sh writes it. `gh` is gh_actions_stub's stub on PATH, answering
the Actions API from the runs the test writes (by default the push runs
at the tag's commit, every required job green). `npm` is a stub on PATH
that logs every call and answers `view`, `publish` and `dist-tag ls` from
the test's environment; nothing reaches GitHub or a registry.
"""
import base64
import hashlib
import io
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path

from gh_actions_stub import GH_STUB, ci_jobs, job, push_runs, run

ROOT = Path(__file__).resolve().parents[2]
TAG = 'v9.9.9-beta.1-rc.1'
VERSION = '9.9.9-beta.1'
TARBALL = f'prisma-streams-{VERSION}.tgz'
# STUB_NPM_VIEW: what `npm view <pkg>@<v> version` meets before a publish:
# "missing" (E404), "empty" (an older npm's answer for a missing version),
# "published" or "error". After a publish, `view ... dist.integrity`
# answers STUB_REGISTRY_INTEGRITY or the published file's sha512, after
# STUB_READBACK_EMPTY empty answers.
NPM_STUB = r'''#!/usr/bin/env python3
import base64, hashlib, json, os, sys
from pathlib import Path
args = sys.argv[1:]
state = Path(os.environ['STUB_STATE'])
with open(os.environ['STUB_LOG'], 'a') as log:
    log.write('npm ' + ' '.join(args) + '\n')
# Every call names the public registry, whatever an npmrc says.
if args[-2:] != ['--registry', 'https://registry.npmjs.org/']:
    sys.exit('stub npm: a call without --registry https://registry.npmjs.org/: ' + ' '.join(args))
args = args[:-2]
if args[:1] == ['publish']:
    state.write_text(json.dumps({'published': args[1], 'reads': 0}))
    sys.exit(0)
if args[:1] == ['view'] and args[2:] == ['version']:
    mode = os.environ.get('STUB_NPM_VIEW', 'missing')
    if mode == 'missing':
        sys.exit('npm error code E404\nnpm error 404 Not Found - GET https://registry.npmjs.org/@prisma%2fstreams')
    if mode == 'error':
        sys.exit('npm error code ETIMEDOUT')
    if mode == 'published':
        print(args[1].rsplit('@', 1)[1])
    sys.exit(0)
if args[:1] == ['view'] and args[2:] == ['dist.integrity']:
    s = json.loads(state.read_text())
    s['reads'] += 1
    state.write_text(json.dumps(s))
    if s['reads'] <= int(os.environ.get('STUB_READBACK_EMPTY', '0')):
        sys.exit(0)
    data = Path(s['published']).read_bytes()
    print(os.environ.get('STUB_REGISTRY_INTEGRITY')
          or 'sha512-' + base64.b64encode(hashlib.sha512(data).digest()).decode())
    sys.exit(0)
if args[:2] == ['dist-tag', 'ls']:
    print('beta: ' + os.environ['STUB_VERSION'])
    sys.exit(0)
sys.exit('stub npm: unexpected call ' + ' '.join(args))
'''
NO_TOKEN = {'NPM_TOKEN': '', 'NODE_AUTH_TOKEN': ''}
REGISTRY = ' --registry https://registry.npmjs.org/'


README = '# @prisma/streams\n\n```ts\nconst client = await StreamsClient.connect({ credential, issuerUrl });\n```\n'


def changelog(version=VERSION):
    return f'# Changelog\n\n## {version}\n\nThe first public beta.\n\n## 0.2.0-rc.4\n'


def tarball(name='@prisma/streams', version=VERSION, readme=README, changes=''):
    """A packed SDK; `readme` or `changes` None leaves that file out, and
    `changes` '' is this version's released changelog."""
    files = {'package/package.json': json.dumps({'name': name, 'version': version}),
             'package/README.md': readme,
             'package/CHANGELOG.md': changelog(version) if changes == '' else changes}
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode='w:gz') as tar:
        for path, text in files.items():
            if text is None:
                continue
            data = text.encode()
            info = tarfile.TarInfo(path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return raw.getvalue()


def integrity(data):
    return 'sha512-' + base64.b64encode(hashlib.sha512(data).digest()).decode()


class PublishSdk(unittest.TestCase):
    def setUp(self):
        # Resolved: the script names paths from its physical working directory.
        self.tmp = Path(tempfile.mkdtemp(prefix='publish-sdk-')).resolve()
        self.addCleanup(shutil.rmtree, self.tmp)
        self.repo = self.tmp / 'repo'
        self.bin = self.tmp / 'bin'
        self.log = self.tmp / 'calls.log'
        self.log.touch()
        self.state = self.tmp / 'npm-state.json'
        (self.repo / 'scripts').mkdir(parents=True)
        shutil.copy(ROOT / 'scripts' / 'publish-sdk.sh', self.repo / 'scripts' / 'publish-sdk.sh')
        shutil.copy(ROOT / 'scripts' / 'required-checks.sh', self.repo / 'scripts' / 'required-checks.sh')
        self.executable(self.bin / 'gh', GH_STUB)
        self.executable(self.bin / 'npm', NPM_STUB)
        self.git('init', '-q')
        self.git('config', 'user.name', 'publish-sdk fixture')
        self.git('config', 'user.email', 'fixture@example.invalid')
        self.git('config', 'commit.gpgsign', 'false')
        self.git('config', 'tag.gpgsign', 'false')
        self.git('config', 'core.hooksPath', str(self.tmp / 'no-hooks'))
        self.git('add', '.')
        self.git('commit', '-qm', 'fixture')
        self.head = self.git('rev-parse', 'HEAD')
        self.runs = self.tmp / 'runs.json'
        self.runs.write_text(json.dumps(push_runs(self.head)))
        self.data = tarball()
        self.kept = self.repo / 'target' / 'rc' / TAG / TARBALL
        self.kept.parent.mkdir(parents=True)
        self.kept.write_bytes(self.data)

    def executable(self, path, text):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        path.chmod(0o755)

    def git(self, *args):
        return subprocess.check_output(['git', '-C', str(self.repo), *args], text=True,
                                       stderr=subprocess.PIPE).strip()

    def mint(self, data=None, sha512=True):
        data = self.data if data is None else data
        lines = ['release candidate ' + TAG, '', 'Artifacts:',
                 '- server binary sha256: ' + 'c' * 64 + '  [LOCAL build only]',
                 f'- sdk tarball sha256:   {hashlib.sha256(data).hexdigest()} ({TARBALL})']
        if sha512:
            lines.append(f'- sdk tarball sha512:   {integrity(data)} (npm integrity)')
        self.git('tag', '-a', TAG, self.head, '-m', '\n'.join(lines))

    def publish(self, *args, **env):
        return subprocess.run(['bash', 'scripts/publish-sdk.sh', *args, TAG], cwd=self.repo,
                              env={**os.environ, 'PATH': f'{self.bin}:{os.environ["PATH"]}',
                                   'STUB_LOG': str(self.log), 'STUB_STATE': str(self.state),
                                   'GH_STUB_RUNS': str(self.runs),
                                   'STUB_VERSION': VERSION, 'PUBLISH_SDK_READBACK_SECS': '0',
                                   **NO_TOKEN, **env},
                              text=True, capture_output=True)

    def npm_calls(self):
        return [line for line in self.log.read_text().splitlines() if line.startswith('npm ')]

    def assert_refused(self, result, reason, npm_calls=()):
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(f'PUBLISH_SDK_FAIL: {reason}', result.stdout)
        self.assertEqual(self.npm_calls(), list(npm_calls))

    def test_a_certified_tarball_is_published_under_the_beta_tag_and_read_back(self):
        self.mint()
        result = self.publish()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.npm_calls(), [
            f'npm view @prisma/streams@{VERSION} version{REGISTRY}',
            f'npm publish {self.kept} --tag beta --access public{REGISTRY}',
            f'npm view @prisma/streams@{VERSION} dist.integrity{REGISTRY}',
            f'npm dist-tag ls @prisma/streams{REGISTRY}',
        ])
        self.assertIn(f'gh api repos/{{owner}}/{{repo}}/actions/runs?head_sha={self.head}&event=push&per_page=100 '
                      '--paginate --slurp\n', self.log.read_text())
        self.assertIn(f'REQUIRED_CHECKS_GREEN {self.head}: rust ', result.stdout)
        self.assertIn(f'beta: {VERSION}\n', result.stdout)
        self.assertIn(
            f'RELEASE_NOTES: @prisma/streams@{VERSION} (npm dist-tag beta) published to '
            f'https://registry.npmjs.org/ from release candidate {TAG} at {self.head}: {TARBALL} sha256 '
            f'{hashlib.sha256(self.data).hexdigest()}, integrity {integrity(self.data)}\n', result.stdout)
        self.assertTrue(result.stdout.endswith('PUBLISH_SDK_OK\n'))

    def test_a_dry_run_runs_the_checks_and_stops_before_publishing(self):
        self.mint()
        result = self.publish('--dry-run')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f'PUBLISH_SDK_DRY_RUN_OK: would publish {self.kept} as @prisma/streams@{VERSION} '
                      'under the dist-tag beta', result.stdout)
        self.assertEqual(self.npm_calls(), [f'npm view @prisma/streams@{VERSION} version{REGISTRY}'])

    def test_a_registry_that_has_the_package_but_not_the_version_is_not_yet_published(self):
        self.mint()
        result = self.publish('--dry-run', STUB_NPM_VIEW='empty')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('PUBLISH_SDK_DRY_RUN_OK', result.stdout)

    def test_a_lightweight_tag_is_refused_before_anything_runs(self):
        self.git('tag', TAG, self.head)
        self.assert_refused(self.publish(), f'{TAG} is not an annotated tag')
        self.assertEqual(self.log.read_text(), '')

    def test_a_red_attempt_hidden_by_a_green_rerun_is_refused(self):
        self.runs.write_text(json.dumps(push_runs(self.head, ci=ci_jobs(1, rust='failure') + ci_jobs(2))))
        self.mint()
        result = self.publish()
        self.assert_refused(result, f'a required job is not green at {self.head}')
        self.assertIn('FAIL: rust: attempt 1 of push run 1001 (.github/workflows/ci.yml) concluded failure', result.stdout)

    def test_a_required_job_red_only_in_a_nightly_run_at_the_commit_is_accepted(self):
        # ci.yml and rust-quality.yml run nightly on slate's head: an RC that
        # stays the head gains red runs that are not its evidence.
        nightly = [run(2001, 'ci.yml', self.head, ci_jobs(rust='failure', noisy_campaign='failure'), event='schedule'),
                   run(2002, 'rust-quality.yml', self.head, [job('quality', 'failure')], event='schedule')]
        self.runs.write_text(json.dumps(push_runs(self.head) + nightly))
        self.mint()
        result = self.publish('--dry-run')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn('PUBLISH_SDK_DRY_RUN_OK', result.stdout)

    def test_a_tarball_other_than_the_tagged_one_is_refused(self):
        self.mint()
        self.kept.write_bytes(tarball(version='9.9.9-beta.2'))
        self.assert_refused(self.publish(), f'{self.kept} has sha256 ')

    def test_a_tag_without_the_npm_integrity_is_refused(self):
        self.mint(sha512=False)
        self.assert_refused(self.publish(), f"{TAG}'s message records no single sdk tarball sha512")

    def test_a_tarball_of_another_package_is_refused(self):
        self.data = tarball(name='@prisma/other')
        self.kept.write_bytes(self.data)
        self.mint()
        self.assert_refused(self.publish(), f'{self.kept} is @prisma/other, not @prisma/streams')

    def assert_refused_in_both_modes(self, data, reason):
        self.data = data
        self.kept.write_bytes(data)
        self.mint()
        for mode in (('--dry-run',), ()):
            with self.subTest(mode=mode):
                self.log.write_text('')
                self.assert_refused(self.publish(*mode), reason)

    def test_a_tarball_whose_version_is_not_the_tags_is_refused(self):
        self.assert_refused_in_both_modes(
            tarball(version='9.9.9-beta.2'),
            f'{self.kept} is @prisma/streams@9.9.9-beta.2, but {TAG} names no release candidate of it '
            '(v9.9.9-beta.2-rc.<n>)')

    def test_a_readme_that_still_holds_a_placeholder_of_the_beta_is_refused(self):
        for line in ('  issuerUrl: "https://<the Streams beta issuer>", // published with the beta',
                     "`<the beta's limits page, published with the beta>`",
                     "`<Re-verify this section against storage layout 6 before publishing; its"):
            with self.subTest(line=line):
                self.assert_refused_in_both_modes(
                    tarball(readme=README + '\n' + line + '\n'),
                    f'{self.kept} README.md line 7 is a placeholder to fill before publishing')
                self.git('tag', '-d', TAG)

    def test_a_changelog_whose_first_release_is_not_this_version_is_refused(self):
        for heading in (f'## {VERSION} (unreleased)', '## 9.9.9-beta.0'):
            with self.subTest(heading=heading):
                self.assert_refused_in_both_modes(
                    tarball(changes=f'# Changelog\n\n{heading}\n'),
                    f"{self.kept} CHANGELOG.md's first release heading is '{heading}', not '## {VERSION}'")
                self.git('tag', '-d', TAG)

    def test_a_tarball_without_its_readme_or_changelog_is_refused(self):
        self.assert_refused_in_both_modes(tarball(readme=None), f'{self.kept} holds no package/README.md')
        self.git('tag', '-d', TAG)
        self.assert_refused_in_both_modes(tarball(changes=None), f'{self.kept} holds no package/CHANGELOG.md')

    def test_a_version_already_on_npm_is_refused(self):
        self.mint()
        self.assert_refused(self.publish(STUB_NPM_VIEW='published'),
                            f'@prisma/streams@{VERSION} is already published',
                            [f'npm view @prisma/streams@{VERSION} version{REGISTRY}'])

    def test_a_registry_that_cannot_answer_is_refused(self):
        self.mint()
        self.assert_refused(self.publish(STUB_NPM_VIEW='error'),
                            f'npm view @prisma/streams@{VERSION} failed',
                            [f'npm view @prisma/streams@{VERSION} version{REGISTRY}'])

    def test_a_token_in_the_environment_is_refused_before_publishing(self):
        self.mint()
        for name in ('NPM_TOKEN', 'NODE_AUTH_TOKEN', 'npm_config__authToken'):
            with self.subTest(name=name):
                self.log.write_text('')
                result = self.publish(**{name: 'npm_secret'})
                self.assert_refused(result, f'{name if name.isupper() else "an npm_config_*_auth* variable"} is set',
                                    [f'npm view @prisma/streams@{VERSION} version{REGISTRY}'])
                self.assertNotIn('npm_secret', result.stdout + result.stderr)

    def test_a_token_is_refused_whatever_else_the_environment_holds(self):
        # `env` lists the token first, then 500 KB more: a scan that stops
        # reading at its first match (grep -q) makes that writer die of
        # SIGPIPE, which under pipefail must not read as "no token". The
        # bulk is eight variables of 64,000 B: Linux's execve refuses any
        # one environment string over 128 KiB (MAX_ARG_STRLEN, E2BIG).
        self.executable(self.bin / 'env', '#!/bin/bash\nprintf "npm_config__authToken=npm_secret\\n"\nexec /usr/bin/env\n')
        self.mint()
        bulk = {f'PUBLISH_SDK_TEST_BULK_{n}': 'x' * 64_000 for n in range(8)}
        result = self.publish(npm_config__authToken='npm_secret', **bulk)
        self.assert_refused(result, 'an npm_config_*_auth* variable is set',
                            [f'npm view @prisma/streams@{VERSION} version{REGISTRY}'])
        self.assertNotIn('npm_secret', result.stdout + result.stderr)

    def test_a_registry_integrity_other_than_the_tarballs_fails_after_publishing(self):
        self.mint()
        wrong = integrity(b'other bytes')
        result = self.publish(STUB_REGISTRY_INTEGRITY=wrong)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"PUBLISH_SDK_FAIL: the registry's integrity for @prisma/streams@{VERSION} is {wrong}; "
                      f"the tarball's is {integrity(self.data)}", result.stdout)
        self.assertNotIn('PUBLISH_SDK_OK', result.stdout)

    def test_the_readback_waits_for_the_registry_to_name_the_version(self):
        self.mint()
        result = self.publish(STUB_READBACK_EMPTY='2')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(self.npm_calls().count(f'npm view @prisma/streams@{VERSION} dist.integrity{REGISTRY}'), 3)
        result_never = self.publish(STUB_READBACK_EMPTY='99', PUBLISH_SDK_READBACK_TRIES='3')
        self.assertIn(f'PUBLISH_SDK_FAIL: the registry names no integrity for @prisma/streams@{VERSION} '
                      'after 3 reads', result_never.stdout)


if __name__ == '__main__':
    unittest.main()
