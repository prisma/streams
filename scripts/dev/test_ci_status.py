"""ci-status.sh prints CI_GREEN from the push runs at the commit, never from a nightly run.

Each test runs the real script in a disposable Git repository with `gh`
stubbed on PATH. The stub answers `gh run list` as gh does: the runs of
the named workflow file at the commit, newest first, of the named event
when `--event` is given, cut to `--limit`, rendered by the script's own
`--jq` template.
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GH_STUB = r'''#!/usr/bin/env python3
import json, os, re, sys
args = sys.argv[1:]
def refuse(why):
    sys.exit(f'stub gh: {why}: ' + ' '.join(args))
options = dict(zip(args[2::2], args[3::2]))
if args[:2] != ['run', 'list'] or len(args) % 2 or \
        set(options) - {'--workflow', '--commit', '--event', '--limit', '--json', '--jq'}:
    refuse('unexpected call')
template = re.fullmatch(r'\.\[0\] \| select\(\. != null\) \| "(.*)"', options['--jq'])
if template is None:
    refuse('unexpected --jq')
runs = [run for run in json.loads(open(os.environ['GH_STUB_RUNS']).read())
        if run['workflow'] == options['--workflow'] and run['headSha'] == options['--commit']
        and run['event'] == options.get('--event', run['event'])][:int(options['--limit'])]
if runs:
    fields = {name: runs[0][name] for name in options['--json'].split(',')}
    print(re.sub(r'\\\(\.(\w+)\)', lambda m: str(fields[m.group(1)]), template.group(1)).replace('\\t', '\t'))
'''


def run(run_id, workflow, sha, conclusion='success', event='push', status='completed'):
    return {'databaseId': run_id, 'workflow': workflow, 'headSha': sha, 'event': event, 'status': status,
            'conclusion': conclusion if status == 'completed' else '',
            'url': f'https://github.com/prisma/streams/actions/runs/{run_id}'}


def line(workflow, run_id, conclusion='success', status='completed'):
    return f'  {workflow:<18} {status:<12} {conclusion:<10} https://github.com/prisma/streams/actions/runs/{run_id}\n'


class CiStatus(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='ci-status-')).resolve()
        self.addCleanup(shutil.rmtree, self.tmp)
        gh = self.tmp / 'bin' / 'gh'
        gh.parent.mkdir()
        gh.write_text(GH_STUB)
        gh.chmod(0o755)
        self.repo = self.tmp / 'repo'
        self.repo.mkdir()
        for args in (['init', '-q'], ['config', 'user.name', 'ci-status fixture'],
                     ['config', 'user.email', 'fixture@example.invalid'], ['config', 'commit.gpgsign', 'false'],
                     ['config', 'core.hooksPath', str(self.tmp / 'no-hooks')],
                     ['commit', '-q', '--allow-empty', '-m', 'fixture']):
            subprocess.run(['git', '-C', str(self.repo), *args], check=True)
        self.sha = subprocess.check_output(['git', '-C', str(self.repo), 'rev-parse', 'HEAD'], text=True).strip()
        self.runs = self.tmp / 'runs.json'

    def status(self, runs):
        """`runs` newest first, as gh lists them."""
        self.runs.write_text(json.dumps(runs))
        return subprocess.run(['bash', str(ROOT / 'scripts' / 'dev' / 'ci-status.sh'), self.sha], cwd=self.repo,
                              env={**os.environ, 'PATH': f'{self.tmp / "bin"}:{os.environ["PATH"]}',
                                   'GH_STUB_RUNS': str(self.runs)},
                              text=True, capture_output=True)

    def test_a_red_nightly_run_at_a_commit_whose_push_runs_are_green_is_ci_green(self):
        result = self.status([run(2002, 'rust-quality.yml', self.sha, 'failure', event='schedule'),
                              run(2001, 'ci.yml', self.sha, 'failure', event='schedule'),
                              run(1003, 'workflow-lint.yml', self.sha), run(1002, 'rust-quality.yml', self.sha),
                              run(1001, 'ci.yml', self.sha)])
        self.assertEqual((result.returncode, result.stdout),
                         (0, f'commit {self.sha}\n' + line('ci.yml', 1001) + line('rust-quality.yml', 1002)
                          + line('workflow-lint.yml', 1003) + 'CI_GREEN\n'), result.stderr)

    def test_a_red_push_run_behind_a_newer_green_nightly_is_ci_fail(self):
        result = self.status([run(2002, 'rust-quality.yml', self.sha, event='schedule'),
                              run(1003, 'workflow-lint.yml', self.sha),
                              run(1002, 'rust-quality.yml', self.sha, 'failure'), run(1001, 'ci.yml', self.sha)])
        self.assertEqual((result.returncode, result.stdout),
                         (1, f'commit {self.sha}\n' + line('ci.yml', 1001) + line('rust-quality.yml', 1002, 'failure')
                          + 'CI_FAIL: rust-quality.yml concluded failure (gh run view 1002 --log-failed)\n'
                          + line('workflow-lint.yml', 1003)), result.stderr)

    def test_a_workflow_with_only_a_nightly_run_at_the_commit_is_pending(self):
        result = self.status([run(2001, 'ci.yml', self.sha, event='schedule'),
                              run(1003, 'workflow-lint.yml', self.sha), run(1002, 'rust-quality.yml', self.sha)])
        self.assertEqual((result.returncode, result.stdout),
                         (1, f'commit {self.sha}\nCI_PENDING: no ci.yml push run for this commit yet\n'
                          + line('rust-quality.yml', 1002) + line('workflow-lint.yml', 1003)), result.stderr)


if __name__ == '__main__':
    unittest.main()
