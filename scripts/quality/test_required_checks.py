"""required-checks.sh: every attempt of every required job in the push runs at the commit succeeded.

Each test runs the real script with `gh` stubbed on PATH by
gh_actions_stub.GH_STUB, which answers the Actions API's runs and jobs
lists from the runs the test writes as the API does: the runs list's
`head_sha` and `event` filters, the jobs list's latest-attempt default,
and pages.
"""
import json
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from gh_actions_stub import GH_STUB, ROOT, ci_jobs, job, push_runs, required_checks, run

SHA = 'a' * 40
OTHER = 'b' * 40
RUNS_CALL = f'gh api repos/{{owner}}/{{repo}}/actions/runs?head_sha={SHA}&event=push&per_page=100 --paginate --slurp'


def jobs_call(run_id):
    return f'gh api repos/{{owner}}/{{repo}}/actions/runs/{run_id}/jobs?filter=all&per_page=100 --paginate --slurp'


def ok_lines():
    return ''.join(f'ok   {name}\n' for name in required_checks())


def green_line():
    return f'REQUIRED_CHECKS_GREEN {SHA}: ' + ' '.join(required_checks()) + '\n'


class RequiredChecks(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix='required-checks-')).resolve()
        self.addCleanup(shutil.rmtree, self.tmp)
        gh = self.tmp / 'bin' / 'gh'
        gh.parent.mkdir()
        gh.write_text(GH_STUB)
        gh.chmod(0o755)
        self.log = self.tmp / 'calls.log'
        self.log.touch()
        self.runs = self.tmp / 'runs.json'

    def check(self, runs, **env):
        self.runs.write_text(json.dumps(runs))
        return subprocess.run(['bash', str(ROOT / 'scripts' / 'required-checks.sh'), SHA], cwd=self.tmp,
                              env={**os.environ, 'PATH': f'{self.tmp / "bin"}:{os.environ["PATH"]}',
                                   'STUB_LOG': str(self.log), 'GH_STUB_RUNS': str(self.runs), **env},
                              text=True, capture_output=True)

    def assert_green(self, result):
        self.assertEqual((result.returncode, result.stdout), (0, ok_lines() + green_line()), result.stderr)

    def assert_refused(self, result, fail_line, name):
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        before, after = ok_lines().split(f'ok   {name}\n')
        self.assertEqual(result.stdout, before + fail_line + '\n' + after + f'REQUIRED_CHECKS_FAIL {SHA}\n')

    def test_the_push_runs_green_in_every_required_job_are_green(self):
        # The nine names promote-rc has required since round 9, unchanged.
        self.assertEqual(required_checks(), [
            'rust', 'livefeed', 'livefeed-fleet-cert', 'platform-e2e', 'mt-cert-1000',
            'durable-streams-server-conformance', 'product-field-gate', 'sdk-package', 'actionlint'])
        self.assert_green(self.check(push_runs(SHA)))
        self.assertEqual(self.log.read_text().splitlines(),
                         [RUNS_CALL, jobs_call(1001), jobs_call(1002), jobs_call(1003)])

    def test_a_required_job_red_only_in_a_nightly_run_at_the_commit_is_accepted(self):
        nightly = [run(2001, 'ci.yml', SHA, ci_jobs(rust='failure', noisy_campaign='failure'), event='schedule'),
                   run(2002, 'rust-quality.yml', SHA, [job('quality'), job('mutants (0)', 'failure')],
                       event='schedule')]
        self.assert_green(self.check(push_runs(SHA) + nightly))
        self.assertNotIn(jobs_call(2001), self.log.read_text())

    def test_a_red_push_run_of_another_commit_is_not_this_commits_evidence(self):
        self.assert_green(self.check(push_runs(SHA) + [run(3001, 'ci.yml', OTHER, ci_jobs(rust='failure'))]))

    def test_a_red_job_no_release_candidate_requires_does_not_refuse(self):
        runs = push_runs(SHA, ci=ci_jobs(architecture_report='failure'))
        runs[1]['jobs'] = [job('quality'), job('mutants (0)', 'failure')]
        self.assert_green(self.check(runs))

    def test_a_red_attempt_hidden_by_a_green_rerun_is_refused(self):
        result = self.check(push_runs(SHA, ci=ci_jobs(1, rust='failure') + ci_jobs(2)))
        self.assert_refused(result, 'FAIL: rust: attempt 1 of push run 1001 (.github/workflows/ci.yml) '
                            'concluded failure (gh run view 1001 --attempt 1 --log-failed)', 'rust')

    def test_a_red_required_job_on_the_second_page_of_a_runs_jobs_is_refused(self):
        # 105 jobs in attempt 1, all green, fill the first page of 100; the
        # rerun's red `rust` is on the second.
        attempt_1 = ci_jobs(1) + [job(f'matrix ({k})') for k in range(95)]
        result = self.check(push_runs(SHA, ci=attempt_1 + ci_jobs(2, rust='failure')))
        self.assert_refused(result, 'FAIL: rust: attempt 2 of push run 1001 (.github/workflows/ci.yml) '
                            'concluded failure (gh run view 1001 --attempt 2 --log-failed)', 'rust')

    def test_a_required_job_missing_from_the_push_runs_is_refused(self):
        result = self.check(push_runs(SHA)[:2])
        self.assert_refused(result, f'FAIL: actionlint: no job of that name in the push runs at {SHA}',
                            'actionlint')

    def test_a_required_job_still_running_is_refused(self):
        ci = [job('platform-e2e', status='in_progress') if j['name'] == 'platform-e2e' else j for j in ci_jobs()]
        result = self.check(push_runs(SHA, ci=ci))
        self.assert_refused(result, 'FAIL: platform-e2e: attempt 1 of push run 1001 (.github/workflows/ci.yml) '
                            'is in_progress', 'platform-e2e')

    def test_an_api_that_cannot_answer_is_never_green(self):
        result = self.check(push_runs(SHA), GH_STUB_FAIL='1')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')
        self.assertIn('HTTP 502: Server Error', result.stderr)


if __name__ == '__main__':
    unittest.main()
