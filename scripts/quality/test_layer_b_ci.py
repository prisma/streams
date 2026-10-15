"""CI runs the tests that pin Layer B's certifying refusals, and the
Compute lifecycle checklist's.

shared_verdict.py's `load` and `harness` legs and the probes leg's
first-contact bar are pinned by bench/k2cost's own tests: its Python unit
tests (test_shared_verdict.py, test_run_shared.py) and its Bun tests (the
legs on the fake cell, the tenants manifests, the pacing). Before the
owner's round-3 call 13 (2026-10-13) no gate or workflow ran either, so a
change that dropped a refusal landed green. That call puts them in CI's
`sdk-package` job (AGENTS.md section 1 (3)): the Python tests, and the Bun
tests beside ./deploy/cell-admin. This file holds the job to that.

bench/lifecycle's tests (the Compute lifecycle checklist's legs on fakes,
its verdicts, the real SIGTERM probe, and sdk-probe.mjs on the built SDK)
ran nowhere either, so a change to the owner's bars or a drift from the
deploy tools' flags showed only on a field run. The owner's round-6 call 8
(2026-10-15, AGENTS.md section 1 (3)) puts them in the same job: the
Python tests with LIFECYCLE_REQUIRE_PROBES=1, so a missing bun, node or
sdk/dist fails them instead of skipping, after the SDK's build, Bun's
setup and the Node 18 setup (the engines floor, named in the step: it is
the Node the SDK probe runs on); and the probe's Bun tests on the line
beside ./deploy/cell-admin.
"""
import re
import unittest

from common import ROOT
from test_sdk_runtimes import sdk_package_steps

PYTHON_TESTS = "python3 -m unittest discover -s bench/k2cost -p 'test_*.py'"
LIFECYCLE_TESTS = "LIFECYCLE_REQUIRE_PROBES=1 python3 -m unittest discover -s bench/lifecycle -p 'test_*.py'"


def command(step):
    """The step's one-line `run:` command, split into words, or None."""
    m = re.search(r'^\s+run: (?!\|)(.+)$', step, re.M)
    return m.group(1).split() if m else None


def name(step):
    """The step's `name:` when it is the step's first key, or None (a step
    from sdk_package_steps starts after its `- `)."""
    m = re.match(r'name: (.+)', step)
    return m.group(1).strip() if m else None


class LayerBTestsInCi(unittest.TestCase):
    def setUp(self):
        steps = sdk_package_steps((ROOT / '.github/workflows/ci.yml').read_text())
        self.commands = [words for words in map(command, steps) if words]

    def test_the_sdk_package_job_runs_bench_k2costs_python_unit_tests(self):
        self.assertIn(PYTHON_TESTS, [' '.join(words) for words in self.commands])

    def test_its_bun_tests_run_beside_cell_admins(self):
        bun = [words for words in self.commands if words[:2] == ['bun', 'test']]
        beside = [words for words in bun if './deploy/cell-admin' in words]
        self.assertEqual(len(beside), 1, bun)
        self.assertIn('./bench/k2cost', beside[0])


class LifecycleTestsInCi(unittest.TestCase):
    def setUp(self):
        self.steps = sdk_package_steps((ROOT / '.github/workflows/ci.yml').read_text())

    def lifecycle_step(self):
        """The index of the one step that runs bench/lifecycle's tests."""
        found = [i for i, step in enumerate(self.steps) if ' '.join(command(step) or []) == LIFECYCLE_TESTS]
        self.assertEqual(len(found), 1, f'one step runs {LIFECYCLE_TESTS!r}')
        return found[0]

    def first(self, what, test):
        """The index of the first step for which test(step) holds."""
        found = next((i for i, step in enumerate(self.steps) if test(step)), None)
        self.assertIsNotNone(found, f'no {what} step')
        return found

    def test_the_sdk_package_job_runs_bench_lifecycles_python_tests_with_the_probes_required(self):
        self.lifecycle_step()

    def test_they_run_after_the_sdk_build_and_bun_and_on_the_node_18_the_step_names(self):
        at = self.lifecycle_step()
        build = self.first('SDK build', lambda step: name(step) == 'Typecheck + build + pack')
        bun = self.first('setup-bun', lambda step: step.startswith('uses: oven-sh/setup-bun@'))
        self.assertIn('npm run build', self.steps[build])
        self.assertLess(build, at)
        self.assertLess(bun, at)
        nodes = [re.search(r'node-version:\s*(\S+)', step).group(1)
                 for step in self.steps[:at] if step.startswith('uses: actions/setup-node@')]
        self.assertEqual(nodes[-1:], ['18'], 'the last Node set up before the step')
        self.assertIn('Node 18', name(self.steps[at]) or '')

    def test_the_sigterm_probes_bun_tests_run_beside_cell_admins(self):
        bun = [words for words in map(command, self.steps) if words and words[:2] == ['bun', 'test']]
        beside = [words for words in bun if './deploy/cell-admin' in words]
        self.assertEqual(len(beside), 1, bun)
        self.assertIn('./bench/lifecycle/sigterm-probe', beside[0])


if __name__ == '__main__':
    unittest.main()
