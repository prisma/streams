"""CI runs the tests that pin Layer B's certifying refusals.

shared_verdict.py's `load` and `harness` legs and the probes leg's
first-contact bar are pinned by bench/k2cost's own tests: its Python unit
tests (test_shared_verdict.py, test_run_shared.py) and its Bun tests (the
legs on the fake cell, the tenants manifests, the pacing). Before the
owner's round-3 call 13 (2026-10-13) no gate or workflow ran either, so a
change that dropped a refusal landed green. That call puts them in CI's
`sdk-package` job (AGENTS.md section 1 (3)): the Python tests, and the Bun
tests beside ./deploy/cell-admin. This file holds the job to that.
"""
import re
import unittest

from common import ROOT
from test_sdk_runtimes import sdk_package_steps

PYTHON_TESTS = "python3 -m unittest discover -s bench/k2cost -p 'test_*.py'"


def command(step):
    """The step's one-line `run:` command, split into words, or None."""
    m = re.search(r'^\s+run: (?!\|)(.+)$', step, re.M)
    return m.group(1).split() if m else None


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


if __name__ == '__main__':
    unittest.main()
