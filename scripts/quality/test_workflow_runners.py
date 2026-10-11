"""Every workflow job runs on one pinned Ubuntu release, never a moving label.

GitHub moves `ubuntu-latest` to Ubuntu 26 from 2026-10-19
(actions/runner-images#14748). Kani, TLC, cargo-mutants, prlimit and the
Python 3.11+ tools are validated on ubuntu-24.04 only, so moving the image
is a deliberate commit that edits PINNED, never a label GitHub moves.
"""
from pathlib import Path
import re
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PINNED = 'ubuntu-24.04'
RUNS_ON = re.compile(r'^\s*runs-on:(.*)$')


def runner_labels(root):
    """[(path:line, label)] for every `runs-on:` key of a workflow file."""
    workflows = root / '.github/workflows'
    labels = []
    for workflow in sorted([*workflows.glob('*.yml'), *workflows.glob('*.yaml')]):
        for number, line in enumerate(workflow.read_text().splitlines(), 1):
            match = RUNS_ON.match(line)
            if match:
                label = match.group(1).split('#', 1)[0].strip()
                labels.append((f'{workflow.relative_to(root)}:{number}', label))
    return labels


def unpinned(root):
    return [f'{place}: {label!r}' for place, label in runner_labels(root) if label != PINNED]


class WorkflowRunners(unittest.TestCase):
    def test_every_job_runs_on_the_pinned_ubuntu_release(self):
        self.assertTrue(runner_labels(ROOT), 'no runs-on key under .github/workflows')
        self.assertEqual(unpinned(ROOT), [], f'every job must run on {PINNED}')

    def test_a_moving_or_list_label_in_either_extension_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / '.github/workflows'
            workflows.mkdir(parents=True)
            (workflows / 'a.yml').write_text(
                f'jobs:\n  ok:\n    runs-on: {PINNED}  # pinned\n'
                '  moving:\n    runs-on: ubuntu-latest\n')
            (workflows / 'b.yaml').write_text(f'jobs:\n  list:\n    runs-on: [{PINNED}]\n')
            self.assertEqual(unpinned(root), [
                ".github/workflows/a.yml:5: 'ubuntu-latest'",
                f".github/workflows/b.yaml:3: '[{PINNED}]'",
            ])


if __name__ == '__main__':
    unittest.main()
