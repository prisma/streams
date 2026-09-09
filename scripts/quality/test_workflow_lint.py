"""Exercise actionlint's real project discovery; do not duplicate its parser."""
from pathlib import Path
import subprocess
import tempfile
import unittest


class WorkflowLint(unittest.TestCase):
    def test_both_workflow_extensions_and_valid_control(self):
        valid = 'on: push\njobs:\n  check:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo valid\n'
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / '.git').mkdir()
            workflows = root / '.github/workflows'
            workflows.mkdir(parents=True)
            for suffix in ('yml', 'yaml'):
                workflow = workflows / f'control.{suffix}'
                workflow.write_text(valid)
            def lint():
                return subprocess.run(['actionlint'], cwd=root, text=True,
                                      stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            self.assertEqual(lint().returncode, 0)
            for suffix in ('yml', 'yaml'):
                workflow = workflows / f'control.{suffix}'
                workflow.write_text(valid.replace('runs-on:', 'runs-onn:'))
                result = lint()
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(f'control.{suffix}', result.stdout)
                self.assertIn('runs-onn', result.stdout)
                workflow.write_text(valid)
            self.assertEqual(lint().returncode, 0)


if __name__ == '__main__':
    unittest.main()
