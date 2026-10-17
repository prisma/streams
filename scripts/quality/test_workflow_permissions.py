"""Every workflow grants its GITHUB_TOKEN read access to the repository only.

Without a top-level `permissions:` block a job's token gets the
repository's default grant, which may include writes. No job here writes
through the token (no release, comment, artifact, package or OIDC step),
so every workflow declares `permissions: contents: read` at its top level
and no block anywhere grants a write.
"""
from pathlib import Path
import re
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
TOP_LEVEL = ['contents: read']
PERMISSIONS = re.compile(r'^(\s*)permissions:\s*(.*?)\s*(#.*)?$')


def permission_blocks(text):
    """[(line, indent, entries)] for every `permissions:` key of a workflow."""
    lines = text.splitlines()
    blocks = []
    for number, line in enumerate(lines, 1):
        match = PERMISSIONS.match(line)
        if not match:
            continue
        indent, scalar = len(match.group(1)), match.group(2)
        entries = [scalar] if scalar else []
        for child in lines[number:]:
            stripped = child.split('#', 1)[0].rstrip()
            if not stripped:
                continue
            if len(stripped) - len(stripped.lstrip()) <= indent:
                break
            entries.append(stripped.strip())
        blocks.append((number, indent, entries))
    return blocks


def findings(root):
    workflows = root / '.github/workflows'
    found = []
    for workflow in sorted([*workflows.glob('*.yml'), *workflows.glob('*.yaml')]):
        place = workflow.relative_to(root)
        blocks = permission_blocks(workflow.read_text())
        top = [entries for _, indent, entries in blocks if indent == 0]
        if top != [TOP_LEVEL]:
            found.append(f'{place}: top-level permissions {top[0] if top else "missing"}, not {TOP_LEVEL}')
        for number, _, entries in blocks:
            for entry in entries:
                if 'write' in entry:
                    found.append(f'{place}:{number}: grants {entry!r}')
    return found


class WorkflowPermissions(unittest.TestCase):
    def test_every_workflow_grants_its_token_contents_read_only(self):
        self.assertTrue(list((ROOT / '.github/workflows').glob('*.yml')), 'no workflow under .github/workflows')
        self.assertEqual(findings(ROOT), [])

    def test_a_missing_block_a_wider_one_and_a_job_write_are_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / '.github/workflows'
            workflows.mkdir(parents=True)
            (workflows / 'a.yml').write_text('on: push\njobs:\n  x:\n    runs-on: ubuntu-24.04\n')
            (workflows / 'b.yaml').write_text('permissions: write-all\njobs: {}\n')
            (workflows / 'c.yml').write_text(
                'permissions:\n  contents: read  # the default\njobs:\n  x:\n'
                '    permissions:\n      pull-requests: write\n')
            (workflows / 'd.yml').write_text('permissions:\n  contents: read\njobs: {}\n')
            self.assertEqual(findings(root), [
                ".github/workflows/a.yml: top-level permissions missing, not ['contents: read']",
                ".github/workflows/b.yaml: top-level permissions ['write-all'], not ['contents: read']",
                ".github/workflows/b.yaml:1: grants 'write-all'",
                ".github/workflows/c.yml:5: grants 'pull-requests: write'",
            ])


if __name__ == '__main__':
    unittest.main()
