"""Every workflow installs one pinned Bun release, never whatever is newest.

CI installs the Composer apps' packages with `bun install
--frozen-lockfile` (deploy/composer and deploy/composer-issuer, whose
bun.lock files Bun 1.4.3 installs as written), and a Bun that would
rewrite a lockfile refuses it. So a new Bun must not reach CI unannounced
under a release candidate, for the reason test_workflow_runners.py pins
Ubuntu: the owner's round-3 call 14 (2026-10-13) pinned setup-bun to
1.4.3. The same step runs the SDK's Bun smoke, so the pin also fixes which
Bun the SDK README's gated "Bun" means. Moving it is a deliberate commit
that edits PINNED and re-verifies both lockfiles.
"""
from pathlib import Path
import re
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
PINNED = '1.4.3'
SETUP_BUN = re.compile(r'\buses:\s*oven-sh/setup-bun@')
ITEM = re.compile(r'^(\s*)- ')
VERSION = re.compile(r'^\s*bun-version:\s*(.*?)\s*(?:#.*)?$')


def bun_versions(root):
    """[(path:line, bun-version or None)] for every oven-sh/setup-bun step."""
    workflows = root / '.github/workflows'
    found = []
    for workflow in sorted([*workflows.glob('*.yml'), *workflows.glob('*.yaml')]):
        lines = workflow.read_text().splitlines()
        for number, line in enumerate(lines, 1):
            if not SETUP_BUN.search(line):
                continue
            start = next(i for i in range(number - 1, -1, -1) if ITEM.match(lines[i]))
            indent = len(ITEM.match(lines[start]).group(1))
            end = next((i for i in range(start + 1, len(lines))
                        if lines[i].strip() and len(lines[i]) - len(lines[i].lstrip()) <= indent),
                       len(lines))
            versions = [m.group(1).strip('\'"') for m in map(VERSION.match, lines[start:end]) if m]
            found.append((f'{workflow.relative_to(root)}:{number}', versions[0] if versions else None))
    return found


def unpinned(root):
    return [f'{place}: {version!r}' for place, version in bun_versions(root) if version != PINNED]


class WorkflowBun(unittest.TestCase):
    def test_every_setup_bun_step_installs_the_pinned_release(self):
        self.assertTrue(bun_versions(ROOT), 'no oven-sh/setup-bun step under .github/workflows')
        self.assertEqual(unpinned(ROOT), [], f'every setup-bun step must set bun-version: {PINNED}')

    def test_a_missing_moving_or_other_version_in_either_extension_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / '.github/workflows'
            workflows.mkdir(parents=True)
            (workflows / 'a.yml').write_text(
                'jobs:\n  a:\n    steps:\n'
                f'      - uses: oven-sh/setup-bun@v2\n        with:\n          bun-version: {PINNED}  # pinned\n'
                '      - uses: oven-sh/setup-bun@v2\n'
                '      - name: next\n        with:\n          bun-version: 9.9.9\n'
                '        uses: oven-sh/setup-bun@v2\n'
                f"      - name: quoted\n        uses: oven-sh/setup-bun@v2\n        with:\n          bun-version: '{PINNED}'\n")
            (workflows / 'b.yaml').write_text(
                'jobs:\n  b:\n    steps:\n'
                '      - uses: oven-sh/setup-bun@v2\n        with:\n          bun-version: latest\n'
                '      - uses: actions/setup-node@v4\n        with:\n          bun-version: 9.9.9\n')
            self.assertEqual(unpinned(root), [
                ".github/workflows/a.yml:7: None",
                ".github/workflows/a.yml:11: '9.9.9'",
                ".github/workflows/b.yaml:4: 'latest'",
            ])


if __name__ == '__main__':
    unittest.main()
