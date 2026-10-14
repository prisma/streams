"""CI boots the shared cell's Composer entry and the issuer's.

Before the owner's round-3 call 14 (2026-10-13) nothing gated the cell's
deployable entry: CI's Bun line runs ./deploy/composer without its
packages and without CELL_ADMIN_E2E_BIN_DIR, so entry.e2e.test.ts skipped
its bundle test, its refusals and its good boot, and the issuer's bundled
entry tests (ISSUER_COMPOSER_E2E) ran nowhere. That call puts both in CI's
`sdk-package` job after cell-admin's end-to-end step (AGENTS.md section
1 (3)): a frozen, uncached install of both Composer apps, then the cell's
entry on the release binaries with COMPOSER_E2E_REQUIRE_BOOT=1, so a good
boot that cannot run fails, and the issuer's opt-in tests. This file holds
the job to that.
"""
import re
import unittest

from common import ROOT
from test_sdk_runtimes import sdk_package_steps

CELL_ADMIN_E2E = 'CELL_ADMIN_E2E_BIN_DIR=target/release bun test ./deploy/cell-admin/e2e.test.ts'
INSTALL = [
    '(cd deploy/composer && bun install --frozen-lockfile)',
    '(cd deploy/composer-issuer && bun install --frozen-lockfile)',
]
ENTRY_E2E = ('CELL_ADMIN_E2E_BIN_DIR=target/release COMPOSER_E2E_REQUIRE_BOOT=1 '
             'bun test ./deploy/composer/streams/entry.e2e.test.ts')
ISSUER_E2E = 'ISSUER_COMPOSER_E2E=1 bun test ./deploy/issuer/composer.test.ts'


def commands(step):
    """The step's `run:` commands, one per line, whether inline or a block.

    A step from sdk_package_steps starts after its `- `, so its first key
    sits at column 0; a block's lines are those indented at least as deep
    as its first one, as YAML reads a literal block.
    """
    lines = step.splitlines()
    for i, line in enumerate(lines):
        m = re.match(r'^\s*run:\s*(.*)$', line)
        if not m:
            continue
        if m.group(1) not in ('|', '>'):
            return [m.group(1).strip()]
        block, depth = [], None
        for body in filter(str.strip, lines[i + 1:]):
            width = len(body) - len(body.lstrip())
            depth = width if depth is None else depth
            if width < depth:
                break
            block.append(body.strip())
        return block
    return []


def first_running(steps, command):
    """The index of the first step whose `run:` includes `command`, or None."""
    return next((i for i, step in enumerate(steps) if command in commands(step)), None)


class ComposerEntriesInCi(unittest.TestCase):
    def setUp(self):
        self.steps = sdk_package_steps((ROOT / '.github/workflows/ci.yml').read_text())

    def test_both_composer_apps_install_frozen_after_cell_admins_end_to_end(self):
        after = first_running(self.steps, CELL_ADMIN_E2E)
        self.assertIsNotNone(after, 'no cell-admin end-to-end step')
        self.assertEqual([first_running(self.steps, c) for c in INSTALL], [after + 1, after + 1])

    def test_the_cells_entry_then_the_issuers_run_after_the_install(self):
        install = first_running(self.steps, INSTALL[0])
        self.assertIsNotNone(install, 'no install step')
        self.assertEqual([first_running(self.steps, c) for c in (ENTRY_E2E, ISSUER_E2E)],
                         [install + 1, install + 1])
        self.assertEqual(commands(self.steps[install + 1]), [ENTRY_E2E, ISSUER_E2E])

    def test_no_step_of_the_job_caches_packages(self):
        self.assertEqual([s.splitlines()[0] for s in self.steps if 'actions/cache' in s], [])

    def test_a_block_and_an_inline_run_are_read_line_by_line(self):
        steps = sdk_package_steps(
            'jobs:\n  sdk-package:\n    steps:\n'
            '      - name: x\n        run: |\n          a b\n\n          c\n        env:\n          K: v\n'
            '      - run: |\n          d\n        env:\n          K: v\n'
            '      - run: e f  \n'
            '      - uses: g\n')
        self.assertEqual([commands(s) for s in steps], [['a b', 'c'], ['d'], ['e f'], []])


if __name__ == '__main__':
    unittest.main()
