import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import common


class EventComparison(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git('init', '-q')
        self.git('config', 'user.email', 'quality@example.test')
        self.git('config', 'user.name', 'Quality Fixture')
        self.write('one')
        self.git('add', 'owner.rs')
        self.git('commit', '-qm', 'one')
        self.first = self.git('rev-parse', 'HEAD')
        self.git('branch', 'target')
        self.write('two')
        self.git('commit', '-qam', 'two')
        self.second = self.git('rev-parse', 'HEAD')

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def write(self, value):
        (self.root / 'owner.rs').write_text(value + '\n')

    def resolve(self, **env):
        environment = {'PATH': os.environ.get('PATH', '')}
        environment.update(env)
        with patch.object(common, 'ROOT', self.root), patch.dict(os.environ, environment, clear=True):
            return common.verification_comparison()

    def ratchet_base(self, **env):
        environment = {'PATH': os.environ.get('PATH', '')}
        environment.update(env)
        with patch.object(common, 'ROOT', self.root), patch.dict(os.environ, environment, clear=True):
            return common.merge_base()

    def test_a_push_ratchet_without_the_previous_revision_fails_closed(self):
        # On a push origin/<branch> is the pushed commit itself: without the
        # event's previous revision the ratchet would compare HEAD with HEAD.
        self.git('update-ref', 'refs/remotes/origin/slate', 'HEAD')
        with self.assertRaisesRegex(ValueError, 'push ratchet requires QUALITY_BEFORE_SHA'):
            self.ratchet_base(GITHUB_EVENT_NAME='push', QUALITY_BASE_REF='origin/slate')

    def test_push_and_local_ratchets_keep_their_bases(self):
        pushed = self.ratchet_base(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA=self.first,
                                   QUALITY_BASE_REF='origin/slate')
        self.assertEqual(pushed, self.first)
        self.assertEqual(self.ratchet_base(QUALITY_BASE_REF='target'), self.first)
        created = self.ratchet_base(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA='0' * 40,
                                    QUALITY_BASE_REF='target')
        self.assertEqual(created, self.first)

    def test_pull_request_uses_target_merge_base(self):
        result = self.resolve(QUALITY_EVENT_NAME='pull_request', QUALITY_BASE_REF='target')
        self.assertEqual(result.comparison_revision, self.first)
        self.assertEqual(result.kind, 'pull-request-merge-base')

    def test_normal_and_multi_commit_push_use_exact_previous_revision(self):
        result = self.resolve(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA=self.first,
                              QUALITY_HEAD_SHA=self.second)
        self.assertEqual(result.comparison_revision, self.first)
        self.assertEqual(result.kind, 'push-previous-revision')

    def test_force_push_keeps_exact_previous_tree(self):
        self.git('checkout', '-q', 'target')
        self.write('replacement')
        self.git('commit', '-qam', 'replacement')
        replacement = self.git('rev-parse', 'HEAD')
        result = self.resolve(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA=self.second,
                              QUALITY_HEAD_SHA=replacement)
        self.assertEqual(result.comparison_revision, self.second)
        self.assertEqual(result.kind, 'push-force-update')

    def test_branch_creation_uses_empty_tree(self):
        result = self.resolve(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA='0' * 40)
        self.assertEqual(result.kind, 'push-branch-creation')
        self.assertEqual(self.git('cat-file', '-t', result.comparison_revision), 'tree')

    def test_missing_previous_push_revision_fails_closed(self):
        with self.assertRaisesRegex(ValueError, 'previous push revision is unavailable'):
            self.resolve(QUALITY_EVENT_NAME='push', QUALITY_BEFORE_SHA='1' * 40)

    def test_schedule_declares_rotation_without_fake_diff(self):
        result = self.resolve(QUALITY_EVENT_NAME='schedule')
        self.assertEqual(result.comparison_revision, '')
        self.assertEqual(result.kind, 'scheduled-owner-rotation')


if __name__ == '__main__':
    unittest.main()
