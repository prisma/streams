"""Exercise the actual build script with Cargo and disposable Git repositories."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from common import ROOT


class BuildIdentity(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='streams-build-identity-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def git(self, directory, *args):
        return subprocess.check_output(['git', '-C', str(directory), *args], stderr=subprocess.PIPE, text=True).strip()

    def source(self, name, repository=True):
        directory = self.root / name
        (directory / 'src').mkdir(parents=True)
        shutil.copy2(ROOT / 'build.rs', directory / 'build.rs')
        (directory / 'Cargo.toml').write_text('[package]\nname="build-identity-fixture"\nversion="0.0.0"\nedition="2024"\n')
        (directory / 'Cargo.lock').write_text('version = 4\n\n[[package]]\nname = "build-identity-fixture"\nversion = "0.0.0"\n')
        (directory / 'src/main.rs').write_text('fn main() { println!("{}\\n{}", env!("STREAMS_GIT_COMMIT"), env!("STREAMS_BUILD_UNIX")); }\n')
        (directory / '.gitignore').write_text('/target\n')
        if repository:
            self.git(directory, 'init', '-q', '--initial-branch=main')
            self.git(directory, 'config', 'user.name', 'Build identity fixture')
            self.git(directory, 'config', 'user.email', 'fixture@example.invalid')
            self.git(directory, 'config', 'commit.gpgsign', 'false')
            self.git(directory, 'config', 'core.hooksPath', str(self.root / 'no-hooks'))
            self.git(directory, 'add', '.')
            self.git(directory, 'commit', '-qm', 'initial')
        return directory

    def build(self, directory, expected, *, fresh=False, override=None, epoch='123456789'):
        environment = os.environ.copy()
        environment.pop('STREAMS_GIT_COMMIT', None)
        environment.pop('CARGO_TARGET_DIR', None)
        environment['SOURCE_DATE_EPOCH'] = epoch
        if override is not None:
            environment['STREAMS_GIT_COMMIT'] = override
        result = subprocess.run(['cargo', 'build', '--locked', '--offline', '-vv', '--message-format=json'],
                                cwd=directory, env=environment, capture_output=True, text=True, check=True)
        rows = [json.loads(line) for line in result.stdout.splitlines()
                if not line.startswith('[build-identity-fixture 0.0.0] ')]
        binary = next(row for row in rows if row.get('reason') == 'compiler-artifact' and row.get('executable'))
        output = subprocess.check_output([binary['executable']], text=True).splitlines()
        self.assertEqual(output, [expected, epoch])
        if fresh:
            self.assertTrue(binary['fresh'], result.stderr)
            self.assertFalse(any('Running ' in line and 'build-script-build' in line
                                 for line in result.stderr.splitlines()), result.stderr)

    def test_normal_checkout_rebuilds_only_when_identity_changes(self):
        directory = self.source('normal')
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'))
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'), fresh=True)
        self.git(directory, 'commit', '--allow-empty', '-qm', 'next')
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'))
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'), fresh=True)

    def test_linked_worktree_uses_its_head_and_shared_branch_ref(self):
        repository = self.source('normal')
        linked = self.root / 'linked'
        self.git(repository, 'worktree', 'add', '-q', '-b', 'codex/fixture', str(linked))
        self.assertTrue((linked / '.git').is_file())
        self.build(linked, self.git(linked, 'rev-parse', 'HEAD'))
        self.build(linked, self.git(linked, 'rev-parse', 'HEAD'), fresh=True)
        self.git(linked, 'commit', '--allow-empty', '-qm', 'linked next')
        self.build(linked, self.git(linked, 'rev-parse', 'HEAD'))
        self.build(linked, self.git(linked, 'rev-parse', 'HEAD'), fresh=True)
        self.assertNotEqual(self.git(repository, 'rev-parse', 'HEAD'), self.git(linked, 'rev-parse', 'HEAD'))

    def test_packed_ref_then_new_loose_ref_updates_identity(self):
        directory = self.source('packed')
        self.git(directory, 'pack-refs', '--all')
        self.assertFalse((directory / '.git/refs/heads/main').exists())
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'))
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'), fresh=True)
        self.git(directory, 'commit', '--allow-empty', '-qm', 'loose next')
        self.assertTrue((directory / '.git/refs/heads/main').exists())
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'))
        self.build(directory, self.git(directory, 'rev-parse', 'HEAD'), fresh=True)

    def test_detached_and_shallow_checkout_track_head(self):
        directory = self.source('normal')
        first = self.git(directory, 'rev-parse', 'HEAD')
        self.git(directory, 'commit', '--allow-empty', '-qm', 'second')
        second = self.git(directory, 'rev-parse', 'HEAD')
        self.git(directory, 'checkout', '-q', '--detach', first)
        self.build(directory, first)
        self.build(directory, first, fresh=True)
        self.git(directory, 'checkout', '-q', '--detach', second)
        self.build(directory, second)
        shallow = self.root / 'shallow'
        self.git(self.root, 'clone', '-q', '--depth=1', directory.as_uri(), str(shallow))
        self.assertEqual(self.git(shallow, 'rev-parse', '--is-shallow-repository'), 'true')
        self.build(shallow, second)
        self.build(shallow, second, fresh=True)

    def test_symbolic_ref_alias_retarget_updates_identity(self):
        directory = self.source('aliases')
        first = self.git(directory, 'rev-parse', 'HEAD')
        self.git(directory, 'commit', '--allow-empty', '-qm', 'second')
        second = self.git(directory, 'rev-parse', 'HEAD')
        self.git(directory, 'branch', 'previous', first)
        self.git(directory, 'symbolic-ref', 'refs/heads/alias', 'refs/heads/main')
        self.git(directory, 'symbolic-ref', 'HEAD', 'refs/heads/alias')
        self.build(directory, second)
        self.build(directory, second, fresh=True)
        self.git(directory, 'symbolic-ref', 'refs/heads/alias', 'refs/heads/previous')
        self.build(directory, first)
        self.build(directory, first, fresh=True)

    def test_archive_and_release_overrides_are_cached_and_refresh(self):
        directory = self.source('archive', repository=False)
        self.build(directory, 'unknown')
        self.build(directory, 'unknown', fresh=True)
        self.build(directory, 'release-revision', override='release-revision')
        self.build(directory, 'release-revision', override='release-revision', fresh=True)
        self.build(directory, 'next-revision', override='next-revision', epoch='987654321')
        self.build(directory, 'next-revision', override='next-revision', epoch='987654321', fresh=True)


if __name__ == '__main__':
    unittest.main()
