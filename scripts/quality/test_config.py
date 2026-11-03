from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import config
from common import ROOT


class Config(unittest.TestCase):
    def test_actionlint_version_and_missing_tool(self):
        pins = config.read_toml('quality-tools.toml')
        versions = {
            ('rustc', '--version'): f"rustc {pins['rust']}",
            ('cargo', '--version'): f"cargo {pins['rust']}",
            ('cargo', 'clippy', '--version'): 'clippy 0.1.98',
            ('cargo', 'machete', '--version'): pins['tools']['cargo-machete'],
            ('cargo', 'deny', '--version'): f"cargo-deny {pins['tools']['cargo-deny']}",
            ('actionlint', '--version'): pins['actionlint'],
        }
        def output(command, **kwargs):
            return versions[tuple(command)]
        with patch.object(config.subprocess, 'check_output', side_effect=output), patch.dict(config.os.environ, {}, clear=True):
            self.assertEqual(config.check(), [])
            versions[('actionlint', '--version')] = '0.0.0'
            self.assertTrue(any('tool version mismatch' in p and 'actionlint' in p for p in config.check()))
        def missing(command, **kwargs):
            if command[0] == 'actionlint':
                raise FileNotFoundError('actionlint')
            return versions[tuple(command)]
        with patch.object(config.subprocess, 'check_output', side_effect=missing), patch.dict(config.os.environ, {}, clear=True):
            self.assertTrue(any('missing required tool: actionlint' in p for p in config.check()))

    def test_pinned_workspace_and_drift_controls(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = ['quality-tools.toml', 'rust-toolchain.toml', 'Cargo.toml',
                     'Cargo.lock', 'clippy.toml', 'deny.toml', 'fuzz/Cargo.toml',
                     'tools/quality-syntax/Cargo.toml', 'tools/quality-invariants/Cargo.toml',
                     'docs/quality/legacy-diagnostics.json',
                     'docs/quality/review-skill-pin.json',
                     '.agents/skills/thermo-nuclear-code-quality-review/SKILL.md']
            for name in files:
                destination = root / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(ROOT / name, destination)
            pins = config.read_toml('quality-tools.toml')
            with patch.object(config, 'ROOT', root), patch.dict(config.os.environ, {}, clear=True):
                self.assertEqual(config.check(tools=False), [])
                for name, old, new in [
                    ('fuzz/Cargo.toml', 'workspace = true', 'workspace = false'),
                    ('clippy.toml', 'threshold = 100', 'threshold = 101'),
                    ('Cargo.lock', f"#{pins['slatedb']['rev']}", '#bad'),
                    ('Cargo.lock', f"#{pins['foyer']['rev']}", '#bad'),
                    ('Cargo.toml', 'foyer-tokio = { git = "https://github.com/sorenbs/foyer.git"',
                     'foyer-tokio = { git = "https://github.com/sorenbs/slatedb.git"'),
                    ('deny.toml', 'crate = "openssl"', 'crate = "openssl-other"'),
                    ('deny.toml', 'crate = "native-tls"', 'crate = "native-tls-other"'),
                    ('.agents/skills/thermo-nuclear-code-quality-review/SKILL.md', '# Core Prompt', '# Changed Prompt'),
                ]:
                    file = root / name
                    original = file.read_text()
                    self.assertIn(old, original)
                    file.write_text(original.replace(old, new))
                    self.assertTrue(config.check(tools=False), name)
                    file.write_text(original)
                with patch.dict(config.os.environ, {'RUSTFLAGS': '--cap-lints=allow'}):
                    self.assertTrue(config.check(tools=False))
                with (root / 'fuzz/Cargo.toml').open('a') as manifest:
                    manifest.write('\n[features]\nunchecked = []\n')
                self.assertTrue(config.check(tools=False))


def run(cwd, *args):
    return subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout.strip()


class ForkPins(unittest.TestCase):
    """A fork pin is its upstream base plus exactly the named PR commits."""

    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())
        up = self.dir / 'upstream'
        up.mkdir()
        run(up, 'init', '--quiet', '-b', 'main')
        (up / 'a.txt').write_text('a\n')
        run(up, 'add', '.'); run(up, 'commit', '--quiet', '-m', 'base')
        self.base = run(up, 'rev-parse', 'HEAD')
        run(up, 'checkout', '--quiet', '-b', 'pr')
        (up / 'b.txt').write_text('b\n')
        run(up, 'add', '.'); run(up, 'commit', '--quiet', '-m', 'fix')
        self.fix = run(up, 'rev-parse', 'HEAD')
        run(up, 'update-ref', 'refs/pull/7/head', self.fix)
        run(up, 'checkout', '--quiet', 'main')
        (up / 'c.txt').write_text('c\n')
        run(up, 'add', '.'); run(up, 'commit', '--quiet', '-m', 'unrelated')
        self.unrelated = run(up, 'rev-parse', 'HEAD')
        fork = self.dir / 'fork'
        run(self.dir, 'clone', '--quiet', str(up), str(fork))
        run(fork, 'checkout', '--quiet', '-b', 'carry', self.base)
        run(fork, 'cherry-pick', '-x', self.fix)
        self.good = run(fork, 'rev-parse', 'HEAD')
        (fork / 'd.txt').write_text('d\n')
        run(fork, 'add', '.'); run(fork, 'commit', '--quiet', '-m', 'ours')
        self.extra = run(fork, 'rev-parse', 'HEAD')
        run(fork, 'checkout', '--quiet', '-b', 'altered', self.base)
        (fork / 'b.txt').write_text('b, edited\n')
        run(fork, 'add', '.'); run(fork, 'commit', '--quiet', '-m', 'fix')
        self.altered = run(fork, 'rev-parse', 'HEAD')
        self.up, self.fork = up, fork

    def tearDown(self):
        shutil.rmtree(self.dir)

    def problems(self, rev, carries=None):
        pin = {'git': str(self.fork), 'rev': rev, 'upstream': str(self.up), 'base': self.base,
               'carries': carries or [{'pr': 7, 'commit': self.fix}]}
        return config.fork_problems('dep', pin, self.dir / f'cache-{rev[:8]}')

    def test_base_plus_the_named_pr_commit_passes(self):
        self.assertEqual(self.problems(self.good), [])

    def test_an_empty_cache_directory_is_initialised(self):
        # CI restores a target cache in which the fork cache can be an empty
        # directory: the check initialises it instead of failing on it.
        cache = self.dir / f'cache-{self.good[:8]}'
        cache.mkdir()
        self.assertEqual(self.problems(self.good), [])

    def test_a_commit_beyond_the_named_ones_fails(self):
        self.assertEqual(self.problems(self.extra), ['dep fork carries 2 commits over its base, not the 1 it names'])

    def test_a_commit_that_differs_from_upstream_fails(self):
        self.assertEqual(self.problems(self.altered), [f'dep fork commit {self.altered} is not upstream {self.fix}'])

    def listed_fork(self, listing, touch='fork/sync.sh', extra=None):
        """A fork of base: a docs commit touching `touch`, then the PR's
        patch, then (when `extra`) a fork-only commit; FORK.md says
        `listing`."""
        run(self.fork, 'checkout', '--quiet', '-B', 'listed', self.base)
        (self.fork / touch).parent.mkdir(parents=True, exist_ok=True)
        (self.fork / 'FORK.md').write_text(listing)
        (self.fork / touch).write_text('docs\n')
        run(self.fork, 'add', '.'); run(self.fork, 'commit', '--quiet', '-m', 'docs')
        docs = run(self.fork, 'rev-parse', 'HEAD')
        run(self.fork, 'cherry-pick', self.fix)
        pr = run(self.fork, 'rev-parse', 'HEAD')
        shas = [docs, pr]
        if extra:
            (self.fork / 'e.txt').write_text(extra)
            run(self.fork, 'add', '.'); run(self.fork, 'commit', '--quiet', '-m', 'ours')
            shas.append(run(self.fork, 'rev-parse', 'HEAD'))
        return shas

    def test_a_listed_fork_passes(self):
        docs, pr, own = self.listed_fork('carries PR #7 and issue #9\n', extra='e\n')
        carries = [{'docs': True, 'fork': docs}, {'pr': 7, 'pr_diff': True, 'fork': pr},
                   {'issue': 9, 'fork': own}]
        self.assertEqual(self.problems(own, carries), [])

    def test_a_docs_commit_that_touches_code_fails(self):
        docs, pr = self.listed_fork('PR #7\n', touch='src/x.rs')
        problems = self.problems(pr, [{'docs': True}, {'pr': 7, 'pr_diff': True}])
        self.assertEqual(problems, [f"dep docs commit {docs} touches ['FORK.md', 'src/x.rs']"])

    def test_a_fork_only_commit_must_be_named_and_listed(self):
        docs, pr, own = self.listed_fork('PR #7\n', extra='e\n')
        problems = self.problems(own, [{'docs': True}, {'pr': 7, 'pr_diff': True}, {'issue': 9}])
        self.assertEqual(problems, ['dep: FORK.md does not list #9',
                                    f'dep fork commit {own} must be named: it matches no upstream commit'])

    def test_a_commit_that_is_not_its_prs_diff_fails(self):
        docs, pr, own = self.listed_fork('PR #7\n', extra='e\n')
        problems = self.problems(own, [{'docs': True}, {'pr': 7}, {'pr': 7, 'pr_diff': True}])
        self.assertIn(f'dep fork commit {own} is not upstream PR #7', problems)

    def test_a_named_commit_outside_its_pr_fails(self):
        problems = self.problems(self.good, [{'pr': 7, 'commit': self.unrelated}])
        self.assertIn(f'dep: {self.unrelated} is not a commit of upstream PR #7', problems)


if __name__ == '__main__':
    unittest.main()
