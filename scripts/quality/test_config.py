from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch
import config
from common import ROOT


class Config(unittest.TestCase):
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
            with patch.object(config, 'ROOT', root), patch.dict(config.os.environ, {}, clear=True):
                self.assertEqual(config.check(tools=False), [])
                for name, old, new in [
                    ('fuzz/Cargo.toml', 'workspace = true', 'workspace = false'),
                    ('clippy.toml', 'threshold = 100', 'threshold = 101'),
                    ('Cargo.lock', '#0717cc1e4e9bad10a4773760f66bac4264ecf05e', '#bad'),
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


if __name__ == '__main__':
    unittest.main()
