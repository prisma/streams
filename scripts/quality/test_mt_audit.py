"""The multitenancy audit sees every source file and fails on drift both ways.

Review item 43: the audit globbed top-level `src/*.rs`
(plus config, dst and bin), so a bare-name identity site moved into a
subdirectory vanished from it and counted as progress; its whitespace
normaliser ate the TAB between file and text, so the `src/crypto.rs`
exclusion never matched; grep errors were discarded; and a GONE
fingerprint still printed MT_AUDIT_OK.
"""
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'scripts' / 'multitenancy-audit.sh'


class MultitenancyAudit(unittest.TestCase):
    # The layout the audit has always scanned, so a fixture exercises the
    # audit's decisions rather than its tolerance of missing directories.
    LAYOUT = {'src/dst/mod.rs': '', 'src/config/mod.rs': '', 'src/bin/tool.rs': '',
              'src/scaler3.rs': '', 'src/registry.rs': ''}

    def tree(self, files):
        files = {**self.LAYOUT, **files}
        tmp = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, tmp)
        (tmp / 'scripts').mkdir()
        shutil.copy(SCRIPT, tmp / 'scripts' / 'multitenancy-audit.sh')
        for path, text in files.items():
            target = tmp / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(text)
        return tmp

    def run_audit(self, tmp, *args):
        return subprocess.run(['bash', 'scripts/multitenancy-audit.sh', *args], cwd=tmp,
                              capture_output=True, text=True)

    def test_a_site_in_a_subdirectory_is_audited_and_crypto_is_excluded(self):
        tmp = self.tree({'src/lib.rs': 'fn a() {}\n',
                         'src/crypto.rs': 'let h = stream_hash("own");\n'})
        self.assertEqual(self.run_audit(tmp, '--regen').returncode, 0)
        baseline = (tmp / 'scripts' / 'mt-audit-baseline.txt').read_text()
        self.assertNotIn('src/crypto.rs', baseline, 'crypto.rs owns stream_hash')
        (tmp / 'src' / 'sse').mkdir()
        (tmp / 'src' / 'sse' / 'feed.rs').write_text('let h = stream_hash(&name);\n')
        result = self.run_audit(tmp)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn('+ stream-hash\tsrc/sse/feed.rs\tlet h = stream_hash(&name);',
                      result.stdout)

    def test_a_gone_fingerprint_fails_until_the_baseline_is_regenerated(self):
        tmp = self.tree({'src/tenant.rs': 'let p = "proj_local";\n'})
        self.assertEqual(self.run_audit(tmp, '--regen').returncode, 0)
        (tmp / 'src' / 'tenant.rs').write_text('let p = project_id();\n')
        result = self.run_audit(tmp)
        self.assertEqual(result.returncode, 1, result.stdout)
        self.assertIn('- tenant-fallback\tsrc/tenant.rs\tlet p = "proj_local";', result.stdout)
        self.assertNotIn('MT_AUDIT_OK', result.stdout)
        self.assertEqual(self.run_audit(tmp, '--regen').returncode, 0)
        self.assertIn('MT_AUDIT_OK', self.run_audit(tmp).stdout)


if __name__ == '__main__':
    unittest.main()
