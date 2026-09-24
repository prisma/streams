"""Pure tests for the effective-configuration comparison: inline fixtures,
no cargo, no network, no git."""
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import effective_config as ec  # noqa: E402

SERVER_CONFIG = '''ServerConfig {
    cli: CliArgs {
        listen: "0.0.0.0:8080",
        initial_shards: Some(
            16,
        ),
        streams_auth_keys_file: Some(
            "/tmp/feeds/keys.json",
        ),
    },
    shard: ShardRuntimeConfig {
        open_deadline: 180s,
        open_wait_ms: 10000,
    },
    history: HistoryConfig {
        gc_interval: Some(
            600s,
        ),
    },
    sse: SseConfig {
        feed_total_bytes_raw: None,
    },
    scaler: ScaleConfig {
        hot_pct: 0.75,
    },
}'''

RENAME = {
    'pairs': [{'old': 'billing.mode_env', 'new': 'cli.billing_mode', 'none_equals': '"off"'}],
    'added': [{'new': 'http.h1_header_timeout'}],
    'removed': [],
}


class FlattenTest(unittest.TestCase):
    def test_nested_struct_option_duration_path(self):
        leaves = ec.flatten_debug(SERVER_CONFIG.splitlines())
        self.assertEqual(leaves['cli.initial_shards'], 'Some(16)')
        self.assertEqual(leaves['shard.open_deadline'], '180s')
        self.assertEqual(leaves['history.gc_interval'], 'Some(600s)')
        self.assertEqual(leaves['sse.feed_total_bytes_raw'], 'None')
        self.assertEqual(leaves['cli.streams_auth_keys_file'], 'Some("/tmp/feeds/keys.json")')
        self.assertEqual(leaves['scaler.hot_pct'], '0.75')
        self.assertEqual(len(leaves), 8)

    def test_sections_prefix_paths(self):
        text = '\n'.join([
            '@@ cli', 'Args {', '    listen: "x",', '}',
            '@@ root', 'AppConfig {', '    http: HttpConfig {', '        h1_max_buf: 65536,', '    },', '}',
            '@@ runtime.tokio_workers', 'None',
            '@@ verdict', 'not-evaluated', '',
        ])
        leaves, verdict = ec.parse_dump(text)
        self.assertEqual(leaves, {'cli.listen': '"x"', 'http.h1_max_buf': '65536', 'runtime.tokio_workers': 'None'})
        self.assertEqual(verdict, 'not-evaluated')

    def test_multiline_section_value_collapses(self):
        leaves, _ = ec.parse_dump('@@ runtime.tokio_workers\nSome(\n    4,\n)\n@@ verdict\naccepted\n')
        self.assertEqual(leaves, {'runtime.tokio_workers': 'Some(4)'})

    def test_string_escapes_and_commas(self):
        leaves = ec.flatten_debug(['S {', '    s: "a,\\"b\\"",', '    t: "ends with {",', '}'])
        self.assertEqual(leaves, {'s': '"a,\\"b\\""', 't': '"ends with {"'})

    def test_unparsed_line_fails(self):
        with self.assertRaises(ValueError) as caught:
            ec.parse_dump('@@ root\nServerConfig {\n    garbage without a colon\n}\n')
        self.assertTrue(str(caught.exception).startswith('unparsed Debug line 3:'), caught.exception)

    def test_verdict_trailing_blank_lines_are_stripped(self):
        text = ('@@ verdict\nrefused\nconfiguration invalid (1 problem(s)):\n'
                '  - SSE_H1_MAX_BUF=4096 is below the floor\n\n')
        _, verdict = ec.parse_dump(text)
        self.assertEqual(verdict.splitlines()[-1], '  - SSE_H1_MAX_BUF=4096 is below the floor')


class FamilyTest(unittest.TestCase):
    def parse(self, text):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / 'deploy/profiles').mkdir(parents=True)
            (Path(root) / 'deploy/profiles/compute-1g.env').write_text('# memory\nSSE_MAX_CONNECTIONS=1200\n')
            return ec.parse_family('f', text, Path(root))

    def test_include_then_override_last_wins(self):
        fam = self.parse('include deploy/profiles/compute-1g.env\nenv SSE_MAX_CONNECTIONS=2000\n')
        self.assertEqual(fam.env['SSE_MAX_CONNECTIONS'], '2000')
        self.assertEqual(fam.overridden, [('SSE_MAX_CONNECTIONS', '1200', '2000')])
        self.assertEqual(fam.provenance['SSE_MAX_CONNECTIONS'], 'server')

    def test_secret_requires_placeholder(self):
        with self.assertRaises(ec.FamilyError) as caught:
            self.parse('env AUTH_TOKEN=abc\n')
        self.assertEqual(str(caught.exception), 'AUTH_TOKEN must be a placeholder')
        fam = self.parse('env AUTH_TOKEN=placeholder-auth\nenv USAGE_STREAM_KEY=' + next(iter(ec.CANNED_SECRETS)))
        self.assertEqual(fam.env['AUTH_TOKEN'], 'placeholder-auth')

    def test_empty_value_rejected(self):
        with self.assertRaises(ec.FamilyError) as caught:
            self.parse('env FOO=\n')
        self.assertEqual(str(caught.exception), 'FOO: empty value (the Compute CLI rejects --env KEY=)')

    def test_url_userinfo_rejected(self):
        with self.assertRaises(ec.FamilyError) as caught:
            self.parse('env SLATE_S3_ENDPOINT=https://id:secret@host.invalid\n')
        self.assertIn('URL userinfo', str(caught.exception))

    def test_key_name_rule(self):
        self.assertTrue(ec.is_secret_name('PLATFORM_API_KEY'))
        self.assertTrue(ec.is_secret_name('BIN_S3_SECRET_ACCESS_KEY'))
        self.assertTrue(ec.is_secret_name('TOKENS_S3_KEY'))  # TOKEN wins over the object-key allowlist
        for name in ('L0_MAX_SSTS_PER_KEY', 'SERVER_BINARY_S3_KEY', 'STREAMS_AUTH_KEYS_FILE', 'FLEET_AUTH_MODE',
                     'WORKLOAD_TOKEN_FILE', 'SSE_MAX_CONNECTIONS'):
            self.assertFalse(ec.is_secret_name(name), name)

    def test_provenance_kept(self):
        fam = self.parse('supervisor APP_BINARY_SHA256=placeholder-sha\nrole gen\nenv STREAMS=32\n'
                         'role server\nplatform PORT=8080\n')
        self.assertEqual(fam.provenance, {'APP_BINARY_SHA256': 'supervisor', 'STREAMS': 'gen', 'PORT': 'platform'})

    def test_unknown_directive_and_role_rejected(self):
        with self.assertRaises(ec.FamilyError):
            self.parse('export FOO=1\n')
        with self.assertRaises(ec.FamilyError):
            self.parse('role client\n')


class CheckFamiliesTest(unittest.TestCase):
    def check(self, script, family):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / 'deploy.sh').write_text(script)
            fam = ec.parse_family('f', family, Path(root))
            return ec.check_family(fam, Path(root))

    SCRIPT = 'deploy --x \\\n  --env A=1 --env "B=$X" \\\n  --env C=3  # server\n'

    def test_every_script_name_is_set_or_omitted(self):
        ok = 'source deploy.sh "# server"\nenv A=1\nenv B=2\nomit C variant only\n'
        self.assertEqual(self.check(self.SCRIPT, ok), [])
        missing = self.check(self.SCRIPT, 'source deploy.sh "# server"\nenv A=1\nenv B=2\n')
        self.assertEqual(missing, ["f: deploy.sh sets ['C'] but the family neither sets nor omits them"])

    def test_stale_omit_and_unsourced_name_fail(self):
        problems = self.check(self.SCRIPT, 'source deploy.sh "# server"\nenv A=1\nenv B=2\nenv C=3\n'
                                           'env D=4\nomit E gone\n')
        self.assertIn("f: omits ['E'] which deploy.sh never sets", problems)
        self.assertIn("f: sets ['D'] which deploy.sh never sets", problems)

    def test_missing_anchor_fails(self):
        self.assertEqual(self.check(self.SCRIPT, 'source deploy.sh "no such line"\n'),
                         ["f: anchor 'no such line' not found in deploy.sh"])

    def test_mjs_block_keys_and_deletes(self):
        text = 'const cellEnv = (id) => {\n  const env = {\n    ...process.env,\n    CELL_ID: id,\n' \
               '    ROLLUP: "1",\n  };\n  delete env.FLEET_INTERNAL_TOKEN;\n  return env;\n};\n'
        self.assertEqual(ec.mjs_block_names(text, 'const cellEnv'), ({'CELL_ID', 'ROLLUP'}, {'FLEET_INTERNAL_TOKEN'}))


class RedactTest(unittest.TestCase):
    def test_platform_export_is_redacted_in_memory(self):
        secret = 'real-looking-secret-value-123'
        key32 = 'q' * 43 + '='
        export = '\n'.join([f'AUTH_TOKEN={secret}', f'export USAGE_STREAM_KEY="{key32}"',
                            'SLATE_S3_ENDPOINT=https://id:pw@fly.storage.tigris.dev', 'INITIAL_SHARDS=4',
                            'EMPTY=', f'PLATFORM_API_KEY={secret}'])
        text = ec.redact_export(export, 'proj-x', 'test export')
        self.assertNotIn(secret, text)
        self.assertNotIn('id:pw', text)
        self.assertIn('env AUTH_TOKEN=placeholder-auth-token-redacted', text)
        self.assertIn('env USAGE_STREAM_KEY=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=', text)
        self.assertIn('env SLATE_S3_ENDPOINT=https://fly.storage.tigris.dev', text)
        self.assertIn('env INITIAL_SHARDS=4', text)
        self.assertIn('# EMPTY: empty in the export', text)
        fam = ec.parse_family('proj-x', text)
        self.assertEqual(fam.env['PLATFORM_API_KEY'], 'placeholder-platform-api-key-redacted')


class RenameTest(unittest.TestCase):
    def test_unaccounted_path_fails(self):
        with self.assertRaisesRegex(ec.CoverageError, 'unaccounted old-only path billing.foo'):
            ec.diff_leaves({'billing.foo': '1', 'a': '1'}, {'a': '1'}, RENAME)
        with self.assertRaisesRegex(ec.CoverageError, 'unaccounted new-only path http.bar'):
            ec.diff_leaves({'a': '1'}, {'a': '1', 'http.bar': '2'}, RENAME)

    def test_declared_pair_compares_values(self):
        rows = ec.diff_leaves({'billing.mode_env': 'Some("required")', 'cli.billing_mode': '"required"'},
                              {'cli.billing_mode': '"required"'}, RENAME)
        self.assertEqual([(r['kind'], r['effective']) for r in rows], [('paired', 'equal-effective')])

    def test_none_equals_the_clap_default(self):
        rows = ec.diff_leaves({'billing.mode_env': 'None', 'cli.billing_mode': '"off"'},
                              {'cli.billing_mode': '"off"'}, RENAME)
        self.assertEqual(rows[0]['effective'], 'equal-effective')
        rows = ec.diff_leaves({'billing.mode_env': 'None', 'cli.billing_mode': '"required"'},
                              {'cli.billing_mode': '"required"'}, RENAME)
        self.assertEqual(rows[0]['effective'], 'unequal')

    def test_value_and_added_rows(self):
        rows = ec.diff_leaves({'cli.x': '6'}, {'cli.x': 'None', 'http.h1_header_timeout': '120s'}, RENAME)
        self.assertEqual([(r['kind'], r['path'], r['old'], r['new']) for r in rows],
                         [('value', 'cli.x', '6', 'None'), ('added', 'http.h1_header_timeout', '<absent>', '120s')])


class ProbeTest(unittest.TestCase):
    def test_classes(self):
        self.assertEqual(ec.classify('A', True, True), 'BOTH')
        self.assertEqual(ec.classify('A', True, False), 'OLD_ONLY')
        self.assertEqual(ec.classify('A', False, True), 'NEW_ONLY')
        self.assertEqual(ec.classify('A', False, False), 'NEITHER')
        self.assertEqual(ec.classify('A', False, False, {'A'}, set()), 'NO_EFFECT_AT_VALUE')
        for name in ('RUST_LOG', 'MIMALLOC_PURGE_DELAY', 'HTTPS_PROXY', 'no_proxy', 'SSL_CERT_FILE'):
            self.assertEqual(ec.classify(name, False, False), 'PROCESS', name)

    def test_perturb(self):
        self.assertEqual(ec.perturb('25'), '251')
        self.assertEqual(ec.perturb('0.75'), '0.751')
        self.assertEqual(ec.perturb('required'), 'requiredx')


class EquivalenceTest(unittest.TestCase):
    BLOCK = ['#[derive(Parser, Debug)]', 'struct Args {', '    #[arg(long, default_value_t = 25)]',
             '    flush_interval_ms: u64,', '}']

    def test_args_block_visibility_normalized(self):
        crate_visible = [line.replace('    flush', '    pub(crate) flush') for line in self.BLOCK]
        self.assertEqual(ec.normalize_args(crate_visible), self.BLOCK)
        changed = [line.replace('25', '26') for line in crate_visible]
        self.assertNotEqual(ec.normalize_args(changed), self.BLOCK)

    def test_args_block_extraction(self):
        text = 'use clap::Parser;\n' + '\n'.join(self.BLOCK) + '\nfn main() {}\n'
        self.assertEqual(ec.args_block(text, r'#\[derive\(Parser, Debug\)\]'), (2, self.BLOCK))

    def test_env_name_extraction_helpers(self):
        files = {
            'src/main.rs': 'fn a() { std::env::var("A").ok(); }\n'
                           'fn env_usize(k: &str, d: usize) -> usize {\n    std::env::var(k)\n        .ok()\n}\n'
                           'fn b() { env_usize("C", 4); }\n'
                           'fn c() {\n    let genv = |k: &str, d: usize| -> usize {\n        std::env::var(k)\n'
                           '    };\n    genv("D", 1);\n    std::env::var(\n        "F",\n    );\n}\n',
            'src/scaler3.rs': 'fn envf(k: &str, d: f64) -> f64 {\n    std::env::var(k)\n}\nfn p() { envf("B", 1.0); }\n',
            'src/backpressure.rs': 'fn l() {\n    fn v(k: &str, d: u64) -> u64 {\n        std::env::var(k)\n    }\n'
                                   '    v("E", 1);\n}\n',
        }
        names, helpers = ec.env_reads(files)
        self.assertEqual(set(names), {'A', 'B', 'C', 'D', 'E', 'F'})
        self.assertEqual(len(helpers), 4)
        with self.assertRaisesRegex(ValueError, 'unresolved generic env helper'):
            ec.env_reads({'src/other.rs': 'fn read(k: &str) {\n    std::env::var(k)\n}\n'})

    def test_eval_literal(self):
        self.assertEqual(ec.eval_literal('64 * 1024 * 1024'), 67108864)
        self.assertEqual(ec.eval_literal('4_000u64'), 4000)
        self.assertEqual(ec.eval_literal('0.75'), 0.75)
        self.assertEqual(ec.eval_literal('1 << 20'), 1048576)
        self.assertIsNone(ec.eval_literal('Duration::from_secs(5)'))

    def test_rc4_default_from_helper_and_unwrap(self):
        self.assertEqual(ec.rc4_default('let x = envf("SCALE_HOT_PCT", 0.75);', 'SCALE_HOT_PCT'), '0.75')
        self.assertEqual(ec.rc4_default('std::env::var("X").ok().and_then(|v| v.parse().ok()).unwrap_or(64 * 1024);',
                                        'X'), '64 * 1024')


if __name__ == '__main__':
    unittest.main()
