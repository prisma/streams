import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from common import syntax
from production_changes import normalized_source, unchanged_production
from verification_plan import plan, source_changes


class ProductionChanges(unittest.TestCase):
    def unchanged(self, before, after, path='src/tasks.rs'):
        old, new = {path: before}, {path: after}
        return unchanged_production(old, new, syntax(old), syntax(new)) == [path]

    def test_lint_annotations_preserve_all_compiler_checks(self):
        before = 'pub fn f() -> u8 { Some(1).unwrap() }'
        after = '#[expect(clippy::unwrap_used, reason="owner; invariant; necessity")]\n' + before
        self.assertTrue(self.unchanged(before, after))
        self.assertTrue(self.unchanged(after, before))
        self.assertTrue(self.unchanged(before, '#![allow(dead_code)]\n' + before))
        checks = plan(['src/tasks.rs'], production_unchanged=['src/tasks.rs'])
        self.assertTrue(checks['compiler'])
        self.assertFalse(checks['mutants'])
        self.assertFalse(checks['loom'])
        self.assertEqual(checks['production_unchanged_files'], ['src/tasks.rs'])
        self.assertTrue(plan(['src/tasks.rs', 'src/shard.rs'], production_unchanged=['src/tasks.rs'])['mutants'])
        self.assertTrue(plan(['src/tasks.rs', 'scripts/quality/gate.py'], production_unchanged=['src/tasks.rs'])['miri'])

    def test_annotations_can_accompany_only_narrowed_visibility(self):
        before = 'pub struct A { pub field: u8 } impl A { pub fn value(&self)->u8 { self.field } }'
        after = '#[allow(dead_code)]\n' + before.replace('pub ', 'pub(crate) ')
        self.assertTrue(self.unchanged(before, after))
        self.assertFalse(self.unchanged(after, before))
        self.assertFalse(self.unchanged(before, after.replace('pub(crate) field', 'pub(in crate::a) field')))
        self.assertFalse(self.unchanged(before, after.replace('self.field', '0')))
        self.assertFalse(self.unchanged(before, after.replace('->u8', '->u16')))

    def test_only_explicit_test_cfg_erases_an_item(self):
        production = 'fn f()->u8 { 1 }\n'
        tests = '#[cfg(test)]\nmod tests { #[test] fn verifies_f() { assert_eq!(super::f(), 1); } }'
        self.assertTrue(self.unchanged(production, production + tests))
        self.assertTrue(self.unchanged(production + tests, production))
        self.assertTrue(self.unchanged(production + tests, production + tests.replace('1);', '2);')))
        self.assertFalse(self.unchanged(production + tests, (production + tests).replace('cfg(test)', 'cfg(any(test, feature="live"))')))
        self.assertFalse(self.unchanged(production + tests, (production + tests).replace('#[cfg(test)]', '')))
        self.assertFalse(self.unchanged(production, '#[cfg(test)]\n' + production))
        self.assertFalse(self.unchanged('#[cfg(test)]\n' + production, production))

    def test_test_files_are_proven_by_the_file_attribute_not_the_filename(self):
        self.assertTrue(self.unchanged('', '#![cfg(test)]\nfn check()->u8 { 1 }', 'src/tasks/poison_tests.rs'))
        self.assertTrue(self.unchanged('#![cfg(test)]\nfn check()->u8 { 1 }', '', 'src/tasks/poison_tests.rs'))
        for path in ('src/tasks_tests.rs', 'src/dst/tests/fake.rs', 'src/tests/fake.rs'):
            self.assertFalse(self.unchanged('fn f()->u8{1}', 'fn f()->u8{2}', path))
        for guard in ('any(test, feature="live")', 'not(test)', 'feature="test"'):
            self.assertFalse(self.unchanged('', f'#![cfg({guard})]\nfn f(){{}}'))

    def test_an_inner_attribute_on_a_nested_scope_cannot_erase_the_file(self):
        for scope in ('mod inner { #![cfg(test)] fn check() {} }',
                      'unsafe extern "C" { #![cfg(test)] fn check(); }'):
            before = scope + '\nfn production()->u8{1}'
            self.assertFalse(self.unchanged(before, before.replace('u8{1}', 'u8{2}')))

    def test_cfg_on_statement_or_expression_cannot_hide_its_function(self):
        for test_statement in ('#[cfg(test)] { return 1; }', '#[cfg(test)] let value = 1;'):
            before = 'fn f()->u8 { ' + test_statement + ' 2 }'
            with self.subTest(statement=test_statement):
                self.assertFalse(self.unchanged(before, before.replace(' 2 }', ' 3 }')))

    def test_cfg_attr_is_not_a_direct_compiler_annotation(self):
        before = '#[cfg_attr(test, allow(dead_code))] fn f()->u8{1}'
        self.assertFalse(self.unchanged(before, before.replace('test,', 'unix,')))
        before = '#[cfg_attr(test, cfg(test))] fn f()->u8{1}'
        self.assertFalse(self.unchanged(before, before.replace('u8{1}', 'u8{2}')))

    def test_literal_doc_and_macro_tokens_are_preserved(self):
        for before, after in (
            ('fn f()->u8 { 1 }', 'fn f()->u8 { 2 }'),
            ('fn f()->&\'static str { "#[allow(dead_code)]" }', 'fn f()->&\'static str { "#[expect(dead_code)]" }'),
            ('fn f()->&\'static str { r#"#[cfg(test)] pub fn x() {}"# }', 'fn f()->&\'static str { r#"#[cfg(test)] pub fn y() {}"# }'),
            ('macro_rules! m { () => { #[allow(dead_code)] fn f() {} } }', 'macro_rules! m { () => { #[expect(dead_code)] fn f() {} } }'),
            ('macro_rules! m { () => { #[cfg(test)] fn f()->u8{1} } }', 'macro_rules! m { () => { #[cfg(test)] fn f()->u8{2} } }'),
            ('/// alpha\nfn f(){}', '/// beta\nfn f(){}'),
            ('#[repr(u8)] enum E { A }', '#[repr(u16)] enum E { A }'),
            ('#[derive(Clone)] struct A;', '#[derive(Clone, Copy)] struct A;'),
            ('#[cfg(feature="a")] fn f(){}', '#[cfg(feature="b")] fn f(){}'),
        ):
            with self.subTest(before=before):
                self.assertFalse(self.unchanged(before, after))

    def test_whitespace_and_ordinary_comments_preserve_tokens(self):
        before = 'pub fn É()->&\'static str { "\u2028" }'
        after = '// ordinary comment\n#[allow(dead_code)]\npub(crate) fn É() -> &\'static str { "\u2028" }'
        self.assertTrue(self.unchanged(before, after))
        self.assertFalse(self.unchanged(before, after.replace('"\u2028"', '" "')))

    def test_overlapping_test_and_lint_ranges_are_removed_once(self):
        before = 'fn f(){}'
        after = before + '#[cfg(test)] #[allow(dead_code)] mod tests { #[cfg(test)] #[expect(dead_code)] fn x(){} }'
        self.assertTrue(self.unchanged(before, after))
        after = before + '#[cfg(test)] pub mod tests { pub struct A; pub fn x(){} }'
        self.assertTrue(self.unchanged(before, after))

    def test_source_introspection_retains_invariant_checks(self):
        for expression in ('line!()', 'column!()', 'file!()', 'include!("body.rs")',
                           'include_str!("body.rs")', 'include_bytes!("body.rs")',
                           'format!("{}", line!())'):
            before = 'fn f() { let _ = ' + expression + '; }'
            with self.subTest(expression=expression):
                self.assertFalse(self.unchanged(before, '#[allow(dead_code)]\n' + before))
        before = 'use std::line as location; fn f() { let _ = format!("{}", location!()); }'
        self.assertFalse(self.unchanged(before, '#![allow(dead_code)]\n' + before))
        before = 'macro_rules! custom { () => { 1 } } fn f() { let _ = custom!(); }'
        self.assertFalse(self.unchanged(before, '#![allow(dead_code)]\n' + before))
        self.assertTrue(self.unchanged('fn f(){}', 'fn f(){} #[cfg(test)] mod tests { fn x() { let _ = line!(); } }'))

    def test_custom_attributes_and_derives_retain_checks(self):
        for annotation in ('derive(Custom)', 'derive(Clone)', 'custom::instrument',
                           'cfg_attr(feature="live", custom::instrument)'):
            before = '#[' + annotation + '] struct A;'
            with self.subTest(annotation=annotation):
                self.assertFalse(self.unchanged(before, '#[allow(dead_code)]\n' + before))
        before = 'fn f(){}'
        self.assertTrue(self.unchanged(before, before + '#[cfg(test)] #[custom] mod tests {}'))

    def test_actual_git_base_accounts_for_new_deleted_and_changed_sources(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(['git', '-C', directory, *args], text=True).strip()
            def write(path, content):
                file = root / path
                file.parent.mkdir(parents=True, exist_ok=True)
                file.write_text(content)
            git('init', '-q')
            git('config', 'user.name', 'Quality fixture')
            git('config', 'user.email', 'quality@example.invalid')
            write('src/tasks.rs', 'pub fn f()->u8 { Some(1).unwrap() }')
            write('src/tasks/old_tests.rs', '#![cfg(test)] fn old_test() {}')
            git('add', '.')
            git('commit', '-qm', 'base')
            base = git('rev-parse', 'HEAD')
            write('src/tasks.rs', '#[expect(clippy::unwrap_used, reason="owner; invariant; necessity")] pub(crate) fn f()->u8 { Some(1).unwrap() }')
            write('src/tasks/new_tests.rs', '#![cfg(test)] fn new_test() {}')
            write('src/tasks/misnamed_tests.rs', 'fn real_production()->u8 { 1 }')
            (root / 'src/tasks/old_tests.rs').unlink()
            paths = ['src/tasks.rs', 'src/tasks/old_tests.rs', 'src/tasks/new_tests.rs', 'src/tasks/misnamed_tests.rs']
            with patch('verification_plan.ROOT', root):
                _, unchanged = source_changes(base, paths)
                self.assertEqual(set(unchanged), set(paths) - {'src/tasks/misnamed_tests.rs'})
                self.assertTrue(plan(paths, production_unchanged=unchanged)['mutants'])
                write('src/tasks.rs', 'pub(crate) fn f()->u8 { Some(2).unwrap() }')
                _, unchanged = source_changes(base, paths)
                self.assertNotIn('src/tasks.rs', unchanged)

    def test_opaque_templates_cannot_prove_an_unchanged_implementation(self):
        self.assertIsNone(normalized_source('opaque!()', {'items': [], 'facts': []}))


if __name__ == '__main__':
    unittest.main()
