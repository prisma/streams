from collections import Counter
import unittest
from common import syntax
from source_rules import absolute, exception_contracts, exception_growth, harness_layout, violations


def source(facts):
    return {'src/application/new.rs': {'facts': facts}}


def fact(kind, value, qualified='crate::f', test=False):
    return dict(kind=kind, value=value, qualified=qualified, test_only=test)


class Rules(unittest.TestCase):
    def check(self, text='', facts=None, before=None, prior=None):
        return violations({'src/application/new.rs': text}, source(facts or []),
                          before or {}, prior or {}, Counter(), {'sse_core_files': []})

    def test_new_and_test_files_cannot_cross_budget(self):
        self.assertFalse(self.check('\n' * 1000))
        self.assertTrue(self.check('\n' * 1001))
        self.assertTrue(self.check('\n' * 1001, before={'src/application/new.rs': 1200},
                                   prior={'src/application/new.rs': 1000}))

    def test_alias_target_and_relative_transport(self):
        self.assertTrue(self.check(facts=[fact('import-target', 'crate::http::AppState')]))
        self.assertTrue(self.check(facts=[fact('path', 'super::super::product::perr')]))
        self.assertFalse(self.check(facts=[fact('path', 'crate::product_cursor::MessageId')]))

    def test_cfg_does_not_hide_new_effect_and_macros_are_reported(self):
        self.assertTrue(self.check(facts=[fact('path', 'tokio::spawn', test=True)]))
        self.assertTrue(self.check(facts=[fact('macro', 'define_service')]))
        self.assertFalse(self.check(facts=[fact('macro', 'assert_eq')]))

    def test_unregistered_typed_spawn_cannot_hide_behind_expect(self):
        self.assertTrue(self.check(facts=[fact('attribute', 'expect (clippy :: disallowed_methods, reason = "owner; invariant; alternative")')]))

    def test_blanket_allow_and_missing_reason_fail(self):
        self.assertTrue(self.check(facts=[fact('attribute', 'allow (clippy :: all, reason = "x")')]))
        self.assertTrue(self.check(facts=[fact('attribute', 'expect (clippy :: too_many_lines)')]))
        self.assertFalse(self.check(facts=[fact('attribute', 'expect (clippy :: too_many_lines, reason = "owner; invariant; alternative")')]))

    def test_groups_and_denied_lints_cannot_be_suppressed_even_with_a_reason(self):
        for lint in ('unused', 'clippy :: correctness', 'unused_must_use', 'clippy :: eq_op'):
            for level in ('allow', 'expect', 'warn'):
                self.assertTrue(self.check(facts=[fact('attribute', f'{level} ({lint}, reason = "owner; invariant; alternative")')]))

    def growth(self, before, after):
        old = {'src/a.rs': before}
        new = {'src/a.rs': after}
        return exception_growth(exception_contracts(new, syntax(new)),
                                exception_contracts(old, syntax(old)))

    def test_impl_expectation_cannot_hide_an_unrelated_optional_unwrap(self):
        before = '#[expect(clippy::unwrap_used, reason = "feed; poison invariant; no recovery")]\nimpl A { fn lock(&self) { self.lock.lock().unwrap(); } }\n'
        after = before.replace(' } }', ' } fn optional(&self) { self.value.unwrap(); } }')
        self.assertTrue(self.growth(before, after))
        self.assertFalse(self.growth(before, before))

        replacement = before.replace('self.lock.lock().unwrap()', 'self.value.unwrap()')
        errors = self.growth(before, replacement)
        self.assertTrue(any('unwrap_site:' in error for error in errors), errors)

    def test_associated_panic_call_cannot_replace_an_unrelated_call(self):
        before = '''#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]
pub fn check(lock: &std::sync::Mutex<()>, value: Option<u8>) {
    let _guard = lock.lock().unwrap();
    let _ = std::hint::black_box(value);
}
'''
        after = before.replace(
            'std::hint::black_box(value)',
            'Option::unwrap(value)',
        )
        errors = self.growth(before, after)
        self.assertTrue(any('unwrap_site:' in error for error in errors), errors)

    def test_associated_panic_aliases_and_error_variants_are_fingerprinted(self):
        candidates = (
            ('expect_used', 'Result::expect(value, "present")'),
            ('expect_used', 'Result::expect_err(value, "error")'),
            ('unwrap_used', 'Result::unwrap_err(value)'),
        )
        for lint, candidate in candidates:
            with self.subTest(lint=lint, candidate=candidate):
                before = f'''#[expect(clippy::{lint}, reason = "owner; protocol invariant; no fallback")]
fn check(value: Result<u8, u16>) {{ let _ = std::hint::black_box(value); }}
'''
                after = before.replace('std::hint::black_box(value)', candidate)
                self.assertTrue(self.growth(before, after))

        before = '''use std::result::Result::expect as require;
#[expect(clippy::expect_used, reason = "owner; protocol invariant; no fallback")]
fn check(value: Result<u8, u16>) { let _ = std::hint::black_box(value); }
'''
        after = before.replace(
            'std::hint::black_box(value)',
            'require(value, "present")',
        )
        self.assertTrue(self.growth(before, after))

        local_before = '''#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]
fn check(value: Option<u8>) {
    let take = std::hint::black_box::<Option<u8>>;
    let _ = take(value);
}
'''
        local_after = local_before.replace(
            'std::hint::black_box::<Option<u8>>',
            'Option::unwrap',
        )
        self.assertTrue(self.growth(local_before, local_after))

    def test_struct_dead_code_expectation_cannot_hide_a_new_field(self):
        before = '#[expect(dead_code, reason = "wire DTO; compatibility field; no wire split")]\nstruct A { old: u8 }\n'
        after = before.replace('old: u8', 'old: u8, added: u8')
        errors = self.growth(before, after)
        self.assertTrue(any('fields 1 -> 2' in error for error in errors), errors)

        replacement = before.replace('old: u8', 'added: u8')
        errors = self.growth(before, replacement)
        self.assertTrue(any('field_site:' in error for error in errors), errors)

    def test_length_exception_has_an_independent_non_growing_size(self):
        before = '#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\nfn long() {\n' + ' let _x = 1;\n' * 101 + '}\n'
        after = before.replace('\n}', '\n let _y = 2;\n}')
        errors = self.growth(before, after)
        self.assertTrue(any('scope_lines' in error for error in errors), errors)

    def test_changed_reason_is_the_explicit_new_exception_decision(self):
        before = '#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\nfn long() {\n' + ' let _x = 1;\n' * 101 + '}\n'
        after = before.replace('no split', 'reviewed 120-line ceiling').replace('\n}', '\n let _y = 2;\n}')
        self.assertFalse(self.growth(before, after))


    def test_kani_harness_file_is_held_to_its_parent_declaration(self):
        harness = 'src/shard/commit_plan/proofs.rs'
        body = 'fn check() { let _: u8 = kani::any(); }'

        def layout(parent, parent_path='src/shard/commit_plan.rs', **extra):
            files = {harness: body, **({parent_path: parent} if parent is not None else {}), **extra}
            return harness_layout(syntax(files))

        self.assertEqual(layout('fn plan() {}\n#[cfg(kani)]\nmod proofs;\n'), [])
        self.assertEqual(layout('/// Harnesses.\n#[cfg(kani)] mod proofs;', 'src/shard/commit_plan/mod.rs'), [])
        for parent in ('mod proofs;', 'pub(crate) mod proofs;', '#[cfg(not(kani))] mod proofs;',
                       '#[cfg(any(kani, unix))] mod proofs;', '#[cfg(all(kani, unix))] mod proofs;',
                       '#[cfg(test)] mod proofs;', '#[cfg(feature = "kani")] mod proofs;',
                       '#[cfg_attr(unix, cfg(kani))] mod proofs;',
                       '#[cfg(kani)] #[path = "other.rs"] mod proofs;',
                       '#[cfg(kani)] mod proofs; #[cfg(not(kani))] mod proofs;',
                       '#[cfg(kani)] mod inner { mod proofs; }', 'fn plan() {}', None):
            with self.subTest(parent=parent):
                self.assertEqual(len(layout(parent)), 1)
        # Two candidate parents are ambiguous, and a crate root is no module parent.
        declared = '#[cfg(kani)] mod proofs;'
        self.assertEqual(len(harness_layout(syntax({harness: body, 'src/shard/commit_plan.rs': declared,
                                                   'src/shard/commit_plan/mod.rs': 'fn other() {}'}))), 1)
        self.assertEqual(len(harness_layout(syntax({'src/proofs.rs': body, 'src/lib.rs': declared}))), 1)
        sources = {harness: body, 'src/shard/commit_plan.rs': 'mod proofs;'}
        failures = violations(sources, syntax(sources), {}, {}, Counter(), {'sse_core_files': []})
        self.assertIn('Kani harness needs exactly `#[cfg(kani)] mod proofs;` in its parent module file: '
                      + harness, failures)


if __name__ == '__main__':
    unittest.main()
