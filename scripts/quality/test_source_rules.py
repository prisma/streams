from collections import Counter
import unittest
from common import syntax
from source_rules import absolute, exception_contracts, exception_growth, growth_ledger, harness_layout, violations


def source(facts):
    return {'src/application/new.rs': {'facts': facts}}


def fact(kind, value, qualified='crate::f', test=False):
    return dict(kind=kind, value=value, qualified=qualified, test_only=test)


LONG = ('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\n'
        'fn long() {\n' + ' let _x = 1;\n' * 101 + '}\n')
GROWN = LONG.replace('\n}', '\n let _y = 2;\n}')
CONTRACT = "('src/a.rs', 'crate::long', 'function', 'clippy::too_many_lines')"
GREW = 'accepted exception grew without an approved growth row: '
STALE = 'stale exception growth row (the contract no longer has these values): '
STRUCT = '#[expect(dead_code, reason = "wire DTO; compatibility field; no wire split")]\nstruct A { old: u8 }\n'
IMPL = ('#[expect(dead_code, reason = "wire DTO; compatibility field; no wire split")]\n'
        'impl A { fn old(&self) -> u8 { self.old } fn added(&self) -> u8 { self.added } }\n')


def row(**fields):
    base = dict(path='src/a.rs', owner='crate::long', scope='function', lint='clippy::too_many_lines',
                rationale='one transaction; a split re-reads its state', approver='Søren Bramer Schmidt')
    metrics = {k: fields.pop(k) for k in list(fields) if k not in base and k != 'metrics'}
    return {**base, 'metrics': metrics, **fields}


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
        for lint in ('unused', 'clippy :: correctness', 'unused_must_use', 'clippy :: eq_op',
                     'renamed_and_removed_lints'):
            for level in ('allow', 'expect', 'warn'):
                self.assertTrue(self.check(facts=[fact('attribute', f'{level} ({lint}, reason = "owner; invariant; alternative")')]))

    def growth(self, before, after, rows=()):
        old = before if isinstance(before, dict) else {'src/a.rs': before}
        new = after if isinstance(after, dict) else {'src/a.rs': after}
        return exception_growth(exception_contracts(new, syntax(new)),
                                exception_contracts(old, syntax(old)), *([rows] if rows else []))

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

    def test_a_changed_reason_does_not_admit_growth(self):
        after = GROWN.replace('no split', 'reviewed 120-line ceiling')
        self.assertEqual(self.growth(LONG, after), [f'{GREW}{CONTRACT}: scope_lines 103 -> 104'])

    def test_a_pure_reason_change_is_an_explanation_update(self):
        self.assertEqual(self.growth(LONG, LONG.replace('no split', 'reviewed ceiling')), [])
        wrapped = LONG.replace('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]',
                               '#[expect(\n    clippy::too_many_lines,\n    reason = "transaction; one sequence; no split"\n)]')
        self.assertEqual(self.growth(wrapped, wrapped.replace('no split', 'a split re-reads its state')), [])
        poisoned = '#[expect(clippy::unwrap_used, reason = "feed; poison invariant; no recovery")]\nimpl A { fn lock(&self) { self.lock.lock().unwrap(); } }\n'
        self.assertEqual(self.growth(poisoned, poisoned.replace('no recovery', 'a poisoned feed must stop')), [])

    def test_a_verbatim_move_is_compared_with_the_exception_it_left(self):
        self.assertEqual(self.growth({'src/a.rs': LONG}, {'src/b.rs': LONG}), [])
        self.assertEqual(self.growth({'src/a.rs': LONG}, {'src/b.rs': GROWN}), [
            f"{GREW}{CONTRACT.replace('src/a.rs', 'src/b.rs')} (moved from src/a.rs): scope_lines 103 -> 104"])

    def test_an_ambiguous_move_is_held_to_its_smallest_origin(self):
        shorter = LONG.replace(' let _x = 1;\n', '', 1)
        origins = {'src/a.rs': LONG, 'src/c.rs': shorter}
        # A verbatim copy of one origin is that origin, moved.
        self.assertEqual(self.growth(origins, {'src/b.rs': LONG}), [])
        self.assertEqual(self.growth(origins, {'src/b.rs': GROWN}), [
            f"{GREW}{CONTRACT.replace('src/a.rs', 'src/b.rs')} (moved from src/a.rs, src/c.rs): scope_lines 102 -> 104"])

    def test_a_renamed_owner_is_compared_with_the_exception_it_replaced(self):
        renamed = GROWN.replace('fn long()', 'fn long_steps()')
        self.assertEqual(self.growth(LONG, renamed), [
            f"{GREW}{CONTRACT.replace('crate::long', 'crate::long_steps')} (paired with vanished crate::long): scope_lines 103 -> 104"])
        # Narrowing onto an extracted helper is the same pairing, and passes
        # when the helper is no larger than the scope it left.
        helper = 'fn long() { helper(); }\n' + LONG.replace('fn long()', 'fn helper()')
        self.assertEqual(self.growth(LONG, helper), [])

    def test_a_renamed_or_narrowed_panic_exception_keeps_its_fingerprints(self):
        publish = ('#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]\n'
                   'fn publish(lock: &std::sync::Mutex<u8>) {\n    let _guard = lock.lock().unwrap();\n    other();\n}\n')
        self.assertEqual(self.growth(publish, publish.replace('fn publish', 'fn announce')), [])
        # The lock and its unwrap move onto an extracted helper whose
        # signature adds no path the owner did not already have.
        narrowed = ('fn publish(lock: &std::sync::Mutex<u8>) {\n    read(lock);\n    other();\n}\n'
                    '#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]\n'
                    'fn read(lock: &std::sync::Mutex<u8>) {\n    let _guard = lock.lock().unwrap();\n}\n')
        self.assertEqual(self.growth(publish, narrowed), [])
        self.assertEqual(self.growth(STRUCT, STRUCT.replace('struct A', 'struct B')), [])

    def test_a_verbatim_move_among_same_named_contracts_is_matched_exactly(self):
        start = ('#[expect(clippy::too_many_lines, reason = "owner; one sequence; no split")]\n'
                 'fn start() {\n' + ' let _x = 1;\n' * 101 + '}\n')
        other = start.replace(' let _x = 1;\n', '', 40)
        before = {'src/fleet.rs': start, 'src/scaler.rs': other}
        self.assertEqual(self.growth(before, {'src/fleet/mod.rs': start, 'src/scaler/mod.rs': other}), [])
        errors = self.growth(before, {'src/fleet/mod.rs': start.replace('\n}', '\n let _y = 2;\n}'),
                                      'src/scaler/mod.rs': other})
        self.assertEqual(errors, ["accepted exception grew without an approved growth row: ('src/fleet/mod.rs', "
                                  "'crate::start', 'function', 'clippy::too_many_lines') (moved from src/fleet.rs): "
                                  "scope_lines 103 -> 104"])

    def test_a_by_path_module_must_be_a_ratcheted_source(self):
        def errors(target, files):
            sources = {'src/a.rs': f'#[path = "{target}"]\nmod split;\n', **files}
            return violations(sources, syntax(sources), {}, {}, Counter(), {'sse_core_files': []})
        outside = 'by-path module outside the ratcheted source set: src/a.rs: '
        self.assertIn(f'{outside}split.rs.in', errors('split.rs.in', {}))
        self.assertIn(f'{outside}.hidden/split.rs', errors('.hidden/split.rs', {}))
        self.assertFalse([e for e in errors('split.rs', {'src/split.rs': ''}) if e.startswith(outside)])

    def test_ambiguous_renames_are_held_to_their_smallest_candidate(self):
        shorter = LONG.replace(' let _x = 1;\n', '', 1).replace('fn long()', 'fn other()')
        renamed = GROWN.replace('fn long()', 'fn first()')
        note = '(paired with vanished crate::long, crate::other)'
        self.assertEqual(self.growth(LONG + shorter, renamed), [
            f"{GREW}{CONTRACT.replace('crate::long', 'crate::first')} {note}: scope_lines 102 -> 104"])
        # An unrelated same-kind deletion does not hide a rename's growth.
        self.assertEqual(self.growth(LONG + shorter, renamed + shorter.replace('fn other()', 'fn kept()')), [
            f"{GREW}{CONTRACT.replace('crate::long', 'crate::first')} {note}: scope_lines 102 -> 104"])

    def test_a_shim_under_the_old_name_cannot_take_a_renamed_exceptions_origin(self):
        shim = LONG.replace(' let _x = 1;\n' * 101, ' let _x = 1;\n')
        after = {'src/a.rs': GROWN.replace('fn long()', 'fn long_steps()'), 'src/b.rs': shim}
        self.assertEqual(self.growth({'src/a.rs': LONG}, after), [
            f"{GREW}{CONTRACT.replace('crate::long', 'crate::long_steps')} (paired with vanished crate::long): "
            "scope_lines 103 -> 104"])

    def test_an_exception_on_a_module_declaration_measures_the_module_files(self):
        child = 'pub fn one(x: Option<u8>) -> u8 {\n    x.unwrap()\n}\n'
        grown = child + 'pub fn two(x: Option<u8>) -> u8 {\n    x.unwrap()\n}\n'
        reason = 'reason = "owner; poison invariant; no recovery"'
        for parent, contract in (
            (f'#[expect(clippy::unwrap_used, {reason})]\nmod child;\n', "('src/a.rs', 'crate::child', 'module', 'clippy::unwrap_used')"),
            (f'#![expect(clippy::unwrap_used, {reason})]\nmod child;\n', "('src/a.rs', 'crate', 'crate', 'clippy::unwrap_used')"),
        ):
            with self.subTest(contract=contract):
                errors = self.growth({'src/a.rs': parent, 'src/a/child.rs': child},
                                     {'src/a.rs': parent, 'src/a/child.rs': grown})
                self.assertIn(f'{GREW}{contract}: unwrap_sites 1 -> 2', errors)
        sources = {'src/a.rs': f'#[expect(clippy::unwrap_used, {reason})]\nmod missing;\n'}
        self.assertIn('exception covers a module file the ratchet does not read: src/a.rs: crate::missing',
                      violations(sources, syntax(sources), {}, {}, Counter(), {'sse_core_files': []}))

    def test_a_panic_site_inside_a_macro_argument_is_counted(self):
        before = ('#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]\n'
                  'fn f(x: Option<u8>, y: Option<u8>) {\n    let _ = x.unwrap();\n    tracing::info!("{:?}", y);\n}\n')
        after = before.replace('tracing::info!("{:?}", y)', 'tracing::info!("{:?}", y.unwrap())')
        self.assertIn(f"{GREW}('src/a.rs', 'crate::f', 'function', 'clippy::unwrap_used'): unwrap_sites 1 -> 2",
                      self.growth(before, after))

    def test_attributes_comments_and_blank_lines_are_not_scope(self):
        split = LONG.replace('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]',
                             '#[expect(clippy::too_many_lines, clippy::cognitive_complexity, reason = "transaction; one sequence; no split")]')
        halves = LONG.replace('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]',
                              '#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]\n'
                              '#[expect(clippy::cognitive_complexity, reason = "transaction; one sequence; no split")]')
        documented = ('/// One transaction.\n///\n/// Its steps stay in order.\n'
                      + LONG.replace(' let _x = 1;\n', ' // first\n\n let _x = 1;\n', 1))
        allowed = ('#[allow(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]\n'
                   'fn f(x: Option<u8>) -> u8 {\n    x.unwrap()\n}\n')
        for before, after in ((split, halves), (LONG, documented),
                              (allowed.replace('#[allow', '#[expect'), allowed)):
            with self.subTest(after=after[:60]):
                self.assertEqual(self.growth(before, after), [])
        # A reason that fits on fewer lines frees no body budget.
        wrapped = LONG.replace('#[expect(clippy::too_many_lines, reason = "transaction; one sequence; no split")]',
                               '#[expect(\n    clippy::too_many_lines,\n    reason = "transaction; one sequence; no split"\n)]')
        grown = LONG.replace('\n}', '\n let _y = 2;\n let _z = 3;\n let _w = 4;\n}')
        self.assertEqual(self.growth(wrapped, grown), [f'{GREW}{CONTRACT}: scope_lines 103 -> 106'])

    def test_a_copy_or_another_contract_elsewhere_is_a_new_exception(self):
        cases = (
            ({'src/a.rs': LONG}, {'src/a.rs': LONG, 'src/b.rs': GROWN}),
            ({'src/a.rs': LONG}, {'src/b.rs': GROWN.replace('too_many_lines', 'excessive_nesting')}),
            ({'src/a.rs': STRUCT}, {'src/b.rs': IMPL}),
        )
        for before, after in cases:
            with self.subTest(after=list(after)):
                self.assertEqual(self.growth(before, after), [])

    def test_a_growth_row_admits_exactly_its_recorded_growth(self):
        self.assertEqual(self.growth(LONG, GROWN, [row(scope_lines=104)]), [])
        further = GROWN.replace('\n}', '\n let _z = 3;\n}')
        self.assertEqual(self.growth(LONG, further, [row(scope_lines=104)]),
                         [f'{GREW}{CONTRACT}: scope_lines 103 -> 105', f'{STALE}{CONTRACT}'])
        self.assertEqual(self.growth(LONG, GROWN, [row(scope_lines=105)]),
                         [f'{GREW}{CONTRACT}: scope_lines 103 -> 104', f'{STALE}{CONTRACT}'])
        # A row describes the contract's current state: it stays valid after
        # the push that landed it, and goes stale once the contract changes.
        self.assertEqual(self.growth(GROWN, GROWN, [row(scope_lines=104)]), [])
        self.assertEqual(self.growth(LONG, LONG, [row(scope_lines=104)]), [f'{STALE}{CONTRACT}'])
        self.assertEqual(self.growth(LONG, LONG, [row(scope_lines=102)]), [f'{STALE}{CONTRACT}'])
        other = CONTRACT.replace('crate::long', 'crate::other')
        self.assertEqual(self.growth(LONG, LONG, [row(owner='crate::other', scope_lines=103)]),
                         [f'{STALE}{other}'])

    def test_a_growth_row_needs_its_contract_rationale_and_approver(self):
        valid = row(scope_lines=105)
        invalid = (
            [{k: v for k, v in valid.items() if k != 'approver'}],
            [dict(valid, approver='')],
            [dict(valid, rationale='  ')],
            [dict(valid, lint=None)],
            [dict(valid, note='extra')],
            [dict(valid, metrics={})],
            [dict(valid, metrics={'scope_lines': 0})],
            [dict(valid, metrics={'scope_lines': True})],
            [dict(valid, metrics={'': 105})],
            [valid, valid],
        )
        for rows in invalid:
            with self.subTest(rows=rows), self.assertRaises(ValueError):
                exception_growth({}, {}, rows)
        self.assertEqual(growth_ledger({'schema': 1, 'rows': [valid]}), [valid])
        for ledger in ({'schema': 2, 'rows': []}, {'rows': []}, {'schema': 1, 'rows': [], 'extra': 1}):
            with self.subTest(ledger=ledger), self.assertRaises(ValueError):
                growth_ledger(ledger)

    def test_merging_exception_attributes_keeps_each_lints_ceiling(self):
        body = ('fn long(lock: &std::sync::Mutex<()>, value: Option<u8>) {\n'
                '    let _guard = lock.lock().unwrap();\n' + ' let _x = 1;\n' * 101 + '}\n')
        before = ('#[expect(clippy::too_many_lines, reason = "owner; one sequence; no split")]\n'
                  '#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]\n' + body)
        after = ('#[expect(clippy::too_many_lines, clippy::unwrap_used, reason = "owner; one sequence; no split")]\n'
                 + body.replace(' let _x = 1;\n', '    let _value = value.unwrap();\n', 1))
        errors = self.growth(before, after)
        self.assertIn(f"{GREW}('src/a.rs', 'crate::long', 'function', 'clippy::unwrap_used'): unwrap_sites 1 -> 2", errors)

    def test_a_redundant_same_lint_attribute_is_not_slack(self):
        a = '#[expect(clippy::let_underscore_must_use, reason = "owner; result is advisory; no reader")]'
        before = f'{a}\nfn drop_results() {{\n    {a}\n    let _ = first();\n    let _ = second();\n}}\n'
        after = (f'{a}\nfn drop_results() {{\n    let _ = first();\n    let _ = second();\n'
                 '    let _ = third();\n    let _ = fourth();\n}\n')
        c = "('src/a.rs', 'crate::drop_results', 'function', 'clippy::let_underscore_must_use')"
        # Summed per attribute, the inner one doubled the ceiling, so
        # deleting it left room for the fn to grow unseen.
        self.assertEqual(self.growth(before, after),
                         [f'{GREW}{c}: scope_lines 4 -> 6', f'{GREW}{c}: syntax_facts 4 -> 8'])

    def test_two_scopes_under_one_identity_are_both_measured(self):
        a = '#[expect(clippy::unwrap_used, reason = "owner; poison invariant; no recovery")]'
        before = f'{a}\nimpl A {{ fn a(&self) {{ self.lock.lock().unwrap(); }} }}\n{a}\nimpl A {{ fn b(&self) {{ self.lock.lock().unwrap(); }} }}\n'
        after = before[:-len(' } }\n')] + ' self.value.unwrap(); } }\n'
        errors = self.growth(before, after)
        self.assertTrue(any(e.endswith('unwrap_sites 2 -> 3') for e in errors), errors)


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
