import unittest
from common import syntax
from verification_plan import is_visibility_only, plan


class Triggers(unittest.TestCase):
    def test_visibility_selection_uses_real_syntax_and_keeps_compiler_checks(self):
        before = 'pub struct A { pub field: u8 }\nimpl A { pub fn value(&self)->u8 { self.field } }'
        after = before.replace('pub ', 'pub(crate) ')
        path = 'src/crypto.rs'
        def classify(candidate):
            return is_visibility_only(before, candidate, syntax({path: before})[path], syntax({path: candidate})[path])
        self.assertTrue(classify(after))
        checks = plan([path], [path])
        self.assertTrue(checks['compiler'])
        self.assertFalse(checks['mutants'])
        self.assertEqual(checks['visibility_only_files'], [path])
        self.assertFalse(classify(after.replace('self.field', '0')))
        self.assertFalse(classify(after.replace('->u8', '->u16')))
        self.assertFalse(classify('#[cfg(test)]\n' + after))
        self.assertFalse(classify(after.replace('pub(crate) field', 'pub(in crate::a) field')))
        self.assertFalse(classify(before))
        self.assertTrue(plan([path, 'src/shard.rs'], [path])['mutants'])
        for text in ('pub struct É { pub field: u8 }', 'pub\nstruct A { pub\nfield: u8 }',
                     'const TEXT: &str = "\u2028"; pub struct A;'):
            narrowed = text.replace('pub', 'pub(crate)')
            self.assertTrue(is_visibility_only(text, narrowed, syntax({path: text})[path], syntax({path: narrowed})[path]))

    def test_visibility_shaped_literals_macros_and_comments_do_not_skip_checks(self):
        path = 'src/crypto.rs'
        for before in [
            'fn f() -> &\'static str { "pub struct A;" }',
            'macro_rules! f { () => { pub struct A; } }',
            '// pub struct A;\nfn f() {}',
            'pub fn f()->u8 { 1 } // pub struct A;',
        ]:
            after = before.replace('pub ', 'pub(crate) ')
            self.assertFalse(is_visibility_only(before, after, syntax({path: before})[path], syntax({path: after})[path]))

    def test_verification_and_dependency_changes_exercise_the_harness(self):
        for path in ('tools/quality-invariants/src/lib.rs', 'scripts/quality/nightly.sh', 'Cargo.lock'):
            checks = plan([path])
            self.assertTrue(checks['miri'])
            self.assertTrue(checks['properties_fuzz'])
            self.assertTrue(checks['compiler'])
            self.assertFalse(checks['mutants'])  # No changed production mutation scope.

    def test_generator_terminal_owner_selects_loom_and_mutations(self):
        for path in ('src/bin/pilot/generator.rs', 'src/bin/pilot/generator/membership.rs'):
            self.assertTrue(plan([path])['loom'])
            self.assertTrue(plan([path])['mutants'])
            self.assertFalse(plan([path], production_unchanged=[path])['mutants'])
        self.assertFalse(plan(['src/bin/pilot/proxy.rs'])['mutants'])

    def test_trigger_controls(self):
        self.assertFalse(plan(['README.md'])['mutants'])
        self.assertTrue(plan(['src/postings/validated.rs'])['properties_fuzz'])
        self.assertTrue(plan(['src/shard/commit_handoff.rs'])['loom'])
        self.assertTrue(plan(['src/application/read_batch.rs'])['miri'])
        self.assertTrue(plan(['src/bootstrap/rss.rs'])['mutants'])
        self.assertTrue(plan(['src/new_owner.rs'])['compiler'])


if __name__ == '__main__':
    unittest.main()
