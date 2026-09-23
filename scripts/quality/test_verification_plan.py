from pathlib import Path
import re
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import verification_plan
from common import ROOT, syntax
from mutation_owners import MutationOwner, OWNERS, validate_plan
from verification_plan import (
    discover_changes,
    is_visibility_only,
    plan,
    plan_changes,
    plan_schedule,
)


class Triggers(unittest.TestCase):
    def test_every_selected_check_gates_a_workflow_step(self):
        workflow = (ROOT / '.github/workflows/rust-quality.yml').read_text()
        exported = re.search(r"for name in \[([^\]]*)\]:", workflow)
        self.assertIsNotNone(exported, 'rust-quality no longer exports plan checks')
        exported = sorted(re.findall(r"'(\w+)'", exported.group(1)))
        for checks in (plan(['src/shard.rs']), plan_schedule(0)):
            self.assertEqual(sorted(k for k, v in checks.items() if type(v) is bool), exported)
        for name in exported:
            self.assertIn(f"if: env.CHECK_{name.upper()} == 'true'", workflow)

    def test_rollup_storage_requires_properties_and_mutations(self):
        checks = plan(['src/rollup/storage.rs'])
        self.assertTrue(checks['properties_fuzz'])
        self.assertTrue(checks['mutants'])

    def test_rollup_allocation_requires_properties_and_mutations(self):
        checks = plan(['src/rollup/allocation.rs'])
        self.assertTrue(checks['properties_fuzz'])
        self.assertTrue(checks['mutants'])

    def test_visibility_selection_uses_real_syntax(self):
        before = 'pub struct A { pub field: u8 }\nimpl A { pub fn value(&self)->u8 { self.field } }'
        after = before.replace('pub ', 'pub(crate) ')
        path = 'src/crypto.rs'
        def classify(candidate):
            return is_visibility_only(before, candidate, syntax({path: before})[path], syntax({path: candidate})[path])
        self.assertTrue(classify(after))
        checks = plan([path], [path])
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
            self.assertFalse(checks['mutants'])  # No changed production mutation scope.

    def test_mutation_source_files_name_exactly_the_critical_executable_changes(self):
        checks = plan(['src/shard_directory.rs', 'src/ops.rs', 'src/sse/feed.rs', 'src/history.rs', 'docs/x.md'],
                      visibility_only=['src/history.rs'], production_unchanged=['src/ops.rs'],
                      formatted_visibility=['src/sse/feed.rs'])
        self.assertEqual(checks['mutation_source_files'], ['src/shard_directory.rs'])
        self.assertTrue(checks['mutants'])
        quiet = plan(['src/shard_directory.rs'], production_unchanged=['src/shard_directory.rs'])
        self.assertEqual(quiet['mutation_source_files'], [])
        self.assertFalse(quiet['mutants'])

    def test_registered_non_prefix_source_is_selected_without_policy_duplication(self):
        checks = plan(['src/scaler3.rs'])
        self.assertEqual(checks['mutation_source_files'], ['src/scaler3.rs'])
        self.assertEqual(checks['selected_mutation_owners'], ['scaler'])
        self.assertEqual(validate_plan(checks), (next(
            owner for owner in OWNERS if owner.name == 'scaler'
        ),))

    def test_every_scheduled_receipt_reaches_the_driver_unchanged(self):
        declared = []
        discovered = []
        for slot in range(7):
            checks = plan_schedule(slot)
            selected = validate_plan(checks)
            self.assertEqual(
                checks['selected_mutation_owners'],
                [owner.name for owner in selected],
            )
            self.assertEqual(
                checks['scheduled_source_files'],
                sorted(path for owner in selected for path in owner.sources),
            )
            declared.extend(checks['selected_mutation_owners'])
            discovered.extend(owner.name for owner in selected)
        self.assertCountEqual(declared, [owner.name for owner in OWNERS])
        self.assertCountEqual(discovered, [owner.name for owner in OWNERS])
        self.assertEqual(declared.count('scaler'), 1)

    def test_deleted_critical_source_is_disposed_not_mutated(self):
        path = 'src/shard/old_owner.rs'
        checks = plan([path], deleted=[path])
        self.assertFalse(checks['mutants'])
        self.assertEqual(checks['mutation_source_files'], [])
        self.assertEqual(checks['deleted_critical_files'], [path])

    def test_unregistered_live_critical_source_remains_selected_to_fail_closed(self):
        path = 'src/shard/new_unregistered_owner.rs'
        checks = plan([path])
        self.assertTrue(checks['mutants'])
        self.assertEqual(checks['unregistered_mutation_source_files'], [path])

    def test_pilot_benchmark_changes_select_mutations(self):
        for path in ('src/bin/pilot/benchmark.rs', 'src/bin/pilot/benchmark/window.rs', 'src/bin/pilot/benchmark/config.rs'):
            self.assertTrue(plan([path])['mutants'])

    def test_generator_terminal_owner_selects_mutations(self):
        for path in ('src/bin/pilot/generator.rs', 'src/bin/pilot/generator/membership.rs'):
            self.assertTrue(plan([path])['mutants'])
            self.assertFalse(plan([path], production_unchanged=[path])['mutants'])
        self.assertFalse(plan(['src/bin/pilot/proxy.rs'])['mutants'])

    def test_touch_and_billing_state_owners_require_mutations(self):
        for path in ('src/touch.rs', 'src/billing/read_accumulator.rs', 'src/billing/read_spool.rs'):
            with self.subTest(path=path):
                self.assertTrue(plan([path])['mutants'])
                self.assertFalse(plan([path], visibility_only=[path])['mutants'])
                self.assertFalse(plan([path], production_unchanged=[path])['mutants'])

    def test_trigger_controls(self):
        self.assertFalse(plan(['README.md'])['mutants'])
        self.assertTrue(plan(['src/postings/validated.rs'])['properties_fuzz'])
        self.assertTrue(plan(['src/product_cursor/decode.rs'])['mutants'])
        self.assertFalse(plan(['src/product_cursor.rs'], ['src/product_cursor.rs'])['mutants'])
        self.assertTrue(plan(['src/queue.rs'])['mutants'])
        self.assertFalse(plan(['src/queue.rs'], ['src/queue.rs'])['mutants'])
        self.assertTrue(plan(['src/shard/commit_handoff.rs'])['mutants'])
        self.assertTrue(plan(['src/application/read_batch.rs'])['miri'])
        self.assertTrue(plan(['src/bootstrap/rss.rs'])['mutants'])
        self.assertFalse(plan(['src/new_owner.rs'])['mutants'])

    def test_quota_arithmetic_selects_its_registered_mutation_owner(self):
        for path in ['src/quota.rs', 'src/quota/bucket.rs']:
            self.assertTrue(plan([path])['mutants'])
            self.assertTrue(plan([path])['properties_fuzz'])
            self.assertFalse(plan([path], [path])['mutants'])


class RenameSelection(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.git('init', '-q')
        self.git('config', 'user.email', 'quality@example.test')
        self.git('config', 'user.name', 'Quality Fixture')
        source = self.root / 'src/shard/commit_handoff.rs'
        source.parent.mkdir(parents=True)
        source.write_text(
            'pub struct Handoff { terminal: bool }\n'
            'impl Handoff {\n'
            '    pub fn open(&self) -> bool { !self.terminal }\n'
            '    pub fn terminal(&self) -> bool { self.terminal }\n'
            '    pub fn close(&mut self) { self.terminal = true; }\n'
            '}\n'
        )
        (self.root / 'src/scaler3.rs').write_text(
            'pub fn desired_instances(load: u64) -> u64 { load.max(1) }\n'
        )
        self.git('add', '.')
        self.git('commit', '-qm', 'base')
        self.base = self.git('rev-parse', 'HEAD')

    def tearDown(self):
        self.tmp.cleanup()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True).strip()

    def moved(self, modify=False, register=True):
        destination = self.root / 'src/commit_handoff.rs'
        self.git('mv', 'src/shard/commit_handoff.rs', 'src/commit_handoff.rs')
        if modify:
            destination.write_text(destination.read_text().replace(
                'self.terminal = true', 'self.terminal = false'
            ))
        changes = discover_changes(self.base, self.root)
        owners = (MutationOwner('commit_handoff', ('src/commit_handoff.rs',), ('shard::',)),) \
            if register else ()
        checks = plan_changes(
            changes,
            previous_registered={'src/shard/commit_handoff.rs': 'commit_handoff'},
            owners=owners,
        )
        return changes, checks, owners

    def test_pure_critical_rename_outside_prefix_retains_the_owner(self):
        changes, checks, owners = self.moved()
        self.assertEqual(changes[0].status, 'R100')
        self.assertEqual(checks['mutation_source_files'], ['src/commit_handoff.rs'])
        self.assertEqual(checks['selected_mutation_owners'], ['commit_handoff'])
        self.assertEqual(validate_plan(checks, owners), owners)
        self.assertEqual(checks['renamed_source_files'], [{
            'status': 'R100',
            'before': 'src/shard/commit_handoff.rs',
            'after': 'src/commit_handoff.rs',
            'previous_owner': 'commit_handoff',
            'current_owner': 'commit_handoff',
            'disposition': 'mutation-selected',
        }])

    def test_critical_rename_plus_executable_edit_cannot_disappear(self):
        changes, checks, owners = self.moved(modify=True)
        self.assertTrue(changes[0].status.startswith('R'))
        self.assertEqual(checks['mutation_source_files'], ['src/commit_handoff.rs'])
        self.assertEqual(validate_plan(checks, owners), owners)

        with patch.object(verification_plan, 'ROOT', self.root):
            visibility, production, formatted = verification_plan.source_changes(
                self.base, changes
            )
        checks = plan_changes(
            changes,
            visibility,
            production,
            formatted,
            {'src/shard/commit_handoff.rs': 'commit_handoff'},
            owners,
        )
        self.assertEqual(checks['production_unchanged_files'], [])
        self.assertEqual(checks['mutation_source_files'], ['src/commit_handoff.rs'])
        self.assertEqual(validate_plan(checks, owners), owners)

    def test_pure_relocation_has_an_explicit_non_execution_disposition(self):
        changes, _, owners = self.moved()
        with patch.object(verification_plan, 'ROOT', self.root):
            visibility, production, formatted = verification_plan.source_changes(
                self.base, changes
            )
        checks = plan_changes(
            changes,
            visibility,
            production,
            formatted,
            {'src/shard/commit_handoff.rs': 'commit_handoff'},
            owners,
        )
        self.assertEqual(checks['mutation_source_files'], [])
        self.assertEqual(checks['production_unchanged_files'], ['src/commit_handoff.rs'])
        self.assertEqual(
            checks['renamed_source_files'][0]['disposition'],
            'production-unchanged',
        )

    def test_unregistered_rename_destination_fails_before_discovery(self):
        _, checks, owners = self.moved(register=False)
        self.assertEqual(checks['unregistered_mutation_source_files'], [
            'src/commit_handoff.rs'
        ])
        with self.assertRaisesRegex(ValueError, 'src/commit_handoff.rs'):
            validate_plan(checks, owners)

    def test_previous_registered_non_prefix_owner_carries_rename_lineage(self):
        self.git('mv', 'src/scaler3.rs', 'src/scaler.rs')
        changes = discover_changes(self.base, self.root)
        owner = MutationOwner('scaler', ('src/scaler.rs',), ('scaler3::',))
        checks = plan_changes(
            changes,
            previous_registered={'src/scaler3.rs': 'scaler'},
            owners=(owner,),
        )
        self.assertEqual(checks['mutation_source_files'], ['src/scaler.rs'])
        self.assertEqual(checks['selected_mutation_owners'], ['scaler'])
        self.assertEqual(validate_plan(checks, (owner,)), (owner,))

    def test_delete_add_replacement_is_conservatively_carried(self):
        self.git('rm', 'src/shard/commit_handoff.rs')
        replacement = self.root / 'src/completely_new_owner.rs'
        replacement.parent.mkdir(exist_ok=True)
        replacement.write_text('pub fn replacement() -> bool { false }\n')
        self.git('add', '.')
        changes = discover_changes(self.base, self.root)
        owner = MutationOwner(
            'commit_handoff', ('src/completely_new_owner.rs',), ('shard::',)
        )
        checks = plan_changes(
            changes,
            previous_registered={'src/shard/commit_handoff.rs': 'commit_handoff'},
            owners=(owner,),
        )
        self.assertEqual(checks['deleted_critical_files'], [
            'src/shard/commit_handoff.rs'
        ])
        self.assertEqual(checks['possible_replacement_files'], [
            'src/completely_new_owner.rs'
        ])
        self.assertEqual(checks['deleted_source_dispositions'], [{
            'path': 'src/shard/commit_handoff.rs',
            'previous_owner': 'commit_handoff',
            'replacement_files': ['src/completely_new_owner.rs'],
            'disposition': 'owner-relocated',
        }])
        self.assertEqual(validate_plan(checks, (owner,)), (owner,))

    def test_true_deletion_is_recorded_without_mutating_absent_source(self):
        self.git('rm', 'src/shard/commit_handoff.rs')
        changes = discover_changes(self.base, self.root)
        checks = plan_changes(
            changes,
            previous_registered={'src/shard/commit_handoff.rs': 'commit_handoff'},
            owners=(),
        )
        self.assertFalse(checks['mutants'])
        self.assertEqual(checks['mutation_source_files'], [])
        self.assertEqual(checks['deleted_critical_files'], [
            'src/shard/commit_handoff.rs'
        ])
        self.assertEqual(checks['deleted_source_dispositions'], [{
            'path': 'src/shard/commit_handoff.rs',
            'previous_owner': 'commit_handoff',
            'replacement_files': [],
            'disposition': 'owner-retired',
        }])


if __name__ == '__main__':
    unittest.main()
