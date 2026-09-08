from collections import Counter
import unittest
from source_rules import absolute, violations


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


if __name__ == '__main__':
    unittest.main()
