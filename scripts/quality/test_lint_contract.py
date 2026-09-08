import unittest
from lint_contract import from_compiler, parse_help


class LintContract(unittest.TestCase):
    def test_actual_compiler_inventory_and_incomplete_input(self):
        groups, denied = from_compiler()
        self.assertIn('rust_2024_compatibility', groups)
        self.assertIn('clippy::correctness', groups)
        self.assertIn('unsafe_op_in_unsafe_fn', denied)
        self.assertIn('clippy::eq_op', denied)
        self.assertNotIn('dead_code', denied)
        with self.assertRaises(ValueError):
            parse_help('missing compiler catalog')


if __name__ == '__main__':
    unittest.main()
