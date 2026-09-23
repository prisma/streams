import copy
import json
from pathlib import Path
import tempfile
import unittest
from collections import Counter
from diagnostics import compare, inventory, parse


class Diagnostics(unittest.TestCase):
    def sample(self, offset=0, line=1, lint='clippy::unwrap_used', level='warning'):
        return {'reason': 'compiler-message', 'message': {
            'code': {'code': lint}, 'level': level, 'message': 'unwrap used', 'spans': [{
                'file_name': 'src/a.rs', 'is_primary': True, 'byte_start': offset,
                'byte_end': offset + 4, 'line_start': line, 'line_end': line, 'column_start': 1}]}}

    def run_log(self, messages):
        with tempfile.TemporaryDirectory() as tmp:
            log = Path(tmp) / 'log'; log.write_text('\n'.join(map(json.dumps, messages)))
            return parse(log, {'src/a.rs': 'x.unwrap();\nx.unwrap();'},
                         {'src/a.rs': {'items': []}})

    def test_compilation_dedup_preserves_distinct_identical_occurrences(self):
        first = self.sample()
        counts, errors = self.run_log([first, copy.deepcopy(first), self.sample(12, 2),
                                      {'reason': 'build-finished', 'success': True}])
        self.assertFalse(errors)
        self.assertEqual(list(counts.values()), [2])
        allowed = Counter({next(iter(counts)): 1})
        self.assertTrue(compare(counts, allowed))

    def test_denied_error_unknown_and_incomplete_fail(self):
        for bad in [self.sample(lint='unused_must_use'), self.sample(level='error'),
                    self.sample(lint=None)]:
            self.assertTrue(self.run_log([bad, {'reason': 'build-finished', 'success': True}])[1])
        self.assertTrue(self.run_log([])[1])

    def test_a_refused_diagnostic_shows_where_the_compiler_found_it(self):
        # Review item 68: under -D warnings every finding is an error, and a
        # failure line that carries only the lint and its message leaves the
        # reader to rerun clippy by hand to learn where it fired.
        bad = self.sample(level='error')
        bad['message']['rendered'] = 'error: unwrap used\n --> src/a.rs:1:1\n'
        # The lib, bin and test compilations each report it: shown once.
        failures = self.run_log([bad, copy.deepcopy(bad),
                                 {'reason': 'build-finished', 'success': False}])[1]
        self.assertEqual(sum('src/a.rs:1:1' in f for f in failures), 1, failures)
        # A compiler error with no lint code is shown the same way.
        broken = self.sample(level='error', lint=None)
        broken['message']['rendered'] = 'error[E0425]: cannot find value\n --> src/a.rs:2:5\n'
        failures = self.run_log([broken, {'reason': 'build-finished', 'success': False}])[1]
        self.assertTrue(any('src/a.rs:2:5' in f for f in failures), failures)

    def test_invalid_allowances_fail(self):
        record = dict(lint='x', path='a', item='f', fingerprint='0', count=1)
        with self.assertRaises(ValueError):
            inventory([record, record])
        with self.assertRaises(ValueError):
            inventory([dict(record, count=0)])

    def test_no_regrowth_and_removal(self):
        self.assertTrue(compare(Counter(a=2), Counter(a=1)))
        self.assertFalse(compare(Counter(), Counter(a=1)))


if __name__ == '__main__':
    unittest.main()
