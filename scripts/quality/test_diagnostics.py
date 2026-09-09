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
