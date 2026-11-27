"""Tests for scripts/dev/local_mutants.py's parsing (it runs nothing here)."""
import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location('local_mutants', ROOT / 'scripts/dev/local_mutants.py')
local_mutants = importlib.util.module_from_spec(spec)
spec.loader.exec_module(local_mutants)


class LocalMutants(unittest.TestCase):
    def test_the_plan_jobs_outputs_are_read_back(self):
        text = 'width=11\nshards=[0, 1]\ndigest=ab12\n'
        self.assertEqual(local_mutants.parse_outputs(text),
                         {'width': '11', 'shards': '[0, 1]', 'digest': 'ab12'})

    def test_every_survivor_line_of_a_log_is_found_in_order(self):
        log = ('ok       Unmutated baseline in 9s build + 1s test\n'
               'MISSED   src/fleet/dead.rs:27:68: replace + with - in 4s build + 1s test\n'
               'caught   src/fleet/dead.rs:30:9: replace x with y\n'
               'TIMEOUT  src/fleet/leave.rs:12:5: replace f with ()\n'
               'INCOMPLETE src/a.rs:1:1: m (signal) [log]\n'
               '  fleet_dead: missed 1 (/x/missed.txt)\n')
        self.assertEqual(local_mutants.survivors(log), [
            'MISSED   src/fleet/dead.rs:27:68: replace + with - in 4s build + 1s test',
            'TIMEOUT  src/fleet/leave.rs:12:5: replace f with ()',
            'INCOMPLETE src/a.rs:1:1: m (signal) [log]',
        ])


if __name__ == '__main__':
    unittest.main()
