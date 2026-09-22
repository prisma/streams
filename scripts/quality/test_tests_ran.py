"""The gate's test-log check fails closed on the logs the old grep accepted."""
import re
import unittest

import tests_ran

CAPACITY = 'dst::dst_tests::topology_scaling::post_split_throughput_scales'
RAN = f'''
running 1 test
test {CAPACITY} ... ok

test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 1097 filtered out; finished in 19.95s
'''
# What cargo prints, with exit code 0, when --exact names a test that was renamed.
RENAMED = '''
running 0 tests

test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 1098 filtered out; finished in 0.00s
'''


class TestsRan(unittest.TestCase):
    def test_the_old_acceptance_passed_a_leg_that_ran_nothing(self):
        self.assertTrue(re.search(r'^test result: ok', RENAMED, re.MULTILINE))
        self.assertEqual(len(tests_ran.problems(RENAMED, 1, [CAPACITY])), 2)

    def test_a_leg_that_ran_its_test_passes(self):
        self.assertEqual(tests_ran.problems(RAN, 1, [CAPACITY]), [])

    def test_the_named_test_must_be_the_one_that_ran(self):
        other = RAN.replace('post_split_throughput_scales', 'post_split_throughput')
        found = tests_ran.problems(other, 1, [CAPACITY])
        self.assertEqual(len(found), 1)
        self.assertIn('is missing', found[0])

    def test_the_floor_counts_every_binary(self):
        two = RAN + RAN.replace('1 passed', '4 passed')
        self.assertEqual(tests_ran.problems(two, 5), [])
        self.assertIn('5 test(s) passed', tests_ran.problems(two, 6)[0])

    def test_a_failed_or_missing_result_is_never_ok(self):
        failed = RAN.replace('ok. 1 passed; 0 failed', 'FAILED. 0 passed; 1 failed')
        self.assertIn('reported FAILED', tests_ran.problems(failed, 0)[0])
        self.assertIn('did not report', tests_ran.problems('error: could not compile', 0)[0])


if __name__ == '__main__':
    unittest.main()
