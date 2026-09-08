import unittest
from verification_plan import plan


class Triggers(unittest.TestCase):
    def test_trigger_controls(self):
        self.assertFalse(plan(['README.md'])['mutants'])
        self.assertTrue(plan(['src/postings/validated.rs'])['properties_fuzz'])
        self.assertTrue(plan(['src/shard/commit_handoff.rs'])['loom'])
        self.assertTrue(plan(['src/application/read_batch.rs'])['miri'])
        self.assertTrue(plan(['src/bootstrap/rss.rs'])['mutants'])
        self.assertTrue(plan(['src/new_owner.rs'])['compiler'])


if __name__ == '__main__':
    unittest.main()
