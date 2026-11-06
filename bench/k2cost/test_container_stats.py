"""scrape.container_stats: a containerised server's CPU and RSS (bench/ramp/k2-server.sh)."""
import unittest

from scrape import container_stats


class ContainerStats(unittest.TestCase):
    def test_reads_cgroup_cpu_microseconds_and_resident_pages(self):
        text = "usage_usec 2500000\n55000 12800 900 1 0 20000 0\n"
        self.assertEqual(container_stats(text), {"cpu_s": 2.5, "rss_kb": 51200})

    def test_refuses_output_without_both_lines(self):
        with self.assertRaises(IndexError):
            container_stats("usage_usec 1\n")


if __name__ == "__main__":
    unittest.main()
