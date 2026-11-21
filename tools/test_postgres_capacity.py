import unittest

from postgres_capacity import route_arrivals, summarize


class CapacityTest(unittest.TestCase):
    def test_partitioning_preserves_offers_and_graph_mix(self):
        routes = route_arrivals(800, 4, 0)
        self.assertEqual(sorted(sum(routes, [])), list(range(800)))
        for route in routes:
            self.assertEqual(len(route), 200)
            self.assertEqual(sum(i % 8 == 2 for i in route), 25)
            self.assertEqual(sum(i % 8 == 6 for i in route), 25)

    def test_hot_shard_takes_the_requested_share_of_each_shape(self):
        hot, *other = route_arrivals(800, 4, 70)
        self.assertEqual(len(hot), 560)
        self.assertEqual([len(route) for route in other], [80, 80, 80])
        self.assertEqual(sum(i % 8 == 2 for i in hot), 70)

    def test_goodput_excludes_drain_and_backlog_includes_unsubmitted_offers(self):
        rows = [(0, 0, 0, 10, 100), (1, 250, 250, 270, 500),
                (2, 500, 500, 520, 999), (3, 750, 1050, 1070, 1200)]
        result = summarize(rows, 1000, 1000, 0, 1000)
        self.assertEqual(result["during_offer_runs_s"], 3000)
        self.assertEqual((result["backlog_at_midpoint"], result["backlog_at_end"]), (1, 1))
        self.assertEqual((result["peak_backlog"], result["drain_us"]), (2, 200))
        self.assertTrue(result["passes"])
        self.assertFalse(summarize(rows, 1000, 1000, 0, 299)["passes"])

    def test_backlog_growth_fails_even_when_tail_latency_passes(self):
        rows = [(0, 0, 0, 10, 100), (1, 500, 500, 510, 1100)]
        self.assertFalse(summarize(rows, 1000, 1000, 0, 25)["passes"])


if __name__ == "__main__":
    unittest.main()
