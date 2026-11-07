import unittest

from linearizability import CheckResult, HistoryOperation, check_history


def op(ident, kind, key=b"k", value=None, start=0, end=1, status="ACK", returned=None, batch=()):
    return HistoryOperation(ident, "client", kind, key, value, batch, start, end, status, returned)


class LinearizabilityTests(unittest.TestCase):
    def test_valid_sequential_history(self):
        history = [op("p", "PUT", value=b"v"), op("g", "GET", end=2, returned=b"v", status="VALUE")]
        self.assertEqual(check_history(history).result, CheckResult.LINEARIZABLE)

    def test_valid_overlapping_put_get(self):
        history = [
            op("p", "PUT", value=b"v", start=1, end=5),
            op("g", "GET", start=2, end=3, status="NOT_FOUND"),
        ]
        self.assertEqual(check_history(history).result, CheckResult.LINEARIZABLE)

    def test_atomic_batch(self):
        history = [
            op("b", "BATCH", key=None, batch=(("PUT", b"a", b"1"), ("PUT", b"b", b"2"))),
            op("g", "GET", key=b"a", start=0, end=2, status="VALUE", returned=b"1"),
        ]
        self.assertEqual(check_history(history).result, CheckResult.LINEARIZABLE)

    def test_partial_batch_is_rejected(self):
        history = [
            op("b", "BATCH", key=None, batch=(("PUT", b"a", b"1"), ("PUT", b"b", b"2"))),
            op("ga", "GET", key=b"a", start=2, end=3, status="VALUE", returned=b"1"),
            op("gb", "GET", key=b"b", start=2, end=3, status="NOT_FOUND"),
        ]
        self.assertEqual(check_history(history).result, CheckResult.NOT_LINEARIZABLE)

    def test_impossible_get_rejected(self):
        history = [op("p", "PUT", value=b"v"), op("g", "GET", status="VALUE", returned=b"wrong", start=2, end=3)]
        self.assertEqual(check_history(history).result, CheckResult.NOT_LINEARIZABLE)

    def test_real_time_violation_rejected(self):
        history = [
            op("p1", "PUT", value=b"1", start=0, end=10),
            op("p2", "PUT", value=b"2", start=11, end=12),
            op("g", "GET", start=13, end=14, status="VALUE", returned=b"1"),
        ]
        self.assertEqual(check_history(history).result, CheckResult.NOT_LINEARIZABLE)

    def test_duplicate_and_inverted_timestamps(self):
        self.assertEqual(check_history([op("x", "GET", status="NOT_FOUND"), op("x", "GET", status="NOT_FOUND")]).result, CheckResult.NOT_LINEARIZABLE)
        self.assertEqual(check_history([op("x", "GET", start=4, end=3, status="NOT_FOUND")]).result, CheckResult.NOT_LINEARIZABLE)
        self.assertEqual(check_history([op("x", "GET", start=None, end=3, status="NOT_FOUND")]).result, CheckResult.NOT_LINEARIZABLE)

    def test_budget_exhaustion_is_inconclusive(self):
        history = [op(str(index), "PUT", key=str(index).encode(), value=b"v", start=0, end=10) for index in range(8)]
        self.assertEqual(check_history(history, max_states=1).result, CheckResult.INCONCLUSIVE)


if __name__ == "__main__":
    unittest.main()
