
import unittest

from protocol import (
    Acknowledgement,
    BatchRequest,
    DeleteRequest,
    DumpResponse,
    GetRequest,
    NotFoundResponse,
    PutRequest,
    ValueResponse,
)
from oracle import Oracle


class TestOracle(unittest.TestCase):

    def setUp(self):
        self.oracle = Oracle()

    def test_put(self):
        self.oracle.apply_put(b"apple", b"red")

        self.assertEqual(
            self.oracle.expected_value(b"apple"),
            b"red"
        )

    def test_update_existing_key(self):
        self.oracle.apply_put(b"apple", b"red")
        self.oracle.apply_put(b"apple", b"green")

        self.assertEqual(
            self.oracle.expected_value(b"apple"),
            b"green"
        )

    def test_delete(self):
        self.oracle.apply_put(b"apple", b"red")
        self.oracle.apply_delete(b"apple")

        self.assertIsNone(
            self.oracle.expected_value(b"apple")
        )

    def test_delete_missing_key(self):
        self.oracle.apply_delete(b"missing")

        self.assertIsNone(
            self.oracle.expected_value(b"missing")
        )

    def test_batch(self):
        batch = BatchRequest(
            request_id="1",
            operations=(
                PutRequest("1", b"apple", b"red"),
                PutRequest("1", b"banana", b"yellow"),
                DeleteRequest("1", b"apple"),
            ),
        )

        self.oracle.apply_batch(batch)

        self.assertIsNone(self.oracle.expected_value(b"apple"))
        self.assertEqual(
            self.oracle.expected_value(b"banana"),
            b"yellow"
        )

    def test_verify_get_value(self):
        self.oracle.apply_put(b"apple", b"red")

        self.oracle.verify_get(
            GetRequest("1", b"apple"),
            ValueResponse("1", b"red"),
        )

    def test_verify_get_missing(self):
        self.oracle.verify_get(
            GetRequest("1", b"apple"),
            NotFoundResponse("1"),
        )

    def test_verify_get_detects_wrong_value(self):
        self.oracle.apply_put(b"apple", b"red")

        with self.assertRaises(AssertionError):
            self.oracle.verify_get(
                GetRequest("1", b"apple"),
                ValueResponse("1", b"green"),
            )

    def test_verify_dump(self):
        self.oracle.apply_put(b"apple", b"red")
        self.oracle.apply_put(b"banana", b"yellow")

        response = DumpResponse(
            "1",
            (
                (b"apple", b"red"),
                (b"banana", b"yellow"),
            ),
        )

        self.oracle.verify_dump(response)

    def test_verify_dump_detects_corruption(self):
        self.oracle.apply_put(b"apple", b"red")

        response = DumpResponse(
            "1",
            ((b"apple", b"green"),),
        )

        with self.assertRaises(AssertionError):
            self.oracle.verify_dump(response)

    def test_confirmed_write(self):
        request = PutRequest("1", b"apple", b"red")

        self.oracle.apply_confirmed_write(
            request,
            Acknowledgement("1"),
        )

        self.assertEqual(
            self.oracle.expected_value(b"apple"),
            b"red"
        )

    def test_snapshot_is_independent(self):
        self.oracle.apply_put(b"apple", b"red")

        snapshot = self.oracle.snapshot()
        snapshot[b"apple"] = b"green"

        self.assertEqual(
            self.oracle.expected_value(b"apple"),
            b"red"
        )


if __name__ == "__main__":
    unittest.main()

