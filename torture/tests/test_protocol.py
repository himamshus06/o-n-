import base64
import unittest

from protocol import (
    Acknowledgement,
    BatchRequest,
    CompactRequest,
    DeleteRequest,
    DumpRequest,
    DumpResponse,
    ErrorResponse,
    GetRequest,
    MAX_BATCH_OPERATIONS,
    MAX_KEY_SIZE,
    MAX_VALUE_SIZE,
    NotFoundResponse,
    ProtocolValidationError,
    PutRequest,
    ValueResponse,
    decode_base64,
    encode_bytes,
    request_from_dict,
    request_to_dict,
    response_from_dict,
    response_to_dict,
)


class ProtocolTests(unittest.TestCase):
    def test_base64_round_trip_arbitrary_bytes(self):
        value = bytes(range(256)) + b"\x00\n"
        encoded = encode_bytes(value, field="value")
        self.assertEqual(decode_base64(encoded, field="value", max_size=1000), value)

    def test_invalid_or_incorrectly_padded_base64_is_rejected(self):
        for value in ("not base64!", "YQ", "Y===", "é"):
            with self.subTest(value=value), self.assertRaises(ProtocolValidationError):
                decode_base64(value, field="key", max_size=100)

    def test_decoded_key_and_value_limits(self):
        with self.assertRaises(ProtocolValidationError):
            decode_base64(base64.b64encode(b"k" * (MAX_KEY_SIZE + 1)).decode(), field="key", max_size=MAX_KEY_SIZE)
        with self.assertRaises(ProtocolValidationError):
            decode_base64(base64.b64encode(b"v" * (MAX_VALUE_SIZE + 1)).decode(), field="value", max_size=MAX_VALUE_SIZE)

    def test_string_integer_and_boolean_ids(self):
        self.assertEqual(request_from_dict({"id": "x", "op": "DUMP"}).request_id, "x")
        self.assertEqual(request_from_dict({"id": 7, "op": "DUMP"}).request_id, 7)
        with self.assertRaises(ProtocolValidationError):
            request_from_dict({"id": True, "op": "DUMP"})

    def test_request_encoding_and_operations(self):
        request = PutRequest("put", b"key\x00", b"value\n")
        encoded = request_to_dict(request)
        self.assertEqual(encoded["key"], base64.b64encode(b"key\x00").decode())
        self.assertEqual(request_from_dict(encoded), request)
        self.assertEqual(request_to_dict(GetRequest(2, b"k"))["op"], "GET")
        self.assertEqual(request_to_dict(DeleteRequest(3, b"k"))["op"], "DELETE")
        self.assertEqual(request_to_dict(DumpRequest(4))["op"], "DUMP")
        self.assertEqual(request_to_dict(CompactRequest(5))["op"], "COMPACT")

    def test_batch_sizes_and_get_rejection(self):
        one = BatchRequest(1, (PutRequest(1, b"k", b"v"),))
        self.assertEqual(len(one.operations), 1)
        many = BatchRequest(2, tuple(PutRequest(i, b"k", b"v") for i in range(MAX_BATCH_OPERATIONS)))
        self.assertEqual(len(many.operations), MAX_BATCH_OPERATIONS)
        with self.assertRaises(ProtocolValidationError):
            BatchRequest(3, tuple(PutRequest(i, b"k", b"v") for i in range(MAX_BATCH_OPERATIONS + 1)))
        with self.assertRaises(ProtocolValidationError):
            request_from_dict({"id": 4, "op": "BATCH", "operations": [{"op": "GET", "key": "aw=="}]})

    def test_batch_validation_happens_before_construction(self):
        with self.assertRaises(ProtocolValidationError):
            request_from_dict({"id": 5, "op": "BATCH", "operations": [{"op": "PUT", "key": "aw=="}, {"op": "PUT", "key": "!!!", "value": "dg=="}]})

    def test_exact_response_statuses(self):
        self.assertEqual(response_from_dict({"id": 1, "status": "ACK"}), Acknowledgement(1))
        self.assertEqual(response_from_dict({"id": 2, "status": "VALUE", "value": "cmVk"}), ValueResponse(2, b"red"))
        self.assertEqual(response_from_dict({"id": 2, "status": "NOT_FOUND"}), NotFoundResponse(2))
        self.assertEqual(response_from_dict({"id": 5, "status": "DUMP", "entries": [{"key": "YXBwbGU=", "value": "cmVk"}]}), DumpResponse(5, ((b"apple", b"red"),)))
        self.assertEqual(response_from_dict({"id": 3, "status": "ERROR", "error": "Invalid request"}), ErrorResponse(3, "Invalid request"))

    def test_plain_ack_is_durable_and_unknown_outcome_is_preserved(self):
        ack = response_from_dict({"id": "1", "status": "ACK"})
        self.assertTrue(ack.is_durable)
        unknown = response_from_dict({"id": "7", "status": "ERROR", "outcome": "UNKNOWN", "error": "uncertain"})
        self.assertEqual(response_to_dict(unknown)["outcome"], "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
