"""Typed implementation of the frozen Go database JSONL protocol."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import base64
import binascii
from typing import Any, Mapping, TypeAlias


RequestId: TypeAlias = int | str
MAX_KEY_SIZE = 1_024
MAX_VALUE_SIZE = 1_024 * 1_024
MAX_BATCH_OPERATIONS = 10_000


class ProtocolValidationError(ValueError):
    """Raised when a request or response violates the frozen protocol."""


class Operation(str, Enum):
    PUT = "PUT"
    GET = "GET"
    DELETE = "DELETE"
    BATCH = "BATCH"
    DUMP = "DUMP"
    COMPACT = "COMPACT"
    CLOSE = "CLOSE"


class Durability(str, Enum):
    DURABLE = "DURABLE"


def validate_request_id(request_id: RequestId) -> RequestId:
    if isinstance(request_id, bool) or not isinstance(request_id, (int, str)):
        raise ProtocolValidationError("id must be a JSON string or integer, not boolean")
    if isinstance(request_id, int) and request_id < 0:
        raise ProtocolValidationError("integer id must not be negative")
    if isinstance(request_id, str) and not request_id:
        raise ProtocolValidationError("string id must not be empty")
    return request_id


def encode_bytes(value: bytes, *, field: str) -> str:
    if not isinstance(value, bytes):
        raise ProtocolValidationError(f"{field} must be bytes")
    return base64.b64encode(value).decode("ascii")


def decode_base64(value: Any, *, field: str, max_size: int) -> bytes:
    if not isinstance(value, str):
        raise ProtocolValidationError(f"{field} must be a Base64 string")
    try:
        decoded = base64.b64decode(value.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error, ValueError) as exc:
        raise ProtocolValidationError(f"{field} is not valid padded Base64") from exc
    if base64.b64encode(decoded).decode("ascii") != value:
        raise ProtocolValidationError(f"{field} is not canonically padded Base64")
    if len(decoded) > max_size:
        raise ProtocolValidationError(f"{field} exceeds the {max_size}-byte decoded limit")
    return decoded


@dataclass(frozen=True, slots=True)
class PutRequest:
    request_id: RequestId
    key: bytes
    value: bytes
    operation: Operation = Operation.PUT

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)
        if not isinstance(self.key, bytes) or len(self.key) > MAX_KEY_SIZE:
            raise ProtocolValidationError("key must be bytes of at most 1024 bytes")
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_SIZE:
            raise ProtocolValidationError("value must be bytes of at most 1 MB")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "PUT", "key": encode_bytes(self.key, field="key"), "value": encode_bytes(self.value, field="value")}


@dataclass(frozen=True, slots=True)
class GetRequest:
    request_id: RequestId
    key: bytes
    operation: Operation = Operation.GET

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)
        if not isinstance(self.key, bytes) or len(self.key) > MAX_KEY_SIZE:
            raise ProtocolValidationError("key must be bytes of at most 1024 bytes")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "GET", "key": encode_bytes(self.key, field="key")}


@dataclass(frozen=True, slots=True)
class DeleteRequest:
    request_id: RequestId
    key: bytes
    operation: Operation = Operation.DELETE

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)
        if not isinstance(self.key, bytes) or len(self.key) > MAX_KEY_SIZE:
            raise ProtocolValidationError("key must be bytes of at most 1024 bytes")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "DELETE", "key": encode_bytes(self.key, field="key")}


BatchItem: TypeAlias = PutRequest | DeleteRequest


@dataclass(frozen=True, slots=True)
class BatchRequest:
    request_id: RequestId
    operations: tuple[BatchItem, ...]
    operation: Operation = Operation.BATCH

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)
        if not 1 <= len(self.operations) <= MAX_BATCH_OPERATIONS:
            raise ProtocolValidationError("batch must contain between 1 and 10,000 operations")
        if any(not isinstance(item, (PutRequest, DeleteRequest)) for item in self.operations):
            raise ProtocolValidationError("batch items must be PUT or DELETE operations")

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "BATCH", "operations": [{key: value for key, value in item.to_dict().items() if key != "id"} for item in self.operations]}


@dataclass(frozen=True, slots=True)
class DumpRequest:
    request_id: RequestId
    operation: Operation = Operation.DUMP

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "DUMP"}


@dataclass(frozen=True, slots=True)
class CompactRequest:
    request_id: RequestId
    operation: Operation = Operation.COMPACT

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "COMPACT"}


@dataclass(frozen=True, slots=True)
class CloseRequest:
    """Crash-probe-only close control; normal servers reject it."""

    request_id: RequestId
    operation: Operation = Operation.CLOSE

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)

    def to_dict(self) -> dict[str, Any]:
        return {"id": self.request_id, "op": "CLOSE"}


Request: TypeAlias = PutRequest | GetRequest | DeleteRequest | BatchRequest | DumpRequest | CompactRequest | CloseRequest


@dataclass(frozen=True, slots=True)
class DatabaseResponse:
    request_id: RequestId

    def __post_init__(self) -> None:
        validate_request_id(self.request_id)


@dataclass(frozen=True, slots=True)
class Acknowledgement(DatabaseResponse):
    @property
    def durability(self) -> Durability:
        return Durability.DURABLE

    @property
    def is_durable(self) -> bool:
        return True


@dataclass(frozen=True, slots=True)
class ValueResponse(DatabaseResponse):
    value: bytes

    def __post_init__(self) -> None:
        super(ValueResponse, self).__post_init__()
        if not isinstance(self.value, bytes) or len(self.value) > MAX_VALUE_SIZE:
            raise ProtocolValidationError("response value must be bytes of at most 1 MB")


@dataclass(frozen=True, slots=True)
class NotFoundResponse(DatabaseResponse):
    pass


@dataclass(frozen=True, slots=True)
class DumpResponse(DatabaseResponse):
    entries: tuple[tuple[bytes, bytes], ...]

    def __post_init__(self) -> None:
        super(DumpResponse, self).__post_init__()
        for key, value in self.entries:
            if not isinstance(key, bytes) or len(key) > MAX_KEY_SIZE:
                raise ProtocolValidationError("DUMP key exceeds the 1024-byte limit")
            if not isinstance(value, bytes) or len(value) > MAX_VALUE_SIZE:
                raise ProtocolValidationError("DUMP value exceeds the 1 MB limit")


@dataclass(frozen=True, slots=True)
class ErrorResponse(DatabaseResponse):
    error: str
    outcome: str | None = None

    def __post_init__(self) -> None:
        super(ErrorResponse, self).__post_init__()
        if not isinstance(self.error, str) or not self.error:
            raise ProtocolValidationError("error must be a non-empty string")
        if self.outcome is not None and self.outcome != "UNKNOWN":
            raise ProtocolValidationError("outcome must be UNKNOWN when present")


Response: TypeAlias = Acknowledgement | ValueResponse | NotFoundResponse | DumpResponse | ErrorResponse


def request_to_dict(request: Request) -> dict[str, Any]:
    return request.to_dict()


def request_from_dict(data: Mapping[str, Any]) -> Request:
    if not isinstance(data, Mapping):
        raise ProtocolValidationError("request must be a JSON object")
    try:
        request_id = validate_request_id(data["id"])
        operation = Operation(data["op"])
    except (KeyError, ValueError) as exc:
        raise ProtocolValidationError("request must contain a valid id and op") from exc
    try:
        if operation is Operation.PUT:
            return PutRequest(request_id, decode_base64(data["key"], field="key", max_size=MAX_KEY_SIZE), decode_base64(data["value"], field="value", max_size=MAX_VALUE_SIZE))
        if operation is Operation.GET:
            return GetRequest(request_id, decode_base64(data["key"], field="key", max_size=MAX_KEY_SIZE))
        if operation is Operation.DELETE:
            return DeleteRequest(request_id, decode_base64(data["key"], field="key", max_size=MAX_KEY_SIZE))
        if operation is Operation.DUMP:
            return DumpRequest(request_id)
        if operation is Operation.COMPACT:
            return CompactRequest(request_id)
        if operation is Operation.BATCH:
            raw_operations = data["operations"]
            if not isinstance(raw_operations, list) or not 1 <= len(raw_operations) <= MAX_BATCH_OPERATIONS:
                raise ProtocolValidationError("batch must contain between 1 and 10,000 operations")
            items: list[BatchItem] = []
            for raw_item in raw_operations:
                if not isinstance(raw_item, Mapping):
                    raise ProtocolValidationError("each batch item must be an object")
                item = dict(raw_item)
                item["id"] = request_id
                parsed = request_from_dict(item)
                if not isinstance(parsed, (PutRequest, DeleteRequest)):
                    raise ProtocolValidationError("GET, BATCH, and DUMP are not allowed inside BATCH")
                items.append(parsed)
            return BatchRequest(request_id, tuple(items))
    except KeyError as exc:
        raise ProtocolValidationError(f"missing request field: {exc.args[0]}") from exc
    raise ProtocolValidationError(f"unsupported operation: {operation.value}")


def response_from_dict(data: Mapping[str, Any]) -> Response:
    if not isinstance(data, Mapping):
        raise ProtocolValidationError("response must be a JSON object")
    try:
        request_id = validate_request_id(data["id"])
        status = data["status"]
    except (KeyError, ValueError) as exc:
        raise ProtocolValidationError("response must contain a valid id and status") from exc
    if not isinstance(status, str):
        raise ProtocolValidationError("status must be a string")
    if status == "ACK":
        return Acknowledgement(request_id)
    if status == "VALUE":
        return ValueResponse(request_id, decode_base64(data["value"], field="value", max_size=MAX_VALUE_SIZE))
    if status == "NOT_FOUND":
        return NotFoundResponse(request_id)
    if status == "DUMP":
        raw_entries = data["entries"]
        if not isinstance(raw_entries, list):
            raise ProtocolValidationError("DUMP entries must be an array")
        entries = []
        for entry in raw_entries:
            if not isinstance(entry, Mapping):
                raise ProtocolValidationError("each DUMP entry must be an object")
            entries.append((decode_base64(entry["key"], field="key", max_size=MAX_KEY_SIZE), decode_base64(entry["value"], field="value", max_size=MAX_VALUE_SIZE)))
        return DumpResponse(request_id, tuple(entries))
    if status == "ERROR":
        return ErrorResponse(request_id, data["error"], data.get("outcome"))
    raise ProtocolValidationError(f"unsupported response status: {status}")


def response_to_dict(response: Response) -> dict[str, Any]:
    if isinstance(response, Acknowledgement):
        return {"id": response.request_id, "status": "ACK"}
    if isinstance(response, ValueResponse):
        return {"id": response.request_id, "status": "VALUE", "value": encode_bytes(response.value, field="value")}
    if isinstance(response, NotFoundResponse):
        return {"id": response.request_id, "status": "NOT_FOUND"}
    if isinstance(response, DumpResponse):
        return {"id": response.request_id, "status": "DUMP", "entries": [{"key": encode_bytes(key, field="key"), "value": encode_bytes(value, field="value")} for key, value in response.entries]}
    if isinstance(response, ErrorResponse):
        result: dict[str, Any] = {"id": response.request_id, "status": "ERROR", "error": response.error}
        if response.outcome is not None:
            result["outcome"] = response.outcome
        return result
    raise ProtocolValidationError(f"unsupported response type: {type(response).__name__}")
