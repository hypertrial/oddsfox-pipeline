"""Minimal Polygon JSON-RPC and pinned Polymarket V2 event decoding."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence
from urllib.parse import urlsplit

import requests

from oddsfox_pipeline.resources.http import APIClient, RateLimiter
from oddsfox_pipeline.resources.outbound_url import validate_outbound_https_url

# Pinned at Polymarket/ctf-exchange-v2 ccc0596074f4dfd62c944fbca4de252893b82b4b.
# V2 emits side+tokenId; do not regress this decoder to V1's two asset-ID words.
ORDER_FILLED_TOPIC = (
    "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
)
ORDERS_MATCHED_TOPIC = (
    "0x174b3811690657c217184f89418266767c87e4805d09680c39fc9c031c0cab7c"
)
EVENT_TOPICS = (ORDER_FILLED_TOPIC, ORDERS_MATCHED_TOPIC)
_RPC_BATCH_SIZE = 50

_HEX_32 = re.compile(r"0x[0-9a-f]{64}\Z")
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z")
_HEX_QUANTITY = re.compile(r"0x(?:0|[1-9a-f][0-9a-f]*)\Z")


class PolygonRPCError(RuntimeError):
    """A sanitized transport, protocol, or provider JSON-RPC failure."""


class PolygonRPCSizeLimitError(PolygonRPCError):
    """A recognized provider range or JSON-RPC batch size limit."""


class PolygonRPCTransportError(PolygonRPCError):
    """A transient HTTP transport failure after the configured retries."""


class PolygonRPCProtocolError(PolygonRPCError):
    """A malformed, incomplete, conflicting, or unknown RPC response."""


@dataclass
class PolygonRPCMetrics:
    http_request_count: int = 0
    log_rpc_call_count: int = 0
    receipt_rpc_call_count: int = 0
    header_rpc_call_count: int = 0
    retry_count: int = 0

    def copy(self) -> "PolygonRPCMetrics":
        return PolygonRPCMetrics(**vars(self))

    def delta(self, earlier: "PolygonRPCMetrics") -> "PolygonRPCMetrics":
        return PolygonRPCMetrics(
            **{
                field: getattr(self, field) - getattr(earlier, field)
                for field in vars(self)
            }
        )


_SIZE_LIMIT_PATTERNS = (
    "batch limit",
    "batch size",
    "block range",
    "limit exceeded",
    "more than",
    "query returned",
    "request entity too large",
    "response size",
    "too many results",
)


def _provider_error(error: Any, *, batch: bool) -> PolygonRPCError:
    if not isinstance(error, dict) or not isinstance(error.get("code"), int):
        return PolygonRPCProtocolError("Polygon RPC returned a malformed error")
    code = int(error["code"])
    message = str(error.get("message", "")).casefold()
    recognized_code = code in {-32005, -32016, -32602}
    recognized_message = any(pattern in message for pattern in _SIZE_LIMIT_PATTERNS)
    if recognized_message and (recognized_code or code == -32000):
        scope = "batch" if batch else "range"
        return PolygonRPCSizeLimitError(f"Polygon RPC {scope} size limit")
    prefix = "batch " if batch else ""
    return PolygonRPCProtocolError(f"Polygon RPC {prefix}error code {code}")


@dataclass(frozen=True)
class PolygonBlock:
    number: int
    hash: str
    timestamp: datetime


@dataclass(frozen=True)
class PolygonReceipt:
    transaction_hash: str
    block_number: int
    block_hash: str
    transaction_index: int
    logs: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class DecodedSettlementEvent:
    kind: str
    exchange_address: str
    block_number: int
    block_hash: str
    transaction_hash: str
    transaction_index: int
    log_index: int
    side: str
    token_id: str
    maker_amount: int
    taker_amount: int


def sanitize_rpc_origin(url: str) -> str:
    """Return only scheme/host/port, excluding credentials, path, and query."""
    parsed = urlsplit(url.strip())
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise ValueError("Polygon RPC URL must be an absolute HTTPS URL")
    host = parsed.hostname.casefold()
    if ":" in host:
        host = f"[{host}]"
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Polygon RPC URL has an invalid port") from exc
    default_port = port in (None, 443)
    return f"https://{host}{'' if default_port else f':{port}'}"


def _hex_quantity(value: Any, field: str) -> int:
    text = str(value).casefold()
    if not _HEX_QUANTITY.fullmatch(text):
        raise PolygonRPCError(f"RPC field {field!r} is not a canonical hex quantity")
    return int(text, 16)


def _hex32(value: Any, field: str) -> str:
    text = str(value).casefold()
    if not _HEX_32.fullmatch(text):
        raise PolygonRPCError(f"RPC field {field!r} is not 32-byte hex")
    return text


def _data_words(value: Any, expected: int) -> tuple[int, ...]:
    text = str(value).casefold()
    if not re.fullmatch(rf"0x[0-9a-f]{{{expected * 64}}}", text):
        raise PolygonRPCError(f"V2 event data must contain exactly {expected} words")
    payload = text[2:]
    return tuple(
        int(payload[offset : offset + 64], 16) for offset in range(0, len(payload), 64)
    )


def decode_settlement_log(raw: dict[str, Any]) -> DecodedSettlementEvent:
    """Decode only non-identifying V2 settlement fields from one complete log."""
    if raw.get("removed") is not False:
        raise PolygonRPCError("Removed or incomplete Polygon log rejected")
    address = str(raw.get("address", "")).casefold()
    if not _ADDRESS.fullmatch(address):
        raise PolygonRPCError("Settlement log has an invalid exchange address")
    topics_value = raw.get("topics")
    if not isinstance(topics_value, list) or not topics_value:
        raise PolygonRPCError("Settlement log topics are missing")
    topics = tuple(_hex32(value, "topic") for value in topics_value)
    common = {
        "exchange_address": address,
        "block_number": _hex_quantity(raw.get("blockNumber"), "blockNumber"),
        "block_hash": _hex32(raw.get("blockHash"), "blockHash"),
        "transaction_hash": _hex32(raw.get("transactionHash"), "transactionHash"),
        "transaction_index": _hex_quantity(
            raw.get("transactionIndex"), "transactionIndex"
        ),
        "log_index": _hex_quantity(raw.get("logIndex"), "logIndex"),
    }
    if topics[0] == ORDER_FILLED_TOPIC:
        if len(topics) != 4:
            raise PolygonRPCError("OrderFilled must have four topics")
        side, token_id, maker_amount, taker_amount, *_ = _data_words(raw.get("data"), 7)
        kind = "order_filled"
    elif topics[0] == ORDERS_MATCHED_TOPIC:
        if len(topics) != 3:
            raise PolygonRPCError("OrdersMatched must have three topics")
        side, token_id, maker_amount, taker_amount = _data_words(raw.get("data"), 4)
        kind = "orders_matched"
    else:
        raise PolygonRPCError("Unexpected Polygon settlement event topic")
    if side not in (0, 1):
        raise PolygonRPCError("V2 settlement side must be BUY(0) or SELL(1)")
    if token_id == 0 or maker_amount == 0 or taker_amount == 0:
        raise PolygonRPCError("V2 settlement token and amounts must be positive")
    return DecodedSettlementEvent(
        kind=kind,
        side="BUY" if side == 0 else "SELL",
        token_id=str(token_id),
        maker_amount=maker_amount,
        taker_amount=taker_amount,
        **common,
    )


class PolygonRPC:
    """Small JSON-RPC facade that never includes the endpoint in its errors."""

    def __init__(
        self,
        rpc_url: str,
        *,
        retries: int = 4,
        backoff_factor: float = 0.5,
        requests_per_second: float = 5,
        rate_limiter: RateLimiter | None = None,
        api_client: APIClient | None = None,
        activity_callback: Callable[[str], None] | None = None,
    ) -> None:
        self.origin = sanitize_rpc_origin(rpc_url)
        validated = validate_outbound_https_url(rpc_url)
        self._client = api_client or APIClient(
            validated,
            retries=retries,
            backoff_factor=backoff_factor,
            requests_per_second=requests_per_second,
            rate_limiter=rate_limiter,
        )
        self._request_id = 0
        self._blocks: dict[int, PolygonBlock] = {}
        self._activity_callback = activity_callback
        self.metrics = PolygonRPCMetrics()

    def _record_methods(self, methods: Iterable[str]) -> None:
        for method in methods:
            if method == "eth_getLogs":
                self.metrics.log_rpc_call_count += 1
            elif method == "eth_getTransactionReceipt":
                self.metrics.receipt_rpc_call_count += 1
            elif method == "eth_getBlockByNumber":
                self.metrics.header_rpc_call_count += 1

    def _post(self, payload: Any, *, activity: str) -> Any:
        post_with_metrics = getattr(self._client, "post_with_metrics", None)
        self.metrics.http_request_count += 1
        try:
            if callable(post_with_metrics):
                response, attempts, retries = post_with_metrics(
                    "",
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
                self.metrics.http_request_count += max(0, int(attempts) - 1)
                self.metrics.retry_count += max(0, int(retries))
                return response
            return self._client.post(
                "",
                json=payload,
                headers={"Content-Type": "application/json"},
            )
        except requests.HTTPError as exc:
            if getattr(exc.response, "status_code", None) == 413:
                raise PolygonRPCSizeLimitError(
                    "Polygon RPC request size limit"
                ) from None
            raise PolygonRPCTransportError("Polygon RPC transport failed") from None
        except requests.RequestException:
            raise PolygonRPCTransportError("Polygon RPC transport failed") from None
        finally:
            if self._activity_callback is not None:
                self._activity_callback(activity)

    def call(self, method: str, params: list[Any]) -> Any:
        self._request_id += 1
        request_id = self._request_id
        self._record_methods((method,))
        response = self._post(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            },
            activity=method,
        )
        if (
            not isinstance(response, dict)
            or response.get("jsonrpc") != "2.0"
            or response.get("id") != request_id
            or ("result" in response and "error" in response)
        ):
            raise PolygonRPCProtocolError(
                "Polygon RPC returned a malformed response envelope"
            )
        error = response.get("error")
        if error is not None:
            raise _provider_error(error, batch=False)
        if "result" not in response:
            raise PolygonRPCProtocolError("Polygon RPC response omitted result")
        return response["result"]

    def batch_call(self, calls: Sequence[tuple[str, list[Any]]]) -> list[Any]:
        """Execute a bounded JSON-RPC batch and restore caller order."""
        if not calls:
            return []
        requests_payload: list[dict[str, Any]] = []
        request_order: list[int] = []
        for method, params in calls:
            self._request_id += 1
            request_id = self._request_id
            request_order.append(request_id)
            requests_payload.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": method,
                    "params": params,
                }
            )
        self._record_methods(method for method, _params in calls)
        response = self._post(requests_payload, activity="json_rpc_batch")
        if not isinstance(response, list) or len(response) != len(calls):
            raise PolygonRPCProtocolError(
                "Polygon RPC returned a malformed batch envelope"
            )

        expected_ids = set(request_order)
        by_id: dict[int, Any] = {}
        for envelope in response:
            if (
                not isinstance(envelope, dict)
                or envelope.get("jsonrpc") != "2.0"
                or not isinstance(envelope.get("id"), int)
                or isinstance(envelope.get("id"), bool)
                or envelope["id"] not in expected_ids
                or envelope["id"] in by_id
                or ("result" in envelope and "error" in envelope)
            ):
                raise PolygonRPCProtocolError(
                    "Polygon RPC returned a malformed batch envelope"
                )
            error = envelope.get("error")
            if error is not None:
                if not isinstance(error, dict) or not isinstance(
                    error.get("code"), int
                ):
                    raise PolygonRPCProtocolError(
                        "Polygon RPC returned a malformed batch error"
                    )
                raise _provider_error(error, batch=True)
            if "result" not in envelope:
                raise PolygonRPCProtocolError(
                    "Polygon RPC batch response omitted result"
                )
            by_id[envelope["id"]] = envelope["result"]
        return [by_id[request_id] for request_id in request_order]

    def _adaptive_batch_call(self, calls: Sequence[tuple[str, list[Any]]]) -> list[Any]:
        try:
            return self.batch_call(calls)
        except PolygonRPCSizeLimitError:
            if len(calls) <= 1:
                raise
            middle = len(calls) // 2
            return [
                *self._adaptive_batch_call(calls[:middle]),
                *self._adaptive_batch_call(calls[middle:]),
            ]

    def chain_id(self) -> int:
        return _hex_quantity(self.call("eth_chainId", []), "chainId")

    def block(self, number_or_tag: int | str) -> PolygonBlock:
        if isinstance(number_or_tag, int) and number_or_tag in self._blocks:
            return self._blocks[number_or_tag]
        tag = hex(number_or_tag) if isinstance(number_or_tag, int) else number_or_tag
        value = self.call("eth_getBlockByNumber", [tag, False])
        if not isinstance(value, dict):
            raise PolygonRPCError(f"Polygon block {tag!r} is unavailable")
        block = PolygonBlock(
            number=_hex_quantity(value.get("number"), "block.number"),
            hash=_hex32(value.get("hash"), "block.hash"),
            timestamp=datetime.fromtimestamp(
                _hex_quantity(value.get("timestamp"), "block.timestamp"),
                tz=timezone.utc,
            ),
        )
        if isinstance(number_or_tag, int) and block.number != number_or_tag:
            raise PolygonRPCError("Polygon RPC returned the wrong block number")
        self._blocks[block.number] = block
        return block

    def blocks(self, numbers: Iterable[int]) -> dict[int, PolygonBlock]:
        """Fetch and cache numeric block headers in bounded adaptive batches."""
        raw_numbers = tuple(numbers)
        if any(
            not isinstance(number, int) or isinstance(number, bool) or number < 0
            for number in raw_numbers
        ):
            raise ValueError("Polygon block numbers must be non-negative integers")
        requested = tuple(dict.fromkeys(raw_numbers))
        missing = [number for number in requested if number not in self._blocks]
        for offset in range(0, len(missing), _RPC_BATCH_SIZE):
            batch = missing[offset : offset + _RPC_BATCH_SIZE]
            values = self._adaptive_batch_call(
                [("eth_getBlockByNumber", [hex(number), False]) for number in batch]
            )
            for expected_number, value in zip(batch, values, strict=True):
                if not isinstance(value, dict):
                    raise PolygonRPCError(
                        f"Polygon block {hex(expected_number)!r} is unavailable"
                    )
                block = PolygonBlock(
                    number=_hex_quantity(value.get("number"), "block.number"),
                    hash=_hex32(value.get("hash"), "block.hash"),
                    timestamp=datetime.fromtimestamp(
                        _hex_quantity(value.get("timestamp"), "block.timestamp"),
                        tz=timezone.utc,
                    ),
                )
                if block.number != expected_number:
                    raise PolygonRPCError("Polygon RPC returned the wrong block number")
                self._blocks[block.number] = block
        return {number: self._blocks[number] for number in requested}

    def finalized_head(self) -> PolygonBlock:
        return self.block("finalized")

    def logs(
        self,
        address: str,
        from_block: int,
        to_block: int,
        *,
        event_topics: Sequence[str] = EVENT_TOPICS,
    ) -> list[dict[str, Any]]:
        if from_block < 0 or to_block < from_block:
            raise ValueError("Invalid inclusive Polygon log range")
        topics = tuple(str(topic).casefold() for topic in event_topics)
        if not topics or any(not _HEX_32.fullmatch(topic) for topic in topics):
            raise ValueError("Polygon event topics must be non-empty 32-byte hashes")
        result = self.call(
            "eth_getLogs",
            [
                {
                    "address": address,
                    "fromBlock": hex(from_block),
                    "toBlock": hex(to_block),
                    "topics": [list(topics)],
                }
            ],
        )
        if not isinstance(result, list) or any(
            not isinstance(row, dict) for row in result
        ):
            raise PolygonRPCError("eth_getLogs result must be a list of objects")
        return result

    @staticmethod
    def _validate_transaction_hashes(
        transaction_hashes: Iterable[str],
    ) -> tuple[str, ...]:
        requested = tuple(
            dict.fromkeys(str(value).casefold() for value in transaction_hashes)
        )
        if any(not _HEX_32.fullmatch(value) for value in requested):
            raise ValueError("Polygon transaction hashes must be 32-byte hex")
        return requested

    def _parse_receipts(
        self, requested: Sequence[str], values: Sequence[Any]
    ) -> dict[str, PolygonReceipt]:
        receipts: dict[str, PolygonReceipt] = {}
        for expected_hash, value in zip(requested, values, strict=True):
            if not isinstance(value, dict):
                raise PolygonRPCProtocolError(
                    "Polygon transaction receipt is unavailable"
                )
            transaction_hash = _hex32(
                value.get("transactionHash"), "receipt.transactionHash"
            )
            if transaction_hash != expected_hash:
                raise PolygonRPCProtocolError("Polygon RPC returned the wrong receipt")
            if _hex_quantity(value.get("status"), "receipt.status") != 1:
                raise PolygonRPCProtocolError(
                    "Polygon settlement receipt was not successful"
                )
            block_number = _hex_quantity(
                value.get("blockNumber"), "receipt.blockNumber"
            )
            block_hash = _hex32(value.get("blockHash"), "receipt.blockHash")
            transaction_index = _hex_quantity(
                value.get("transactionIndex"), "receipt.transactionIndex"
            )
            raw_logs = value.get("logs")
            if not isinstance(raw_logs, list) or any(
                not isinstance(row, dict) for row in raw_logs
            ):
                raise PolygonRPCProtocolError(
                    "Polygon receipt logs must be a list of objects"
                )
            logs: list[dict[str, Any]] = []
            for raw in raw_logs:
                if (
                    raw.get("removed") is not False
                    or _hex32(raw.get("transactionHash"), "log.transactionHash")
                    != transaction_hash
                    or _hex_quantity(raw.get("blockNumber"), "log.blockNumber")
                    != block_number
                    or _hex32(raw.get("blockHash"), "log.blockHash") != block_hash
                    or _hex_quantity(
                        raw.get("transactionIndex"), "log.transactionIndex"
                    )
                    != transaction_index
                ):
                    raise PolygonRPCProtocolError(
                        "Polygon receipt contains an inconsistent or removed log"
                    )
                logs.append(raw)
            receipts[transaction_hash] = PolygonReceipt(
                transaction_hash=transaction_hash,
                block_number=block_number,
                block_hash=block_hash,
                transaction_index=transaction_index,
                logs=tuple(logs),
            )
        return receipts

    def transaction_receipt_batch(
        self, transaction_hashes: Iterable[str]
    ) -> dict[str, PolygonReceipt]:
        """Fetch exactly one caller-sized receipt batch without hidden splitting."""
        requested = self._validate_transaction_hashes(transaction_hashes)
        if not requested:
            return {}
        values = self.batch_call(
            [
                ("eth_getTransactionReceipt", [transaction_hash])
                for transaction_hash in requested
            ]
        )
        return self._parse_receipts(requested, values)

    def transaction_receipts(
        self, transaction_hashes: Iterable[str]
    ) -> dict[str, PolygonReceipt]:
        """Fetch complete finalized receipts without persisting identifying fields."""
        requested = self._validate_transaction_hashes(transaction_hashes)
        receipts: dict[str, PolygonReceipt] = {}
        for offset in range(0, len(requested), _RPC_BATCH_SIZE):
            batch = requested[offset : offset + _RPC_BATCH_SIZE]
            values = self._adaptive_batch_call(
                [
                    ("eth_getTransactionReceipt", [transaction_hash])
                    for transaction_hash in batch
                ]
            )
            receipts.update(self._parse_receipts(batch, values))
        return receipts

    def first_block_at_or_after(
        self,
        timestamp: datetime,
        *,
        finalized_head: PolygonBlock,
        low: int = 0,
    ) -> int:
        """Binary-search the first finalized block whose timestamp reaches target."""
        target = timestamp.astimezone(timezone.utc)
        if target > finalized_head.timestamp:
            raise PolygonRPCError(
                "Target window extends past the finalized Polygon head"
            )
        lower = max(0, low)
        upper = finalized_head.number
        while lower < upper:
            middle = (lower + upper) // 2
            if self.block(middle).timestamp < target:
                lower = middle + 1
            else:
                upper = middle
        return lower

    def first_blocks_at_or_after(
        self,
        timestamps: Iterable[datetime],
        *,
        finalized_head: PolygonBlock,
    ) -> tuple[int, ...]:
        """Vectorized timestamp search using one bounded header batch per round."""
        targets = tuple(value.astimezone(timezone.utc) for value in timestamps)
        if any(target > finalized_head.timestamp for target in targets):
            raise PolygonRPCError(
                "Target window extends past the finalized Polygon head"
            )
        lower = [0] * len(targets)
        upper = [finalized_head.number] * len(targets)
        while any(left < right for left, right in zip(lower, upper, strict=True)):
            middles = {
                (left + right) // 2
                for left, right in zip(lower, upper, strict=True)
                if left < right
            }
            headers = self.blocks(middles)
            for index, target in enumerate(targets):
                if lower[index] >= upper[index]:
                    continue
                middle = (lower[index] + upper[index]) // 2
                if headers[middle].timestamp < target:
                    lower[index] = middle + 1
                else:
                    upper[index] = middle
        return tuple(lower)


def adaptive_log_leaves(
    rpc: PolygonRPC,
    address: str,
    from_block: int,
    to_block: int,
    *,
    event_topics: Sequence[str] = EVENT_TOPICS,
) -> Iterable[tuple[int, int, list[dict[str, Any]]]]:
    """Yield successful leaves, splitting provider-limited ranges until one block."""
    try:
        if tuple(event_topics) == EVENT_TOPICS:
            logs = rpc.logs(address, from_block, to_block)
        else:
            logs = rpc.logs(
                address,
                from_block,
                to_block,
                event_topics=event_topics,
            )
        yield from_block, to_block, logs
    except PolygonRPCSizeLimitError:
        if from_block == to_block:
            raise
        middle = (from_block + to_block) // 2
        yield from adaptive_log_leaves(
            rpc,
            address,
            from_block,
            middle,
            event_topics=event_topics,
        )
        yield from adaptive_log_leaves(
            rpc,
            address,
            middle + 1,
            to_block,
            event_topics=event_topics,
        )


__all__ = [
    "EVENT_TOPICS",
    "ORDERS_MATCHED_TOPIC",
    "ORDER_FILLED_TOPIC",
    "DecodedSettlementEvent",
    "PolygonBlock",
    "PolygonReceipt",
    "PolygonRPC",
    "PolygonRPCError",
    "PolygonRPCMetrics",
    "PolygonRPCProtocolError",
    "PolygonRPCSizeLimitError",
    "PolygonRPCTransportError",
    "adaptive_log_leaves",
    "decode_settlement_log",
    "sanitize_rpc_origin",
]
