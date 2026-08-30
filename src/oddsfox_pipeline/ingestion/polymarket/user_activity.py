"""Resumable, bounded export of public Polymarket user activity."""

from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Final, Protocol
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from oddsfox_pipeline.resources.http import APIClient

CONTRACT_ID: Final = "oddsfox.polymarket.user-activity.v1"
SOURCE_ENDPOINT: Final = "https://data-api.polymarket.com/activity"
PAGE_SIZE: Final = 500
MAX_OFFSET: Final = 5_000
HOURLY_WORKERS: Final = 48
DAY_WORKERS: Final = 3
DECIMAL_SCALE: Final = 18
_ADDRESS_RE: Final = re.compile(r"^0x[0-9a-fA-F]{40}$")
_MONEY_TYPE: Final = pa.decimal128(38, DECIMAL_SCALE)

ACTIVITY_SCHEMA: Final = pa.schema(
    [
        pa.field("wallet", pa.string(), nullable=False),
        pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("condition_id", pa.string()),
        pa.field("asset", pa.string()),
        pa.field("activity_type", pa.string(), nullable=False),
        pa.field("transaction_hash", pa.string()),
        pa.field("size", _MONEY_TYPE),
        pa.field("usdc_size", _MONEY_TYPE),
        pa.field("price", _MONEY_TYPE),
        pa.field("side", pa.string()),
        pa.field("outcome_index", pa.int32()),
        pa.field("market_title", pa.string()),
        pa.field("market_slug", pa.string()),
        pa.field("event_slug", pa.string()),
        pa.field("outcome", pa.string()),
        pa.field("is_combo", pa.bool_()),
        pa.field(
            "source_window_start_utc",
            pa.timestamp("us", tz="UTC"),
            nullable=False,
        ),
        pa.field("source_api_offset", pa.int32(), nullable=False),
        pa.field("source_row_ordinal", pa.int32(), nullable=False),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("acquired_at_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    ],
    metadata={b"contract_id": CONTRACT_ID.encode()},
)


class ActivityClient(Protocol):
    def get(self, endpoint: str, params: dict[str, Any]) -> Any: ...


class SaturatedActivityWindowError(RuntimeError):
    """Raised when one second contains more rows than the API offset budget."""


def validate_wallet(wallet: str) -> str:
    """Validate a 20-byte hexadecimal address and return its canonical form."""
    if not _ADDRESS_RE.fullmatch(wallet):
        raise ValueError("wallet must be a 0x-prefixed 20-byte hex address")
    return wallet.lower()


def parse_utc(value: str) -> datetime:
    """Parse an explicit, whole-second UTC timestamp."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid ISO timestamp: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("timestamps must include an explicit UTC offset")
    if parsed.microsecond:
        raise ValueError("timestamps must use whole-second precision")
    return parsed.astimezone(timezone.utc)


def validate_bounds(start_utc: datetime, end_utc: datetime) -> None:
    if start_utc.tzinfo is None or end_utc.tzinfo is None:
        raise ValueError("bounds must be timezone-aware")
    if start_utc.utcoffset() != timedelta(0) or end_utc.utcoffset() != timedelta(0):
        raise ValueError("bounds must be UTC")
    if start_utc >= end_utc:
        raise ValueError("start must be earlier than end")


def build_client() -> APIClient:
    return APIClient(
        base_url="https://data-api.polymarket.com",
        retries=3,
        backoff_factor=1.0,
        requests_per_second=75,
        source_id="polymarket",
    )


def fetch_window(
    client: ActivityClient,
    wallet: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[tuple[dict[str, Any], datetime, int, int]]:
    """Fetch one half-open window, bisecting before the offset limit can truncate."""
    validate_bounds(start_utc, end_utc)
    rows: list[tuple[dict[str, Any], datetime, int, int]] = []
    for offset in range(0, MAX_OFFSET + 1, PAGE_SIZE):
        payload = client.get(
            "/activity",
            params={
                "user": wallet,
                "start": int(start_utc.timestamp()),
                "end": int(end_utc.timestamp()) - 1,
                "limit": PAGE_SIZE,
                "offset": offset,
                "sortBy": "TIMESTAMP",
                "sortDirection": "ASC",
                "excludeDepositsWithdrawals": "false",
            },
        )
        if not isinstance(payload, list):
            raise TypeError("Polymarket activity response must be a JSON list")
        if len(payload) > PAGE_SIZE:
            raise ValueError("Polymarket activity page exceeded the requested limit")
        rows.extend(
            (item, start_utc, offset, ordinal) for ordinal, item in enumerate(payload)
        )
        if len(payload) < PAGE_SIZE:
            return rows
    duration_seconds = int((end_utc - start_utc).total_seconds())
    if duration_seconds <= 1:
        raise SaturatedActivityWindowError(
            f"one-second activity window saturated at {start_utc.isoformat()}"
        )
    midpoint = start_utc + timedelta(seconds=duration_seconds // 2)
    return fetch_window(client, wallet, start_utc, midpoint) + fetch_window(
        client, wallet, midpoint, end_utc
    )


def normalize_activity(
    raw: dict[str, Any],
    *,
    wallet: str,
    window_start_utc: datetime,
    api_offset: int,
    row_ordinal: int,
    acquired_at_utc: datetime,
) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise TypeError("activity row must be a JSON object")
    timestamp_value = raw.get("timestamp")
    if isinstance(timestamp_value, bool):
        raise ValueError("activity timestamp is malformed")
    try:
        timestamp = datetime.fromtimestamp(int(timestamp_value), tz=timezone.utc)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("activity timestamp is malformed") from exc
    activity_type = _optional_text(raw.get("type"))
    if activity_type is None:
        raise ValueError("activity type is required")
    proxy_wallet = _optional_text(raw.get("proxyWallet"))
    if proxy_wallet is not None and proxy_wallet.casefold() != wallet.casefold():
        raise ValueError("activity row wallet does not match the requested wallet")
    return {
        "wallet": wallet,
        "timestamp": timestamp,
        "condition_id": _optional_text(raw.get("conditionId")),
        "asset": _optional_text(raw.get("asset")),
        "activity_type": activity_type.upper(),
        "transaction_hash": _optional_text(raw.get("transactionHash")),
        "size": _optional_decimal(raw.get("size"), "size"),
        "usdc_size": _optional_decimal(raw.get("usdcSize"), "usdcSize"),
        "price": _optional_decimal(raw.get("price"), "price"),
        "side": _upper_optional_text(raw.get("side")),
        "outcome_index": _optional_int(raw.get("outcomeIndex"), "outcomeIndex"),
        "market_title": _optional_text(raw.get("title")),
        "market_slug": _optional_text(raw.get("slug")),
        "event_slug": _optional_text(raw.get("eventSlug")),
        "outcome": _optional_text(raw.get("outcome")),
        "is_combo": _optional_bool(raw.get("isCombo"), "isCombo"),
        "source_window_start_utc": window_start_utc,
        "source_api_offset": api_offset,
        "source_row_ordinal": row_ordinal,
        "source_endpoint": SOURCE_ENDPOINT,
        "acquired_at_utc": acquired_at_utc,
    }


def export_user_activity(
    *,
    wallet: str,
    start_utc: datetime,
    end_utc: datetime,
    output_dir: Path,
    replace: bool = False,
    client: ActivityClient | None = None,
    acquired_at_utc: datetime | None = None,
) -> dict[str, Any]:
    """Export validated daily Parquet partitions and return the final manifest."""
    wallet = validate_wallet(wallet)
    validate_bounds(start_utc, end_utc)
    output_dir = output_dir.resolve()
    acquired_at_utc = acquired_at_utc or datetime.now(timezone.utc)
    acquisition_client = client or build_client()
    _validate_existing_root(output_dir, wallet, start_utc, end_utc, replace)
    output_dir.mkdir(parents=True, exist_ok=True)

    day_bounds: list[tuple[datetime, datetime]] = []
    cursor = start_utc
    while cursor < end_utc:
        day_end = min(
            end_utc,
            datetime.combine(
                cursor.date() + timedelta(days=1),
                datetime.min.time(),
                timezone.utc,
            ),
        )
        day_bounds.append((cursor, day_end))
        cursor = day_end
    with ThreadPoolExecutor(max_workers=DAY_WORKERS) as executor:
        partitions = list(
            executor.map(
                lambda bounds: _export_day(
                    client=acquisition_client,
                    wallet=wallet,
                    start_utc=bounds[0],
                    end_utc=bounds[1],
                    output_dir=output_dir,
                    replace=replace,
                    acquired_at_utc=acquired_at_utc,
                ),
                day_bounds,
            )
        )

    manifest = _build_manifest(
        wallet=wallet,
        start_utc=start_utc,
        end_utc=end_utc,
        acquired_at_utc=acquired_at_utc,
        partitions=partitions,
    )
    _atomic_json(output_dir / "_manifest.json", manifest)
    return manifest


def _export_day(
    *,
    client: ActivityClient,
    wallet: str,
    start_utc: datetime,
    end_utc: datetime,
    output_dir: Path,
    replace: bool,
    acquired_at_utc: datetime,
) -> dict[str, Any]:
    relative_path = Path(f"date={start_utc.date().isoformat()}") / "activity.parquet"
    parquet_path = output_dir / relative_path
    sidecar_path = parquet_path.with_suffix(".partition.json")
    expected = {
        "contract_id": CONTRACT_ID,
        "wallet": wallet,
        "start_utc": _iso(start_utc),
        "end_utc": _iso(end_utc),
        "path": relative_path.as_posix(),
    }
    if parquet_path.exists() or sidecar_path.exists():
        if not replace:
            return _validate_partition(parquet_path, sidecar_path, expected)

    windows: list[tuple[datetime, datetime]] = []
    hour_start = start_utc
    while hour_start < end_utc:
        hour_end = min(end_utc, hour_start + timedelta(hours=1))
        windows.append((hour_start, hour_end))
        hour_start = hour_end
    source_rows: list[tuple[dict[str, Any], datetime, int, int]] = []
    with ThreadPoolExecutor(max_workers=HOURLY_WORKERS) as executor:
        fetched = executor.map(
            lambda bounds: fetch_window(client, wallet, bounds[0], bounds[1]),
            windows,
        )
        for window_rows in fetched:
            source_rows.extend(window_rows)
    normalized = [
        normalize_activity(
            raw,
            wallet=wallet,
            window_start_utc=window_start,
            api_offset=offset,
            row_ordinal=ordinal,
            acquired_at_utc=acquired_at_utc,
        )
        for raw, window_start, offset, ordinal in source_rows
    ]
    normalized.sort(
        key=lambda row: (
            row["timestamp"],
            row["source_window_start_utc"],
            row["source_api_offset"],
            row["source_row_ordinal"],
        )
    )
    table = pa.Table.from_pylist(normalized, schema=ACTIVITY_SCHEMA)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.tmp")
    try:
        pq.write_table(table, temporary_path, compression="zstd")
        pq.read_schema(temporary_path)
        temporary_path.replace(parquet_path)
    finally:
        temporary_path.unlink(missing_ok=True)
    partition = {
        **expected,
        "rows": table.num_rows,
        "type_counts": dict(
            sorted(Counter(row["activity_type"] for row in normalized).items())
        ),
        "first_timestamp": _iso(normalized[0]["timestamp"]) if normalized else None,
        "last_timestamp": _iso(normalized[-1]["timestamp"]) if normalized else None,
        "sha256": _sha256(parquet_path),
        "file_bytes": parquet_path.stat().st_size,
    }
    _atomic_json(sidecar_path, partition)
    return partition


def _validate_existing_root(
    output_dir: Path,
    wallet: str,
    start_utc: datetime,
    end_utc: datetime,
    replace: bool,
) -> None:
    manifest_path = output_dir / "_manifest.json"
    if not output_dir.exists() or not any(output_dir.iterdir()):
        return
    if replace:
        return
    if not manifest_path.exists():
        unexpected = [
            path.name
            for path in output_dir.iterdir()
            if not (path.is_dir() and path.name.startswith("date="))
        ]
        if unexpected:
            raise ValueError("existing output is incompatible; use --replace")
        return
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(
            "existing output manifest is unreadable; use --replace"
        ) from exc
    identity = (
        manifest.get("contract_id"),
        manifest.get("wallet"),
        manifest.get("start_utc"),
        manifest.get("end_utc"),
    )
    expected = (CONTRACT_ID, wallet, _iso(start_utc), _iso(end_utc))
    if identity != expected:
        raise ValueError("existing output is incompatible; use --replace")


def _validate_partition(
    parquet_path: Path,
    sidecar_path: Path,
    expected: dict[str, Any],
) -> dict[str, Any]:
    if not parquet_path.is_file() or not sidecar_path.is_file():
        raise ValueError(f"incomplete existing partition: {parquet_path.parent}")
    try:
        partition = json.loads(sidecar_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"unreadable partition metadata: {sidecar_path}") from exc
    if any(partition.get(key) != value for key, value in expected.items()):
        raise ValueError(f"incompatible existing partition: {parquet_path}")
    if partition.get("sha256") != _sha256(parquet_path):
        raise ValueError(f"partition checksum mismatch: {parquet_path}")
    table = pq.ParquetFile(parquet_path).read()
    if table.schema.remove_metadata() != ACTIVITY_SCHEMA.remove_metadata():
        raise ValueError(f"partition schema mismatch: {parquet_path}")
    if table.num_rows != partition.get("rows"):
        raise ValueError(f"partition row count mismatch: {parquet_path}")
    return partition


def _build_manifest(
    *,
    wallet: str,
    start_utc: datetime,
    end_utc: datetime,
    acquired_at_utc: datetime,
    partitions: list[dict[str, Any]],
) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    for partition in partitions:
        counts.update(partition["type_counts"])
    first_values = [p["first_timestamp"] for p in partitions if p["first_timestamp"]]
    last_values = [p["last_timestamp"] for p in partitions if p["last_timestamp"]]
    return {
        "contract_id": CONTRACT_ID,
        "schema_version": 1,
        "wallet": wallet,
        "start_utc": _iso(start_utc),
        "end_utc": _iso(end_utc),
        "source_endpoint": SOURCE_ENDPOINT,
        "acquired_at_utc": _iso(acquired_at_utc),
        "pagination": {
            "base_window_seconds": 3_600,
            "page_size": PAGE_SIZE,
            "maximum_offset": MAX_OFFSET,
            "saturated_source_windows": 0,
        },
        "rows": sum(int(p["rows"]) for p in partitions),
        "type_counts": dict(sorted(counts.items())),
        "first_timestamp": min(first_values) if first_values else None,
        "last_timestamp": max(last_values) if last_values else None,
        "partitions": partitions,
    }


def _optional_decimal(value: Any, field: str) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative finite decimal")
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative finite decimal") from exc
    if (
        not number.is_finite()
        or number < 0
        or number.as_tuple().exponent < -DECIMAL_SCALE
    ):
        raise ValueError(f"{field} must be a non-negative finite decimal")
    return number


def _optional_int(value: Any, field: str) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if str(number) != str(value) and not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    return number


def _optional_bool(value: Any, field: str) -> bool | None:
    if value is None:
        return None
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _upper_optional_text(value: Any) -> str | None:
    text = _optional_text(value)
    return text.upper() if text is not None else None


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        temporary_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


__all__ = [
    "ACTIVITY_SCHEMA",
    "CONTRACT_ID",
    "MAX_OFFSET",
    "PAGE_SIZE",
    "SaturatedActivityWindowError",
    "export_user_activity",
    "fetch_window",
    "normalize_activity",
    "parse_utc",
    "validate_bounds",
    "validate_wallet",
]
