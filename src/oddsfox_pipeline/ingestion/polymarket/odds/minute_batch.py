"""Shared high-throughput helpers for minute-fidelity CLOB backfills.

Ports the hourly pipeline's batch POST, preemptive window chunking, status-hook
rate accounting, and RPS auto-tune onto the match/futures minute paths while
preserving their all-success atomic publish contract.
"""

from __future__ import annotations

import array
import gc
import hashlib
import inspect
import itertools
import json
import logging
import math
import os
import shutil
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock, local
from typing import Any, Callable, Iterator, Mapping, Protocol, Sequence, TypeVar

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from oddsfox_pipeline.config.settings import (
    BASE_DIR,
    CLOB_API_URL,
    ODDS_REQUESTS_PER_SECOND,
)
from oddsfox_pipeline.ingestion.polymarket.odds.execution import (
    fetch_group_window_with_auto_split,
    fetch_window_with_auto_split,
    iter_windows,
)
from oddsfox_pipeline.ingestion.polymarket.odds.fetch import build_client
from oddsfox_pipeline.ingestion.polymarket.odds.writer import maybe_auto_tune_rps
from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.resources.http import RateLimiter
from oddsfox_pipeline.resources.progress_guardrails import ProgressGuardrail

logger = logging.getLogger(__name__)

DEFAULT_MINUTE_WORKERS = 40
DEFAULT_MINUTE_REQUESTS_PER_SECOND = 40
DEFAULT_MINUTE_BATCH_GROUP_SIZE = 20
DEFAULT_MINUTE_WINDOW_HOURS = 24
DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS = 90
MIN_SPLIT_WINDOW_SECONDS = 300
FIDELITY_MINUTES = 1
DEFAULT_MINUTE_PUBLISH_SHARD_ROWS = 4_000_000
DEFAULT_MINUTE_PUBLISH_BATCH_ROWS = 256_000
DEFAULT_MINUTE_PUBLISH_COMPRESSION = "snappy"
DEFAULT_MINUTE_MARKET_SAMPLE_METHOD = "hash_rank_limit"
# ponytail: ~4M rows/shard won the disposable futures-minute publish matrix
# (1M/2M/4M × uncompressed/snappy/zstd) on equality-correct runs; upgrade path
# is re-running `make futures-minute-publish-benchmark` with
# FUTURES_MINUTE_PUBLISH_BENCHMARK_MATRIX=true.

_MINUTE_TIMESTAMP_TYPE = pa.timestamp("us", tz="UTC")
_MINUTE_PUBLISH_COLUMNS = (
    "market_id",
    "clob_token_id",
    "timestamp",
    "price",
    "fidelity_minutes",
    "window_start_at",
    "window_end_at",
    "ingested_at",
)
_MINUTE_PUBLISH_PARQUET_COLUMNS = (
    "market_id",
    "clobTokenId",
    "timestamp",
    "price",
    "fidelity_minutes",
    "window_start_at",
    "window_end_at",
    "ingested_at",
)


class MinutePlanLike(Protocol):
    market_id: str
    token_id: str
    started_at: datetime
    finished_at: datetime


_PlanT = TypeVar("_PlanT", bound=MinutePlanLike)


class MinuteHistoryResultLike(Protocol):
    plan: MinutePlanLike
    history: Sequence[tuple[str, int, float]]


@contextmanager
def borrow_duckdb_connection(
    conn: Any | None = None,
    *,
    connection_factory: Callable[[], AbstractContextManager[Any]] | None = None,
) -> Iterator[Any]:
    """Open a DuckDB connection for one sync phase, then release it.

    Production minute assets pass ``connection_factory=get_connection`` so the
    warehouse file lock is not held across long CLOB fetches. Unit tests may
    keep passing a single in-memory ``conn`` for the whole sync.
    """
    if (conn is None) == (connection_factory is None):
        raise ValueError("Provide exactly one of conn or connection_factory")
    if connection_factory is not None:
        with connection_factory() as active:
            yield active
        return
    yield conn


@dataclass(frozen=True)
class MinuteFetchResult:
    plan: MinutePlanLike
    fetch_status: str
    history: tuple[tuple[str, int, float], ...]
    request_start_epoch: int
    request_end_epoch: int
    source_row_count: int
    history_sha256: str | None
    fetch_started_at: datetime
    fetch_finished_at: datetime
    error_type: str | None = None
    error_message: str | None = None


def _dedupe_history_by_timestamp(
    rows: Sequence[tuple[str, int, float]],
) -> tuple[tuple[str, int, float], ...]:
    """Keep one row per timestamp; last sorted occurrence wins.

    Matches the former publish SQL winner when ``ingested_at`` is constant and
    ``row_order`` increases in ``(timestamp, token, price)`` order: the last
    duplicate for a timestamp is retained.
    """
    if not rows:
        return ()
    ordered = sorted(rows, key=lambda row: (row[1], row[0], row[2]))
    by_ts: dict[int, tuple[str, int, float]] = {}
    for row in ordered:
        by_ts[row[1]] = row
    return tuple(sorted(by_ts.values(), key=lambda row: (row[1], row[0], row[2])))


def ensure_unique_success_token_ids(
    results: Sequence[MinuteHistoryResultLike],
) -> None:
    """Fail closed when the same success token would publish twice."""
    seen: set[str] = set()
    for result in results:
        if not result.history:
            continue
        token_id = result.plan.token_id
        if token_id in seen:
            raise ValueError(f"Duplicate success token plan for publish: {token_id}")
        seen.add(token_id)


def sample_minute_market_plans(
    plans: Sequence[_PlanT],
    *,
    fraction: float,
    seed: str,
) -> tuple[list[_PlanT], dict[str, Any]]:
    """Deterministically sample markets and keep every token for each market.

    Selects ``max(1, ceil(population_markets * fraction))`` markets by hashing
    ``f"{seed}:{market_id}"`` and taking the lowest digests. Production callers
    leave ``fraction`` unset; smoke uses a fixed seed for stable reruns.
    """
    if not plans:
        raise ValueError("plans must not be empty")
    if not (0.0 < float(fraction) <= 1.0):
        raise ValueError("market sample fraction must be in (0, 1]")
    seed_text = str(seed).strip()
    if not seed_text:
        raise ValueError("market sample seed must not be blank")

    by_market: dict[str, list[_PlanT]] = defaultdict(list)
    for plan in plans:
        by_market[str(plan.market_id)].append(plan)
    population_markets = len(by_market)
    population_tokens = len(plans)
    selected_count = max(1, math.ceil(population_markets * float(fraction)))
    selected_count = min(selected_count, population_markets)

    ranked = sorted(
        by_market,
        key=lambda market_id: hashlib.sha256(
            f"{seed_text}:{market_id}".encode("utf-8")
        ).hexdigest(),
    )
    selected_ids = ranked[:selected_count]
    selected_set = set(selected_ids)
    selected_plans = [plan for plan in plans if str(plan.market_id) in selected_set]
    selected_ids_sorted = sorted(selected_ids)
    digest = hashlib.sha256("\n".join(selected_ids_sorted).encode("utf-8")).hexdigest()
    manifest = {
        "sample_enabled": True,
        "sample_method": DEFAULT_MINUTE_MARKET_SAMPLE_METHOD,
        "sample_fraction": float(fraction),
        "sample_seed": seed_text,
        "population_markets": population_markets,
        "population_tokens": population_tokens,
        "selected_markets": len(selected_ids_sorted),
        "selected_tokens": len(selected_plans),
        "selected_market_ids": selected_ids_sorted,
        "selected_market_ids_sha256": digest,
    }
    return selected_plans, manifest


def cap_minute_plan_window_tail(
    plan: _PlanT,
    *,
    window_hours: int,
) -> _PlanT:
    """Keep ``finished_at`` and move ``started_at`` to the final N hours."""
    hours = int(window_hours)
    if hours < 1:
        raise ValueError("sample_window_hours must be >= 1")
    capped_start = plan.finished_at - timedelta(hours=hours)
    if capped_start <= plan.started_at:
        return plan
    if capped_start >= plan.finished_at:
        raise ValueError(
            f"sample_window_hours={hours} empties window for market {plan.market_id}"
        )
    return replace(plan, started_at=capped_start)


def release_minute_history_payloads(
    results: Sequence[MinuteHistoryResultLike],
) -> int:
    """Drop in-memory history tuples after Parquet spill.

    Production futures publishes hold ~10^8 Python ``(token, ts, price)`` tuples
    until spill completes. Clearing them before snapshot publish avoids keeping
    that payload resident beside DuckDB work (the SIGKILL failure mode).
    ``MinuteFetchResult`` is frozen, so clear via ``object.__setattr__``.
    """
    released = 0
    for result in results:
        history = result.history
        if not history:
            continue
        released += len(history)
        object.__setattr__(result, "history", ())
    if released:
        gc.collect()
    return released


def _build_minute_history_arrow_batch(
    results: Sequence[MinuteHistoryResultLike],
    *,
    ingested_at: datetime,
    fidelity_minutes: int = FIDELITY_MINUTES,
    include_row_order: bool = False,
) -> pa.Table:
    """Build one Arrow batch from success tokens with non-empty history."""
    filtered = [result for result in results if result.history]
    if not filtered:
        raise ValueError("rows must not be empty")

    counts = [len(result.history) for result in filtered]
    total_rows = sum(counts)
    offsets = pa.array(
        itertools.chain([0], itertools.accumulate(counts)), type=pa.int32()
    )
    placeholder = pa.nulls(total_rows, type=pa.int8())
    parent_idx = pc.list_parent_indices(pa.ListArray.from_arrays(offsets, placeholder))
    dict_indices = parent_idx.cast(pa.int32())

    small_market_ids = pa.array(
        [result.plan.market_id for result in filtered], type=pa.string()
    )
    small_token_ids = pa.array(
        [result.plan.token_id for result in filtered], type=pa.string()
    )
    small_starts = pa.array(
        [result.plan.started_at for result in filtered], type=_MINUTE_TIMESTAMP_TYPE
    )
    small_ends = pa.array(
        [result.plan.finished_at for result in filtered], type=_MINUTE_TIMESTAMP_TYPE
    )

    ts_buf = array.array("q")
    price_buf = array.array("d")
    for result in filtered:
        for _token, ts, price in result.history:
            ts_buf.append(ts)
            price_buf.append(price)

    columns: dict[str, Any] = {
        "market_id": pa.DictionaryArray.from_arrays(dict_indices, small_market_ids),
        "clob_token_id": pa.DictionaryArray.from_arrays(dict_indices, small_token_ids),
        "timestamp": pa.array(ts_buf, type=pa.int64()),
        "price": pa.array(price_buf, type=pa.float64()),
        "fidelity_minutes": pa.repeat(fidelity_minutes, total_rows).cast(pa.int32()),
        "window_start_at": small_starts.take(parent_idx),
        "window_end_at": small_ends.take(parent_idx),
        "ingested_at": pa.repeat(
            pa.scalar(ingested_at, type=_MINUTE_TIMESTAMP_TYPE), total_rows
        ),
    }
    if include_row_order:
        columns["row_order"] = pc.subtract(
            pc.cumulative_sum(pa.repeat(1, total_rows)), 1
        ).cast(pa.int64())
    return pa.table(columns)


def build_minute_history_arrow_table(
    results: Sequence[MinuteHistoryResultLike],
    *,
    ingested_at: datetime,
    fidelity_minutes: int = FIDELITY_MINUTES,
    include_row_order: bool = True,
) -> pa.Table:
    """Build the minute-odds Arrow table without per-row Python dicts.

    Broadcasts per-token scalars via Arrow ``take`` / ``repeat`` / dictionary
    encoding. String columns stay dictionary-encoded so expanded row counts
    cannot overflow Arrow's ``string`` int32 value offsets. Timestamp/price use
    typed ``array.array`` buffers instead of boxed Python lists.

    Skips results with empty history. Raises ``ValueError`` when no points remain.
    """
    return _build_minute_history_arrow_batch(
        results,
        ingested_at=ingested_at,
        fidelity_minutes=fidelity_minutes,
        include_row_order=include_row_order,
    )


def iter_minute_history_arrow_batches(
    results: Sequence[MinuteHistoryResultLike],
    *,
    ingested_at: datetime,
    fidelity_minutes: int = FIDELITY_MINUTES,
    max_rows: int = DEFAULT_MINUTE_PUBLISH_BATCH_ROWS,
) -> Iterator[pa.Table]:
    """Yield bounded Arrow batches that never split mid-token when possible."""
    filtered = [result for result in results if result.history]
    if not filtered:
        raise ValueError("rows must not be empty")
    batch_cap = max(1, int(max_rows))
    batch: list[MinuteHistoryResultLike] = []
    batch_rows = 0
    for result in filtered:
        history_rows = len(result.history)
        if history_rows > batch_cap:
            if batch:
                yield _build_minute_history_arrow_batch(
                    batch,
                    ingested_at=ingested_at,
                    fidelity_minutes=fidelity_minutes,
                    include_row_order=False,
                )
                batch = []
                batch_rows = 0
            # Oversized single token: emit one batch for the whole token.
            yield _build_minute_history_arrow_batch(
                [result],
                ingested_at=ingested_at,
                fidelity_minutes=fidelity_minutes,
                include_row_order=False,
            )
            continue
        if batch and batch_rows + history_rows > batch_cap:
            yield _build_minute_history_arrow_batch(
                batch,
                ingested_at=ingested_at,
                fidelity_minutes=fidelity_minutes,
                include_row_order=False,
            )
            batch = []
            batch_rows = 0
        batch.append(result)
        batch_rows += history_rows
    if batch:
        yield _build_minute_history_arrow_batch(
            batch,
            ingested_at=ingested_at,
            fidelity_minutes=fidelity_minutes,
            include_row_order=False,
        )


def minute_odds_publish_cache_dir(fetch_run_id: str) -> Path:
    """Return the ignored runtime cache directory for one publish run."""
    root = Path(
        os.getenv("ODDSFOX_RUNTIME_ROOT", str(BASE_DIR / ".cache" / "runtime"))
    ).expanduser()
    resolved_root = root.resolve()
    target = (resolved_root / "minute-odds-publish" / str(fetch_run_id)).resolve()
    if not target.is_relative_to(resolved_root):
        raise ValueError("minute-odds publish cache path escaped runtime root")
    return target


def write_minute_history_parquet_shards(
    results: Sequence[MinuteHistoryResultLike],
    *,
    fetch_run_id: str,
    ingested_at: datetime,
    fidelity_minutes: int = FIDELITY_MINUTES,
    max_rows_per_shard: int = DEFAULT_MINUTE_PUBLISH_SHARD_ROWS,
    batch_rows: int = DEFAULT_MINUTE_PUBLISH_BATCH_ROWS,
    compression: str | None = DEFAULT_MINUTE_PUBLISH_COMPRESSION,
    log: Any = logger,
) -> list[Path]:
    """Spill publish Arrow batches to temporary Parquet shards under runtime cache."""
    ensure_unique_success_token_ids(results)
    cache_dir = minute_odds_publish_cache_dir(fetch_run_id)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    estimated_rows = sum(len(result.history) for result in results if result.history)
    # Rough upper bound: ~40 bytes/row uncompressed Arrow plus Parquet overhead.
    # Fail before writing when free space is clearly insufficient for spill.
    free_bytes = shutil.disk_usage(cache_dir).free
    estimated_bytes = max(estimated_rows, 1) * 40
    log.info(
        "Minute-odds publish spill planning %s rows under %s "
        "(free_bytes=%s estimated_bytes=%s)",
        estimated_rows,
        cache_dir,
        free_bytes,
        estimated_bytes,
    )
    if free_bytes < estimated_bytes:
        shutil.rmtree(cache_dir, ignore_errors=True)
        raise OSError(
            f"Insufficient local free space for minute-odds parquet spill: "
            f"need~{estimated_bytes} bytes, free={free_bytes}"
        )

    shard_paths: list[Path] = []
    shard_rows = 0
    shard_index = 0
    total_rows = 0
    shard_cap = max(1, int(max_rows_per_shard))
    # Stream one Arrow batch at a time into ParquetWriter so spill peak stays
    # near batch_rows instead of concatenating up to max_rows_per_shard in RAM
    # while Python histories are still alive.
    writer: pq.ParquetWriter | None = None
    writer_rows = 0
    token_ids = sorted({result.plan.token_id for result in results if result.history})

    def _close_writer() -> None:
        nonlocal writer, shard_rows, shard_index, writer_rows
        if writer is None:
            return
        path = shard_paths[-1]
        writer.close()
        writer = None
        log.info(
            "Minute-odds publish wrote shard %s (%s rows, %s bytes)",
            path.name,
            writer_rows,
            path.stat().st_size,
        )
        shard_index += 1
        shard_rows = 0
        writer_rows = 0

    def _open_writer(schema: pa.Schema) -> None:
        nonlocal writer, writer_rows
        path = cache_dir / f"shard-{shard_index:05d}.parquet"
        writer = pq.ParquetWriter(
            where=path,
            schema=schema,
            compression=compression,
            write_statistics=False,
        )
        shard_paths.append(path)
        writer_rows = 0

    def _batch_for_parquet(batch: pa.Table) -> pa.Table:
        renamed = batch.select(list(_MINUTE_PUBLISH_COLUMNS)).rename_columns(
            list(_MINUTE_PUBLISH_PARQUET_COLUMNS)
        )
        # Decode dictionary columns so successive batches share one writer schema
        # even when token dictionaries differ.
        columns = []
        for name in renamed.column_names:
            column = renamed.column(name)
            if pa.types.is_dictionary(column.type):
                column = column.cast(column.type.value_type)
            columns.append(column)
        return pa.Table.from_arrays(columns, names=list(renamed.column_names))

    try:
        for batch in iter_minute_history_arrow_batches(
            results,
            ingested_at=ingested_at,
            fidelity_minutes=fidelity_minutes,
            max_rows=batch_rows,
        ):
            parquet_batch = _batch_for_parquet(batch)
            if writer is not None and shard_rows + parquet_batch.num_rows > shard_cap:
                _close_writer()
            if writer is None:
                _open_writer(parquet_batch.schema)
            assert writer is not None
            writer.write_table(parquet_batch)
            shard_rows += parquet_batch.num_rows
            writer_rows += parquet_batch.num_rows
            total_rows += parquet_batch.num_rows
            if shard_rows >= shard_cap:
                _close_writer()
        _close_writer()
    except Exception:
        if writer is not None:
            writer.close()
        shutil.rmtree(cache_dir, ignore_errors=True)
        raise
    if not shard_paths:
        shutil.rmtree(cache_dir, ignore_errors=True)
        raise ValueError("rows must not be empty")
    manifest_path = cache_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "fetch_run_id": fetch_run_id,
                "token_count": len(token_ids),
                "token_ids": token_ids,
                "row_count": total_rows,
                "shard_count": len(shard_paths),
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    log.info(
        "Minute-odds publish spilled %s rows into %s parquet shard(s) under %s",
        total_rows,
        len(shard_paths),
        cache_dir,
    )
    return shard_paths


def cleanup_minute_odds_publish_cache(fetch_run_id: str) -> None:
    """Delete temporary Parquet shards for a fetch run if present."""
    cache_dir = minute_odds_publish_cache_dir(fetch_run_id)
    if cache_dir.exists():
        shutil.rmtree(cache_dir)


def sanitize_error_message(exc: Exception) -> str:
    return " ".join(str(exc).split())[:500]


def padded_epoch_bounds(started_at: datetime, finished_at: datetime) -> tuple[int, int]:
    exact_start = started_at.timestamp()
    exact_end = finished_at.timestamp()
    padded_start = (math.floor(exact_start) // 60) * 60
    padded_end = math.ceil(math.ceil(exact_end) / 60) * 60
    return int(padded_start), int(padded_end)


def group_minute_plans(
    plans: Sequence[MinutePlanLike],
    *,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
) -> list[tuple[MinutePlanLike, ...]]:
    """Cluster plans that share an exact window, then chunk to batch size."""
    size = max(1, int(batch_group_size))
    by_window: dict[tuple[datetime, datetime], list[MinutePlanLike]] = defaultdict(list)
    for plan in plans:
        key = (plan.started_at, plan.finished_at)
        by_window[key].append(plan)
    groups: list[tuple[MinutePlanLike, ...]] = []
    for key in sorted(by_window):
        bucket = by_window[key]
        for offset in range(0, len(bucket), size):
            groups.append(tuple(bucket[offset : offset + size]))
    return groups


def _normalize_rows(
    raw_rows: Sequence[tuple[Any, Any, Any]],
) -> list[tuple[str, int, float]]:
    return sorted(
        (
            (str(token), int(timestamp), float(price))
            for token, timestamp, price in raw_rows
        ),
        key=lambda row: (row[1], row[0], row[2]),
    )


def _finalize_history(
    *,
    plan: MinutePlanLike,
    raw_rows: Sequence[tuple[str, int, float]],
    exact_start: float,
    exact_end: float,
    padded_start: int,
    padded_end: int,
    source_row_count: int,
    fetch_started_at: datetime,
    empty_error_message: str,
) -> MinuteFetchResult:
    filtered = _dedupe_history_by_timestamp(
        tuple(row for row in raw_rows if exact_start <= row[1] <= exact_end)
    )
    if not filtered:
        return MinuteFetchResult(
            plan=plan,
            fetch_status="empty",
            history=(),
            request_start_epoch=padded_start,
            request_end_epoch=padded_end,
            source_row_count=source_row_count,
            history_sha256=None,
            fetch_started_at=fetch_started_at,
            fetch_finished_at=datetime.now(timezone.utc),
            error_type="EmptyHistory",
            error_message=empty_error_message,
        )
    if any(token != plan.token_id for token, _, _ in filtered):
        raise ValueError(f"CLOB returned a mismatched token for {plan.token_id}")
    if any(not 0.0 <= price <= 1.0 for _, _, price in filtered):
        raise ValueError(f"CLOB returned an invalid probability for {plan.token_id}")
    # Typed buffers avoid JSON-serializing every (token, ts, price) row. Token is
    # constant across filtered rows (validated above), so hash it once.
    ts_bytes = array.array("q", (row[1] for row in filtered)).tobytes()
    price_bytes = array.array("d", (row[2] for row in filtered)).tobytes()
    history_sha256 = hashlib.sha256(
        plan.token_id.encode("utf-8") + ts_bytes + price_bytes
    ).hexdigest()
    return MinuteFetchResult(
        plan=plan,
        fetch_status="success",
        history=filtered,
        request_start_epoch=padded_start,
        request_end_epoch=padded_end,
        source_row_count=source_row_count,
        history_sha256=history_sha256,
        fetch_started_at=fetch_started_at,
        fetch_finished_at=datetime.now(timezone.utc),
    )


def fetch_minute_plan(
    plan: MinutePlanLike,
    client: Any,
    fetch_window_fn: Callable[..., Any],
    *,
    transient_retries: int,
    transient_backoff_seconds: float,
    window_seconds: int,
    status_hook: Callable[[int], None] | None = None,
    empty_error_message: str | None = None,
) -> MinuteFetchResult:
    """Per-token fetch with preemptive window chunking (hourly parity)."""
    fetch_started_at = datetime.now(timezone.utc)
    source_row_count = 0
    exact_start = plan.started_at.timestamp()
    exact_end = plan.finished_at.timestamp()
    padded_start, padded_end = padded_epoch_bounds(plan.started_at, plan.finished_at)
    empty_message = empty_error_message or (
        f"Empty in-window CLOB history for token {plan.token_id}"
    )
    try:
        collected: list[tuple[str, int, float]] = []
        for window_start, window_end in iter_windows(
            padded_start, padded_end, max(1, int(window_seconds))
        ):
            rows = fetch_window_fn(
                client,
                plan.token_id,
                window_start,
                window_end,
                FIDELITY_MINUTES,
                MIN_SPLIT_WINDOW_SECONDS,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
            if rows is None:
                raise RuntimeError(f"Transient CLOB failure for token {plan.token_id}")
            chunk = list(rows)
            source_row_count += len(chunk)
            collected.extend(_normalize_rows(chunk))
        # Window chunks are disjoint and ascending; each chunk is already
        # normalized+sorted, so collected is already globally sorted.
        return _finalize_history(
            plan=plan,
            raw_rows=collected,
            exact_start=exact_start,
            exact_end=exact_end,
            padded_start=padded_start,
            padded_end=padded_end,
            source_row_count=source_row_count,
            fetch_started_at=fetch_started_at,
            empty_error_message=empty_message,
        )
    except Exception as exc:
        return MinuteFetchResult(
            plan=plan,
            fetch_status="error",
            history=(),
            request_start_epoch=padded_start,
            request_end_epoch=padded_end,
            source_row_count=source_row_count,
            history_sha256=None,
            fetch_started_at=fetch_started_at,
            fetch_finished_at=datetime.now(timezone.utc),
            error_type=exc.__class__.__name__,
            error_message=sanitize_error_message(exc),
        )


def fetch_minute_plan_group(
    plans: Sequence[MinutePlanLike],
    client: Any,
    fetch_group_window_fn: Callable[..., Any],
    *,
    transient_retries: int,
    transient_backoff_seconds: float,
    window_seconds: int,
    status_hook: Callable[[int], None] | None = None,
    empty_error_message_fn: Callable[[MinutePlanLike], str] | None = None,
) -> list[MinuteFetchResult]:
    """Batch-fetch tokens that share one exact window via POST /batch-prices-history."""
    if not plans:
        return []
    if len({(p.started_at, p.finished_at) for p in plans}) != 1:
        raise ValueError("Minute batch group requires identical exact windows")
    fetch_started_at = datetime.now(timezone.utc)
    first = plans[0]
    exact_start = first.started_at.timestamp()
    exact_end = first.finished_at.timestamp()
    padded_start, padded_end = padded_epoch_bounds(first.started_at, first.finished_at)
    token_ids = [plan.token_id for plan in plans]
    accumulated: dict[str, list[tuple[str, int, float]]] = {
        token_id: [] for token_id in token_ids
    }
    source_counts = {token_id: 0 for token_id in token_ids}
    permanent: dict[str, Exception] = {}
    transient: set[str] = set()
    active = set(token_ids)

    try:
        for window_start, window_end in iter_windows(
            padded_start, padded_end, max(1, int(window_seconds))
        ):
            active_ids = [token_id for token_id in token_ids if token_id in active]
            if not active_ids:
                break
            chunk_map = fetch_group_window_fn(
                client,
                active_ids,
                window_start,
                window_end,
                FIDELITY_MINUTES,
                MIN_SPLIT_WINDOW_SECONDS,
                transient_retries,
                transient_backoff_seconds,
                status_hook,
            )
            if not isinstance(chunk_map, dict):
                for token_id in active_ids:
                    transient.add(token_id)
                    active.discard(token_id)
                continue
            for token_id in active_ids:
                value = chunk_map.get(token_id)
                if isinstance(value, Exception):
                    permanent[token_id] = value
                    active.discard(token_id)
                    continue
                if value is None:
                    transient.add(token_id)
                    active.discard(token_id)
                    continue
                rows = _normalize_rows(list(value))
                source_counts[token_id] += len(rows)
                accumulated[token_id].extend(rows)
    except Exception as exc:
        return [
            MinuteFetchResult(
                plan=plan,
                fetch_status="error",
                history=(),
                request_start_epoch=padded_start,
                request_end_epoch=padded_end,
                source_row_count=source_counts.get(plan.token_id, 0),
                history_sha256=None,
                fetch_started_at=fetch_started_at,
                fetch_finished_at=datetime.now(timezone.utc),
                error_type=exc.__class__.__name__,
                error_message=sanitize_error_message(exc),
            )
            for plan in plans
        ]

    results: list[MinuteFetchResult] = []
    for plan in plans:
        token_id = plan.token_id
        if token_id in permanent:
            exc = permanent[token_id]
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type=exc.__class__.__name__,
                    error_message=sanitize_error_message(exc),
                )
            )
            continue
        if token_id in transient:
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type="TransientAPIError",
                    error_message=f"Transient CLOB failure for token {token_id}",
                )
            )
            continue
        empty_message = (
            empty_error_message_fn(plan)
            if empty_error_message_fn
            else f"Empty in-window CLOB history for token {token_id}"
        )
        try:
            # Per-window slices are already normalized before extend; windows are
            # disjoint and ascending, so accumulated[token_id] is sorted.
            results.append(
                _finalize_history(
                    plan=plan,
                    raw_rows=accumulated[token_id],
                    exact_start=exact_start,
                    exact_end=exact_end,
                    padded_start=padded_start,
                    padded_end=padded_end,
                    source_row_count=source_counts[token_id],
                    fetch_started_at=fetch_started_at,
                    empty_error_message=empty_message,
                )
            )
        except Exception as exc:
            results.append(
                MinuteFetchResult(
                    plan=plan,
                    fetch_status="error",
                    history=(),
                    request_start_epoch=padded_start,
                    request_end_epoch=padded_end,
                    source_row_count=source_counts[token_id],
                    history_sha256=None,
                    fetch_started_at=fetch_started_at,
                    fetch_finished_at=datetime.now(timezone.utc),
                    error_type=exc.__class__.__name__,
                    error_message=sanitize_error_message(exc),
                )
            )
    return results


def execute_minute_fetches(
    plans: Sequence[MinutePlanLike],
    *,
    asset_name: str,
    log: Any = logger,
    workers: int = DEFAULT_MINUTE_WORKERS,
    requests_per_second: int | None = DEFAULT_MINUTE_REQUESTS_PER_SECOND,
    batch_group_size: int = DEFAULT_MINUTE_BATCH_GROUP_SIZE,
    window_hours: int = DEFAULT_MINUTE_WINDOW_HOURS,
    auto_tune_rps: bool = True,
    auto_tune_max_rps: int | None = DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS,
    auto_tune_window_requests: int = 40,
    auto_tune_threshold_429: float = 0.05,
    auto_tune_threshold_error: float = 0.15,
    transient_retries: int = 2,
    transient_backoff_seconds: float = 0.25,
    progress_log_interval_seconds: int = 60,
    no_progress_soft_timeout_seconds: int | None = 900,
    no_progress_hard_timeout_seconds: int | None = 2700,
    progress_poll_seconds: int = 5,
    client_factory: Callable[[], Any] | None = None,
    fetch_window_fn: Callable[..., Any] = fetch_window_with_auto_split,
    fetch_group_window_fn: Callable[..., Any] = fetch_group_window_with_auto_split,
    empty_error_message_fn: Callable[[MinutePlanLike], str] | None = None,
) -> list[MinuteFetchResult]:
    """Run minute fetches with hourly-parity concurrency, batching, and auto-tune."""
    configured_rps = (
        requests_per_second
        if requests_per_second is not None
        else (ODDS_REQUESTS_PER_SECOND or DEFAULT_MINUTE_REQUESTS_PER_SECOND)
    )
    configured_rps = max(1, int(configured_rps))
    limiter = RateLimiter(configured_rps)
    runtime_status = {"total": 0, "429": 0, "error": 0}
    runtime_status_lock = Lock()
    tune_state = {"last_total": 0, "last_429": 0, "last_error": 0}
    worker_state = local()
    window_seconds = max(60, int(window_hours) * 3600)
    use_batch = int(batch_group_size) > 1

    def on_http_status(status_code: int) -> None:
        with runtime_status_lock:
            runtime_status["total"] += 1
            code = int(status_code)
            if code == 429:
                runtime_status["429"] += 1
            if code < 200 or code >= 400:
                runtime_status["error"] += 1

    def client() -> Any:
        value = getattr(worker_state, "client", None)
        if value is None:
            value = (
                client_factory()
                if client_factory
                else build_client(CLOB_API_URL, rate_limiter=limiter)
            )
            worker_state.client = value
        return value

    def fetch_group_unit(
        group: tuple[MinutePlanLike, ...],
    ) -> list[MinuteFetchResult]:
        return fetch_minute_plan_group(
            group,
            client(),
            fetch_group_window_fn,
            transient_retries=transient_retries,
            transient_backoff_seconds=transient_backoff_seconds,
            window_seconds=window_seconds,
            status_hook=on_http_status,
            empty_error_message_fn=empty_error_message_fn,
        )

    def fetch_single_unit(plan: MinutePlanLike) -> list[MinuteFetchResult]:
        return [
            fetch_minute_plan(
                plan,
                client(),
                fetch_window_fn,
                transient_retries=transient_retries,
                transient_backoff_seconds=transient_backoff_seconds,
                window_seconds=window_seconds,
                status_hook=on_http_status,
                empty_error_message=(
                    empty_error_message_fn(plan) if empty_error_message_fn else None
                ),
            )
        ]

    work_units: list[Any]
    fetch_unit: Callable[[Any], list[MinuteFetchResult]]
    if use_batch:
        work_units = group_minute_plans(plans, batch_group_size=batch_group_size)
        fetch_unit = fetch_group_unit
    else:
        work_units = list(plans)
        fetch_unit = fetch_single_unit

    guardrail = ProgressGuardrail(
        asset=asset_name,
        logger=log,
        progress_log_interval_seconds=progress_log_interval_seconds,
        no_progress_soft_timeout_seconds=no_progress_soft_timeout_seconds,
        no_progress_hard_timeout_seconds=no_progress_hard_timeout_seconds,
        work_log_interval=25,
    )
    fetched: list[MinuteFetchResult] = []
    effective_workers = max(1, int(workers))
    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        futures: dict[Future[list[MinuteFetchResult]], Any] = {
            pool.submit(fetch_unit, unit): unit for unit in work_units
        }
        pending = set(futures.keys())
        while pending:
            done, pending = wait(
                pending,
                timeout=max(1, progress_poll_seconds),
                return_when=FIRST_COMPLETED,
            )
            if auto_tune_rps:
                with runtime_status_lock:
                    status_snapshot = dict(runtime_status)
                maybe_auto_tune_rps(
                    limiter=limiter,
                    runtime_status=status_snapshot,
                    tune_state=tune_state,
                    window_requests=auto_tune_window_requests,
                    threshold_429=auto_tune_threshold_429,
                    threshold_error=auto_tune_threshold_error,
                    min_rps=max(1, configured_rps),
                    max_rps=max(
                        configured_rps,
                        int(auto_tune_max_rps or configured_rps),
                    ),
                )
            if not done:
                guardrail.check(
                    phase="fetch_token_wait",
                    diagnostics={"inflight_futures": len(pending)},
                )
                continue
            for future in done:
                unit = futures[future]
                try:
                    results = future.result()
                except Exception as exc:  # pragma: no cover - defensive worker boundary
                    now = datetime.now(timezone.utc)
                    unit_plans = list(unit) if use_batch else [unit]
                    results = [
                        MinuteFetchResult(
                            plan=plan,
                            fetch_status="error",
                            history=(),
                            request_start_epoch=int(plan.started_at.timestamp()),
                            request_end_epoch=int(plan.finished_at.timestamp()),
                            source_row_count=0,
                            history_sha256=None,
                            fetch_started_at=now,
                            fetch_finished_at=now,
                            error_type=exc.__class__.__name__,
                            error_message=sanitize_error_message(exc),
                        )
                        for plan in unit_plans
                    ]
                fetched.extend(results)
                for result in results:
                    guardrail.record_progress(
                        work_increment=1,
                        phase="fetch_token",
                        diagnostics={
                            "token_id": result.plan.token_id,
                            "status": result.fetch_status,
                        },
                    )
                    guardrail.check(
                        phase="fetch_token",
                        diagnostics={
                            "token_id": result.plan.token_id,
                            "status": result.fetch_status,
                        },
                    )

    fetched.sort(key=lambda result: result.plan.token_id)
    return fetched


def resolve_minute_token_reuse(
    plans: Sequence[MinutePlanLike],
    *,
    leg: str,
    conn: Any,
    scope_name: str = SCOPE_WC2026,
):
    """Return ``(previous_snapshot, reusable_token_ids, published_windows)``."""
    import duckdb

    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        active_snapshot_dir,
        load_latest_published_token_windows,
        minute_odds_snapshot_root,
        tokens_reusable_by_window,
        validate_minute_odds_snapshot,
    )

    root = minute_odds_snapshot_root(leg=leg, scope_name=scope_name)
    previous_dir = active_snapshot_dir(root)
    previous = (
        validate_minute_odds_snapshot(previous_dir)
        if previous_dir is not None
        else None
    )
    try:
        published = load_latest_published_token_windows(
            conn, leg=leg, scope_name=scope_name
        )
    except duckdb.Error:
        # Fresh/mocked warehouses may not have ops audit tables yet.
        published = {}
    reusable = tokens_reusable_by_window(
        plans,
        previous=previous,
        published_windows=published,
    )
    return previous, reusable, published


def call_minute_persist(
    persist_fn: Callable[..., Any],
    shard_paths: Sequence[Path],
    conn: Any,
    *,
    fetch_run_id: str,
    reuse_token_ids: set[str] | None = None,
) -> Any:
    """Call a minute persist function, passing reuse when the callee accepts it."""
    kwargs: dict[str, Any] = {"fetch_run_id": fetch_run_id}
    try:
        signature = inspect.signature(persist_fn)
    except (TypeError, ValueError):
        signature = None
    if signature is not None and (
        "reuse_token_ids" in signature.parameters
        or any(
            param.kind is inspect.Parameter.VAR_KEYWORD
            for param in signature.parameters.values()
        )
    ):
        kwargs["reuse_token_ids"] = set(reuse_token_ids or ())
    return persist_fn(shard_paths, conn, **kwargs)


def synthesize_reused_minute_fetch_results(
    plans: Sequence[MinutePlanLike],
    *,
    published_windows: Mapping[str, Any],
    now: datetime | None = None,
) -> list[MinuteFetchResult]:
    """Build success fetch results for tokens reused from the prior snapshot."""
    finished = now or datetime.now(timezone.utc)
    out: list[MinuteFetchResult] = []
    for plan in plans:
        prior = published_windows[plan.token_id]
        start_epoch = int(plan.started_at.timestamp())
        end_epoch = int(plan.finished_at.timestamp())
        out.append(
            MinuteFetchResult(
                plan=plan,
                fetch_status="success",
                history=tuple(),
                request_start_epoch=start_epoch,
                request_end_epoch=end_epoch,
                source_row_count=int(prior.row_count),
                history_sha256=str(prior.history_sha256),
                fetch_started_at=finished,
                fetch_finished_at=finished,
            )
        )
    return out


__all__ = [
    "DEFAULT_MINUTE_AUTO_TUNE_MAX_RPS",
    "DEFAULT_MINUTE_BATCH_GROUP_SIZE",
    "DEFAULT_MINUTE_MARKET_SAMPLE_METHOD",
    "DEFAULT_MINUTE_PUBLISH_BATCH_ROWS",
    "DEFAULT_MINUTE_PUBLISH_COMPRESSION",
    "DEFAULT_MINUTE_PUBLISH_SHARD_ROWS",
    "DEFAULT_MINUTE_REQUESTS_PER_SECOND",
    "DEFAULT_MINUTE_WINDOW_HOURS",
    "DEFAULT_MINUTE_WORKERS",
    "FIDELITY_MINUTES",
    "MIN_SPLIT_WINDOW_SECONDS",
    "MinuteFetchResult",
    "MinuteHistoryResultLike",
    "MinutePlanLike",
    "borrow_duckdb_connection",
    "build_minute_history_arrow_table",
    "call_minute_persist",
    "cap_minute_plan_window_tail",
    "cleanup_minute_odds_publish_cache",
    "ensure_unique_success_token_ids",
    "execute_minute_fetches",
    "fetch_minute_plan",
    "fetch_minute_plan_group",
    "group_minute_plans",
    "iter_minute_history_arrow_batches",
    "minute_odds_publish_cache_dir",
    "padded_epoch_bounds",
    "release_minute_history_payloads",
    "resolve_minute_token_reuse",
    "sample_minute_market_plans",
    "sanitize_error_message",
    "synthesize_reused_minute_fetch_results",
    "write_minute_history_parquet_shards",
]
