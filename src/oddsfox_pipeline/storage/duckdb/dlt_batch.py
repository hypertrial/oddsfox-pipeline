"""dlt batch landing helpers for DuckDB canonical table finalizers."""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import tempfile
import time
from collections.abc import Callable, Sequence
from hashlib import blake2b
from pathlib import Path
from typing import Any, Literal

import dlt
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from oddsfox_pipeline.naming import (
    SCOPE_SOCCER,
    SCOPE_WC2026,
    SOURCE_KALSHI,
    SOURCE_POLYMARKET,
    schema_name,
)
from oddsfox_pipeline.storage.duckdb import connection as duckdb_connection
from oddsfox_pipeline.storage.duckdb.polymarket_scope import get_active_polymarket_scope
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_ops_schema,
    polymarket_ops_tbl,
    polymarket_q,
    polymarket_raw_schema,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
    EVENT_MARKET_SNAPSHOT_COLUMNS,
    EVENT_SNAPSHOT_COLUMNS,
    EVENT_TAG_SNAPSHOT_COLUMNS,
    FUTURES_MINUTE_ODDS_HISTORY_COLUMNS,
    INGESTION_RUN_EVENT_COLUMNS,
    MARKET_SCOPE_REGISTRY_COLUMNS,
    MARKET_TOKEN_COLUMNS,
    MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS,
    ODDS_HISTORY_COLUMNS,
)

logger = logging.getLogger(__name__)

DLT_STRICT_SCHEMA_CONTRACT = {
    "tables": "evolve",
    "columns": "freeze",
    "data_type": "freeze",
}

_PIPELINES: dict[tuple[str, str], dlt.Pipeline] = {}
_BATCH_PIPELINE_RUN_ID = f"{os.getpid():x}"
_DLT_PIPELINE_BY_PATH: dict[str, dlt.Pipeline] = {}


def reset_dlt_batch_pipelines() -> None:
    """Clear cached pipelines; useful when tests swap DUCKDB_NAME."""
    _PIPELINES.clear()
    _DLT_PIPELINE_BY_PATH.clear()


def _dlt_pipeline_name(dataset_name: str) -> str:
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        return f"{dataset_name}_{worker}_landing"
    return f"{dataset_name}_landing"


def get_cached_dlt_pipeline(
    *,
    dataset_name: str,
    active_duckdb_path_fn: Callable[[], Any],
    dlt_module: Any = dlt,
    pipeline_cache: dict[str, dlt.Pipeline] | None = None,
) -> dlt.Pipeline:
    cache = _DLT_PIPELINE_BY_PATH if pipeline_cache is None else pipeline_cache
    db_path = str(active_duckdb_path_fn())
    cache_key = f"{db_path}:{dataset_name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    pipe = dlt_module.pipeline(
        pipeline_name=_dlt_pipeline_name(dataset_name),
        destination=dlt_module.destinations.duckdb(credentials=db_path),
        dataset_name=dataset_name,
    )
    cache[cache_key] = pipe
    return pipe


def get_polymarket_dlt_pipeline(
    *,
    scope_name: str = SCOPE_WC2026,
    active_duckdb_path_fn: Callable[[], Any] | None = None,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    path_fn = (
        duckdb_connection.active_duckdb_path
        if active_duckdb_path_fn is None
        else active_duckdb_path_fn
    )
    dataset_name = schema_name(SOURCE_POLYMARKET, scope_name, "raw")
    return get_cached_dlt_pipeline(
        dataset_name=dataset_name,
        active_duckdb_path_fn=path_fn,
        dlt_module=dlt_module,
    )


def get_kalshi_dlt_pipeline(
    *,
    scope_name: str = SCOPE_WC2026,
    active_duckdb_path_fn: Callable[[], Any] | None = None,
    dlt_module: Any = dlt,
) -> dlt.Pipeline:
    path_fn = (
        duckdb_connection.active_duckdb_path
        if active_duckdb_path_fn is None
        else active_duckdb_path_fn
    )
    dataset_name = schema_name(SOURCE_KALSHI, scope_name, "raw")
    return get_cached_dlt_pipeline(
        dataset_name=dataset_name,
        active_duckdb_path_fn=path_fn,
        dlt_module=dlt_module,
    )


def _pipeline(schema: str) -> dlt.Pipeline:
    duckdb_connection.ensure_duck_db()
    db_path = str(duckdb_connection.active_duckdb_path())
    key = (schema, db_path)
    if key not in _PIPELINES:
        # dlt persists pipeline state outside DuckDB; these stage tables are
        # replace-only scratch space, so avoid cross-process stale schemas.
        path_hash = blake2b(db_path.encode("utf-8"), digest_size=12).hexdigest()
        _PIPELINES[key] = dlt.pipeline(
            pipeline_name=(
                f"polymarket_{schema}_batch_v1_{path_hash}_{_BATCH_PIPELINE_RUN_ID}"
            ),
            destination=dlt.destinations.duckdb(credentials=db_path),
            dataset_name=schema,
        )
    return _PIPELINES[key]


def load_stage_rows(
    *,
    schema: str,
    stage_table: str,
    rows: Sequence[dict[str, Any]],
    columns: dict[str, dict[str, Any]],
) -> str:
    """Replace a dlt stage table and return its fully qualified DuckDB name."""
    if not rows:
        raise ValueError("rows must not be empty")
    pipe = _pipeline(schema)
    if pipe.has_pending_data:
        pipe.drop_pending_packages()
    pipe.run(
        list(rows),
        table_name=stage_table,
        write_disposition="replace",
        columns=columns,
        schema_contract=DLT_STRICT_SCHEMA_CONTRACT,
    )
    return polymarket_q(schema, stage_table)


def _with_row_order(rows: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "row_order": idx} for idx, row in enumerate(rows)]


def load_market_tokens_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    raw_schema = polymarket_raw_schema(scope_name)
    target = polymarket_raw_tbl(scope_name, "market_tokens")
    stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_market_tokens_v1",
        rows=_with_row_order(rows),
        columns=MARKET_TOKEN_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target}
        (market_id, clobTokenIds, updated_at)
        SELECT market_id, clob_token_ids, updated_at
        FROM (
            SELECT
                market_id,
                clob_token_ids,
                updated_at,
                row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY updated_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        """
    )


def _load_odds_history_stage_arrow(
    conn: duckdb.DuckDBPyConnection,
    rows: Sequence[dict[str, Any]],
    *,
    schema: str,
    stage_table: str,
) -> str:
    """Replace an odds stage table on ``conn`` without a dlt pipeline round-trip."""
    if not rows:
        raise ValueError("rows must not be empty")
    ordered = _with_row_order(rows)
    # Explicit types: empty Python lists otherwise infer Arrow null columns.
    table = pa.table(
        {
            "clob_token_id": pa.array(
                [row["clobTokenId"] for row in ordered], type=pa.string()
            ),
            "timestamp": pa.array(
                [row["timestamp"] for row in ordered], type=pa.int64()
            ),
            "price": pa.array([row["price"] for row in ordered], type=pa.float64()),
            "ingested_at": [row["ingested_at"] for row in ordered],
            "row_order": pa.array(
                [row["row_order"] for row in ordered], type=pa.int64()
            ),
        }
    )
    qualified = polymarket_q(schema, stage_table)
    conn.register("_oddsfox_odds_stage_arrow", table)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {qualified} AS SELECT * FROM _oddsfox_odds_stage_arrow"
        )
    finally:
        conn.unregister("_oddsfox_odds_stage_arrow")
    return qualified


def _replace_minute_odds_stage_from_table(
    conn: duckdb.DuckDBPyConnection,
    table: pa.Table,
    *,
    schema: str,
    stage_table: str,
) -> str:
    """Register an Arrow table and replace the DuckDB minute-odds stage relation.

    Kept for the benchmark baseline oracle only; production publish uses Parquet
    candidate swap.
    """
    if table.num_rows == 0:
        raise ValueError("rows must not be empty")
    qualified = polymarket_q(schema, stage_table)
    conn.register("_oddsfox_minute_odds_stage_arrow", table)
    try:
        conn.execute(
            f"CREATE OR REPLACE TABLE {qualified} AS "
            f"SELECT * FROM _oddsfox_minute_odds_stage_arrow"
        )
    finally:
        conn.unregister("_oddsfox_minute_odds_stage_arrow")
    return qualified


def _load_minute_odds_history_stage_arrow(
    conn: duckdb.DuckDBPyConnection,
    rows: Sequence[dict[str, Any]] | pa.Table,
    *,
    schema: str,
    stage_table: str,
) -> str:
    """Replace a minute-odds stage table on ``conn`` without a dlt round-trip."""
    if isinstance(rows, pa.Table):
        return _replace_minute_odds_stage_from_table(
            conn, rows, schema=schema, stage_table=stage_table
        )
    if not rows:
        raise ValueError("rows must not be empty")
    market_ids: list[Any] = []
    clob_token_ids: list[Any] = []
    timestamps: list[Any] = []
    prices: list[Any] = []
    fidelity_minutes: list[Any] = []
    window_starts: list[Any] = []
    window_ends: list[Any] = []
    ingested_ats: list[Any] = []
    row_orders: list[int] = []
    for idx, row in enumerate(rows):
        market_ids.append(row["market_id"])
        clob_token_ids.append(row.get("clob_token_id", row.get("clobTokenId")))
        timestamps.append(row["timestamp"])
        prices.append(row["price"])
        fidelity_minutes.append(row["fidelity_minutes"])
        window_starts.append(row["window_start_at"])
        window_ends.append(row["window_end_at"])
        ingested_ats.append(row["ingested_at"])
        row_orders.append(idx)
    timestamp_type = pa.timestamp("us", tz="UTC")
    table = pa.table(
        {
            "market_id": pa.array(market_ids, type=pa.string()),
            "clob_token_id": pa.array(clob_token_ids, type=pa.string()),
            "timestamp": pa.array(timestamps, type=pa.int64()),
            "price": pa.array(prices, type=pa.float64()),
            "fidelity_minutes": pa.array(fidelity_minutes, type=pa.int32()),
            "window_start_at": pa.array(window_starts, type=timestamp_type),
            "window_end_at": pa.array(window_ends, type=timestamp_type),
            "ingested_at": pa.array(ingested_ats, type=timestamp_type),
            "row_order": pa.array(row_orders, type=pa.int64()),
        }
    )
    return _replace_minute_odds_stage_from_table(
        conn, table, schema=schema, stage_table=stage_table
    )


def load_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    stage = prepare_odds_history_stage(rows, conn, scope_name=scope)
    merge_odds_history_stage(conn, stage, scope_name=scope)


def prepare_odds_history_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> str:
    """Load odds rows into a stage table on ``conn``; call before ``BEGIN``."""
    scope = scope_name or get_active_polymarket_scope()
    return _load_odds_history_stage_arrow(
        conn,
        rows,
        schema=polymarket_raw_schema(scope),
        stage_table="stage_odds_history_v1",
    )


def merge_odds_history_stage(
    conn: duckdb.DuckDBPyConnection,
    stage: str,
    *,
    scope_name: str | None = None,
) -> None:
    """Append new source points without rewriting an observed token/timestamp."""
    target = polymarket_raw_tbl(
        scope_name or get_active_polymarket_scope(), "odds_history"
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (clobTokenId, timestamp, price, ingested_at)
        SELECT clob_token_id, timestamp, price, ingested_at
        FROM (
            SELECT
                clob_token_id,
                timestamp,
                price,
                ingested_at,
                row_number() OVER (
                    PARTITION BY clob_token_id, timestamp
                    ORDER BY ingested_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT DO NOTHING
        """
    )


def _configure_minute_publish_connection(conn: duckdb.DuckDBPyConnection) -> None:
    """Apply publish-only DuckDB settings for snapshot publish / view registration."""
    runtime_root = (
        Path(
            os.getenv(
                "ODDSFOX_RUNTIME_ROOT",
                str(
                    Path(os.getenv("ODDSFOX_PIPELINE_ROOT", ".")).resolve()
                    / ".cache"
                    / "runtime"
                ),
            )
        )
        .expanduser()
        .resolve()
    )
    temp_dir = runtime_root / "duckdb-temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    conn.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    conn.execute("SET preserve_insertion_order=false")
    # Cap DuckDB RSS so publish-time inventory/primary resolution spills to
    # temp_directory instead of competing with the OS until SIGKILL. Override
    # with ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT (e.g. 8GB, 50%).
    memory_limit = os.getenv("ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT", "12GB").strip()
    if not re.fullmatch(r"\d+(\.\d+)?\s*(KB|MB|GB|TB|%)", memory_limit, flags=re.I):
        raise ValueError(
            "ODDSFOX_MINUTE_PUBLISH_MEMORY_LIMIT must look like 12GB or 50%"
        )
    conn.execute(f"SET memory_limit='{memory_limit.replace(' ', '')}'")
    threads_raw = os.getenv("ODDSFOX_MINUTE_PUBLISH_THREADS", "").strip()
    if threads_raw:
        threads = int(threads_raw)
        if threads < 1:
            raise ValueError("ODDSFOX_MINUTE_PUBLISH_THREADS must be >= 1")
        conn.execute(f"SET threads={threads}")


def _minute_publish_input_to_parquet_paths(
    rows: Sequence[dict[str, Any]] | pa.Table | Sequence[str | Path],
    *,
    fetch_run_id: str,
) -> tuple[list[Path], Path | None]:
    """Normalize publish input to parquet paths.

    Returns ``(paths, cleanup_dir)``. ``cleanup_dir`` is set when this helper
    created a temporary directory that the caller should remove.
    """
    if isinstance(rows, (str, Path)):
        path = Path(rows)
        if not path.is_file():
            raise ValueError(f"Parquet shard does not exist: {path}")
        return [path.resolve()], None
    if isinstance(rows, pa.Table):
        if rows.num_rows == 0:
            raise ValueError("rows must not be empty")
        temp_root = Path(tempfile.mkdtemp(prefix=f"minute-odds-{fetch_run_id}-"))
        path = temp_root / "shard-00000.parquet"
        rename_map = {
            "market_id": "market_id",
            "clob_token_id": "clobTokenId",
            "timestamp": "timestamp",
            "price": "price",
            "fidelity_minutes": "fidelity_minutes",
            "window_start_at": "window_start_at",
            "window_end_at": "window_end_at",
            "ingested_at": "ingested_at",
        }
        selected = list(rename_map)
        missing = [name for name in selected if name not in rows.column_names]
        if missing:
            raise ValueError(f"Arrow publish table missing columns: {missing}")
        table = rows.select(selected).rename_columns(
            [rename_map[name] for name in selected]
        )
        pq.write_table(
            table,
            path,
            compression="snappy",
            use_dictionary=False,
            write_statistics=False,
        )
        return [path], temp_root
    if not rows:
        raise ValueError("rows must not be empty")
    first = rows[0]
    if isinstance(first, (str, Path)):
        paths = [Path(item).resolve() for item in rows]  # type: ignore[arg-type]
        missing = [str(path) for path in paths if not path.is_file()]
        if missing:
            raise ValueError(f"Parquet shard(s) missing: {missing[:3]}")
        return paths, None
    temp_root = Path(tempfile.mkdtemp(prefix=f"minute-odds-{fetch_run_id}-"))
    path = temp_root / "shard-00000.parquet"
    table_rows = []
    for row in rows:  # type: ignore[arg-type]
        table_rows.append(
            {
                "market_id": row["market_id"],
                "clobTokenId": row.get("clob_token_id", row.get("clobTokenId")),
                "timestamp": int(row["timestamp"]),
                "price": float(row["price"]),
                "fidelity_minutes": int(row["fidelity_minutes"]),
                "window_start_at": row["window_start_at"],
                "window_end_at": row["window_end_at"],
                "ingested_at": row["ingested_at"],
            }
        )
    table = pa.Table.from_pylist(table_rows)
    pq.write_table(
        table,
        path,
        compression="snappy",
        use_dictionary=False,
        write_statistics=False,
    )
    return [path], temp_root


def _resolve_primary_token_ids(
    conn: duckdb.DuckDBPyConnection,
    parquet_paths: Sequence[Path],
    *,
    extra_token_market_rows: Sequence[tuple[str, str]] | None = None,
    scope_name: str = SCOPE_WC2026,
) -> set[str]:
    """Prefer Yes outcome tokens per market; fall back to lowest token id."""
    extra_rows = [(str(m), str(t)) for m, t in (extra_token_market_rows or ())]
    if not parquet_paths and not extra_rows:
        return set()
    path_literals = ", ".join("?" for _ in parquet_paths) if parquet_paths else ""
    parquet_names = (
        set(pq.ParquetFile(parquet_paths[0]).schema.names) if parquet_paths else set()
    )
    token_column = (
        '"clobTokenId"' if "clobTokenId" in parquet_names else "clob_token_id"
    )
    if scope_name == SCOPE_SOCCER:
        present = {token for _, token in extra_rows}
        if parquet_paths:
            present.update(
                str(row[0])
                for row in conn.execute(
                    f"SELECT DISTINCT {token_column} FROM read_parquet("
                    f"[{path_literals}], hive_partitioning=false)",
                    [str(path) for path in parquet_paths],
                ).fetchall()
            )
        registry = polymarket_ops_tbl(scope_name, "match_result_registry")
        return {
            str(row[0])
            for row in conn.execute(f"SELECT yes_token_id FROM {registry}").fetchall()
            if str(row[0]) in present
        }
    markets = polymarket_raw_tbl(scope_name, "markets")
    has_markets = (
        conn.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_name = 'markets'
            """,
            [polymarket_raw_schema(scope_name)],
        ).fetchone()[0]
        > 0
    )
    if has_markets:
        raw_tokens_sql = (
            f"""
            SELECT DISTINCT
                market_id,
                {token_column} AS clob_token_id
            FROM read_parquet([{path_literals}], hive_partitioning=false)
            """
            if parquet_paths
            else """
            SELECT CAST(NULL AS VARCHAR) AS market_id,
                   CAST(NULL AS VARCHAR) AS clob_token_id
            WHERE FALSE
            """
        )
        if extra_rows:
            conn.register(
                "_minute_primary_extra_tokens",
                pa.table(
                    {
                        "market_id": [row[0] for row in extra_rows],
                        "clob_token_id": [row[1] for row in extra_rows],
                    }
                ),
            )
        try:
            rows = conn.execute(
                f"""
                WITH raw_tokens AS (
                    {raw_tokens_sql}
                    {
                    "UNION SELECT market_id, clob_token_id "
                    "FROM _minute_primary_extra_tokens"
                    if extra_rows
                    else ""
                }
                ),
                exploded AS (
                    SELECT
                        m.id AS market_id,
                        CAST(
                            from_json(m.clob_token_ids, '["VARCHAR"]')[i] AS VARCHAR
                        ) AS clob_token_id,
                        CAST(
                            from_json(m.outcomes, '["VARCHAR"]')[i] AS VARCHAR
                        ) AS outcome_label,
                        i AS outcome_idx
                    FROM {markets} AS m
                    CROSS JOIN generate_series(
                        1,
                        len(from_json(m.clob_token_ids, '["VARCHAR"]'))
                    ) AS g(i)
                    WHERE len(from_json(m.clob_token_ids, '["VARCHAR"]'))
                        = len(from_json(m.outcomes, '["VARCHAR"]'))
                ),
                candidates AS (
                    SELECT
                        r.market_id,
                        r.clob_token_id,
                        coalesce(e.outcome_label, '') AS outcome_label,
                        coalesce(e.outcome_idx, 0) AS outcome_idx,
                        bool_or(lower(coalesce(e.outcome_label, '')) = 'yes') OVER (
                            PARTITION BY r.market_id
                        ) AS market_has_yes
                    FROM raw_tokens AS r
                    LEFT JOIN exploded AS e
                        ON r.market_id = e.market_id
                        AND r.clob_token_id = e.clob_token_id
                )
                SELECT clob_token_id
                FROM candidates
                WHERE
                    (market_has_yes AND lower(outcome_label) = 'yes')
                    OR NOT market_has_yes
                QUALIFY row_number() OVER (
                    PARTITION BY market_id
                    ORDER BY outcome_idx ASC, clob_token_id ASC
                ) = 1
                """,
                [str(path) for path in parquet_paths],
            ).fetchall()
        finally:
            if extra_rows:
                conn.unregister("_minute_primary_extra_tokens")
        if rows:
            return {str(row[0]) for row in rows}
    if not parquet_paths:
        # Reuse-only publish without markets: one primary per market (Yes tip
        # when present in the token id, else lowest id). Never treat every
        # reused token as primary.
        by_market: dict[str, list[str]] = {}
        for market_id, token_id in extra_rows:
            by_market.setdefault(market_id, []).append(token_id)
        primary: set[str] = set()
        for tokens in by_market.values():
            yes = [
                token
                for token in tokens
                if token.casefold().endswith("-yes") or "yes" in token.casefold()
            ]
            primary.add(sorted(yes)[0] if yes else sorted(tokens)[0])
        return primary
    rows = conn.execute(
        f"""
        SELECT clob_token_id
        FROM (
            SELECT DISTINCT
                market_id,
                {token_column} AS clob_token_id
            FROM read_parquet([{path_literals}], hive_partitioning=false)
        )
        QUALIFY row_number() OVER (
            PARTITION BY market_id
            ORDER BY clob_token_id ASC
        ) = 1
        """,
        [str(path) for path in parquet_paths],
    ).fetchall()
    return {str(row[0]) for row in rows}


def _publish_minute_odds_from_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_paths: Sequence[Path],
    *,
    relation: Literal["match_minute_odds_history", "futures_minute_odds_history"],
    fetch_run_id: str,
    audit_mode: Literal["all", "success_only"],
    reuse_token_ids: set[str] | None = None,
    scope_name: str = SCOPE_WC2026,
) -> int:
    """Publish immutable Parquet snapshot + register DuckDB views (no heap PK rebuild)."""
    reuse_token_ids = {str(token) for token in (reuse_token_ids or set())}
    if not parquet_paths and not reuse_token_ids:
        raise ValueError("rows must not be empty")
    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        build_and_publish_snapshot_from_shards,
    )

    _configure_minute_publish_connection(conn)
    audit = polymarket_ops_tbl(
        scope_name,
        "match_minute_odds_fetch_audit"
        if relation == "match_minute_odds_history"
        else "futures_minute_odds_fetch_audit",
    )
    leg = "match" if relation == "match_minute_odds_history" else "futures"

    expected_rows = 0
    parquet_bytes = 0
    for path in parquet_paths:
        expected_rows += int(pq.ParquetFile(path).metadata.num_rows)
        parquet_bytes += path.stat().st_size
    space_root = (
        Path(parquet_paths[0]).parent
        if parquet_paths
        else Path(os.getenv("ODDSFOX_RUNTIME_ROOT", ".")).expanduser()
    )
    free_bytes = shutil.disk_usage(space_root).free
    # Snapshot rewrite + OHLC roughly 2x parquet bytes.
    if parquet_paths and free_bytes < max(parquet_bytes * 2, 1):
        raise OSError(
            "Insufficient local free space for minute-odds snapshot publish: "
            f"parquet_bytes={parquet_bytes} free={free_bytes}"
        )

    if parquet_paths:
        parquet_names = set(pq.ParquetFile(parquet_paths[0]).schema.names)
        if "clobTokenId" in parquet_names:
            token_column = '"clobTokenId"'
        elif "clob_token_id" in parquet_names:
            token_column = "clob_token_id"
        else:
            raise RuntimeError(
                f"Parquet shards missing token column (have {sorted(parquet_names)})"
            )
        path_literals = ", ".join("?" for _ in parquet_paths)
    else:
        token_column = '"clobTokenId"'
        path_literals = ""

    # Token inventory: prefer shard manifest token_ids (written beside Parquet)
    # so we never DISTINCT-scan hundreds of millions of rows.
    manifest_token_ids: set[str] | None = None
    if parquet_paths:
        manifest_path = Path(parquet_paths[0]).resolve().parent / "manifest.json"
        if manifest_path.is_file():
            try:
                payload = json.loads(manifest_path.read_text(encoding="utf-8"))
                raw_ids = payload["token_ids"]
                if not isinstance(raw_ids, list) or not raw_ids:
                    raise TypeError("token_ids must be a non-empty list")
                manifest_token_ids = {str(token_id) for token_id in raw_ids}
                if len(manifest_token_ids) != len(raw_ids):
                    raise ValueError("manifest token_ids contain duplicates")
                if int(payload["token_count"]) != len(manifest_token_ids):
                    raise ValueError(
                        f"token_count={payload['token_count']} != "
                        f"len(token_ids)={len(manifest_token_ids)}"
                    )
                if int(payload["row_count"]) != expected_rows:
                    raise RuntimeError(
                        f"Manifest row_count mismatch for {relation}: "
                        f"manifest={payload['row_count']} parquet={expected_rows}"
                    )
                if str(payload.get("fetch_run_id", fetch_run_id)) != str(fetch_run_id):
                    raise RuntimeError(
                        f"Manifest fetch_run_id mismatch for {relation}: "
                        f"manifest={payload.get('fetch_run_id')} expected={fetch_run_id}"
                    )
            except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"Invalid minute-odds publish manifest at {manifest_path}"
                ) from exc

    if audit_mode == "all":
        audit_inventory = conn.execute(
            f"""
            SELECT
                count(*),
                count(*) FILTER (
                    WHERE fetch_status = 'success' AND NOT raw_published
                )
            FROM {audit}
            WHERE fetch_run_id = ?
            """,
            [fetch_run_id],
        ).fetchone()
        distinct_tokens = int(audit_inventory[1])
        if audit_inventory != (distinct_tokens, distinct_tokens):
            raise RuntimeError(
                f"Fetch audit inventory does not match {distinct_tokens} staged tokens "
                f"for run {fetch_run_id}: {audit_inventory}"
            )
    else:
        distinct_tokens = int(
            conn.execute(
                f"""
                SELECT count(*) FILTER (
                    WHERE fetch_status = 'success' AND NOT raw_published
                )
                FROM {audit}
                WHERE fetch_run_id = ?
                """,
                [fetch_run_id],
            ).fetchone()[0]
        )

    audit_token_ids = {
        str(row[0])
        for row in conn.execute(
            f"""
            SELECT "clobTokenId"
            FROM {audit}
            WHERE fetch_run_id = ?
              AND fetch_status = 'success'
              AND NOT raw_published
            """,
            [fetch_run_id],
        ).fetchall()
    }
    if len(audit_token_ids) != distinct_tokens:
        raise RuntimeError(
            f"Fetch audit success tokens are not unique for run {fetch_run_id}: "
            f"rows={distinct_tokens} distinct={len(audit_token_ids)}"
        )

    if reuse_token_ids - audit_token_ids:
        raise RuntimeError(
            f"reuse_token_ids not present in fetch audit for run {fetch_run_id}: "
            f"{sorted(reuse_token_ids - audit_token_ids)[:5]}"
        )
    parquet_token_ids: set[str]
    if manifest_token_ids is not None:
        parquet_token_ids = set(manifest_token_ids)
        if expected_rows <= 5_000_000 and parquet_paths:
            measured_tokens = int(
                conn.execute(
                    f"""
                    SELECT count(DISTINCT {token_column})
                    FROM read_parquet([{path_literals}], hive_partitioning=false)
                    """,
                    [str(path) for path in parquet_paths],
                ).fetchone()[0]
            )
            if measured_tokens != len(parquet_token_ids):
                raise RuntimeError(
                    f"Candidate token inventory exceeds manifest for run "
                    f"{fetch_run_id}: manifest={len(parquet_token_ids)} "
                    f"candidate={measured_tokens}"
                )
    elif parquet_paths:
        parquet_token_ids = {
            str(row[0])
            for row in conn.execute(
                f"""
                SELECT DISTINCT {token_column}
                FROM read_parquet([{path_literals}], hive_partitioning=false)
                """,
                [str(path) for path in parquet_paths],
            ).fetchall()
        }
    else:
        parquet_token_ids = set()
    if parquet_token_ids & reuse_token_ids:
        raise RuntimeError(
            "reuse_token_ids overlap parquet shards for run "
            f"{fetch_run_id}: {sorted(parquet_token_ids & reuse_token_ids)[:5]}"
        )
    staged_token_ids = parquet_token_ids | reuse_token_ids
    if staged_token_ids != audit_token_ids:
        raise RuntimeError(
            f"Fetch audit inventory does not match staged tokens for run "
            f"{fetch_run_id}: "
            f"staged_only={sorted(staged_token_ids - audit_token_ids)[:5]} "
            f"audit_only={sorted(audit_token_ids - staged_token_ids)[:5]}"
        )

    if parquet_paths:
        missing_tokens = int(
            conn.execute(
                f"""
                SELECT count(*)
                FROM {audit} AS a
                WHERE a.fetch_run_id = ?
                  AND a.fetch_status = 'success'
                  AND NOT a.raw_published
                  AND a."clobTokenId" NOT IN (
                      SELECT CAST(token AS VARCHAR)
                      FROM (SELECT UNNEST(?::VARCHAR[]) AS token)
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM read_parquet([{path_literals}], hive_partitioning=false) AS c
                      WHERE c.{token_column} = a."clobTokenId"
                  )
                """,
                [
                    fetch_run_id,
                    list(reuse_token_ids),
                    *[str(path) for path in parquet_paths],
                ],
            ).fetchone()[0]
        )
    else:
        missing_tokens = len(audit_token_ids - reuse_token_ids)
    if missing_tokens:
        raise RuntimeError(
            f"Candidate missing {missing_tokens} audited success token(s) "
            f"for run {fetch_run_id}"
        )

    hash_column = (
        "in_game_history_sha256"
        if relation == "match_minute_odds_history"
        else "window_history_sha256"
    )
    window_hashes = {
        str(row[0]): str(row[1])
        for row in conn.execute(
            f"""
            SELECT "clobTokenId", {hash_column}
            FROM {audit}
            WHERE fetch_run_id = ?
              AND fetch_status = 'success'
              AND NOT raw_published
              AND {hash_column} IS NOT NULL
            """,
            [fetch_run_id],
        ).fetchall()
    }
    extra_rows = [
        (str(row[0]), str(row[1]))
        for row in conn.execute(
            f"""
            SELECT market_id, "clobTokenId"
            FROM {audit}
            WHERE fetch_run_id = ?
              AND fetch_status = 'success'
              AND NOT raw_published
              AND "clobTokenId" IN (
                  SELECT CAST(token AS VARCHAR)
                  FROM (SELECT UNNEST(?::VARCHAR[]) AS token)
              )
            """,
            [fetch_run_id, list(reuse_token_ids)],
        ).fetchall()
    ]
    primary_token_ids = _resolve_primary_token_ids(
        conn,
        parquet_paths,
        extra_token_market_rows=extra_rows,
        scope_name=scope_name,
    )
    logger.info(
        "Minute-odds publishing parquet snapshot "
        "(relation=%s tokens=%s rows=%s reused=%s primary_tokens=%s fetch_run_id=%s)",
        relation,
        distinct_tokens,
        expected_rows,
        len(reuse_token_ids),
        len(primary_token_ids),
        fetch_run_id,
    )
    publish_started = time.perf_counter()
    from oddsfox_pipeline.storage.minute_odds_snapshots import (
        minute_odds_snapshot_root,
        register_snapshot_views,
        retain_snapshots,
        rollback_snapshot_pointer,
    )

    # Promote CURRENT before DuckDB registration, but defer retain_snapshots
    # and roll CURRENT back if warehouse registration/audit flip fails.
    snapshot = build_and_publish_snapshot_from_shards(
        leg=leg,
        fetch_run_id=fetch_run_id,
        shard_paths=parquet_paths,
        primary_token_ids=primary_token_ids,
        conn=None,
        register=False,
        retain=False,
        reuse_token_ids=reuse_token_ids,
        window_hashes=window_hashes,
        scope_name=scope_name,
    )
    if (
        audit_mode == "all"
        and not reuse_token_ids
        and int(snapshot.raw_row_count) > expected_rows
    ):
        raise RuntimeError(
            f"Snapshot row count exceeds parquet for {relation}: "
            f"parquet={expected_rows} snapshot={snapshot.raw_row_count}"
        )
    if int(snapshot.raw_row_count) < 1:
        raise RuntimeError(f"Snapshot for {relation} has no raw rows")

    root = minute_odds_snapshot_root(leg=leg, scope_name=scope_name)
    conn.execute("BEGIN TRANSACTION")
    try:
        register_snapshot_views(conn, snapshot, scope_name=scope_name)
        if audit_mode == "all":
            updated = conn.execute(
                f"UPDATE {audit} SET raw_published = TRUE WHERE fetch_run_id = ?",
                [fetch_run_id],
            ).fetchone()[0]
        else:
            updated = conn.execute(
                f"""
                UPDATE {audit}
                SET raw_published = TRUE
                WHERE fetch_run_id = ?
                  AND fetch_status = 'success'
                """,
                [fetch_run_id],
            ).fetchone()[0]
        if int(updated) != distinct_tokens:
            raise RuntimeError(
                f"Published {updated} audit rows for {distinct_tokens} staged tokens "
                f"in run {fetch_run_id}"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        rollback_snapshot_pointer(
            root,
            previous_snapshot_id=snapshot.previous_snapshot_id,
        )
        raise
    retain_snapshots(root, keep=2)
    logger.info(
        "Minute-odds parquet snapshot committed "
        "(%s tokens, %s rows, primary_ohlc=%s, snapshot_id=%s, elapsed_s=%.3f)",
        distinct_tokens,
        expected_rows,
        snapshot.primary_row_count,
        snapshot.snapshot_id,
        time.perf_counter() - publish_started,
    )
    return distinct_tokens


def baseline_publish_minute_odds_from_table(
    conn: duckdb.DuckDBPyConnection,
    rows: Sequence[dict[str, Any]] | pa.Table,
    *,
    relation: Literal["match_minute_odds_history", "futures_minute_odds_history"],
    fetch_run_id: str,
    audit_mode: Literal["all", "success_only"],
) -> int:
    """Legacy stage/DELETE/windowed-INSERT publish path for benchmark baseline only."""
    stage_table = (
        "stage_match_minute_odds_history_v1"
        if relation == "match_minute_odds_history"
        else "stage_futures_minute_odds_history_v1"
    )
    target = polymarket_raw_tbl(SCOPE_WC2026, relation)
    stage = _load_minute_odds_history_stage_arrow(
        conn,
        rows,
        schema=polymarket_raw_schema(SCOPE_WC2026),
        stage_table=stage_table,
    )
    audit = polymarket_ops_tbl(
        SCOPE_WC2026,
        "match_minute_odds_fetch_audit"
        if relation == "match_minute_odds_history"
        else "futures_minute_odds_fetch_audit",
    )
    conn.execute("BEGIN TRANSACTION")
    try:
        stage_tokens = int(
            conn.execute(
                f"SELECT count(DISTINCT clob_token_id) FROM {stage}"
            ).fetchone()[0]
        )
        if audit_mode == "all":
            audit_inventory = conn.execute(
                f"""
                SELECT
                    count(*),
                    count(*) FILTER (
                        WHERE fetch_status = 'success' AND NOT raw_published
                    )
                FROM {audit}
                WHERE fetch_run_id = ?
                """,
                [fetch_run_id],
            ).fetchone()
            if audit_inventory != (stage_tokens, stage_tokens):
                raise RuntimeError(
                    f"Fetch audit inventory does not match {stage_tokens} staged tokens "
                    f"for run {fetch_run_id}: {audit_inventory}"
                )
        else:
            success_unpublished = int(
                conn.execute(
                    f"""
                    SELECT count(*) FILTER (
                        WHERE fetch_status = 'success' AND NOT raw_published
                    )
                    FROM {audit}
                    WHERE fetch_run_id = ?
                    """,
                    [fetch_run_id],
                ).fetchone()[0]
            )
            if success_unpublished != stage_tokens:
                raise RuntimeError(
                    f"Fetch audit inventory does not match {stage_tokens} staged tokens "
                    f"for run {fetch_run_id}: success_unpublished={success_unpublished}"
                )
        conn.execute(f"DELETE FROM {target}")
        conn.execute(
            f"""
            INSERT INTO {target}
            (market_id, clobTokenId, timestamp, price, fidelity_minutes,
             window_start_at, window_end_at, ingested_at)
            SELECT market_id, clob_token_id, timestamp, price, fidelity_minutes,
                   window_start_at, window_end_at, ingested_at
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY clob_token_id, timestamp
                    ORDER BY ingested_at DESC, row_order DESC
                ) AS rn
                FROM {stage}
            )
            WHERE rn = 1
            """
        )
        if audit_mode == "all":
            updated = conn.execute(
                f"UPDATE {audit} SET raw_published = TRUE WHERE fetch_run_id = ?",
                [fetch_run_id],
            ).fetchone()[0]
        else:
            updated = conn.execute(
                f"""
                UPDATE {audit}
                SET raw_published = TRUE
                WHERE fetch_run_id = ?
                  AND fetch_status = 'success'
                """,
                [fetch_run_id],
            ).fetchone()[0]
        if int(updated) != stage_tokens:
            raise RuntimeError(
                f"Published {updated} audit rows for {stage_tokens} staged tokens "
                f"in run {fetch_run_id}"
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return stage_tokens


def load_match_minute_odds_history_stage(
    rows: Sequence[dict[str, Any]] | pa.Table | Sequence[str | Path],
    conn: duckdb.DuckDBPyConnection,
    *,
    fetch_run_id: str,
    reuse_token_ids: set[str] | None = None,
    scope_name: str = SCOPE_WC2026,
    audit_mode: Literal["all", "success_only"] = "all",
) -> None:
    """Publish a bounded match-minute snapshot for one Polymarket scope."""
    if not rows and reuse_token_ids:
        _publish_minute_odds_from_parquet(
            conn,
            [],
            relation="match_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode=audit_mode,
            reuse_token_ids=reuse_token_ids,
            scope_name=scope_name,
        )
        return
    paths, cleanup_dir = _minute_publish_input_to_parquet_paths(
        rows, fetch_run_id=fetch_run_id
    )
    try:
        _publish_minute_odds_from_parquet(
            conn,
            paths,
            relation="match_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode=audit_mode,
            reuse_token_ids=reuse_token_ids,
            scope_name=scope_name,
        )
    finally:
        if cleanup_dir is not None:
            shutil.rmtree(cleanup_dir, ignore_errors=True)


def load_match_minute_fetch_audit(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    """Append one immutable operational audit row per run and token."""
    if not rows:
        return
    target = polymarket_ops_tbl(scope_name, "match_minute_odds_fetch_audit")
    columns = (
        "fetch_run_id",
        "market_id",
        "clobTokenId",
        "fetch_status",
        "raw_published",
        "fidelity_minutes",
        "exact_window_start_at",
        "exact_window_end_at",
        "request_start_epoch",
        "request_end_epoch",
        "source_row_count",
        "in_game_row_count",
        "in_game_history_sha256",
        "source_endpoint",
        "fetch_started_at",
        "fetch_finished_at",
        "error_type",
        "error_message",
    )
    placeholders = ", ".join(["?"] * len(columns))
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.executemany(
            f"INSERT INTO {target} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row.get(column) for column in columns) for row in rows],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_futures_minute_fetch_audit(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Append one immutable operational audit row per futures-minute run and token."""
    if not rows:
        return
    target = polymarket_ops_tbl(SCOPE_WC2026, "futures_minute_odds_fetch_audit")
    columns = (
        "fetch_run_id",
        "market_id",
        "clobTokenId",
        "fetch_status",
        "raw_published",
        "fidelity_minutes",
        "exact_window_start_at",
        "exact_window_end_at",
        "request_start_epoch",
        "request_end_epoch",
        "source_row_count",
        "window_row_count",
        "window_history_sha256",
        "source_endpoint",
        "fetch_started_at",
        "fetch_finished_at",
        "error_type",
        "error_message",
    )
    placeholders = ", ".join(["?"] * len(columns))
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.executemany(
            f"INSERT INTO {target} ({', '.join(columns)}) VALUES ({placeholders})",
            [tuple(row.get(column) for column in columns) for row in rows],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def load_futures_minute_odds_history_stage(
    rows: Sequence[dict[str, Any]] | pa.Table | Sequence[str | Path],
    conn: duckdb.DuckDBPyConnection,
    *,
    fetch_run_id: str,
    reuse_token_ids: set[str] | None = None,
) -> None:
    """Atomically replace the complete bounded WC2026 futures-minute snapshot.

    Empty in-window audit rows are allowed in the same fetch run; only
    unpublished ``success`` tokens must match the staged token inventory, and
    only those rows flip ``raw_published``.
    """
    row_count = len(rows)
    logger.info(
        "Futures-minute publish loading input (%s item(s), fetch_run_id=%s reused=%s)",
        row_count,
        fetch_run_id,
        len(reuse_token_ids or ()),
    )
    if not rows and reuse_token_ids:
        _publish_minute_odds_from_parquet(
            conn,
            [],
            relation="futures_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode="success_only",
            reuse_token_ids=reuse_token_ids,
        )
        return
    paths, cleanup_dir = _minute_publish_input_to_parquet_paths(
        rows, fetch_run_id=fetch_run_id
    )
    try:
        _publish_minute_odds_from_parquet(
            conn,
            paths,
            relation="futures_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode="success_only",
            reuse_token_ids=reuse_token_ids,
        )
    finally:
        if cleanup_dir is not None:
            shutil.rmtree(cleanup_dir, ignore_errors=True)


def merge_match_order_book_snapshots(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Land a bounded dlt batch, then merge it into the canonical raw table.

    dlt owns the replaceable staging relation and may add its internal columns
    there. The canonical relation remains an explicit project contract for dbt
    and recovery logic.
    """
    if not rows:
        return
    normalized_rows = []
    for source in rows:
        row = dict(source)
        if not row.get("landscape_role"):
            label = str(row.get("outcome_label") or "")
            if label.casefold() == str(row.get("home_team") or "").casefold():
                row["landscape_role"] = "home"
            elif label.casefold() == str(row.get("away_team") or "").casefold():
                row["landscape_role"] = "away"
            else:
                raise ValueError("snapshot row requires an explicit landscape_role")
        row.setdefault("provider_sequence", 0)
        normalized_rows.append(row)
    raw_schema = polymarket_raw_schema(SCOPE_WC2026)
    target = polymarket_raw_tbl(SCOPE_WC2026, "match_order_book_snapshots")
    stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_match_order_book_snapshots_v1",
        rows=normalized_rows,
        columns=MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS,
    )
    target_columns = ", ".join(MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS)
    conn.execute(
        f"""
        INSERT OR REPLACE INTO {target} ({target_columns})
        SELECT {target_columns}
        FROM {stage}
        """
    )


def append_ingestion_run_event_stage(
    row: dict[str, Any],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str | None = None,
) -> None:
    scope = scope_name or get_active_polymarket_scope()
    ops_schema = polymarket_ops_schema(scope)
    target = polymarket_ops_tbl(scope, "ingestion_run_events")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_ingestion_run_events_v1",
        rows=[row],
        columns=INGESTION_RUN_EVENT_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (run_id, task_name, recorded_at, metrics_json)
        SELECT run_id, task_name, recorded_at, metrics_json
        FROM {stage}
        """
    )


def load_market_scope_registry_stage(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
    *,
    scope_name: str = SCOPE_WC2026,
) -> None:
    ops_schema = polymarket_ops_schema(scope_name)
    target = polymarket_ops_tbl(scope_name, "market_scope_registry")
    stage = load_stage_rows(
        schema=ops_schema,
        stage_table="stage_market_scope_registry_v1",
        rows=_with_row_order(rows),
        columns=MARKET_SCOPE_REGISTRY_COLUMNS,
    )
    conn.execute(
        f"""
        INSERT INTO {target}
        (
            scope_name,
            market_id,
            event_slug,
            event_id,
            source,
            refreshed_at,
            event_volume_usd_lifetime_reported,
            is_event_volume_eligible,
            first_eligible_at
        )
        SELECT
            scope_name,
            market_id,
            event_slug,
            event_id,
            source,
            refreshed_at,
            event_volume_usd_lifetime_reported,
            is_event_volume_eligible,
            first_eligible_at
        FROM (
            SELECT
                scope_name,
                market_id,
                event_slug,
                event_id,
                source,
                refreshed_at,
                event_volume_usd_lifetime_reported,
                is_event_volume_eligible,
                first_eligible_at,
                row_number() OVER (
                    PARTITION BY scope_name, market_id
                    ORDER BY refreshed_at DESC, row_order DESC
                ) AS rn
            FROM {stage}
        )
        WHERE rn = 1
        ON CONFLICT(scope_name, market_id) DO UPDATE SET
          event_slug=COALESCE(
              excluded.event_slug,
              {target}.event_slug
          ),
          event_id=COALESCE(
              excluded.event_id,
              {target}.event_id
          ),
          source=excluded.source,
          refreshed_at=excluded.refreshed_at,
          event_volume_usd_lifetime_reported=COALESCE(
              excluded.event_volume_usd_lifetime_reported,
              {target}.event_volume_usd_lifetime_reported
          ),
          is_event_volume_eligible=(
              coalesce({target}.is_event_volume_eligible, false)
              OR coalesce(excluded.is_event_volume_eligible, false)
          ),
          first_eligible_at=COALESCE(
              {target}.first_eligible_at,
              excluded.first_eligible_at
          )
        """
    )


__all__ = [
    "DLT_STRICT_SCHEMA_CONTRACT",
    "MARKET_TOKEN_COLUMNS",
    "ODDS_HISTORY_COLUMNS",
    "INGESTION_RUN_EVENT_COLUMNS",
    "MARKET_SCOPE_REGISTRY_COLUMNS",
    "EVENT_SNAPSHOT_COLUMNS",
    "EVENT_TAG_SNAPSHOT_COLUMNS",
    "EVENT_MARKET_SNAPSHOT_COLUMNS",
    "EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS",
    "MATCH_MINUTE_ODDS_HISTORY_COLUMNS",
    "FUTURES_MINUTE_ODDS_HISTORY_COLUMNS",
    "MATCH_ORDER_BOOK_SNAPSHOT_COLUMNS",
    "_DLT_PIPELINE_BY_PATH",
    "_dlt_pipeline_name",
    "append_ingestion_run_event_stage",
    "get_cached_dlt_pipeline",
    "get_kalshi_dlt_pipeline",
    "get_polymarket_dlt_pipeline",
    "load_market_tokens_stage",
    "load_odds_history_stage",
    "load_stage_rows",
    "load_market_scope_registry_stage",
    "load_match_minute_fetch_audit",
    "load_match_minute_odds_history_stage",
    "load_futures_minute_fetch_audit",
    "load_futures_minute_odds_history_stage",
    "baseline_publish_minute_odds_from_table",
    "merge_match_order_book_snapshots",
    "merge_odds_history_stage",
    "prepare_odds_history_stage",
    "reset_dlt_batch_pipelines",
]
