"""dlt batch landing helpers for DuckDB canonical table finalizers."""

from __future__ import annotations

import json
import logging
import os
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
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    minute_odds_history_create_ddl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
    EVENT_MARKET_SNAPSHOT_COLUMNS,
    EVENT_SNAPSHOT_COLUMNS,
    EVENT_TAG_SNAPSHOT_COLUMNS,
    INGESTION_RUN_EVENT_COLUMNS,
    MARKET_SCOPE_REGISTRY_COLUMNS,
    MARKET_TOKEN_COLUMNS,
    MATCH_MINUTE_ODDS_HISTORY_COLUMNS,
    FUTURES_MINUTE_ODDS_HISTORY_COLUMNS,
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
    """Apply publish-only DuckDB settings for bulk candidate creation."""
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


def _validate_minute_candidate_constraints(
    conn: duckdb.DuckDBPyConnection,
    *,
    candidate: str,
) -> None:
    table_name = candidate.rsplit(".", 1)[-1].strip('"')
    types = {
        str(row[0]).upper()
        for row in conn.execute(
            """
            SELECT constraint_type
            FROM duckdb_constraints()
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
    }
    if "PRIMARY KEY" not in types:
        raise RuntimeError(f"Candidate {candidate} is missing PRIMARY KEY")
    if "CHECK" not in types:
        raise RuntimeError(f"Candidate {candidate} is missing fidelity CHECK")


def _publish_minute_odds_from_parquet(
    conn: duckdb.DuckDBPyConnection,
    parquet_paths: Sequence[Path],
    *,
    relation: Literal["match_minute_odds_history", "futures_minute_odds_history"],
    fetch_run_id: str,
    audit_mode: Literal["all", "success_only"],
) -> int:
    """Bulk-load a candidate table from Parquet and atomically swap it in."""
    if not parquet_paths:
        raise ValueError("rows must not be empty")
    _configure_minute_publish_connection(conn)
    schema = polymarket_raw_schema(SCOPE_WC2026)
    target = polymarket_raw_tbl(SCOPE_WC2026, relation)
    target_name = relation
    candidate_name = f"{relation}_candidate"
    previous_name = f"{relation}_previous"
    candidate = polymarket_q(schema, candidate_name)
    previous = polymarket_q(schema, previous_name)
    audit = polymarket_ops_tbl(
        SCOPE_WC2026,
        "match_minute_odds_fetch_audit"
        if relation == "match_minute_odds_history"
        else "futures_minute_odds_fetch_audit",
    )

    expected_rows = 0
    parquet_bytes = 0
    for path in parquet_paths:
        expected_rows += int(pq.ParquetFile(path).metadata.num_rows)
        parquet_bytes += path.stat().st_size
    free_bytes = shutil.disk_usage(Path(parquet_paths[0]).parent).free
    # Candidate table + indexes roughly 2-4x parquet for this schema.
    if free_bytes < max(parquet_bytes * 3, 1):
        raise OSError(
            "Insufficient local free space for minute-odds candidate load: "
            f"parquet_bytes={parquet_bytes} free={free_bytes}"
        )

    logger.info(
        "Minute-odds candidate loading %s rows from %s parquet shard(s) "
        "(relation=%s fetch_run_id=%s parquet_bytes=%s free_bytes=%s)",
        expected_rows,
        len(parquet_paths),
        relation,
        fetch_run_id,
        parquet_bytes,
        free_bytes,
    )
    conn.execute(f"DROP TABLE IF EXISTS {candidate}")
    conn.execute(f"DROP TABLE IF EXISTS {previous}")
    load_started = time.perf_counter()
    # Create empty constrained heap table, then bulk-insert Parquet shards.
    # Accept either canonical DuckDB names or the Arrow builder names.
    conn.execute(
        f"""
        CREATE TABLE {candidate} (
            {minute_odds_history_create_ddl(relation, with_primary_key=False)}
        )
        """
    )
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
    conn.execute(
        f"""
        INSERT INTO {candidate}
        (market_id, clobTokenId, timestamp, price, fidelity_minutes,
         window_start_at, window_end_at, ingested_at)
        SELECT
            market_id,
            {token_column},
            timestamp,
            price,
            fidelity_minutes,
            window_start_at,
            window_end_at,
            ingested_at
        FROM read_parquet([{path_literals}], hive_partitioning=false)
        """,
        [str(path) for path in parquet_paths],
    )
    load_seconds = time.perf_counter() - load_started
    loaded_rows = int(conn.execute(f"SELECT count(*) FROM {candidate}").fetchone()[0])
    if loaded_rows != expected_rows:
        raise RuntimeError(
            f"Candidate row count mismatch for {relation}: "
            f"parquet={expected_rows} loaded={loaded_rows}"
        )
    logger.info(
        "Minute-odds candidate loaded in %.3fs; building primary key "
        "(rows=%s relation=%s)",
        load_seconds,
        loaded_rows,
        relation,
    )
    pk_started = time.perf_counter()
    conn.execute(
        f'ALTER TABLE {candidate} ADD PRIMARY KEY ("clobTokenId", timestamp)'
    )
    pk_seconds = time.perf_counter() - pk_started
    logger.info(
        "Minute-odds candidate primary key built in %.3fs (rows=%s relation=%s)",
        pk_seconds,
        loaded_rows,
        relation,
    )
    _validate_minute_candidate_constraints(conn, candidate=candidate)

    # Token inventory: prefer shard manifest token_ids (written beside Parquet)
    # so we never DISTINCT-scan hundreds of millions of candidate rows. A bare
    # token_count integer is not enough — understated counts plus audit ⊆
    # candidate would admit unaudited extras. Fall back to DISTINCT only for
    # ad-hoc Arrow/dict publishes without a manifest (unit tests).
    manifest_token_ids: set[str] | None = None
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
            if int(payload["row_count"]) != loaded_rows:
                raise RuntimeError(
                    f"Manifest row_count mismatch for {relation}: "
                    f"manifest={payload['row_count']} loaded={loaded_rows}"
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

    if manifest_token_ids is not None:
        if manifest_token_ids != audit_token_ids:
            raise RuntimeError(
                f"Manifest token_ids do not match fetch audit for run {fetch_run_id}: "
                f"manifest_only={sorted(manifest_token_ids - audit_token_ids)[:5]} "
                f"audit_only={sorted(audit_token_ids - manifest_token_ids)[:5]}"
            )
        staged_tokens = len(manifest_token_ids)
        # Fail-closed against understated manifests that omit unaudited extras
        # present in Parquet. Full DISTINCT on ~377M production rows is the cost
        # we avoided with the manifest; keep the scan for ordinary/test sizes.
        if loaded_rows <= 5_000_000:
            measured_tokens = int(
                conn.execute(
                    f'SELECT count(DISTINCT "clobTokenId") FROM {candidate}'
                ).fetchone()[0]
            )
            if measured_tokens != staged_tokens:
                raise RuntimeError(
                    f"Candidate token inventory exceeds manifest for run "
                    f"{fetch_run_id}: manifest={staged_tokens} "
                    f"candidate={measured_tokens}"
                )
    else:
        staged_tokens = int(
            conn.execute(
                f'SELECT count(DISTINCT "clobTokenId") FROM {candidate}'
            ).fetchone()[0]
        )
    if staged_tokens != distinct_tokens:
        raise RuntimeError(
            f"Fetch audit inventory does not match {staged_tokens} staged tokens "
            f"for run {fetch_run_id}: success_unpublished={distinct_tokens}"
        )
    missing_tokens = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {audit} AS a
            WHERE a.fetch_run_id = ?
              AND a.fetch_status = 'success'
              AND NOT a.raw_published
              AND NOT EXISTS (
                  SELECT 1
                  FROM {candidate} AS c
                  WHERE c."clobTokenId" = a."clobTokenId"
              )
            """,
            [fetch_run_id],
        ).fetchone()[0]
    )
    if missing_tokens:
        raise RuntimeError(
            f"Candidate missing {missing_tokens} audited success token(s) "
            f"for run {fetch_run_id}"
        )

    logger.info(
        "Minute-odds swapping candidate into %s (tokens=%s fetch_run_id=%s "
        "load_s=%.3f pk_s=%.3f)",
        target,
        distinct_tokens,
        fetch_run_id,
        load_seconds,
        pk_seconds,
    )
    conn.execute("BEGIN TRANSACTION")
    try:
        conn.execute(f"DROP TABLE IF EXISTS {previous}")
        conn.execute(f"ALTER TABLE {target} RENAME TO {previous_name}")
        conn.execute(f"ALTER TABLE {candidate} RENAME TO {target_name}")
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
        conn.execute(f"DROP TABLE {previous}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    logger.info(
        "Minute-odds raw snapshot replace committed "
        "(%s tokens, %s rows, fetch_run_id=%s)",
        distinct_tokens,
        loaded_rows,
        fetch_run_id,
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
) -> None:
    """Atomically replace the complete bounded WC2026 minute snapshot."""
    paths, cleanup_dir = _minute_publish_input_to_parquet_paths(
        rows, fetch_run_id=fetch_run_id
    )
    try:
        _publish_minute_odds_from_parquet(
            conn,
            paths,
            relation="match_minute_odds_history",
            fetch_run_id=fetch_run_id,
            audit_mode="all",
        )
    finally:
        if cleanup_dir is not None:
            shutil.rmtree(cleanup_dir, ignore_errors=True)


def load_match_minute_fetch_audit(
    rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Append one immutable operational audit row per run and token."""
    if not rows:
        return
    target = polymarket_ops_tbl(SCOPE_WC2026, "match_minute_odds_fetch_audit")
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
) -> None:
    """Atomically replace the complete bounded WC2026 futures-minute snapshot.

    Empty in-window audit rows are allowed in the same fetch run; only
    unpublished ``success`` tokens must match the staged token inventory, and
    only those rows flip ``raw_published``.
    """
    row_count = len(rows)
    logger.info(
        "Futures-minute publish loading input (%s item(s), fetch_run_id=%s)",
        row_count,
        fetch_run_id,
    )
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
