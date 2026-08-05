"""Event catalog batch landing helpers for DuckDB."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

import duckdb

from oddsfox_pipeline.ingestion.polymarket.polymarket_ids import (
    is_numeric_polymarket_id,
)
from oddsfox_pipeline.naming import SCOPE_WC2026
from oddsfox_pipeline.storage.duckdb.dlt_batch import _with_row_order, load_stage_rows
from oddsfox_pipeline.storage.duckdb.schemas.constants import (
    polymarket_raw_schema,
    polymarket_raw_tbl,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)
from oddsfox_pipeline.storage.duckdb.schemas.polymarket_raw_columns import (
    EVENT_CATALOG_MARKET_COLUMNS,
    EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
    EVENT_MARKET_SNAPSHOT_COLUMNS,
    EVENT_SNAPSHOT_COLUMNS,
    EVENT_TAG_SNAPSHOT_COLUMNS,
)

logger = logging.getLogger(__name__)


def _assert_append_only_snapshot(
    conn: duckdb.DuckDBPyConnection,
    *,
    stage: str,
    target: str,
    columns: tuple[str, ...],
    key_columns: tuple[str, ...],
    order_by: str,
    label: str,
) -> None:
    """Reject a reused snapshot key unless every persisted value is identical."""
    compared_columns = tuple(column for column in columns if column not in key_columns)
    stage_key_match = " AND ".join(
        f'a."{column}" IS NOT DISTINCT FROM b."{column}"' for column in key_columns
    )
    stage_value_diff = " OR ".join(
        f'a."{column}" IS DISTINCT FROM b."{column}"' for column in compared_columns
    )
    staged_divergence = int(
        conn.execute(
            f"""
            SELECT count(*)
            FROM {stage} AS a
            INNER JOIN {stage} AS b
                ON {stage_key_match}
                AND a.row_order < b.row_order
            WHERE {stage_value_diff}
            """
        ).fetchone()[0]
    )
    if staged_divergence:
        raise RuntimeError(f"Divergent append-only {label} rows share one snapshot key")

    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    partition_by = ", ".join(f'"{column}"' for column in key_columns)
    target_key_match = " AND ".join(
        f'target."{column}" IS NOT DISTINCT FROM candidate."{column}"'
        for column in key_columns
    )
    difference_counts = ", ".join(
        "count(*) filter (where "
        f'target."{column}" is distinct from candidate."{column}") '
        f'as "{column}"'
        for column in compared_columns
    )
    result = conn.execute(
        f"""
            WITH candidate AS (
                SELECT {quoted_columns}
                FROM {stage}
                QUALIFY row_number() OVER (
                    PARTITION BY {partition_by} ORDER BY {order_by}
                ) = 1
            )
            SELECT {difference_counts}
            FROM candidate
            INNER JOIN {target} AS target
                ON {target_key_match}
            """
    )
    row = result.fetchone()
    persisted_divergence = {
        column[0]: int(count)
        for column, count in zip(result.description, row, strict=True)
        if count
    }
    if persisted_divergence:
        raise RuntimeError(
            f"Divergent append-only replay for {label} at an existing snapshot key: "
            f"{persisted_divergence}"
        )


def _assert_exact_observation_replay(
    conn: duckdb.DuckDBPyConnection,
    *,
    observed_at: Any,
    events_target: str,
    relations: tuple[
        tuple[
            str,
            str | None,
            str,
            tuple[str, ...],
            tuple[str, ...],
            str,
        ],
        ...,
    ],
) -> None:
    """Make reuse of a complete catalog observation exactly idempotent."""
    persisted_event_count = int(
        conn.execute(
            f"SELECT count(*) FROM {events_target} WHERE observed_at = ?",
            [observed_at],
        ).fetchone()[0]
    )
    if persisted_event_count == 0:
        return

    for label, stage, target, columns, key_columns, order_by in relations:
        quoted_columns = ", ".join(f'"{column}"' for column in columns)
        if stage is None:
            candidate_query = f"SELECT {quoted_columns} FROM {target} WHERE FALSE"
        else:
            partition_by = ", ".join(f'"{column}"' for column in key_columns)
            candidate_query = f"""
                SELECT {quoted_columns}
                FROM {stage}
                QUALIFY row_number() OVER (
                    PARTITION BY {partition_by} ORDER BY {order_by}
                ) = 1
            """
        difference_count = int(
            conn.execute(
                f"""
                WITH candidate AS ({candidate_query}),
                persisted AS (
                    SELECT {quoted_columns}
                    FROM {target}
                    WHERE observed_at = ?
                ),
                differences AS (
                    (SELECT * FROM candidate EXCEPT ALL SELECT * FROM persisted)
                    UNION ALL
                    (SELECT * FROM persisted EXCEPT ALL SELECT * FROM candidate)
                )
                SELECT count(*) FROM differences
                """,
                [observed_at],
            ).fetchone()[0]
        )
        if difference_count:
            raise RuntimeError(
                "Divergent append-only replay for complete "
                f"{label} relation at observed_at; differences={difference_count}"
            )


def merge_event_catalog_batch(
    *,
    event_rows: Sequence[dict[str, Any]],
    tag_rows: Sequence[dict[str, Any]],
    event_market_rows: Sequence[dict[str, Any]],
    market_rows: Sequence[dict[str, Any]],
    conn: duckdb.DuckDBPyConnection,
) -> None:
    """Stage and atomically append one complete WC2026 event catalog observation."""
    if not event_rows:
        raise ValueError("event_rows must not be empty")
    raw_schema = polymarket_raw_schema(SCOPE_WC2026)
    events_target = polymarket_raw_tbl(SCOPE_WC2026, "event_snapshots")
    tags_target = polymarket_raw_tbl(SCOPE_WC2026, "event_tag_snapshots")
    bridge_target = polymarket_raw_tbl(SCOPE_WC2026, "event_market_snapshots")
    market_payloads_target = polymarket_raw_tbl(
        SCOPE_WC2026, "event_market_payload_snapshots"
    )
    observed_at_values = {row.get("observed_at") for row in event_rows}
    if None in observed_at_values or len(observed_at_values) != 1:
        raise ValueError("event_rows must share one non-null observed_at")
    observed_at = next(iter(observed_at_values))
    for label, rows in (
        ("tag_rows", tag_rows),
        ("event_market_rows", event_market_rows),
    ):
        relation_observed_at_values = {row.get("observed_at") for row in rows}
        if relation_observed_at_values and relation_observed_at_values != {observed_at}:
            raise ValueError(f"{label} must share event_rows observed_at")
    for row in market_rows:
        if not str(row.get("id") or "").strip():
            raise ValueError("market_rows must contain non-empty id values")

    skipped_events = [
        str(row.get("event_id") or "")
        for row in event_rows
        if not is_numeric_polymarket_id(str(row.get("event_id") or ""))
    ]
    event_rows = [
        row
        for row in event_rows
        if is_numeric_polymarket_id(str(row.get("event_id") or ""))
    ]
    if not event_rows:
        raise ValueError(
            "event_rows must contain at least one numeric Polymarket event_id"
        )
    allowed_event_ids = {str(row["event_id"]) for row in event_rows}
    tag_rows = [
        row for row in tag_rows if str(row.get("event_id") or "") in allowed_event_ids
    ]
    skipped_bridges = [
        f"{row.get('event_id')}/{row.get('market_id')}"
        for row in event_market_rows
        if not is_numeric_polymarket_id(str(row.get("event_id") or ""))
        or not is_numeric_polymarket_id(str(row.get("market_id") or ""))
    ]
    event_market_rows = [
        row
        for row in event_market_rows
        if is_numeric_polymarket_id(str(row.get("event_id") or ""))
        and is_numeric_polymarket_id(str(row.get("market_id") or ""))
    ]
    skipped_markets = [
        str(row.get("id") or "")
        for row in market_rows
        if not is_numeric_polymarket_id(str(row.get("id") or ""))
    ]
    market_rows = [
        row for row in market_rows if is_numeric_polymarket_id(str(row.get("id") or ""))
    ]
    skipped = [*skipped_events, *skipped_bridges, *skipped_markets]
    if skipped:
        logger.warning(
            "Skipping non-numeric event catalog IDs: %s",
            skipped[:20] if len(skipped) > 20 else skipped,
        )

    market_payload_rows: list[dict[str, Any]] = []
    for row in market_rows:
        market_id = str(row.get("id") or "").strip()
        if not market_id:
            raise ValueError("market_rows must contain non-empty id values")
        market_payload_rows.append(
            {
                "market_id": market_id,
                **{
                    column: row.get(column)
                    for column in EVENT_CATALOG_MARKET_COLUMNS
                    if column not in {"id", "row_order"}
                },
                "observed_at": observed_at,
            }
        )

    # Existing warehouses predate the dedicated payload snapshot table. Keep
    # dlt-owned ``markets`` untouched and migrate only project-owned raw tables.
    bootstrap_polymarket_tables(conn, scope_name=SCOPE_WC2026)

    events_stage = load_stage_rows(
        schema=raw_schema,
        stage_table="stage_event_snapshots_v1",
        rows=_with_row_order(event_rows),
        columns=EVENT_SNAPSHOT_COLUMNS,
    )
    tags_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_tag_snapshots_v1",
            rows=_with_row_order(tag_rows),
            columns=EVENT_TAG_SNAPSHOT_COLUMNS,
        )
        if tag_rows
        else None
    )
    bridge_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_market_snapshots_v1",
            rows=_with_row_order(event_market_rows),
            columns=EVENT_MARKET_SNAPSHOT_COLUMNS,
        )
        if event_market_rows
        else None
    )
    market_payloads_stage = (
        load_stage_rows(
            schema=raw_schema,
            stage_table="stage_event_market_payload_snapshots_v1",
            rows=_with_row_order(market_payload_rows),
            columns=EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS,
        )
        if market_payload_rows
        else None
    )

    market_payload_columns = tuple(
        column
        for column in EVENT_MARKET_PAYLOAD_SNAPSHOT_COLUMNS
        if column != "row_order"
    )
    event_columns = tuple(
        column for column in EVENT_SNAPSHOT_COLUMNS if column != "row_order"
    )
    tag_columns = tuple(
        column for column in EVENT_TAG_SNAPSHOT_COLUMNS if column != "row_order"
    )
    bridge_columns = tuple(
        column for column in EVENT_MARKET_SNAPSHOT_COLUMNS if column != "row_order"
    )
    quoted_market_payload_columns = ", ".join(market_payload_columns)
    quoted_event_columns = ", ".join(event_columns)
    quoted_tag_columns = ", ".join(tag_columns)
    quoted_bridge_columns = ", ".join(bridge_columns)
    conn.execute("BEGIN TRANSACTION")
    try:
        _assert_append_only_snapshot(
            conn,
            stage=events_stage,
            target=events_target,
            columns=event_columns,
            key_columns=("event_id", "observed_at"),
            order_by="row_order DESC",
            label="event snapshots",
        )
        if tags_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=tags_stage,
                target=tags_target,
                columns=tag_columns,
                key_columns=("event_id", "tag_key", "observed_at"),
                order_by="row_order DESC",
                label="event tag snapshots",
            )
        if bridge_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=bridge_stage,
                target=bridge_target,
                columns=bridge_columns,
                key_columns=("event_id", "market_id", "observed_at"),
                order_by="row_order DESC",
                label="event market snapshots",
            )
        if market_payloads_stage is not None:
            _assert_append_only_snapshot(
                conn,
                stage=market_payloads_stage,
                target=market_payloads_target,
                columns=market_payload_columns,
                key_columns=("market_id", "observed_at"),
                order_by="scraped_at DESC, row_order DESC",
                label="event market payload snapshots",
            )
        _assert_exact_observation_replay(
            conn,
            observed_at=observed_at,
            events_target=events_target,
            relations=(
                (
                    "event snapshots",
                    events_stage,
                    events_target,
                    event_columns,
                    ("event_id", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event tag snapshots",
                    tags_stage,
                    tags_target,
                    tag_columns,
                    ("event_id", "tag_key", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event market snapshots",
                    bridge_stage,
                    bridge_target,
                    bridge_columns,
                    ("event_id", "market_id", "observed_at"),
                    "row_order DESC",
                ),
                (
                    "event market payload snapshots",
                    market_payloads_stage,
                    market_payloads_target,
                    market_payload_columns,
                    ("market_id", "observed_at"),
                    "scraped_at DESC, row_order DESC",
                ),
            ),
        )
        conn.execute(
            f"""
            INSERT INTO {events_target} ({quoted_event_columns})
            SELECT {quoted_event_columns}
            FROM {events_stage}
            QUALIFY row_number() OVER (
                PARTITION BY event_id, observed_at ORDER BY row_order DESC
            ) = 1
            ON CONFLICT (event_id, observed_at) DO NOTHING
            """
        )
        if tags_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {tags_target} ({quoted_tag_columns})
                SELECT {quoted_tag_columns}
                FROM {tags_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY event_id, tag_key, observed_at ORDER BY row_order DESC
                ) = 1
                ON CONFLICT (event_id, tag_key, observed_at) DO NOTHING
                """
            )
        if bridge_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {bridge_target} ({quoted_bridge_columns})
                SELECT {quoted_bridge_columns}
                FROM {bridge_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY event_id, market_id, observed_at ORDER BY row_order DESC
                ) = 1
                ON CONFLICT (event_id, market_id, observed_at) DO NOTHING
                """
            )
        if market_payloads_stage is not None:
            conn.execute(
                f"""
                INSERT INTO {market_payloads_target} ({quoted_market_payload_columns})
                SELECT {quoted_market_payload_columns}
                FROM {market_payloads_stage}
                QUALIFY row_number() OVER (
                    PARTITION BY market_id, observed_at
                    ORDER BY scraped_at DESC, row_order DESC
                ) = 1
                ON CONFLICT (market_id, observed_at) DO NOTHING
                """
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


__all__ = ["merge_event_catalog_batch"]
