"""Load operator-reviewed WC2026 event membership into local warehouse state."""

from __future__ import annotations

import csv
import hashlib
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from oddsfox_pipeline.storage.duckdb.connection import _use_conn
from oddsfox_pipeline.storage.duckdb.schemas.constants import polymarket_raw_tbl
from oddsfox_pipeline.storage.duckdb.schemas.polymarket import (
    bootstrap_polymarket_tables,
)

REVIEWED_MEMBERSHIP_COLUMNS = (
    "event_id",
    "membership_status",
    "membership_class",
    "tournament_part",
    "membership_basis",
    "reason",
    "reviewed_by",
    "reviewed_at_utc",
)
_MEMBERSHIP_STATUSES = {"included", "excluded"}
_MEMBERSHIP_CLASSES = {
    "sporting",
    "qualification",
    "administrative",
    "culture_mentions",
    "pre_tournament_participation",
    "other_adjacent",
}
_TOURNAMENT_PARTS = {
    "pre_tournament",
    "tournament_wide",
    "group_stage",
    "round_of_32",
    "round_of_16",
    "quarterfinal",
    "semifinal",
    "third_place",
    "final",
    "awards",
}
_INCLUDED_TOURNAMENT_PARTS = _TOURNAMENT_PARTS - {"pre_tournament"}
_PLACEHOLDER_REVIEWERS = {
    "oddsfox_maintainers",
    "unknown",
    "tbd",
    "n/a",
    "none",
}


def _required(row: dict[str, str | None], column: str, row_number: int) -> str:
    value = str(row.get(column) or "").strip()
    if not value:
        raise ValueError(f"Reviewed membership row {row_number} has blank {column}")
    return value


def load_reviewed_membership_csv(
    path: Path,
) -> tuple[list[tuple[object, ...]], str]:
    """Parse a nonempty reviewed CSV and return validated rows plus file hash."""
    path = path.expanduser().resolve()
    if not path.is_file():
        raise ValueError("Reviewed membership CSV does not exist")
    payload = path.read_bytes()
    source_sha256 = hashlib.sha256(payload).hexdigest()
    try:
        text = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("Reviewed membership CSV must be UTF-8") from exc
    reader = csv.DictReader(text.splitlines())
    if tuple(reader.fieldnames or ()) != REVIEWED_MEMBERSHIP_COLUMNS:
        raise ValueError("Reviewed membership CSV header does not match contract")

    rows: list[tuple[object, ...]] = []
    seen_event_ids: set[str] = set()
    for row_number, row in enumerate(reader, start=2):
        event_id = _required(row, "event_id", row_number)
        if not event_id.isascii() or not event_id.isdecimal() or int(event_id) <= 0:
            raise ValueError(
                f"Reviewed membership row {row_number} has invalid event_id"
            )
        if event_id in seen_event_ids:
            raise ValueError(
                f"Reviewed membership CSV has duplicate event_id {event_id}"
            )
        seen_event_ids.add(event_id)
        membership_status = _required(row, "membership_status", row_number)
        membership_class = _required(row, "membership_class", row_number)
        tournament_part = _required(row, "tournament_part", row_number)
        membership_basis = _required(row, "membership_basis", row_number)
        reason = _required(row, "reason", row_number)
        reviewed_by = _required(row, "reviewed_by", row_number)
        reviewed_at_text = _required(row, "reviewed_at_utc", row_number)
        if membership_status not in _MEMBERSHIP_STATUSES:
            raise ValueError(
                f"Reviewed membership row {row_number} has invalid membership_status"
            )
        if membership_class not in _MEMBERSHIP_CLASSES:
            raise ValueError(
                f"Reviewed membership row {row_number} has invalid membership_class"
            )
        if tournament_part not in _TOURNAMENT_PARTS:
            raise ValueError(
                f"Reviewed membership row {row_number} has invalid tournament_part"
            )
        if membership_status == "included" and (
            membership_class != "sporting"
            or tournament_part not in _INCLUDED_TOURNAMENT_PARTS
        ):
            raise ValueError(
                f"Reviewed membership row {row_number} includes a non-final-tournament "
                "scope"
            )
        if reviewed_by.casefold() in _PLACEHOLDER_REVIEWERS:
            raise ValueError(
                f"Reviewed membership row {row_number} has placeholder reviewed_by"
            )
        try:
            reviewed_at = datetime.fromisoformat(
                reviewed_at_text.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                f"Reviewed membership row {row_number} has invalid reviewed_at_utc"
            ) from exc
        if reviewed_at.tzinfo is None:
            raise ValueError(
                f"Reviewed membership row {row_number} reviewed_at_utc lacks timezone"
            )
        rows.append(
            (
                event_id,
                membership_status,
                membership_class,
                tournament_part,
                membership_basis,
                reason,
                reviewed_by,
                reviewed_at.astimezone(timezone.utc).replace(tzinfo=None),
                source_sha256,
            )
        )
    if not rows:
        raise ValueError("Reviewed membership CSV must contain at least one decision")
    return rows, source_sha256


def replace_reviewed_membership(
    path: Path,
    conn: duckdb.DuckDBPyConnection | None = None,
) -> dict[str, object]:
    """Atomically replace the operator-local reviewed membership relation."""
    rows, source_sha256 = load_reviewed_membership_csv(path)
    relation = polymarket_raw_tbl("wc2026", "reviewed_event_membership")
    with _use_conn(conn) as active:
        bootstrap_polymarket_tables(active, scope_name="wc2026")
        active.execute("BEGIN TRANSACTION")
        try:
            active.execute(f"DELETE FROM {relation}")
            active.executemany(
                f"""
                INSERT INTO {relation} (
                    event_id, membership_status, membership_class, tournament_part,
                    membership_basis, reason, reviewed_by, reviewed_at_utc,
                    source_sha256, loaded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                """,
                rows,
            )
            active.execute("COMMIT")
        except Exception:
            active.execute("ROLLBACK")
            raise
    return {
        "rows": len(rows),
        "source_sha256": source_sha256,
        "reviewer_count": len({str(row[6]) for row in rows}),
    }


__all__ = [
    "REVIEWED_MEMBERSHIP_COLUMNS",
    "load_reviewed_membership_csv",
    "replace_reviewed_membership",
]
