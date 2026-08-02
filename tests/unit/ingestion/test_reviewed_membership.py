from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pytest

from oddsfox_pipeline.ingestion.polymarket import reviewed_membership as membership
from oddsfox_pipeline.ingestion.polymarket.reviewed_membership import (
    REVIEWED_MEMBERSHIP_COLUMNS,
    load_reviewed_membership_csv,
    replace_reviewed_membership,
)


def _write_review(path: Path, row: str) -> None:
    path.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS) + "\n" + row + "\n",
        encoding="utf-8",
    )


def test_reviewed_membership_loads_valid_operator_input_atomically(tmp_path) -> None:
    source = tmp_path / "reviewed.csv"
    _write_review(
        source,
        "30615,included,sporting,tournament_wide,reviewed_inclusion,"
        "Reviewed sporting event,codex_scope_review_2026-08-02,"
        "2026-08-02T00:00:00Z",
    )
    with duckdb.connect(":memory:") as conn:
        conn.execute("create schema polymarket_wc2026_raw")
        conn.execute("create schema polymarket_wc2026_ops")
        summary = replace_reviewed_membership(source, conn)
        row = conn.execute(
            """
            select event_id, reviewed_by, source_sha256
            from polymarket_wc2026_raw.reviewed_event_membership
            """
        ).fetchone()

    assert summary["rows"] == 1
    assert summary["reviewer_count"] == 1
    assert row == (
        "30615",
        "codex_scope_review_2026-08-02",
        summary["source_sha256"],
    )


@pytest.mark.parametrize(
    ("row", "message"),
    [
        (
            "not-an-id,included,sporting,tournament_wide,basis,Reason,reviewer,"
            "2026-08-02T00:00:00Z",
            "invalid event_id",
        ),
        (
            "30615,included,sporting,tournament_wide,,Reason,reviewer,"
            "2026-08-02T00:00:00Z",
            "blank membership_basis",
        ),
        (
            "30615,included,sporting,tournament_wide,basis,,reviewer,"
            "2026-08-02T00:00:00Z",
            "blank reason",
        ),
        (
            "30615,included,sporting,tournament_wide,basis,Reason,"
            "oddsfox_maintainers,2026-08-02T00:00:00Z",
            "placeholder reviewed_by",
        ),
        (
            "30615,included,sporting,tournament_wide,basis,Reason,reviewer,not-a-time",
            "invalid reviewed_at_utc",
        ),
        (
            "30615,included,sporting,not_a_stage,basis,Reason,reviewer,"
            "2026-08-02T00:00:00Z",
            "invalid tournament_part",
        ),
        (
            "30615,pending,sporting,tournament_wide,basis,Reason,reviewer,"
            "2026-08-02T00:00:00Z",
            "invalid membership_status",
        ),
        (
            "30615,included,unknown,tournament_wide,basis,Reason,reviewer,"
            "2026-08-02T00:00:00Z",
            "invalid membership_class",
        ),
        (
            "30615,included,sporting,tournament_wide,basis,Reason,reviewer,"
            "2026-08-02T00:00:00",
            "lacks timezone",
        ),
    ],
)
def test_reviewed_membership_rejects_unattested_or_malformed_decisions(
    tmp_path, row: str, message: str
) -> None:
    source = tmp_path / "reviewed.csv"
    _write_review(source, row)

    with pytest.raises(ValueError, match=message):
        load_reviewed_membership_csv(source)


def test_reviewed_membership_rejects_missing_encoding_header_and_duplicates(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="does not exist"):
        load_reviewed_membership_csv(tmp_path / "missing.csv")

    source = tmp_path / "reviewed.csv"
    source.write_bytes(b"\xff")
    with pytest.raises(ValueError, match="must be UTF-8"):
        load_reviewed_membership_csv(source)

    source.write_text("event_id\n30615\n", encoding="utf-8")
    with pytest.raises(ValueError, match="header does not match"):
        load_reviewed_membership_csv(source)

    row = (
        "30615,included,sporting,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z"
    )
    source.write_text(
        ",".join(REVIEWED_MEMBERSHIP_COLUMNS) + f"\n{row}\n{row}\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate event_id"):
        load_reviewed_membership_csv(source)


def test_reviewed_membership_rejects_header_only_shell(tmp_path) -> None:
    source = tmp_path / "reviewed.csv"
    source.write_text(",".join(REVIEWED_MEMBERSHIP_COLUMNS) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="at least one decision"):
        load_reviewed_membership_csv(source)


@pytest.mark.parametrize(
    ("membership_class", "tournament_part"),
    [
        ("qualification", "group_stage"),
        ("administrative", "tournament_wide"),
        ("culture_mentions", "tournament_wide"),
        ("pre_tournament_participation", "pre_tournament"),
        ("other_adjacent", "tournament_wide"),
        ("sporting", "pre_tournament"),
    ],
)
def test_reviewed_membership_rejects_included_nonfinal_scope(
    tmp_path: Path,
    membership_class: str,
    tournament_part: str,
) -> None:
    source = tmp_path / "reviewed.csv"
    _write_review(
        source,
        f"30615,included,{membership_class},{tournament_part},basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z",
    )

    with pytest.raises(ValueError, match="non-final-tournament scope"):
        load_reviewed_membership_csv(source)


def test_reviewed_membership_allows_excluded_adjacent_pre_tournament(
    tmp_path: Path,
) -> None:
    source = tmp_path / "reviewed.csv"
    _write_review(
        source,
        "30615,excluded,pre_tournament_participation,pre_tournament,basis,"
        "Reason,reviewer,2026-08-02T00:00:00Z",
    )

    rows, _ = load_reviewed_membership_csv(source)

    assert rows[0][1:4] == (
        "excluded",
        "pre_tournament_participation",
        "pre_tournament",
    )


def test_reviewed_membership_rolls_back_failed_replace(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    source = tmp_path / "reviewed.csv"
    _write_review(
        source,
        "30615,included,sporting,tournament_wide,basis,Reason,reviewer,"
        "2026-08-02T00:00:00Z",
    )
    conn = MagicMock()
    conn.executemany.side_effect = RuntimeError("write failed")
    monkeypatch.setattr(
        membership, "bootstrap_polymarket_tables", lambda *_a, **_k: None
    )

    with pytest.raises(RuntimeError, match="write failed"):
        replace_reviewed_membership(source, conn)

    conn.execute.assert_any_call("ROLLBACK")
