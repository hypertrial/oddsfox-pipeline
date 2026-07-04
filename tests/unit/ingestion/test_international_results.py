from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.ingestion.international_results.match_results import (
    fetch_match_results_csv,
    parse_wc2026_match_results_csv,
    sync_wc2026_match_results,
)
from oddsfox_pipeline.storage.duckdb.international_results import (
    replace_wc2026_match_results,
)

CSV_HEADER = (
    "date,home_team,away_team,home_score,away_score,tournament,city,country,neutral\n"
)


def test_parse_wc2026_match_results_filters_scores_and_stable_ids() -> None:
    loaded_at = datetime(2026, 7, 4, tzinfo=timezone.utc)
    text = CSV_HEADER + "\n".join(
        [
            "2026-06-10,England,Costa Rica,3,0,Friendly,Orlando,United States,TRUE",
            "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE",
            "2026-07-04,Canada,Morocco,NA,NA,FIFA World Cup,Houston,United States,TRUE",
        ]
    )

    rows = parse_wc2026_match_results_csv(text, loaded_at=loaded_at)

    assert len(rows) == 2
    assert rows[0]["home_team"] == "Mexico"
    assert rows[0]["match_status"] == "completed"
    assert rows[1]["home_score"] is None
    assert rows[1]["match_status"] == "scheduled"
    assert rows[1]["source_row_number"] == 4

    updated = text.replace(
        "2026-07-04,Canada,Morocco,NA,NA",
        "2026-07-04,Canada,Morocco,1,0",
    )
    updated_rows = parse_wc2026_match_results_csv(updated, loaded_at=loaded_at)
    assert rows[1]["match_id"] == updated_rows[1]["match_id"]
    assert rows[1]["source_row_hash"] != updated_rows[1]["source_row_hash"]


def test_parse_wc2026_match_results_rejects_schema_changes() -> None:
    with pytest.raises(ValueError, match="CSV schema changed"):
        parse_wc2026_match_results_csv("date,home_team\n2026-06-11,Mexico\n")


def test_fetch_match_results_csv_uses_validated_url(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class Response:
        text = CSV_HEADER

        def raise_for_status(self) -> None:
            calls.append(("raise", 0))

    def fake_get(url: str, *, timeout: object) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.international_results.match_results.requests.get",
        fake_get,
    )

    assert fetch_match_results_csv("https://example.com/results.csv") == CSV_HEADER
    assert calls == [
        ("https://example.com/results.csv", HTTP_REQUEST_TIMEOUT),
        ("raise", 0),
    ]


def test_sync_wc2026_match_results_replaces_raw_rows(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "results.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()

    first = (
        CSV_HEADER
        + "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE\n"
        + "2026-07-04,Canada,Morocco,NA,NA,FIFA World Cup,Houston,United States,TRUE\n"
    )
    second = (
        CSV_HEADER
        + "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE\n"
    )

    summary = sync_wc2026_match_results(fetch_csv=lambda _url: first)
    assert summary["rows"] == 2
    assert summary["completed_rows"] == 1
    assert summary["scheduled_rows"] == 1

    summary = sync_wc2026_match_results(fetch_csv=lambda _url: second)
    assert summary["rows"] == 1

    with connection.get_connection() as conn:
        assert (
            conn.execute(
                "select count(*) from international_results_wc2026_raw.match_results"
            ).fetchone()[0]
            == 1
        )

        summary = replace_wc2026_match_results([], conn)
        assert summary["inserted_rows"] == 0
        assert (
            conn.execute(
                "select count(*) from international_results_wc2026_raw.match_results"
            ).fetchone()[0]
            == 0
        )


def test_replace_wc2026_match_results_rolls_back_on_insert_error(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "rollback.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()

    row = parse_wc2026_match_results_csv(
        CSV_HEADER
        + "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE\n"
    )[0]

    with connection.get_connection() as conn:
        replace_wc2026_match_results([row], conn)

        with pytest.raises(duckdb.ConstraintException):
            replace_wc2026_match_results([row, row], conn)

        assert (
            conn.execute(
                "select count(*) from international_results_wc2026_raw.match_results"
            ).fetchone()[0]
            == 1
        )
