from __future__ import annotations

from datetime import datetime, timezone

import duckdb
import pytest

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.ingestion.international_results import historical
from oddsfox_pipeline.ingestion.international_results.historical import (
    GOALSCORERS_URL,
    RESULTS_URL,
    SHOOTOUTS_URL,
    HistoricalResultsError,
    fetch_csv,
    parse_historical_csvs,
    sync_historical_international_results,
)
from oddsfox_pipeline.ingestion.international_results.match_results import (
    build_match_results_url,
    fetch_match_results_csv,
    parse_wc2026_match_results_csv,
    resolve_latest_results_revision,
    sync_wc2026_match_results,
)
from oddsfox_pipeline.storage.duckdb.international_results import (
    replace_historical_international_results,
    replace_wc2026_match_results,
)

CSV_HEADER = (
    "date,home_team,away_team,home_score,away_score,tournament,city,country,neutral\n"
)
SOURCE_REVISION = "a" * 40


def test_parse_wc2026_match_results_filters_scores_and_stable_ids() -> None:
    loaded_at = datetime(2026, 7, 4, tzinfo=timezone.utc)
    text = CSV_HEADER + "\n".join(
        [
            "2026-06-10,England,Costa Rica,3,0,Friendly,Orlando,United States,TRUE",
            "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE",
            "2026-07-04,Canada,Morocco,NA,NA,FIFA World Cup,Houston,United States,TRUE",
        ]
    )

    rows = parse_wc2026_match_results_csv(
        text, source_revision=SOURCE_REVISION, loaded_at=loaded_at
    )

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
    updated_rows = parse_wc2026_match_results_csv(
        updated, source_revision=SOURCE_REVISION, loaded_at=loaded_at
    )
    assert rows[1]["match_id"] == updated_rows[1]["match_id"]
    assert rows[1]["source_row_hash"] != updated_rows[1]["source_row_hash"]
    assert rows[0]["source_revision"] == SOURCE_REVISION
    assert len(str(rows[0]["source_payload_sha256"])) == 64


def test_parse_wc2026_match_results_rejects_schema_changes() -> None:
    with pytest.raises(ValueError, match="CSV schema changed"):
        parse_wc2026_match_results_csv(
            "date,home_team\n2026-06-11,Mexico\n",
            source_revision=SOURCE_REVISION,
        )


def test_resolve_latest_results_revision_validates_github_payload(monkeypatch) -> None:
    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self):
            return [{"sha": SOURCE_REVISION.upper()}]

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.international_results.match_results.requests.get",
        lambda _url, *, timeout: Response(),
    )

    assert resolve_latest_results_revision() == SOURCE_REVISION
    assert SOURCE_REVISION in build_match_results_url(SOURCE_REVISION)


@pytest.mark.parametrize(
    "payload",
    [[], [1], [{}], [{"sha": "bad"}], {"sha": SOURCE_REVISION}],
)
def test_resolve_latest_results_revision_rejects_malformed_payload(
    monkeypatch, payload
) -> None:
    class Response:
        def raise_for_status(self) -> None:
            pass

        def json(self):
            return payload

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.international_results.match_results.requests.get",
        lambda _url, *, timeout: Response(),
    )

    with pytest.raises(ValueError):
        resolve_latest_results_revision()


def test_fetch_match_results_csv_uses_validated_url(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class Response:
        content = CSV_HEADER.encode("utf-8")

        def raise_for_status(self) -> None:
            calls.append(("raise", 0))

    def fake_get(url: str, *, timeout: object) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.international_results.match_results.requests.get",
        fake_get,
    )

    assert fetch_match_results_csv(
        "https://example.com/results.csv"
    ) == CSV_HEADER.encode("utf-8")
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

    summary = sync_wc2026_match_results(
        resolve_revision=lambda: SOURCE_REVISION,
        fetch_csv=lambda _url: first.encode("utf-8"),
    )
    assert summary["rows"] == 2
    assert summary["completed_rows"] == 1
    assert summary["scheduled_rows"] == 1

    summary = sync_wc2026_match_results(
        resolve_revision=lambda: SOURCE_REVISION,
        fetch_csv=lambda _url: second,
    )
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


def test_sync_wc2026_match_results_revision_failure_preserves_raw_rows(
    monkeypatch, tmp_path
) -> None:
    db_path = tmp_path / "results-fail-closed.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()
    csv_text = (
        CSV_HEADER
        + "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE\n"
    )
    sync_wc2026_match_results(
        resolve_revision=lambda: SOURCE_REVISION,
        fetch_csv=lambda _url: csv_text,
    )

    with pytest.raises(RuntimeError, match="revision unavailable"):
        sync_wc2026_match_results(
            resolve_revision=lambda: (_ for _ in ()).throw(
                RuntimeError("revision unavailable")
            ),
            fetch_csv=lambda _url: pytest.fail("unpinned fetch must not run"),
        )

    with connection.get_connection() as conn:
        assert conn.execute(
            "select source_revision from international_results_wc2026_raw.match_results"
        ).fetchall() == [(SOURCE_REVISION,)]

    with pytest.raises(RuntimeError, match="download unavailable"):
        sync_wc2026_match_results(
            resolve_revision=lambda: SOURCE_REVISION,
            fetch_csv=lambda _url: (_ for _ in ()).throw(
                RuntimeError("download unavailable")
            ),
        )

    with connection.get_connection() as conn:
        assert conn.execute(
            "select source_revision from international_results_wc2026_raw.match_results"
        ).fetchall() == [(SOURCE_REVISION,)]


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
        + "2026-06-11,Mexico,South Africa,2,0,FIFA World Cup,Mexico City,Mexico,FALSE\n",
        source_revision=SOURCE_REVISION,
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


def test_historical_results_parse_and_persist_referential_snapshot(
    monkeypatch,
    tmp_path,
) -> None:
    loaded_at = datetime(2026, 7, 18, tzinfo=timezone.utc)
    parsed = parse_historical_csvs(
        results_csv=(
            CSV_HEADER
            + "2005-01-01,Old,Match,1,0,Friendly,Paris,France,TRUE\n"
            + "2022-12-18,Argentina,France,NA,NA,FIFA World Cup,Lusail,Qatar,TRUE\n"
        ),
        shootouts_csv=(
            "date,home_team,away_team,winner,first_shooter\n"
            "2005-01-01,Old,Match,Old,Match\n"
            "2022-12-18,Argentina,France,Argentina,France\n"
            "2023-01-01,Missing,Match,Missing,Missing\n"
        ),
        goalscorers_csv=(
            "date,home_team,away_team,team,scorer,minute,own_goal,penalty\n"
            "2005-01-01,Old,Match,Old,Someone,1,FALSE,FALSE\n"
            "2022-12-18,Argentina,France,Argentina,Lionel Messi,23,FALSE,TRUE\n"
            "2023-01-01,Missing,Match,Missing,Nobody,1,FALSE,FALSE\n"
        ),
        loaded_at=loaded_at,
    )
    assert len(parsed["matches"]) == 1
    assert len(parsed["shootouts"]) == 1
    assert len(parsed["goalscorers"]) == 1
    assert parsed["matches"][0]["home_score"] is None
    assert parsed["dropped_shootouts_without_match"] == 1
    assert parsed["dropped_goalscorers_without_match"] == 1

    db_path = tmp_path / "historical.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()
    summary = replace_historical_international_results(
        matches=parsed["matches"],
        shootouts=parsed["shootouts"],
        goalscorers=parsed["goalscorers"],
    )
    assert summary["inserted_matches"] == 1
    with connection.get_connection() as conn:
        assert conn.execute(
            """
            select m.home_team, s.shootout_winner, g.is_penalty_goal
            from international_results_wc2026_raw.historical_matches as m
            join international_results_wc2026_raw.historical_shootouts as s
                using (match_id)
            join international_results_wc2026_raw.historical_goalscorers as g
                using (match_id)
            """
        ).fetchone() == ("Argentina", "Argentina", True)


def test_historical_results_rejects_upstream_header_change() -> None:
    with pytest.raises(HistoricalResultsError, match="schema changed"):
        parse_historical_csvs(
            results_csv="date,home_team\n",
            shootouts_csv="date,home_team,away_team,winner,first_shooter\n",
            goalscorers_csv=(
                "date,home_team,away_team,team,scorer,minute,own_goal,penalty\n"
            ),
        )


def test_historical_results_rejects_duplicate_match_ids() -> None:
    duplicate = "2022-12-18,Argentina,France,3,3,FIFA World Cup,Lusail,Qatar,TRUE\n"
    with pytest.raises(HistoricalResultsError, match="duplicate match ID"):
        parse_historical_csvs(
            results_csv=CSV_HEADER + duplicate + duplicate,
            shootouts_csv="date,home_team,away_team,winner,first_shooter\n",
            goalscorers_csv=(
                "date,home_team,away_team,team,scorer,minute,own_goal,penalty\n"
            ),
        )


def test_fetch_historical_csv_uses_validated_url(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class Response:
        text = CSV_HEADER

        def raise_for_status(self) -> None:
            calls.append(("raise", 0))

    def fake_get(url: str, *, timeout: object) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(historical.requests, "get", fake_get)

    assert fetch_csv("https://example.com/results.csv") == CSV_HEADER
    assert calls == [
        ("https://example.com/results.csv", HTTP_REQUEST_TIMEOUT),
        ("raise", 0),
    ]


def test_sync_historical_results_fetches_and_persists_default_sources(
    monkeypatch,
) -> None:
    payloads = {
        RESULTS_URL: (
            CSV_HEADER
            + "2022-12-18,Argentina,France,3,3,FIFA World Cup,Lusail,Qatar,TRUE\n"
        ),
        SHOOTOUTS_URL: (
            "date,home_team,away_team,winner,first_shooter\n"
            "2022-12-18,Argentina,France,Argentina,France\n"
        ),
        GOALSCORERS_URL: (
            "date,home_team,away_team,team,scorer,minute,own_goal,penalty\n"
            "2022-12-18,Argentina,France,Argentina,Lionel Messi,23,FALSE,TRUE\n"
        ),
    }
    fetched: list[str] = []
    persisted: dict[str, object] = {}

    def fake_fetch(url: str) -> str:
        fetched.append(url)
        return payloads[url]

    def fake_replace(**rows: object) -> dict[str, int]:
        persisted.update(rows)
        return {
            "deleted_matches": 0,
            "deleted_shootouts": 0,
            "deleted_goalscorers": 0,
            "inserted_matches": len(rows["matches"]),
            "inserted_shootouts": len(rows["shootouts"]),
            "inserted_goalscorers": len(rows["goalscorers"]),
        }

    monkeypatch.setattr(
        historical,
        "replace_historical_international_results",
        fake_replace,
    )

    summary = sync_historical_international_results(fetch=fake_fetch)

    assert fetched == [RESULTS_URL, SHOOTOUTS_URL, GOALSCORERS_URL]
    assert len(persisted["matches"]) == 1
    assert summary["inserted_matches"] == 1
    assert summary["dropped_shootouts_without_match"] == 0
    assert summary["dropped_goalscorers_without_match"] == 0


def test_replace_historical_results_rolls_back_on_insert_error(
    monkeypatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "historical-rollback.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()
    parsed = parse_historical_csvs(
        results_csv=(
            CSV_HEADER
            + "2022-12-18,Argentina,France,3,3,FIFA World Cup,Lusail,Qatar,TRUE\n"
        ),
        shootouts_csv=(
            "date,home_team,away_team,winner,first_shooter\n"
            "2022-12-18,Argentina,France,Argentina,France\n"
        ),
        goalscorers_csv=(
            "date,home_team,away_team,team,scorer,minute,own_goal,penalty\n"
            "2022-12-18,Argentina,France,Argentina,Lionel Messi,23,FALSE,TRUE\n"
        ),
    )

    with connection.get_connection() as conn:
        replace_historical_international_results(
            matches=parsed["matches"],
            shootouts=parsed["shootouts"],
            goalscorers=parsed["goalscorers"],
            conn=conn,
        )
        invalid_match = dict(parsed["matches"][0])
        invalid_match["match_id"] = "invalid-match"
        invalid_match["match_date"] = "not-a-date"

        with pytest.raises(duckdb.ConversionException):
            replace_historical_international_results(
                matches=[invalid_match],
                shootouts=[],
                goalscorers=[],
                conn=conn,
            )

        assert conn.execute(
            "select count(*) from international_results_wc2026_raw.historical_matches"
        ).fetchone() == (1,)
        assert conn.execute(
            "select count(*) from international_results_wc2026_raw.historical_shootouts"
        ).fetchone() == (1,)
        assert conn.execute(
            "select count(*) from international_results_wc2026_raw.historical_goalscorers"
        ).fetchone() == (1,)
