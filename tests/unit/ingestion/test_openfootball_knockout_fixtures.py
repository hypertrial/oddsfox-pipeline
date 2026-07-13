from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.ingestion.openfootball.knockout_fixtures import (
    fetch_knockout_fixtures,
    parse_knockout_fixtures,
    sync_knockout_fixtures,
)
from oddsfox_pipeline.storage.duckdb.openfootball import replace_knockout_fixtures


def _fixture_text() -> str:
    stage_ids = (
        ("Round of 32", range(73, 89)),
        ("Round of 16", range(89, 97)),
        ("Quarter-final", range(97, 101)),
        ("Semi-final", range(101, 103)),
        ("Match for third place", range(103, 104)),
        ("Final", range(104, 105)),
    )
    lines: list[str] = []
    for stage, ids in stage_ids:
        lines.extend([f"▪ {stage}", "Sun Jun 28"])
        for match_id in ids:
            matchup = f"Team {match_id}A v Team {match_id}B"
            if match_id == 73:
                matchup = "South Africa 0-1 (0-0) Canada"
            lines.append(f"  ({match_id}) 12:00 UTC-4  {matchup} @ Test Venue ## slots")
    return "\n".join(lines)


def test_parse_knockout_fixtures_validates_ids_stages_and_utc() -> None:
    rows = parse_knockout_fixtures(
        _fixture_text(),
        loaded_at=datetime(2026, 7, 13),
    )

    assert len(rows) == 32
    assert rows[0]["fifa_match_id"] == 73
    assert rows[0]["stage_key"] == "round_of_32"
    assert rows[0]["home_team"] == "South Africa"
    assert rows[0]["away_team"] == "Canada"
    assert rows[0]["match_status"] == "completed"
    assert rows[0]["kickoff_at_utc"] == datetime(2026, 6, 28, 16)
    assert rows[-1]["fifa_match_id"] == 104
    assert rows[-1]["stage_key"] == "final"
    assert rows[-1]["match_status"] == "scheduled"


def test_parse_knockout_fixtures_fails_closed_on_missing_or_wrong_stage() -> None:
    missing = _fixture_text().replace(
        "  (104) 12:00 UTC-4  Team 104A v Team 104B @ Test Venue ## slots",
        "",
    )
    with pytest.raises(ValueError, match=r"missing=\[104\]"):
        parse_knockout_fixtures(missing)

    wrong_stage = _fixture_text().replace("▪ Final", "▪ Semi-final")
    with pytest.raises(ValueError, match="FIFA match 104 expected final"):
        parse_knockout_fixtures(wrong_stage)


def test_parse_knockout_fixtures_rejects_bad_matchup_and_duplicate_ids() -> None:
    bad_matchup = _fixture_text().replace(
        "Team 101A v Team 101B", "Team 101A versus Team 101B"
    )
    with pytest.raises(ValueError, match="Unsupported OpenFootball matchup"):
        parse_knockout_fixtures(bad_matchup)

    duplicate = _fixture_text().replace(
        "  (104) 12:00 UTC-4",
        "  (103) 12:00 UTC-4",
    )
    with pytest.raises(ValueError, match="duplicate match IDs"):
        parse_knockout_fixtures(duplicate)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (
            "ignored note\n  (73) 12:00 UTC-4  Team A v Team B @ Test Venue",
            "before its stage or date",
        ),
        (
            "▪ Round of 32\n  (73) 12:00 UTC-4  Team A v Team B @ Test Venue",
            "before its stage or date",
        ),
        (
            _fixture_text().replace(
                "Team 101A v Team 101B @ Test Venue",
                "Team 101A v Team 101B",
            ),
            "fixture 101 has no venue",
        ),
        (
            _fixture_text().replace("Team 101B", "Team 101A"),
            "match 101 has identical teams",
        ),
    ],
)
def test_parse_knockout_fixtures_rejects_malformed_schedule(
    text: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        parse_knockout_fixtures(text)


def test_fetch_knockout_fixtures_uses_validated_url(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class Response:
        text = _fixture_text()

        def raise_for_status(self) -> None:
            calls.append(("raise", 0))

    def fake_get(url: str, *, timeout: object) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(
        "oddsfox_pipeline.ingestion.openfootball.knockout_fixtures.requests.get",
        fake_get,
    )

    assert fetch_knockout_fixtures("https://example.com/cup.txt") == _fixture_text()
    assert calls == [
        ("https://example.com/cup.txt", HTTP_REQUEST_TIMEOUT),
        ("raise", 0),
    ]


def test_sync_knockout_fixtures_replaces_raw_slice(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "fixtures.duckdb"
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    connection.reset_duckdb_connection_state()

    summary = sync_knockout_fixtures(fetch_text=lambda _url: _fixture_text())

    assert summary["rows"] == 32
    assert summary["completed_rows"] == 1
    assert summary["scheduled_rows"] == 31
    with connection.get_connection() as conn:
        row = conn.execute(
            """
            select fifa_match_id, stage_key
            from openfootball_wc2026_raw.knockout_fixtures
            where fifa_match_id = 104
            """
        ).fetchone()
    assert row == (104, "final")


def test_replace_knockout_fixtures_is_atomic_and_accepts_empty_slice(tmp_path) -> None:
    row = parse_knockout_fixtures(_fixture_text())[0]
    with duckdb.connect(str(tmp_path / "atomic.duckdb")) as conn:
        conn.execute("create schema openfootball_wc2026_raw")
        assert replace_knockout_fixtures([row], conn) == {
            "deleted_rows": 0,
            "inserted_rows": 1,
        }

        with pytest.raises(duckdb.ConstraintException):
            replace_knockout_fixtures([{}], conn)
        assert conn.execute(
            "select fifa_match_id from openfootball_wc2026_raw.knockout_fixtures"
        ).fetchall() == [(73,)]

        assert replace_knockout_fixtures([], conn) == {
            "deleted_rows": 1,
            "inserted_rows": 0,
        }
