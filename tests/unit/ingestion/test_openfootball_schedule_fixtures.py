from __future__ import annotations

import hashlib
from datetime import datetime
from unittest.mock import MagicMock

import duckdb
import pytest

from oddsfox_pipeline.config.settings import HTTP_REQUEST_TIMEOUT
from oddsfox_pipeline.ingestion.openfootball import schedule_fixtures as fixtures
from oddsfox_pipeline.storage.duckdb.openfootball import replace_schedule_fixtures
from oddsfox_pipeline.storage.duckdb.schemas.openfootball import (
    seed_test_openfootball_schedule_fixtures,
)


def _fixture_sources() -> tuple[str, str, dict[str, int]]:
    group_lines = ["= World Cup 2026"]
    team_names: dict[str, tuple[str, ...]] = {}
    for group in "ABCDEFGHIJKL":
        teams = tuple(f"{group} Team {index}" for index in range(1, 5))
        team_names[group] = teams
        group_lines.append(f"Group {group} | " + "  ".join(teams))
    pairings = ((0, 1), (2, 3), (0, 2), (1, 3), (0, 3), (1, 2))
    slices: list[tuple[int, int]] = []
    for group in "ABCDEFGHIJKL":
        group_lines.extend(["", f"▪ Group {group}"])
        for ordinal, (home_index, away_index) in enumerate(pairings, start=1):
            group_lines.append(f"Thu June {10 + ordinal}")
            start = len(group_lines)
            home = team_names[group][home_index]
            away = team_names[group][away_index]
            group_lines.append(
                f"  12:00 UTC-4  {home} v {away} @ Venue {group} ## bracket"
            )
            slices.append((start, len(group_lines)))
    mapping = {
        fixtures._line_hash(group_lines, start, end): 72 - index
        for index, (start, end) in enumerate(slices)
    }

    stage_ids = (
        ("Round of 32", range(73, 89)),
        ("Round of 16", range(89, 97)),
        ("Quarter-final", range(97, 101)),
        ("Semi-final", range(101, 103)),
        ("Match for third place", range(103, 104)),
        ("Final", range(104, 105)),
    )
    finals_lines = ["= World Cup 2026 Finals"]
    for stage, ids in stage_ids:
        finals_lines.extend([f"▪ {stage}", "Sun June 28"])
        for match_id in ids:
            finals_lines.append(
                f"  ({match_id}) 12:00 UTC-4  A Team 1 v A Team 2 "
                "@ Knockout Venue ## bracket"
            )
    return "\n".join(group_lines) + "\n", "\n".join(finals_lines) + "\n", mapping


def _parse(monkeypatch: pytest.MonkeyPatch):
    group_text, finals_text, mapping = _fixture_sources()
    monkeypatch.setattr(fixtures, "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH", mapping)
    return fixtures.parse_schedule_fixtures(
        group_text,
        finals_text,
        loaded_at=datetime(2026, 8, 2),
    )


def test_complete_schedule_uses_reviewed_hash_mapping_not_group_ordinal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _parse(monkeypatch)

    assert [row["fifa_match_id"] for row in rows] == list(range(1, 105))
    assert rows[0]["stage_key"] == "group_stage"
    assert rows[0]["group_label"] == "L"
    assert rows[0]["venue"] == "Venue L"
    assert rows[72]["stage_key"] == "round_of_32"
    assert rows[-1]["stage_key"] == "final"
    assert rows[-1]["venue"] == "Knockout Venue"


def test_complete_schedule_fails_closed_on_unreviewed_group_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_text, finals_text, mapping = _fixture_sources()
    mapping.pop(next(iter(mapping)))
    monkeypatch.setattr(fixtures, "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH", mapping)

    with pytest.raises(ValueError, match="absent from the reviewed FIFA"):
        fixtures.parse_schedule_fixtures(group_text, finals_text)


def test_schedule_parser_rejects_malformed_helpers_and_context() -> None:
    with pytest.raises(ValueError, match="group A is malformed"):
        fixtures._parse_group_teams(["Group A | One  Two  Three"])
    with pytest.raises(ValueError, match="exactly groups A-L"):
        fixtures._parse_group_teams([])
    with pytest.raises(ValueError, match="identify two"):
        fixtures._fixture_teams("A Team 1", ("A Team 1", "A Team 2"))
    with pytest.raises(ValueError, match="lacks UTC offset"):
        fixtures._kickoff(datetime(2026, 6, 11), "12:00 A Team 1 v A Team 2")
    with pytest.raises(ValueError, match="lacks venue"):
        fixtures._venue("A Team 1 v A Team 2")

    without_comment = MagicMock()
    without_comment.__contains__.return_value = True
    without_comment.rsplit.return_value = ("fixture", " ")
    line = MagicMock()
    line.split.return_value = (without_comment,)
    without_comment.rstrip.return_value = without_comment
    with pytest.raises(ValueError, match="lacks venue"):
        fixtures._venue(line)


def test_schedule_parser_rejects_incomplete_group_fixture_context() -> None:
    group_text, _, _ = _fixture_sources()
    definitions = group_text.splitlines()[:13]
    definitions.append("12:00 UTC-4 A Team 1 v A Team 2 @ Venue")
    with pytest.raises(ValueError, match="lacks group/date context"):
        fixtures.parse_openfootball_fixtures("\n".join(definitions), "")

    with pytest.raises(ValueError, match="Expected 72"):
        fixtures.parse_openfootball_fixtures("\n".join(definitions[:-1]), "")


def test_schedule_parser_rejects_duplicate_and_incomplete_reviewed_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_text, finals_text, mapping = _fixture_sources()
    original_line_hash = fixtures._line_hash
    monkeypatch.setattr(fixtures, "_line_hash", lambda *_args: "same")
    monkeypatch.setattr(fixtures, "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH", {"same": 1})
    with pytest.raises(ValueError, match="evidence is duplicated"):
        fixtures.parse_openfootball_fixtures(group_text, finals_text)

    monkeypatch.setattr(fixtures, "_line_hash", original_line_hash)
    group_text, finals_text, mapping = _fixture_sources()
    monkeypatch.setattr(
        fixtures,
        "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH",
        {**mapping, "extra": 999},
    )
    with pytest.raises(ValueError, match="mapping is incomplete"):
        fixtures.parse_openfootball_fixtures(group_text, finals_text)


def test_schedule_parser_rejects_invalid_knockout_context_and_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_text, finals_text, mapping = _fixture_sources()
    monkeypatch.setattr(fixtures, "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH", mapping)
    with pytest.raises(ValueError, match="lacks stage/date context"):
        fixtures.parse_openfootball_fixtures(
            group_text,
            "(73) 12:00 UTC-4 A Team 1 v A Team 2 @ Venue",
        )

    with pytest.raises(ValueError, match="match IDs 1..104"):
        fixtures.parse_openfootball_fixtures(
            group_text,
            finals_text.replace("(74)", "(73)", 1),
        )

    with pytest.raises(ValueError, match="expected round_of_32"):
        fixtures.parse_openfootball_fixtures(
            group_text,
            finals_text.replace("▪ Round of 32", "▪ Round of 16", 1),
        )

    monkeypatch.setattr(
        fixtures,
        "_fixture_teams",
        lambda *_args: ("A Team 1", "A Team 1"),
    )
    with pytest.raises(ValueError, match="identical teams"):
        fixtures.parse_openfootball_fixtures(group_text, finals_text)


def test_replace_complete_schedule_is_atomic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _parse(monkeypatch)
    with duckdb.connect(":memory:") as conn:
        summary = replace_schedule_fixtures(rows, conn)
        count, minimum, maximum = conn.execute(
            """
            select count(*), min(fifa_match_id), max(fifa_match_id)
            from openfootball_wc2026_raw.schedule_fixtures
            """
        ).fetchone()

    assert summary["inserted_rows"] == 104
    assert (count, minimum, maximum) == (104, 1, 104)


def test_replace_schedule_fixtures_accepts_empty_and_rolls_back_on_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _parse(monkeypatch)
    with duckdb.connect(":memory:") as conn:
        replace_schedule_fixtures(rows[:1], conn)
        empty = replace_schedule_fixtures([], conn)
        assert empty == {"deleted_rows": 1, "inserted_rows": 0}
        assert (
            conn.execute(
                "select count(*) from openfootball_wc2026_raw.schedule_fixtures"
            ).fetchone()[0]
            == 0
        )

        replace_schedule_fixtures(rows[:1], conn)
        bad = dict(rows[0])
        bad["fifa_match_id"] = "not-an-integer"
        with pytest.raises(Exception):
            replace_schedule_fixtures([bad], conn)
        assert conn.execute(
            """
            select fifa_match_id
            from openfootball_wc2026_raw.schedule_fixtures
            """
        ).fetchone() == (1,)


def test_sync_validates_both_pinned_source_hashes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    group_text, finals_text, mapping = _fixture_sources()
    sources = {"group.txt": group_text, "finals.txt": finals_text}
    monkeypatch.setattr(fixtures, "REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH", mapping)
    monkeypatch.setattr(
        fixtures,
        "OPENFOOTBALL_FILES",
        {
            path: hashlib.sha256(text.encode()).hexdigest()
            for path, text in sources.items()
        },
    )
    monkeypatch.setattr(fixtures, "OPENFOOTBALL_BASE", "https://example.com/")
    monkeypatch.setattr(
        fixtures,
        "replace_schedule_fixtures",
        lambda rows: {"deleted_rows": 0, "inserted_rows": len(rows)},
    )

    # The sync contract uses the two canonical path names to select parse inputs.
    sources["2026--usa/cup.txt"] = sources.pop("group.txt")
    sources["2026--usa/cup_finals.txt"] = sources.pop("finals.txt")
    monkeypatch.setattr(
        fixtures,
        "OPENFOOTBALL_FILES",
        {
            path: hashlib.sha256(text.encode()).hexdigest()
            for path, text in sources.items()
        },
    )
    summary = fixtures.sync_schedule_fixtures(
        fetch_text=lambda url: sources[url.removeprefix("https://example.com/")]
    )

    assert summary["rows"] == 104
    assert summary["inserted_rows"] == 104


def test_sync_rejects_pinned_source_hash_mismatch(monkeypatch) -> None:
    monkeypatch.setattr(
        fixtures,
        "OPENFOOTBALL_FILES",
        {"2026--usa/cup.txt": "0" * 64},
    )

    with pytest.raises(ValueError, match="hash mismatch"):
        fixtures.sync_schedule_fixtures(fetch_text=lambda _url: "changed")


def test_seed_test_openfootball_schedule_fixtures_covers_fifa_ids() -> None:
    with duckdb.connect(":memory:") as conn:
        seed_test_openfootball_schedule_fixtures(conn)
        count, minimum, maximum, knockout = conn.execute(
            """
            select
                count(*),
                min(fifa_match_id),
                max(fifa_match_id),
                count(*) filter (
                    where fifa_match_id between 73 and 104
                    and fifa_match_id <> 103
                )
            from openfootball_wc2026_raw.schedule_fixtures
            """
        ).fetchone()

    assert (count, minimum, maximum, knockout) == (104, 1, 104, 31)


def test_fetch_schedule_fixtures_uses_validated_url(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []

    class Response:
        text = "schedule"

        def raise_for_status(self) -> None:
            calls.append(("raise", 0))

    def fake_get(url: str, *, timeout: object) -> Response:
        calls.append((url, timeout))
        return Response()

    monkeypatch.setattr(fixtures.requests, "get", fake_get)

    assert fixtures.fetch_schedule_fixtures("https://example.com/cup.txt") == "schedule"
    assert calls == [
        ("https://example.com/cup.txt", HTTP_REQUEST_TIMEOUT),
        ("raise", 0),
    ]
