from __future__ import annotations

import hashlib
import io
import tarfile
from dataclasses import replace
from datetime import date
from pathlib import Path

import pytest

from oddsfox_pipeline.features.pre_match_elo.identity import (
    IdentityContractError,
    IdentityRegistry,
    IdentityRow,
    canonicalize_and_deduplicate,
)
from oddsfox_pipeline.features.pre_match_elo.sources import (
    RawResult,
    SourceContractError,
    SourceSnapshot,
    acquire_snapshots,
    load_source_catalog,
    parse_football_txt,
    parse_international_csv,
    parse_openfootball_archive,
    parse_openfootball_json,
)


def snapshot(payload: bytes, *, parser: str = "openfootball_json") -> SourceSnapshot:
    revision = "a" * 40
    return SourceSnapshot(
        snapshot_id="test",
        source="openfootball-test",
        url=f"https://example.com/{revision}/results",
        revision=revision,
        sha256=hashlib.sha256(payload).hexdigest(),
        acquired_at="2026-08-19T00:00:00Z",
        license="CC0-1.0",
        parser=parser,
        competition="Test League",
        scope="club",
        gender="men",
        filename="results.txt" if parser == "football_txt" else "results.json",
        default_year=2024,
    )


def test_openfootball_json_parsing_and_schema_drift() -> None:
    payload = b'{"name":"League","matches":[{"date":"2024-01-02","team1":"A","team2":"B","score":{"ft":[2,1]}}]}'
    result = parse_openfootball_json(payload, snapshot(payload))
    assert [
        (row.home_name, row.away_name, row.home_score, row.away_score)
        for row in result.rows
    ] == [("A", "B", 2, 1)]
    with pytest.raises(SourceContractError, match="invalid teams"):
        bad = b'{"matches":[{"date":"2024-01-02","team1":7,"team2":"B","score":{"ft":[2,1]}}]}'
        parse_openfootball_json(bad, snapshot(bad))
    with pytest.raises(SourceContractError, match="no matches"):
        empty = b'{"games":[]}'
        parse_openfootball_json(empty, snapshot(empty))


def test_tracked_catalog_covers_required_pinned_sources() -> None:
    root = Path(__file__).resolve().parents[3]
    rows = load_source_catalog(root / "config" / "pre-match-elo-sources.yml")
    assert {row.source for row in rows} == {
        "international-results",
        "openfootball-football-json",
        "openfootball-worldcup-json",
        "openfootball-england",
        "openfootball-deutschland",
        "openfootball-italy",
        "openfootball-espana",
        "openfootball-europe",
        "openfootball-south-america",
        "openfootball-world",
        "openfootball-champions-league",
        "openfootball-club-world-cup",
    }
    assert all(len(row.revision) == 40 and len(row.sha256) == 64 for row in rows)


def test_football_txt_reports_every_unparsed_scored_line() -> None:
    payload = (
        b"2024-08-10\n"
        b"  A FC  2-1  B FC\n"
        b"  14:00  Future FC  v  Later FC  [cancelled]\n"
        b"malformed 3-2 line\n"
    )
    result = parse_football_txt(payload, snapshot(payload, parser="football_txt"))
    assert len(result.rows) == 1
    assert result.rows[0].match_date == date(2024, 8, 10)
    assert [issue.reason for issue in result.issues] == ["unparsed_scored_match_line"]


def test_archive_member_selection_v_format_dates_and_shootout_score() -> None:
    member = b"Tue Sep 18 2022\n  20:00  A FC  v  B FC  2-2 a.e.t., 5-4 pen.\n"
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        info = tarfile.TarInfo("repo/2022-23/results.txt")
        info.size = len(member)
        archive.addfile(info, io.BytesIO(member))
    payload = buffer.getvalue()
    source = replace(
        snapshot(payload, parser="football_txt"),
        parser="openfootball_archive",
        filename="results.tar.gz",
        include_regex=r"/2022-23/.*\.txt$",
    )
    result = parse_openfootball_archive(payload, source)
    assert len(result.rows) == 1
    assert result.rows[0].match_date == date(2022, 9, 18)
    assert (result.rows[0].home_name, result.rows[0].away_name) == ("A FC", "B FC")
    assert (result.rows[0].home_score, result.rows[0].away_score) == (2, 2)


def test_football_txt_season_years_do_not_depend_on_section_order() -> None:
    payload = (
        b"Fri Jul 19 2024\n  A  v  B  1-0\n"
        b"Sun Jan 5\n  A  v  B  2-0\n"
        b"Fri Aug 1\n  A  v  B  3-0\n"
        b"Sun Feb 2\n  A  v  B  4-0\n"
    )
    source = replace(snapshot(payload, parser="football_txt"), default_end_year=2025)
    result = parse_football_txt(payload, source)
    assert [row.match_date for row in result.rows] == [
        date(2024, 7, 19),
        date(2025, 1, 5),
        date(2024, 8, 1),
        date(2025, 2, 2),
    ]


def test_international_csv_contract_and_friendlies() -> None:
    payload = b"date,home_team,away_team,home_score,away_score,tournament,city,country,neutral\n2024-01-01,A,B,0,0,Friendly,X,Y,TRUE\n"
    source = snapshot(payload, parser="international_csv")
    result = parse_international_csv(payload, source)
    assert result.rows[0].neutral is True
    assert result.rows[0].friendly is True
    with pytest.raises(SourceContractError, match="schema changed"):
        parse_international_csv(
            b"date,home_team\n",
            snapshot(b"date,home_team\n", parser="international_csv"),
        )


def test_acquisition_is_pinned_checksummed_and_non_overwriting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "oddsfox_pipeline.features.pre_match_elo.sources.validate_outbound_https_url",
        lambda _: None,
    )
    payload = b"source bytes"
    source = snapshot(payload)
    paths = acquire_snapshots([source], tmp_path, fetch=lambda _: payload)
    assert paths[0].read_bytes() == payload
    with pytest.raises(SourceContractError, match="checksum mismatch"):
        acquire_snapshots([source], tmp_path, fetch=lambda _: b"changed")


def identity(
    source: str,
    name: str,
    team_id: str,
    *,
    pool: str = "club_men",
    status: str = "exact",
) -> IdentityRow:
    return IdentityRow(source, name, team_id, name, pool, "X", "UEFA", status)


def raw(source: str, score: tuple[int, int], match_id: str) -> RawResult:
    return RawResult(
        match_id,
        date(2024, 1, 1),
        "A",
        "B",
        *score,
        "League",
        "club_men",
        False,
        False,
        source,
        "snap",
        "1",
    )


def test_identity_resolution_pool_isolation_fuzzy_review_and_conflicts() -> None:
    registry = IdentityRegistry(
        [
            identity("source", "A", "club:a"),
            identity("source", "B", "club:b", status="reviewed_alias"),
            identity("source", "A Women", "women:a", pool="club_women"),
            identity("polymarket", "A", "club:a"),
            identity("polymarket", "B", "club:b"),
        ]
    )
    assert registry.resolve("source", "A", "club_men").status == "exact"
    assert registry.resolve("source", "B", "club_men").status == "reviewed_alias"
    fuzzy = registry.resolve("source", "A FC", "club_men")
    assert fuzzy.status == "unmapped"
    assert fuzzy.team_id is None
    assert registry.resolve("source", "A", "club_women").team_id is None
    exact_home, exact_away = registry.resolve_pair("polymarket", "A", "B")
    assert (exact_home.team_id, exact_away.team_id) == ("club:a", "club:b")
    assert (exact_home.status, exact_away.status) == ("exact", "exact")

    source_local = IdentityRegistry(
        [
            identity("one", "United", "one:united"),
            identity("two", "United", "two:united"),
        ]
    )
    assert source_local.resolve("one", "United", "club_men").team_id == "one:united"
    assert source_local.resolve("polymarket", "United", "club_men").team_id is None

    ambiguous_registry = IdentityRegistry(
        [
            IdentityRow(
                "polymarket",
                "United",
                None,
                None,
                None,
                None,
                None,
                "ambiguous",
                ("one:united", "two:united"),
            )
        ]
    )
    ambiguous = ambiguous_registry.resolve_without_pool("polymarket", "United")
    assert ambiguous.status == "ambiguous"
    assert ambiguous.candidate_team_ids == ("one:united", "two:united")

    duplicate_registry = IdentityRegistry(
        [
            identity("one", "A", "club:a"),
            identity("one", "B", "club:b"),
            identity("two", "A", "club:a"),
            identity("two", "B", "club:b"),
        ]
    )
    rows, conflicts, unresolved = canonicalize_and_deduplicate(
        [raw("one", (1, 0), "m1"), raw("two", (2, 0), "m2")], duplicate_registry
    )
    assert rows == ()
    assert len(conflicts) == 1
    assert unresolved == ()

    with pytest.raises(IdentityContractError, match="multiple teams"):
        IdentityRegistry(
            [identity("source", "A", "one"), identity("source", "A", "two")]
        )
    with pytest.raises(IdentityContractError, match="metadata is inconsistent"):
        IdentityRegistry(
            [
                identity("source", "A", "shared"),
                identity("source", "A Women", "shared", pool="club_women"),
            ]
        )
