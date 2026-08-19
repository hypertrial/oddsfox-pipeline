from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from oddsfox_pipeline.features.pre_match_elo.release import (
    COVERAGE_STATUSES,
    build_release,
)
from oddsfox_pipeline.features.pre_match_elo.sources import SourceSnapshot


def _payload(matches: list[dict[str, object]]) -> bytes:
    return json.dumps({"name": "Test", "matches": matches}, sort_keys=True).encode()


def _snapshot(
    root: Path,
    snapshot_id: str,
    source: str,
    scope: str,
    gender: str,
    matches: list[dict[str, object]],
) -> SourceSnapshot:
    payload = _payload(matches)
    revision = hashlib.sha1(snapshot_id.encode()).hexdigest()
    row = SourceSnapshot(
        snapshot_id=snapshot_id,
        source=source,
        url=f"https://example.com/{revision}/results.json",
        revision=revision,
        sha256=hashlib.sha256(payload).hexdigest(),
        acquired_at="2026-08-19T00:00:00Z",
        license="CC0-1.0",
        parser="openfootball_json",
        competition="Test",
        scope=scope,
        gender=gender,
        filename="results.json",
    )
    directory = root / snapshot_id
    directory.mkdir(parents=True)
    (directory / row.filename).write_bytes(payload)
    return row


def _match(
    match_id: str, day: str, home: str, away: str, score: tuple[int, int]
) -> dict[str, object]:
    return {
        "id": match_id,
        "date": day,
        "team1": home,
        "team2": away,
        "score": {"ft": list(score)},
    }


def _write_target(path: Path) -> None:
    rows = [
        ("e1", "club-men", "A", "B", '["men-league"]'),
        ("e2", "club-women", "WA", "WB", '["women-league"]'),
        ("e3", "national-men", "NA", "NB", '["men-national"]'),
        ("e4", "national-women", "NWA", "NWB", '["women-national"]'),
        ("e5", "unmapped", "U", "V", "[]"),
        ("e6", "conflict", "C", "D", '["men-league"]'),
    ]
    table = pa.Table.from_pylist(
        [
            {
                "event_id": event_id,
                "event_slug": slug,
                "match_started_at_utc": datetime(2024, 1, 1, 12),
                "series_slugs_json": slugs,
                "home_team": home,
                "away_team": away,
            }
            for event_id, slug, home, away, slugs in rows
        ]
    )
    pq.write_table(table, path)


def _write_identity(path: Path, mappings: list[dict[str, object]]) -> None:
    lines = ["mappings:"]
    for row in mappings:
        lines.append("  - " + json.dumps(row))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_identity_review(
    path: Path,
    identity: Path,
    target_sha: str,
    *,
    target_labels: int,
    identity_rows: int,
) -> None:
    path.write_text(
        json.dumps(
            {
                "authoring_version": "oddsfox.soccer.identity-review.v1",
                "target_snapshot_sha256": target_sha,
                "source_catalog_sha256": "a" * 64,
                "review_ledger_sha256": "b" * 64,
                "identity_map_sha256": hashlib.sha256(
                    identity.read_bytes()
                ).hexdigest(),
                "reviewer_labels": ["test-reviewer"],
                "reviewed_at_utc": ["2026-08-19T00:00:00Z"],
                "decision_counts": {"approve": target_labels},
                "target_label_count": target_labels,
                "unresolved_target_labels": 0,
                "source_identity_count": identity_rows,
                "compiled_identity_rows": identity_rows,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )


def test_release_all_pools_conflict_unmapped_determinism_and_benchmark_independence(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    snapshots = [
        _snapshot(
            raw,
            "club-men-one",
            "club-men-one",
            "club",
            "men",
            [
                _match("ab", "2023-01-01", "A", "B", (1, 0)),
                _match("cd-prior", "2023-01-01", "C", "D", (0, 0)),
                _match("cd-target", "2024-01-01", "C", "D", (1, 0)),
            ],
        ),
        _snapshot(
            raw,
            "club-men-two",
            "club-men-two",
            "club",
            "men",
            [_match("cd-target", "2024-01-01", "C", "D", (2, 0))],
        ),
        _snapshot(
            raw,
            "club-women",
            "club-women",
            "club",
            "women",
            [_match("women", "2023-01-01", "WA", "WB", (1, 1))],
        ),
        _snapshot(
            raw,
            "national-men",
            "national-men",
            "national",
            "men",
            [_match("national", "2023-01-01", "NA", "NB", (0, 1))],
        ),
        _snapshot(
            raw,
            "national-women",
            "national-women",
            "national",
            "women",
            [_match("national-women", "2023-01-01", "NWA", "NWB", (2, 0))],
        ),
    ]
    team_specs = [
        ("A", "B", "club_men", "club-men-one"),
        ("C", "D", "club_men", "club-men-one"),
        ("C", "D", "club_men", "club-men-two"),
        ("WA", "WB", "club_women", "club-women"),
        ("NA", "NB", "national_men", "national-men"),
        ("NWA", "NWB", "national_women", "national-women"),
    ]
    mappings: list[dict[str, object]] = []
    for home, away, pool, source in team_specs:
        for name in (home, away):
            team_id = f"{pool}:{name.casefold()}"
            for system in (source, "polymarket"):
                mappings.append(
                    {
                        "source_system": system,
                        "source_name": name,
                        "team_id": team_id,
                        "canonical_display_name": name,
                        "rating_pool": pool,
                        "country": "Test",
                        "confederation": "Test",
                        "mapping_status": "exact",
                    }
                )
    mappings.extend(
        [
            {
                "source_system": "polymarket",
                "source_name": "U",
                "team_id": None,
                "canonical_display_name": None,
                "rating_pool": "club_men",
                "country": None,
                "confederation": None,
                "mapping_status": "ambiguous",
                "candidate_team_ids_json": '["club_men:u-one","club_men:u-two"]',
            },
            {
                "source_system": "polymarket",
                "source_name": "V",
                "team_id": "club_men:v",
                "canonical_display_name": "V",
                "rating_pool": "club_men",
                "country": None,
                "confederation": None,
                "mapping_status": "exact",
            },
        ]
    )
    identity = tmp_path / "identity.yml"
    _write_identity(identity, mappings)
    target = tmp_path / "target.parquet"
    _write_target(target)
    target_sha = hashlib.sha256(target.read_bytes()).hexdigest()
    identity_review = tmp_path / "identity-review.json"
    _write_identity_review(
        identity_review,
        identity,
        target_sha,
        target_labels=12,
        identity_rows=len(mappings),
    )

    common = {
        "target_parquet": target,
        "snapshots": snapshots,
        "raw_root": raw,
        "identity_map": identity,
        "identity_review_report": identity_review,
        "source_catalog_sha256": "a" * 64,
        "build_revision": "f" * 40,
        "expected_target_sha256": target_sha,
        "expected_event_count": 6,
    }
    first = build_release(output_directory=tmp_path / "release-one", **common)
    second = build_release(output_directory=tmp_path / "release-two", **common)
    for filename in (
        "event_pre_match_elo.parquet",
        "team_identity_map.parquet",
        "coverage_by_competition.parquet",
        "manifest.json",
        "checksums.sha256",
    ):
        assert (first / filename).read_bytes() == (second / filename).read_bytes()

    rows = pq.read_table(first / "event_pre_match_elo.parquet").to_pylist()
    by_id = {row["event_id"]: row for row in rows}
    assert {by_id[event]["rating_pool"] for event in ("e1", "e2", "e3", "e4")} == {
        "club_men",
        "club_women",
        "national_men",
        "national_women",
    }
    assert by_id["e1"]["coverage_status"] == "rated_provisional"
    assert by_id["e5"]["coverage_status"] == "ambiguous_target_match"
    assert by_id["e5"]["home_mapping_status"] == "ambiguous"
    assert by_id["e6"]["coverage_status"] == "source_conflict"
    assert {row["coverage_status"] for row in rows} <= COVERAGE_STATUSES
    assert len(rows) == len({row["event_id"] for row in rows}) == 6
    coverage = pq.read_table(first / "coverage_by_competition.parquet").to_pylist()
    assert any(row["dimension"] == "connected_component" for row in coverage)

    benchmark = tmp_path / "benchmarks.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "system": "ClubElo",
                    "team_id": "club_men:a",
                    "rating": 1600.0,
                    "as_of_date": "2023-12-31",
                    "snapshot_id": "clubelo-test",
                    "mapping_method": "exact",
                    "is_pre_match": False,
                }
            ]
        ),
        benchmark,
    )
    with_benchmark = build_release(
        output_directory=tmp_path / "release-benchmark",
        benchmark_path=benchmark,
        **common,
    )
    benchmark_rows = {
        row["event_id"]: row
        for row in pq.read_table(
            with_benchmark / "event_pre_match_elo.parquet"
        ).to_pylist()
    }
    for event_id in by_id:
        for field in (
            "home_pre_match_elo",
            "away_pre_match_elo",
            "pre_match_elo_difference",
            "coverage_status",
        ):
            assert benchmark_rows[event_id][field] == by_id[event_id][field]
    assert benchmark_rows["e1"]["home_benchmark_rating"] == 1600
    assert benchmark_rows["e1"]["away_benchmark_rating"] is None
