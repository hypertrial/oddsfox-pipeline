from __future__ import annotations

import json
from datetime import date, datetime, timezone
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest

from oddsfox_pipeline.features.pre_match_elo.identity_authoring import (
    IdentityAuthoringError,
    compile_identity_map,
    infer_event_pool,
    prepare_identity_review,
    review_identity_candidates,
    source_team_id,
)
from oddsfox_pipeline.features.pre_match_elo.release import TargetEvent
from oddsfox_pipeline.features.pre_match_elo.sources import RawResult


def _event(event_id: str, home: str, away: str, slugs: list[str]) -> TargetEvent:
    return TargetEvent(
        event_id=event_id,
        event_slug=event_id,
        kickoff_at_utc=datetime(2024, 1, 2, 12, tzinfo=timezone.utc),
        competition_slugs_json=json.dumps(slugs),
        home_source_name=home,
        away_source_name=away,
    )


def _result(home: str, away: str) -> RawResult:
    return RawResult(
        source_match_id=f"{home}-{away}",
        match_date=date(2024, 1, 2),
        home_name=home,
        away_name=away,
        home_score=1,
        away_score=0,
        competition="Test League",
        rating_pool="club_men",
        neutral=False,
        friendly=False,
        source="source-one",
        snapshot_id="snapshot-one",
        source_locator="results:1",
    )


def test_prepare_review_and_compile_keeps_pool_boundaries(tmp_path: Path) -> None:
    events = (
        _event("men", "Ájax", "PSV", ["epl"]),
        _event("women", "Gotham FC", "Angel City", ["soccer-nwsl"]),
        _event("unknown", "Unknown A", "Unknown B", []),
    )
    assert infer_event_pool(events[0]) == "club_men"
    assert infer_event_pool(events[1]) == "club_women"
    assert infer_event_pool(events[2]) is None

    workspace = prepare_identity_review(
        events=events,
        results=(_result("Ajax", "PSV"),),
        target_sha256="a" * 64,
        source_catalog_sha256="b" * 64,
        output_directory=tmp_path / "review",
    )
    review_identity_candidates(
        workspace,
        reviewer="agent",
        reviewed_at_utc="2026-08-19T00:00:00Z",
    )
    identity = compile_identity_map(
        workspace,
        output_path=tmp_path / "identity.parquet",
        report_path=tmp_path / "review-report.json",
    )

    rows = pq.read_table(identity).to_pylist()
    polymarket = {
        row["source_name"]: row for row in rows if row["source_system"] == "polymarket"
    }
    assert polymarket["Ájax"]["team_id"] == source_team_id(
        "source-one", "Ajax", "club_men"
    )
    assert polymarket["Gotham FC"]["rating_pool"] == "club_women"
    assert "Unknown A" not in polymarket
    report = json.loads((tmp_path / "review-report.json").read_text())
    assert report["target_label_count"] == 6
    assert report["unresolved_target_labels"] == 2


def test_compile_rejects_an_unreviewed_candidate(tmp_path: Path) -> None:
    workspace = prepare_identity_review(
        events=(_event("men", "Ajax", "PSV", ["epl"]),),
        results=(_result("Ajax", "PSV"),),
        target_sha256="a" * 64,
        source_catalog_sha256="b" * 64,
        output_directory=tmp_path / "review",
    )
    dispositions = pl.read_csv(workspace / "target_dispositions.csv")
    dispositions.write_csv(workspace / "target_dispositions.csv")
    with pytest.raises(IdentityAuthoringError, match="alias candidate"):
        compile_identity_map(
            workspace,
            output_path=tmp_path / "identity.parquet",
            report_path=tmp_path / "review-report.json",
        )
