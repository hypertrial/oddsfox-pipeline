"""Minimal and full-scale fixtures for Polygon settlement audit release tests."""

from __future__ import annotations

import shutil
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pytest

from oddsfox_pipeline.publishing import polygon_settlement as publishing


def minimal_market_row(
    *,
    proposition_id: str = "prop_001",
    fifa_match_id: int = 1,
    proposition_type: str = "home_win",
    stage: str = "group_stage",
    seed_sha256: str = "a" * 64,
) -> dict:
    kickoff = datetime(2026, 6, 11, 12, 0, 0)
    window_minutes = 150 if fifa_match_id <= 72 else 210
    return {
        "proposition_id": proposition_id,
        "fifa_match_id": fifa_match_id,
        "stage": stage,
        "group_label": "A" if stage == "group_stage" else None,
        "home_team": f"Home {fifa_match_id}",
        "away_team": f"Away {fifa_match_id}",
        "proposition_type": proposition_type,
        "yes_represents": f"Yes {proposition_id}",
        "no_represents": f"No {proposition_id}",
        "kickoff_at_utc": kickoff,
        "window_start_at_utc": kickoff,
        "window_end_at_utc": kickoff + timedelta(minutes=window_minutes),
        "yes_token_id": f"{proposition_id}-yes",
        "no_token_id": f"{proposition_id}-no",
        "manifest_sha256": seed_sha256,
        "manifest_version": "1.0.0",
    }


def minimal_mart_row(
    market: dict,
    *,
    elapsed_window_minute: int = 0,
    minute_status: str = "both_observed",
    yes_observed: bool = True,
    no_observed: bool = True,
) -> dict:
    minute = market["window_start_at_utc"] + timedelta(minutes=elapsed_window_minute)
    yes_vals = {
        "yes_open": Decimal("0.4"),
        "yes_high": Decimal("0.5"),
        "yes_low": Decimal("0.3"),
        "yes_close": Decimal("0.45"),
        "yes_vwap": Decimal("0.44"),
        "yes_normalized_fill_count": 1,
        "yes_derived_fill_count": 0,
        "yes_share_volume": Decimal("10"),
        "yes_gross_collateral_volume": Decimal("4.4"),
        "yes_first_settlement_at_utc": minute,
        "yes_last_settlement_at_utc": minute,
    }
    no_vals = {
        "no_open": Decimal("0.6"),
        "no_high": Decimal("0.7"),
        "no_low": Decimal("0.5"),
        "no_close": Decimal("0.55"),
        "no_vwap": Decimal("0.56"),
        "no_normalized_fill_count": 1,
        "no_derived_fill_count": 0,
        "no_share_volume": Decimal("10"),
        "no_gross_collateral_volume": Decimal("5.6"),
        "no_first_settlement_at_utc": minute,
        "no_last_settlement_at_utc": minute,
    }
    if not yes_observed:
        yes_vals = {
            "yes_open": None,
            "yes_high": None,
            "yes_low": None,
            "yes_close": None,
            "yes_vwap": None,
            "yes_normalized_fill_count": 0,
            "yes_derived_fill_count": 0,
            "yes_share_volume": Decimal("0"),
            "yes_gross_collateral_volume": Decimal("0"),
            "yes_first_settlement_at_utc": None,
            "yes_last_settlement_at_utc": None,
        }
    if not no_observed:
        no_vals = {
            "no_open": None,
            "no_high": None,
            "no_low": None,
            "no_close": None,
            "no_vwap": None,
            "no_normalized_fill_count": 0,
            "no_derived_fill_count": 0,
            "no_share_volume": Decimal("0"),
            "no_gross_collateral_volume": Decimal("0"),
            "no_first_settlement_at_utc": None,
            "no_last_settlement_at_utc": None,
        }
    return {
        "fifa_match_id": market["fifa_match_id"],
        "stage": market["stage"],
        "group_name": market["group_label"],
        "home_team": market["home_team"],
        "away_team": market["away_team"],
        "proposition_id": market["proposition_id"],
        "proposition_type": market["proposition_type"],
        "yes_represents": market["yes_represents"],
        "no_represents": market["no_represents"],
        "scheduled_kickoff_at_utc": market["kickoff_at_utc"],
        "analysis_window_start_at_utc": market["window_start_at_utc"],
        "analysis_window_end_at_utc": market["window_end_at_utc"],
        "settlement_minute_utc": minute,
        "elapsed_window_minute": elapsed_window_minute,
        **yes_vals,
        "yes_observed": yes_observed,
        **no_vals,
        "no_observed": no_observed,
        "minute_complete": yes_observed and no_observed,
        "minute_status": minute_status,
    }


def minimal_quality_row(
    *,
    scan_id: str = "scan-1",
    publication_ready: bool = True,
    blocking_issue_keys: str = "",
    error_issue_count: int = 0,
) -> dict:
    return {
        "scan_id": scan_id,
        "scan_status": "published",
        "publication_ready": publication_ready,
        "blocking_issue_keys": blocking_issue_keys,
        "warning_issue_count": 0,
        "error_issue_count": error_issue_count,
    }


def _build_full_release_db(path: Path) -> None:
    conn = duckdb.connect(str(path))
    conn.execute("create schema polymarket_wc2026_staging")
    conn.execute("create schema polymarket_wc2026_marts")
    conn.execute("create schema polymarket_wc2026_observability")
    conn.execute(
        """
        create table polymarket_wc2026_staging.stg_polymarket_wc2026_polygon_settlement_markets as
        with markets as (
            select
                i,
                case when i <= 216 then cast(((i - 1) // 3) + 1 as integer)
                     else cast(i - 144 as integer) end as fifa_match_id
            from range(1, 249) as source(i)
        )
        select
            'prop_' || lpad(cast(i as varchar), 3, '0') as proposition_id,
            fifa_match_id,
            case
                when fifa_match_id <= 72 then 'group_stage'
                when fifa_match_id <= 88 then 'round_of_32'
                when fifa_match_id <= 96 then 'round_of_16'
                when fifa_match_id <= 100 then 'quarterfinal'
                when fifa_match_id <= 102 then 'semifinal'
                when fifa_match_id = 103 then 'third_place'
                else 'final'
            end as stage,
            case when fifa_match_id <= 72 then 'A' else null end as group_name,
            'Home ' || fifa_match_id as home_team,
            'Away ' || fifa_match_id as away_team,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                as scheduled_kickoff_at_utc,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                as analysis_window_start_at_utc,
            timestamp '2026-06-11 12:00:00' + fifa_match_id * interval '1 day'
                + case when fifa_match_id <= 72 then interval '150 minutes'
                       else interval '210 minutes' end as analysis_window_end_at_utc,
            case when fifa_match_id <= 72 then
                case (i - 1) % 3 when 0 then 'home_win' when 1 then 'draw'
                    else 'away_win' end
            when fifa_match_id <= 102 then 'home_advances'
            when fifa_match_id = 103 then 'home_win_third_place'
            else 'home_wins_final' end as proposition_type,
            'Yes meaning ' || i as yes_represents,
            'No meaning ' || i as no_represents,
            '0x' || lpad(to_hex(i), 64, '0') as condition_id,
            cast(i * 2 as varchar) as yes_token_id,
            cast(i * 2 + 1 as varchar) as no_token_id,
            case when i % 2 = 0 then 'standard' else 'neg_risk' end
                as market_structure,
            case when i % 2 = 0
                then '0xE111180000d2663C0091e4f400237545B87B996B'
                else '0xe2222d279d744050d28e00520010520000310F59'
            end as exchange_address,
            repeat('a', 40) as openfootball_revision,
            case when fifa_match_id <= 72 then '2026--usa/cup.txt'
                 else '2026--usa/cup_finals.txt' end as openfootball_path,
            '1-2' as openfootball_source_lines,
            repeat('b', 64) as openfootball_line_hash,
            '0x' || repeat('1', 64) as condition_init_tx_hash,
            i as condition_init_log_index,
            '0x' || repeat('2', 64) as question_init_tx_hash,
            i + 1 as question_init_log_index,
            repeat('c', 64) as ancillary_data_sha256,
            100000 + i as token_verification_block_number,
            '0x' || repeat('3', 64) as token_verification_block_hash,
            repeat('a', 64) as manifest_sha256,
            '1.0.0' as manifest_version,
            timestamp '2026-07-22 00:00:00' as reviewed_at_utc
        from markets
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_marts.polymarket_wc2026_polygon_settlement_minute_odds as
        with markets as (
            select *
            from polymarket_wc2026_staging.stg_polymarket_wc2026_polygon_settlement_markets
        )
        select
            fifa_match_id,
            stage,
            group_name,
            home_team,
            away_team,
            proposition_id,
            proposition_type,
            yes_represents,
            no_represents,
            scheduled_kickoff_at_utc,
            analysis_window_start_at_utc,
            analysis_window_end_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as settlement_minute_utc,
            cast(minute_index as integer) as elapsed_window_minute,
            cast(0.4 as decimal(38,18)) as yes_open,
            cast(0.5 as decimal(38,18)) as yes_high,
            cast(0.3 as decimal(38,18)) as yes_low,
            cast(0.45 as decimal(38,18)) as yes_close,
            cast(0.44 as decimal(38,18)) as yes_vwap,
            1::bigint as yes_normalized_fill_count,
            0::bigint as yes_derived_fill_count,
            cast(10 as decimal(38,6)) as yes_share_volume,
            cast(4.4 as decimal(38,6)) as yes_gross_collateral_volume,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as yes_first_settlement_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as yes_last_settlement_at_utc,
            true as yes_observed,
            cast(0.6 as decimal(38,18)) as no_open,
            cast(0.7 as decimal(38,18)) as no_high,
            cast(0.5 as decimal(38,18)) as no_low,
            cast(0.55 as decimal(38,18)) as no_close,
            cast(0.56 as decimal(38,18)) as no_vwap,
            1::bigint as no_normalized_fill_count,
            0::bigint as no_derived_fill_count,
            cast(10 as decimal(38,6)) as no_share_volume,
            cast(5.6 as decimal(38,6)) as no_gross_collateral_volume,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as no_first_settlement_at_utc,
            analysis_window_start_at_utc + minute_index * interval '1 minute'
                as no_last_settlement_at_utc,
            true as no_observed,
            true as minute_complete,
            'both_observed' as minute_status
        from markets
        cross join lateral range(
            0,
            case when fifa_match_id <= 72 then 150 else 210 end
        ) as minutes(minute_index)
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_data_quality as
        select
            'scan-1' as scan_id,
            'published' as scan_status,
            true as publication_ready,
            '' as blocking_issue_keys,
            0::bigint as warning_issue_count,
            0::bigint as error_issue_count
        """
    )
    conn.execute(
        """
        create table polymarket_wc2026_observability.polymarket_wc2026_polygon_settlement_quality_issues (
            issue_key varchar,
            severity varchar,
            issue_type varchar,
            proposition_id varchar,
            fifa_match_id integer,
            token_id varchar,
            settlement_minute_utc timestamp,
            measured_value double,
            threshold_value double,
            issue_detail varchar,
            observed_at timestamp
        )
        """
    )
    conn.close()


@pytest.fixture(scope="module")
def full_release_template(tmp_path_factory) -> Path:
    path = tmp_path_factory.mktemp("polygon-release") / "template.duckdb"
    _build_full_release_db(path)
    return path


@pytest.fixture
def release_connection(full_release_template, monkeypatch, tmp_path):
    db_path = tmp_path / "release.duckdb"
    shutil.copy2(full_release_template, db_path)
    conn = duckdb.connect(str(db_path))
    seed_rows = publishing._read_market_rows(conn)
    monkeypatch.setattr(
        publishing,
        "load_polygon_market_seed",
        MagicMock(
            return_value=SimpleNamespace(
                markets=tuple(SimpleNamespace(**row) for row in seed_rows),
                sha256="a" * 64,
                version="1.0.0",
            )
        ),
    )
    monkeypatch.setattr(
        publishing,
        "load_polygon_resolution_attestation",
        MagicMock(
            return_value=SimpleNamespace(
                as_mapping=lambda: {
                    "schema_version": 1,
                    "manifest_version": "1.0.0",
                    "manifest_sha256": "a" * 64,
                    "resolved_condition_count": 248,
                    "verified_at_utc": "2026-07-22T11:02:27Z",
                    "authoring_evidence_sha256": "b" * 64,
                    "finalized_head_block_number": 123456,
                    "finalized_head_block_hash": "0x" + "c" * 64,
                }
            )
        ),
    )
    yield conn
    conn.close()
