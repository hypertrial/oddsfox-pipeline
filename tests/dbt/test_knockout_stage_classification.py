"""Exercise knockout stage classification from the real dbt model SQL."""

from __future__ import annotations

from pathlib import Path

import duckdb

DBT_ROOT = Path(__file__).resolve().parents[2] / "dbt"
MODEL_SQL = (
    DBT_ROOT
    / "models"
    / "polymarket_wc2026"
    / "marts"
    / "polymarket_wc2026_knockout_market_tokens.sql"
).read_text()

_FIXTURE_ROWS = """
    ('m-winner', 0, 'tok-w-yes', 'Will Argentina win the 2026 FIFA World Cup?', 'Yes'),
    ('m-winner', 1, 'tok-w-no', 'Will Argentina win the 2026 FIFA World Cup?', 'No'),
    ('m-final', 0, 'tok-f-yes', 'Will Argentina reach the 2026 FIFA World Cup final?', 'Yes'),
    ('m-final', 1, 'tok-f-no', 'Will Argentina reach the 2026 FIFA World Cup final?', 'No'),
    ('m-semi', 0, 'tok-s-yes', 'Will Argentina reach the Semifinals at the 2026 FIFA World Cup?', 'Yes'),
    ('m-semi', 1, 'tok-s-no', 'Will Argentina reach the Semifinals at the 2026 FIFA World Cup?', 'No'),
    ('m-qf', 0, 'tok-q-yes', 'Will Argentina reach the Quarterfinals at the 2026 FIFA World Cup?', 'Yes'),
    ('m-qf', 1, 'tok-q-no', 'Will Argentina reach the Quarterfinals at the 2026 FIFA World Cup?', 'No'),
    ('m-r16-reach', 0, 'tok-r16r-yes', 'Will Mexico reach the Round of 16 at the 2026 FIFA World Cup?', 'Yes'),
    ('m-r16-reach', 1, 'tok-r16r-no', 'Will Mexico reach the Round of 16 at the 2026 FIFA World Cup?', 'No'),
    ('m-r32-reach', 0, 'tok-r32r-yes', 'Will USA reach the Round of 32 at the 2026 FIFA World Cup?', 'Yes'),
    ('m-r32-reach', 1, 'tok-r32r-no', 'Will USA reach the Round of 32 at the 2026 FIFA World Cup?', 'No'),
    ('m-r16-elim', 0, 'tok-r16e-yes', 'Will Mexico be eliminated in the Round of 16 of the World Cup?', 'Yes'),
    ('m-r16-elim', 1, 'tok-r16e-no', 'Will Mexico be eliminated in the Round of 16 of the World Cup?', 'No'),
    ('m-r32-elim', 0, 'tok-r32e-yes', 'Will USA be eliminated in the Round of 32 of the World Cup?', 'Yes'),
    ('m-r32-elim', 1, 'tok-r32e-no', 'Will USA be eliminated in the Round of 32 of the World Cup?', 'No'),
    ('m-italy', 0, 'tok-italy-yes', 'Will Italy win the 2026 FIFA World Cup?', 'Yes'),
    ('m-other', 0, 'tok-other', 'Will someone win the Golden Ball at the 2026 FIFA World Cup?', 'Yes')
"""

_FIXTURE_SOURCE = f"""
    select
        market_id,
        outcome_index,
        clob_token_id,
        cast(null as timestamp) as token_updated_at,
        question,
        outcome_label,
        cast(null as varchar) as event_slug,
        cast(null as varchar) as market_slug,
        cast(null as varchar) as condition_id,
        cast(null as varchar) as sports_market_type,
        cast(null as timestamp) as game_start_time,
        cast(null as varchar) as group_item_title,
        cast(null as varchar) as tags,
        cast(null as varchar) as clob_token_ids,
        market_id in ('m-winner', 'm-final') as is_active,
        market_id in ('m-final', 'm-r32-elim') as is_closed,
        market_id = 'm-semi' as is_resolved,
        cast(null as varchar) as winning_outcome,
        cast(null as varchar) as winning_clob_token_id,
        cast(null as double) as market_volume_usd
    from (values {_FIXTURE_ROWS}) as t(
        market_id,
        outcome_index,
        clob_token_id,
        question,
        outcome_label
    )
"""

_ALIASES = """
    select *
    from (values ('USA', 'United States')) as t(market_team_name, canonical_team_name)
"""

_TEAM_STATUS = """
    select *
    from (
        values
        ('Argentina', 'active', true, null, null, null, null, 0, 0, 0, 0, 0, 0, null, null),
        ('Mexico', 'active', true, null, null, null, null, 0, 0, 0, 0, 0, 0, null, null),
        ('United States', 'active', true, null, null, null, null, 0, 0, 0, 0, 0, 0, null, null)
    ) as t(
        team_name,
        tournament_status,
        is_still_alive,
        eliminated_stage_key,
        eliminated_match_date,
        next_match_date,
        next_stage_key,
        matches_played,
        wins,
        draws,
        losses,
        goals_for,
        goals_against,
        latest_completed_match_date,
        latest_completed_stage_key
    )
"""


def _run_classification() -> list[tuple]:
    sql = MODEL_SQL.replace(
        "from {{ ref('int_polymarket_wc2026_market_tokens') }} as t",
        f"from ({_FIXTURE_SOURCE.strip()}) as t",
    )
    sql = sql.replace(
        "{{ ref('international_results_wc2026_team_aliases') }}",
        f"({_ALIASES.strip()})",
    ).replace(
        "{{ ref('international_results_wc2026_team_status') }}",
        f"({_TEAM_STATUS.strip()})",
    )
    conn = duckdb.connect()
    try:
        rows = conn.execute(
            f"""
            select
                clob_token_id,
                stage_key,
                team_name,
                canonical_team_name,
                stage_rank,
                market_direction,
                source_outcome_label,
                price_represents,
                progression_outcome_label,
                market_status,
                is_live_market,
                source_state_anomaly
            from ({sql}) as classified
            order by clob_token_id
            """
        ).fetchall()
    finally:
        conn.close()
    return rows


def test_knockout_stage_classification_covers_all_stages() -> None:
    rows = _run_classification()
    by_token = {
        clob_token_id: (
            stage_key,
            team_name,
            canonical_team_name,
            stage_rank,
            market_direction,
            source_outcome_label,
            price_represents,
            progression_outcome_label,
            market_status,
            is_live_market,
            source_state_anomaly,
        )
        for (
            clob_token_id,
            stage_key,
            team_name,
            canonical_team_name,
            stage_rank,
            market_direction,
            source_outcome_label,
            price_represents,
            progression_outcome_label,
            market_status,
            is_live_market,
            source_state_anomaly,
        ) in rows
    }

    assert by_token["tok-w-yes"] == (
        "winner",
        "Argentina",
        "Argentina",
        5,
        "winner",
        "Yes",
        "progression",
        "win_world_cup",
        "live",
        True,
        False,
    )
    assert by_token["tok-f-yes"] == (
        "final",
        "Argentina",
        "Argentina",
        4,
        "advance",
        "Yes",
        "progression",
        "reach_final",
        "closed",
        False,
        True,
    )
    assert by_token["tok-s-yes"] == (
        "semifinal",
        "Argentina",
        "Argentina",
        3,
        "advance",
        "Yes",
        "progression",
        "reach_semifinal",
        "resolved",
        False,
        False,
    )
    assert by_token["tok-q-yes"] == (
        "quarterfinal",
        "Argentina",
        "Argentina",
        2,
        "advance",
        "Yes",
        "progression",
        "reach_quarterfinal",
        "inactive",
        False,
        False,
    )
    assert by_token["tok-r16r-yes"][:8] == (
        "round_of_16",
        "Mexico",
        "Mexico",
        1,
        "advance",
        "Yes",
        "progression",
        "reach_round_of_16",
    )
    assert by_token["tok-r32r-yes"][:8] == (
        "round_of_32",
        "USA",
        "United States",
        0,
        "advance",
        "Yes",
        "progression",
        "reach_round_of_32",
    )
    assert by_token["tok-r16e-no"][:8] == (
        "round_of_16",
        "Mexico",
        "Mexico",
        1,
        "elimination",
        "No",
        "progression",
        "not_eliminated_in_round_of_16",
    )
    assert by_token["tok-r32e-no"] == (
        "round_of_32",
        "USA",
        "United States",
        0,
        "elimination",
        "No",
        "progression",
        "not_eliminated_in_round_of_32",
        "closed",
        False,
        False,
    )
    assert "tok-r16e-yes" not in by_token
    assert "tok-r32e-yes" not in by_token
    assert "tok-italy-yes" not in by_token
    assert "tok-other" not in by_token
