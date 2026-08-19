#!/usr/bin/env python3
"""Build top-N World Cup winner odds after each WC2026 game."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Final

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import REPO_ROOT

MONOREPO_ROOT: Final[Path] = REPO_ROOT.parent
DEFAULT_DUCKDB: Final[Path] = REPO_ROOT / "oddsfox.duckdb"
DEFAULT_HOURLY_PARQUET: Final[Path] = (
    REPO_ROOT / "artifacts" / "polymarket_wc2026_market_hourly_odds.parquet"
)
DEFAULT_OUTPUT: Final[Path] = MONOREPO_ROOT / "top5_odds_to_win.parquet"
DEFAULT_TEAM_COUNT: Final = 5

_CHECKPOINTS_SQL = """
with schedule as (
    select *
    from oddsfox_reference.openfootball_wc2026_schedule_fixtures
),
schedule_canon as (
    select
        s.*,
        coalesce(ah.canonical_match_key, lower(s.home_team)) as home_canon,
        coalesce(aa.canonical_match_key, lower(s.away_team)) as away_canon
    from schedule as s
    left join team_aliases as ah
        on lower(ah.variant_match_key) = lower(s.home_team)
    left join team_aliases as aa
        on lower(aa.variant_match_key) = lower(s.away_team)
),
results_canon as (
    select
        r.*,
        coalesce(ah.canonical_match_key, lower(r.home_team)) as home_canon,
        coalesce(aa.canonical_match_key, lower(r.away_team)) as away_canon
    from oddsfox_reference.international_results_wc2026_matches as r
    left join team_aliases as ah
        on lower(ah.variant_match_key) = lower(r.home_team)
    left join team_aliases as aa
        on lower(aa.variant_match_key) = lower(r.away_team)
),
matched as (
    select
        s.fifa_match_id,
        s.stage_key,
        s.group_label,
        s.home_team,
        s.away_team,
        s.kickoff_at_utc,
        s.venue,
        case
            when
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
                then r.home_score
            when
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
                then r.away_score
        end as home_score,
        case
            when
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
                then r.away_score
            when
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
                then r.home_score
        end as away_score,
        case
            when
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
                then r.winner_team
            when
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
                then case
                    when r.winner_team = r.home_team then r.away_team
                    when r.winner_team = r.away_team then r.home_team
                    else r.winner_team
                end
        end as winner_team,
        case
            when s.stage_key = 'group_stage'
                then s.kickoff_at_utc + interval '105 minutes'
            else s.kickoff_at_utc + interval '135 minutes'
        end as game_finished_at_utc
    from schedule_canon as s
    left join results_canon as r
        on (
            (
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
            )
            or (
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
            )
        )
        and abs(date_diff('day', cast(s.kickoff_at_utc as date), r.match_date)) <= 1
),
games as (
    select *
    from matched
),
tournament_start as (
    select min(kickoff_at_utc) as tournament_start_at_utc
    from games
),
ordered_games as (
    select
        row_number() over (
            order by game_finished_at_utc, fifa_match_id
        ) as game_sequence,
        *
    from games
),
checkpoints as (
    select
        0 as game_sequence,
        null::integer as fifa_match_id,
        null::varchar as stage_key,
        null::varchar as group_label,
        null::varchar as home_team,
        null::varchar as away_team,
        null::integer as home_score,
        null::integer as away_score,
        null::varchar as winner_team,
        ts.tournament_start_at_utc as checkpoint_at_utc,
        'Tournament start' as gamestamp
    from tournament_start as ts

    union all

    select
        game_sequence,
        fifa_match_id,
        stage_key,
        group_label,
        home_team,
        away_team,
        home_score,
        away_score,
        winner_team,
        game_finished_at_utc as checkpoint_at_utc,
        'G'
        || lpad(cast(fifa_match_id as varchar), 3, '0')
        || ' | '
        || home_team
        || ' '
        || cast(home_score as varchar)
        || '-'
        || cast(away_score as varchar)
        || ' '
        || away_team
        || ' | '
        || case stage_key
            when 'group_stage' then 'Group Stage'
            when 'round_of_32' then 'Round of 32'
            when 'round_of_16' then 'Round of 16'
            when 'quarterfinal' then 'Quarterfinal'
            when 'semifinal' then 'Semifinal'
            when 'third_place' then 'Third Place'
            when 'final' then 'Final'
            else stage_key
        end as gamestamp
    from ordered_games
)
select * from checkpoints
"""

_BUILD_SQL = """
with checkpoints as (
    {checkpoints}
),
tournament_start as (
    select min(checkpoint_at_utc) as tournament_start_at_utc
    from checkpoints
    where game_sequence = 0
),
opening_odds as (
    select
        h.group_item_title as team,
        h.close_odds as opening_odds_to_win,
        row_number() over (order by h.close_odds desc) as opening_rank
    from winner_hourly as h
    inner join (
        select max(o.odds_hour_utc) as snapshot_hour
        from winner_hourly as o
        inner join tournament_start as ts
            on o.odds_hour_utc <= ts.tournament_start_at_utc
    ) as snap
        on h.odds_hour_utc = snap.snapshot_hour
    qualify opening_rank <= {team_count}
),
with_odds as (
    select
        c.gamestamp,
        c.game_sequence,
        c.fifa_match_id,
        c.stage_key,
        c.group_label,
        c.home_team,
        c.away_team,
        c.home_score,
        c.away_score,
        c.winner_team,
        c.checkpoint_at_utc,
        t.team,
        t.opening_rank,
        t.opening_odds_to_win,
        lower(c.home_team) = lower(t.team) or lower(c.away_team) = lower(t.team) as team_involved,
        o.odds_hour_utc,
        o.odds_hour_epoch,
        o.open_odds,
        o.high_odds,
        o.low_odds,
        o.close_odds as odds_to_win,
        o.avg_odds,
        o.observed_points,
        row_number() over (
            partition by c.game_sequence, t.team
            order by
                case when c.game_sequence = 0 then 0 else 1 end,
                case when c.game_sequence = 0 then o.odds_hour_utc end desc,
                case when c.game_sequence > 0 then o.odds_hour_utc end asc
        ) as odds_pick
    from checkpoints as c
    cross join opening_odds as t
    left join winner_hourly as o
        on o.group_item_title = t.team
        and (
            (c.game_sequence = 0 and o.odds_hour_utc <= c.checkpoint_at_utc)
            or (c.game_sequence > 0 and o.odds_hour_utc > c.checkpoint_at_utc)
        )
)
select
    gamestamp,
    game_sequence,
    fifa_match_id,
    stage_key,
    group_label,
    home_team,
    away_team,
    home_score,
    away_score,
    winner_team,
    checkpoint_at_utc,
    team,
    opening_rank,
    opening_odds_to_win,
    team_involved,
    odds_hour_utc,
    odds_hour_epoch,
    open_odds,
    high_odds,
    low_odds,
    odds_to_win,
    avg_odds,
    observed_points
from with_odds
where odds_pick = 1
order by game_sequence, opening_rank
"""

_MISSING_SCORES_SQL = """
with schedule as (
    select *
    from oddsfox_reference.openfootball_wc2026_schedule_fixtures
),
schedule_canon as (
    select
        s.*,
        coalesce(ah.canonical_match_key, lower(s.home_team)) as home_canon,
        coalesce(aa.canonical_match_key, lower(s.away_team)) as away_canon
    from schedule as s
    left join team_aliases as ah
        on lower(ah.variant_match_key) = lower(s.home_team)
    left join team_aliases as aa
        on lower(aa.variant_match_key) = lower(s.away_team)
),
results_canon as (
    select
        r.*,
        coalesce(ah.canonical_match_key, lower(r.home_team)) as home_canon,
        coalesce(aa.canonical_match_key, lower(r.away_team)) as away_canon
    from oddsfox_reference.international_results_wc2026_matches as r
    left join team_aliases as ah
        on lower(ah.variant_match_key) = lower(r.home_team)
    left join team_aliases as aa
        on lower(aa.variant_match_key) = lower(r.away_team)
),
matched as (
    select
        s.fifa_match_id,
        case
            when
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
                then r.home_score
            when
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
                then r.away_score
        end as home_score
    from schedule_canon as s
    left join results_canon as r
        on (
            (
                s.home_canon = r.home_canon
                and s.away_canon = r.away_canon
            )
            or (
                s.home_canon = r.away_canon
                and s.away_canon = r.home_canon
            )
        )
        and abs(date_diff('day', cast(s.kickoff_at_utc as date), r.match_date)) <= 1
)
select fifa_match_id
from matched
where home_score is null
order by fifa_match_id
"""


def _resolve_hourly_parquet(path: Path | None) -> Path:
    if path is not None:
        return path.resolve()
    if DEFAULT_HOURLY_PARQUET.exists():
        return DEFAULT_HOURLY_PARQUET
    fallback = (
        MONOREPO_ROOT
        / "oddsfox-graph"
        / "polymarket_wc2026_market_hourly_odds_20260805T183112Z.parquet"
    )
    if fallback.exists():
        return fallback
    raise FileNotFoundError(
        "Hourly odds parquet not found. Pass --hourly-parquet explicitly."
    )


def _build_sql(team_count: int) -> str:
    return _BUILD_SQL.format(checkpoints=_CHECKPOINTS_SQL, team_count=team_count)


def build_top5_odds_to_win(
    *,
    duckdb_path: Path,
    hourly_parquet: Path,
    output_path: Path,
    team_count: int = DEFAULT_TEAM_COUNT,
) -> tuple[int, list[tuple[str, int, float]]]:
    if team_count < 1:
        raise ValueError("team_count must be at least 1")
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB not found: {duckdb_path}")
    if not hourly_parquet.exists():
        raise FileNotFoundError(f"Hourly parquet not found: {hourly_parquet}")

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    build_sql = _build_sql(team_count)

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        con.execute(
            "create temp view team_aliases as "
            "select * from oddsfox_reference.wc2026_team_canonical_aliases"
        )
        con.execute(
            """
            create temp table winner_hourly as
            select
                group_item_title,
                odds_hour_utc,
                odds_hour_epoch,
                open_odds,
                high_odds,
                low_odds,
                close_odds,
                avg_odds,
                observed_points
            from read_parquet(?)
            where event_slug = 'world-cup-winner'
              and group_item_title is not null
            """,
            [str(hourly_parquet)],
        )

        missing_ids = con.execute(_MISSING_SCORES_SQL).fetchall()
        if missing_ids:
            missing = [row[0] for row in missing_ids]
            raise RuntimeError(f"Missing match results for FIFA games: {missing}")

        teams = con.execute(
            f"""
            select team, opening_rank, opening_odds_to_win
            from ({build_sql})
            where game_sequence = 0
            order by opening_rank
            """
        ).fetchall()
        if len(teams) != team_count:
            raise RuntimeError(
                f"Expected {team_count} opening favorites, found {len(teams)}"
            )

        row_count = con.execute(f"select count(*) from ({build_sql})").fetchone()[0]
        expected_rows = (104 + 1) * team_count
        if row_count != expected_rows:
            raise RuntimeError(
                f"Expected {expected_rows} rows ({team_count} teams x 105 checkpoints), got {row_count}"
            )

        con.execute(
            f"copy ({build_sql}) to ? (format parquet)",
            [str(output_path)],
        )
    finally:
        con.close()

    return int(row_count), [(str(t), int(r), float(o)) for t, r, o in teams]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export top-N World Cup winner odds after each WC2026 game. "
            "Teams are ranked by implied probability at tournament start."
        )
    )
    parser.add_argument("--duckdb-path", type=Path, default=DEFAULT_DUCKDB)
    parser.add_argument("--hourly-parquet", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--team-count",
        type=int,
        default=DEFAULT_TEAM_COUNT,
        help="Number of pre-tournament favorites to track (default: 5).",
    )
    args = parser.parse_args(argv)

    hourly_parquet = _resolve_hourly_parquet(args.hourly_parquet)
    row_count, teams = build_top5_odds_to_win(
        duckdb_path=args.duckdb_path.resolve(),
        hourly_parquet=hourly_parquet,
        output_path=args.output.resolve(),
        team_count=args.team_count,
    )
    print(f"Wrote {row_count} rows to {args.output.resolve()}")
    print("Opening favorites:")
    for team, rank, opening_odds in teams:
        print(f"  {rank}. {team} ({opening_odds:.4f})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
