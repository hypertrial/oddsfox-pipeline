#!/usr/bin/env python3
"""Build Spain World Cup winner odds after each WC2026 game."""

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
DEFAULT_ALIASES: Final[Path] = REPO_ROOT / "dbt" / "seeds" / "wc2026_team_canonical_aliases.csv"
DEFAULT_HOURLY_PARQUET: Final[Path] = (
    MONOREPO_ROOT / "oddsfox-scraper" / "polymarket_wc2026_market_hourly_odds_20260805T183112Z.parquet"
)
DEFAULT_OUTPUT: Final[Path] = MONOREPO_ROOT / "spain_odds_to_win.parquet"

_BUILD_SQL = """
with schedule as (
    select *
    from openfootball_wc2026_staging.stg_openfootball_wc2026_schedule_fixtures
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
    from international_results_wc2026_marts.international_results_wc2026_matches as r
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
    select
        *,
        lower(home_team) = 'spain' or lower(away_team) = 'spain' as spain_involved
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
        false as spain_involved,
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
        spain_involved,
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
),
with_odds as (
    select
        c.*,
        o.odds_hour_utc,
        o.odds_hour_epoch,
        o.open_odds as spain_open_odds_to_win,
        o.high_odds as spain_high_odds_to_win,
        o.low_odds as spain_low_odds_to_win,
        o.close_odds as spain_odds_to_win,
        o.avg_odds as spain_avg_odds_to_win,
        o.observed_points,
        row_number() over (
            partition by c.game_sequence
            order by
                case when c.game_sequence = 0 then 0 else 1 end,
                case when c.game_sequence = 0 then o.odds_hour_utc end desc,
                case when c.game_sequence > 0 then o.odds_hour_utc end asc
        ) as odds_pick
    from checkpoints as c
    left join spain_hourly as o
        on (
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
    spain_involved,
    checkpoint_at_utc,
    odds_hour_utc,
    odds_hour_epoch,
    spain_open_odds_to_win,
    spain_high_odds_to_win,
    spain_low_odds_to_win,
    spain_odds_to_win,
    spain_avg_odds_to_win,
    observed_points
from with_odds
where odds_pick = 1
order by game_sequence
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


def build_spain_odds_to_win(
    *,
    duckdb_path: Path,
    aliases_path: Path,
    hourly_parquet: Path,
    output_path: Path,
) -> int:
    if not duckdb_path.exists():
        raise FileNotFoundError(f"DuckDB not found: {duckdb_path}")
    if not aliases_path.exists():
        raise FileNotFoundError(f"Team aliases seed not found: {aliases_path}")
    if not hourly_parquet.exists():
        raise FileNotFoundError(f"Hourly parquet not found: {hourly_parquet}")

    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        con.execute(
            "create temp table team_aliases as select * from read_csv(?, header=true)",
            [str(aliases_path)],
        )
        con.execute(
            """
            create temp table spain_hourly as
            select
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
              and group_item_title = 'Spain'
            """,
            [str(hourly_parquet)],
        )

        missing_ids = con.execute(
            """
            with schedule as (
                select *
                from openfootball_wc2026_staging.stg_openfootball_wc2026_schedule_fixtures
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
                from international_results_wc2026_marts.international_results_wc2026_matches as r
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
                    and abs(
                        date_diff('day', cast(s.kickoff_at_utc as date), r.match_date)
                    ) <= 1
            )
            select fifa_match_id
            from matched
            where home_score is null
            order by fifa_match_id
            """
        ).fetchall()
        if missing_ids:
            missing = [row[0] for row in missing_ids]
            raise RuntimeError(f"Missing match results for FIFA games: {missing}")

        row_count = con.execute(f"select count(*) from ({_BUILD_SQL})").fetchone()[0]
        if row_count != 105:
            raise RuntimeError(f"Expected 105 rows (start + 104 games), got {row_count}")

        con.execute(
            f"copy ({_BUILD_SQL}) to ? (format parquet)",
            [str(output_path)],
        )
    finally:
        con.close()

    return int(row_count)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb-path", type=Path, default=DEFAULT_DUCKDB)
    parser.add_argument("--aliases-path", type=Path, default=DEFAULT_ALIASES)
    parser.add_argument("--hourly-parquet", type=Path, default=None)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    hourly_parquet = _resolve_hourly_parquet(args.hourly_parquet)
    row_count = build_spain_odds_to_win(
        duckdb_path=args.duckdb_path.resolve(),
        aliases_path=args.aliases_path.resolve(),
        hourly_parquet=hourly_parquet,
        output_path=args.output.resolve(),
    )
    print(f"Wrote {row_count} rows to {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
