"""Deterministic, date-batched Elo ratings for soccer matches."""

from __future__ import annotations

import hashlib
import itertools
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from math import fsum
from typing import Final

RATING_POOLS: Final = frozenset(
    {"club_men", "club_women", "national_men", "national_women"}
)
DEFAULT_RATING: Final = 1500.0


@dataclass(frozen=True, slots=True)
class HistoricalMatch:
    """One canonical, completed result used by the rating model."""

    match_id: str
    match_date: date
    home_team_id: str
    away_team_id: str
    home_score: int
    away_score: int
    rating_pool: str
    neutral: bool = False
    friendly: bool = False

    def __post_init__(self) -> None:
        if self.rating_pool not in RATING_POOLS:
            raise ValueError(f"unsupported rating pool: {self.rating_pool!r}")
        if not self.match_id or not self.home_team_id or not self.away_team_id:
            raise ValueError("match and team identifiers must be non-empty")
        if self.home_team_id == self.away_team_id:
            raise ValueError("a team cannot play itself")
        if self.home_score < 0 or self.away_score < 0:
            raise ValueError("scores must be non-negative")

    @property
    def home_result(self) -> float:
        if self.home_score > self.away_score:
            return 1.0
        if self.home_score < self.away_score:
            return 0.0
        return 0.5


@dataclass(frozen=True, slots=True)
class EloParameters:
    d: int
    k: int
    home_advantage: int
    validation_matches: int = 0
    validation_mse: float | None = None
    used_fallback: bool = False


@dataclass(frozen=True, slots=True)
class PreMatchRating:
    team_id: str
    rating_pool: str
    target_date: date
    rating: float | None
    quality: str
    prior_match_count: int
    last_result_date: date | None
    rating_age_days: int | None
    connected_component_id: str | None


def expected_score(
    home_rating: float,
    away_rating: float,
    *,
    d: int,
    home_advantage: int,
) -> float:
    """Return the home team's expected score under the configured Elo curve."""
    if d <= 0:
        raise ValueError("d must be positive")
    return 1.0 / (1.0 + 10.0 ** (-(home_rating + home_advantage - away_rating) / d))


class _Components:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}
        self.minimum: dict[str, str] = {}

    def add(self, team_id: str) -> None:
        if team_id not in self.parent:
            self.parent[team_id] = team_id
            self.minimum[team_id] = team_id

    def find(self, team_id: str) -> str:
        parent = self.parent[team_id]
        if parent != team_id:
            self.parent[team_id] = self.find(parent)
        return self.parent[team_id]

    def union(self, left: str, right: str) -> None:
        self.add(left)
        self.add(right)
        left_root = self.find(left)
        right_root = self.find(right)
        if left_root == right_root:
            return
        first, second = sorted((left_root, right_root))
        self.parent[second] = first
        self.minimum[first] = min(self.minimum[left_root], self.minimum[right_root])
        self.minimum.pop(second, None)

    def component_id(self, rating_pool: str, team_id: str) -> str | None:
        if team_id not in self.parent:
            return None
        minimum = self.minimum[self.find(team_id)]
        return hashlib.sha256(f"{rating_pool}\0{minimum}".encode()).hexdigest()[:16]


def _ordered(matches: Iterable[HistoricalMatch]) -> tuple[HistoricalMatch, ...]:
    rows = tuple(matches)
    match_ids = [row.match_id for row in rows]
    if len(match_ids) != len(set(match_ids)):
        raise ValueError("match_id must be unique")
    return tuple(
        sorted(
            rows,
            key=lambda row: (
                row.rating_pool,
                row.match_date,
                row.home_team_id,
                row.away_team_id,
                row.match_id,
            ),
        )
    )


def _quality(prior_matches: int, age_days: int | None) -> str:
    if prior_matches == 0:
        return "missing"
    if prior_matches >= 10 and age_days is not None and age_days <= 365:
        return "stable"
    return "provisional"


def compute_pre_match_ratings(
    matches: Iterable[HistoricalMatch],
    parameters: Mapping[str, EloParameters],
    targets: Iterable[tuple[str, date, str]],
) -> dict[tuple[str, date, str], PreMatchRating]:
    """Replay results and capture each requested team strictly before its date.

    All matches on a calendar date use ratings from the start of that date. Their
    deltas are summed before state is mutated, making input order irrelevant.
    """
    ordered = _ordered(matches)
    targets_by_pool_date: dict[tuple[str, date], set[str]] = defaultdict(set)
    for rating_pool, target_date, team_id in targets:
        if rating_pool not in RATING_POOLS:
            raise ValueError(f"unsupported rating pool: {rating_pool!r}")
        targets_by_pool_date[(rating_pool, target_date)].add(team_id)

    output: dict[tuple[str, date, str], PreMatchRating] = {}
    matches_by_pool: dict[str, list[HistoricalMatch]] = defaultdict(list)
    for match in ordered:
        matches_by_pool[match.rating_pool].append(match)

    for rating_pool in sorted({pool for pool, _ in targets_by_pool_date}):
        if rating_pool not in parameters:
            raise ValueError(f"missing Elo parameters for {rating_pool}")
        params = parameters[rating_pool]
        ratings: dict[str, float] = {}
        counts: dict[str, int] = defaultdict(int)
        last_dates: dict[str, date] = {}
        components = _Components()
        pool_matches = matches_by_pool.get(rating_pool, [])
        matches_by_date = {
            match_date: tuple(group)
            for match_date, group in itertools.groupby(
                pool_matches, key=lambda row: row.match_date
            )
        }
        dates = sorted(
            set(matches_by_date)
            | {
                target_date
                for pool, target_date in targets_by_pool_date
                if pool == rating_pool
            }
        )
        for current_date in dates:
            for team_id in sorted(
                targets_by_pool_date.get((rating_pool, current_date), set())
            ):
                prior = counts[team_id]
                last_date = last_dates.get(team_id)
                age = (current_date - last_date).days if last_date else None
                output[(rating_pool, current_date, team_id)] = PreMatchRating(
                    team_id=team_id,
                    rating_pool=rating_pool,
                    target_date=current_date,
                    rating=ratings.get(team_id) if prior else None,
                    quality=_quality(prior, age),
                    prior_match_count=prior,
                    last_result_date=last_date,
                    rating_age_days=age,
                    connected_component_id=components.component_id(
                        rating_pool, team_id
                    ),
                )

            deltas: dict[str, list[float]] = defaultdict(list)
            appearances: dict[str, int] = defaultdict(int)
            day_matches = matches_by_date.get(current_date, ())
            for match in day_matches:
                home_rating = ratings.get(match.home_team_id, DEFAULT_RATING)
                away_rating = ratings.get(match.away_team_id, DEFAULT_RATING)
                expected = expected_score(
                    home_rating,
                    away_rating,
                    d=params.d,
                    home_advantage=0 if match.neutral else params.home_advantage,
                )
                effective_k = params.k * (0.5 if match.friendly else 1.0)
                delta = effective_k * (match.home_result - expected)
                deltas[match.home_team_id].append(delta)
                deltas[match.away_team_id].append(-delta)
                appearances[match.home_team_id] += 1
                appearances[match.away_team_id] += 1

            for team_id in sorted(deltas):
                ratings[team_id] = ratings.get(team_id, DEFAULT_RATING) + fsum(
                    sorted(deltas[team_id])
                )
                counts[team_id] += appearances[team_id]
                last_dates[team_id] = current_date
            for match in day_matches:
                components.union(match.home_team_id, match.away_team_id)

    return output


def _validation_mse(
    matches: tuple[HistoricalMatch, ...],
    params: EloParameters,
    *,
    validation_start: date,
    validation_end: date,
) -> tuple[int, float]:
    ratings: dict[str, float] = {}
    squared_errors: list[float] = []
    for current_date, day_group in itertools.groupby(
        matches, key=lambda row: row.match_date
    ):
        day_matches = tuple(day_group)
        deltas: dict[str, list[float]] = defaultdict(list)
        for match in day_matches:
            home_rating = ratings.get(match.home_team_id, DEFAULT_RATING)
            away_rating = ratings.get(match.away_team_id, DEFAULT_RATING)
            expected = expected_score(
                home_rating,
                away_rating,
                d=params.d,
                home_advantage=0 if match.neutral else params.home_advantage,
            )
            if validation_start <= current_date <= validation_end:
                squared_errors.append((match.home_result - expected) ** 2)
            effective_k = params.k * (0.5 if match.friendly else 1.0)
            delta = effective_k * (match.home_result - expected)
            deltas[match.home_team_id].append(delta)
            deltas[match.away_team_id].append(-delta)
        for team_id in sorted(deltas):
            ratings[team_id] = ratings.get(team_id, DEFAULT_RATING) + fsum(
                sorted(deltas[team_id])
            )
    count = len(squared_errors)
    return count, fsum(squared_errors) / count if count else float("inf")


def tune_parameters(
    matches: Iterable[HistoricalMatch],
    *,
    validation_start: date = date(2022, 1, 1),
    validation_end: date = date(2023, 12, 31),
    minimum_validation_matches: int = 500,
) -> dict[str, EloParameters]:
    """Tune one chronological parameter set per pool using validation MSE."""
    ordered = _ordered(matches)
    by_pool: dict[str, tuple[HistoricalMatch, ...]] = {}
    for rating_pool, group in itertools.groupby(
        ordered, key=lambda row: row.rating_pool
    ):
        by_pool[rating_pool] = tuple(group)

    selected: dict[str, EloParameters] = {}
    for rating_pool in sorted(RATING_POOLS):
        pool_matches = tuple(
            row
            for row in by_pool.get(rating_pool, ())
            if row.match_date <= validation_end
        )
        validation_count = sum(
            validation_start <= row.match_date <= validation_end for row in pool_matches
        )
        if validation_count < minimum_validation_matches:
            selected[rating_pool] = EloParameters(
                d=400,
                k=20,
                home_advantage=60,
                validation_matches=validation_count,
                validation_mse=None,
                used_fallback=True,
            )
            continue

        candidates: list[tuple[float, int, int, int, EloParameters]] = []
        for d, k, home_advantage in itertools.product(
            (300, 400, 500), (10, 20, 30, 40), (0, 50, 75, 100)
        ):
            params = EloParameters(d=d, k=k, home_advantage=home_advantage)
            count, mse = _validation_mse(
                pool_matches,
                params,
                validation_start=validation_start,
                validation_end=validation_end,
            )
            candidates.append((mse, abs(d - 400), k, home_advantage, params))
        mse, _, _, _, winner = min(candidates, key=lambda row: row[:4])
        selected[rating_pool] = EloParameters(
            d=winner.d,
            k=winner.k,
            home_advantage=winner.home_advantage,
            validation_matches=validation_count,
            validation_mse=mse,
            used_fallback=False,
        )
    return selected


__all__ = [
    "DEFAULT_RATING",
    "RATING_POOLS",
    "EloParameters",
    "HistoricalMatch",
    "PreMatchRating",
    "compute_pre_match_ratings",
    "expected_score",
    "tune_parameters",
]
