"""Optional external Elo benchmark normalization and strict as-of lookup."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from math import isfinite


@dataclass(frozen=True, slots=True)
class BenchmarkRating:
    system: str
    team_id: str
    rating: float
    as_of_date: date
    snapshot_id: str
    mapping_method: str
    is_pre_match: bool = False

    def __post_init__(self) -> None:
        if not self.system or not self.team_id or not self.snapshot_id:
            raise ValueError("benchmark system, team, and snapshot are required")
        if not isfinite(self.rating):
            raise ValueError("benchmark rating must be finite")


class BenchmarkIndex:
    def __init__(self, rows: Iterable[BenchmarkRating]) -> None:
        grouped: dict[tuple[str, str], list[BenchmarkRating]] = defaultdict(list)
        for row in rows:
            grouped[(row.system.casefold(), row.team_id)].append(row)
        self._rows = {
            key: tuple(
                sorted(
                    values,
                    key=lambda row: (
                        row.as_of_date,
                        row.snapshot_id,
                        row.mapping_method,
                        row.rating,
                    ),
                )
            )
            for key, values in grouped.items()
        }

    def latest_before(
        self, system: str, team_id: str, match_date: date
    ) -> BenchmarkRating | None:
        eligible = [
            row
            for row in self._rows.get((system.casefold(), team_id), ())
            if row.as_of_date < match_date
            or (row.is_pre_match and row.as_of_date == match_date)
        ]
        return eligible[-1] if eligible else None


def reconstruct_eloratings(
    rows: Iterable[Mapping[str, object]],
) -> tuple[BenchmarkRating, ...]:
    """Reverse EloRatings post-match values and changes into pre-match ratings."""
    output: list[BenchmarkRating] = []
    for row in rows:
        match_date = row["match_date"]
        if not isinstance(match_date, date):
            match_date = date.fromisoformat(str(match_date)[:10])
        home_post = float(row["home_post_rating"])
        away_post = float(row["away_post_rating"])
        change = float(row["home_rating_change"])
        common = {
            "system": "EloRatings",
            "as_of_date": match_date,
            "snapshot_id": str(row["snapshot_id"]),
            "mapping_method": str(row.get("mapping_method") or "reviewed_alias"),
            "is_pre_match": True,
        }
        output.append(
            BenchmarkRating(
                team_id=str(row["home_team_id"]),
                rating=home_post - change,
                **common,
            )
        )
        output.append(
            BenchmarkRating(
                team_id=str(row["away_team_id"]),
                rating=away_post + change,
                **common,
            )
        )
    return tuple(output)


__all__ = ["BenchmarkIndex", "BenchmarkRating", "reconstruct_eloratings"]
