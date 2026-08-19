"""Reviewed, pool-safe soccer team identity resolution."""

from __future__ import annotations

import difflib
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Final

from oddsfox_pipeline.features.pre_match_elo.elo import RATING_POOLS

MAPPING_STATUSES: Final = frozenset({"exact", "reviewed_alias"})


class IdentityContractError(ValueError):
    """Raised when reviewed identity data is invalid or contradictory."""


def normalize_team_name(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(char for char in normalized if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", ascii_text.casefold()).split())


@dataclass(frozen=True, slots=True)
class IdentityRow:
    source_system: str
    source_name: str
    team_id: str
    canonical_display_name: str
    rating_pool: str
    country: str | None
    confederation: str | None
    mapping_status: str

    def __post_init__(self) -> None:
        if self.rating_pool not in RATING_POOLS:
            raise IdentityContractError(f"invalid rating pool: {self.rating_pool}")
        if self.mapping_status not in MAPPING_STATUSES:
            raise IdentityContractError(
                "identity rows must be exact or reviewed_alias mappings"
            )
        if not all(
            (
                self.source_system,
                self.source_name,
                self.team_id,
                self.canonical_display_name,
            )
        ):
            raise IdentityContractError("identity fields must be non-empty")


@dataclass(frozen=True, slots=True)
class Resolution:
    source_system: str
    source_name: str
    rating_pool: str | None
    team_id: str | None
    canonical_display_name: str | None
    country: str | None
    confederation: str | None
    status: str
    candidate_team_ids: tuple[str, ...] = ()


class IdentityRegistry:
    def __init__(self, rows: Iterable[IdentityRow]) -> None:
        self.rows = tuple(rows)
        self._aliases: dict[tuple[str, str, str], IdentityRow] = {}
        self._canonical: dict[tuple[str, str], list[IdentityRow]] = defaultdict(list)
        self._systems: dict[tuple[str, str], list[IdentityRow]] = defaultdict(list)
        teams: dict[str, tuple[str, str, str | None, str | None]] = {}
        for row in self.rows:
            identity = (
                row.rating_pool,
                normalize_team_name(row.canonical_display_name),
                row.country,
                row.confederation,
            )
            if row.team_id in teams and teams[row.team_id] != identity:
                raise IdentityContractError(
                    f"team identity metadata is inconsistent: {row.team_id}"
                )
            teams[row.team_id] = identity
            alias_key = (
                row.rating_pool,
                row.source_system,
                normalize_team_name(row.source_name),
            )
            existing = self._aliases.get(alias_key)
            if existing and existing.team_id != row.team_id:
                raise IdentityContractError(
                    f"alias maps to multiple teams: {row.source_system}/{row.source_name}"
                )
            self._aliases[alias_key] = row
            canonical_key = (
                row.rating_pool,
                normalize_team_name(row.canonical_display_name),
            )
            canonical = self._canonical[canonical_key]
            if all(existing.team_id != row.team_id for existing in canonical):
                canonical.append(row)
            self._systems[
                (row.source_system, normalize_team_name(row.source_name))
            ].append(row)

    def _unscoped_rows(self, source_system: str, source_name: str) -> list[IdentityRow]:
        normalized = normalize_team_name(source_name)
        rows = list(self._systems.get((source_system, normalized), ()))
        return list({(row.rating_pool, row.team_id): row for row in rows}.values())

    def resolve(
        self, source_system: str, source_name: str, rating_pool: str
    ) -> Resolution:
        normalized = normalize_team_name(source_name)
        row = self._aliases.get((rating_pool, source_system, normalized))
        status = row.mapping_status if row else None
        if row:
            return Resolution(
                source_system,
                source_name,
                rating_pool,
                row.team_id,
                row.canonical_display_name,
                row.country,
                row.confederation,
                str(status),
            )
        candidates = self.fuzzy_candidates(source_name, rating_pool)
        return Resolution(
            source_system,
            source_name,
            rating_pool,
            None,
            None,
            None,
            None,
            "unmapped",
            candidates,
        )

    def resolve_without_pool(self, source_system: str, source_name: str) -> Resolution:
        candidates = self._unscoped_rows(source_system, source_name)
        unique = {(row.rating_pool, row.team_id): row for row in candidates}
        if len(unique) == 1:
            row = next(iter(unique.values()))
            return self.resolve(source_system, source_name, row.rating_pool)
        if len(unique) > 1:
            return Resolution(
                source_system,
                source_name,
                None,
                None,
                None,
                None,
                None,
                "ambiguous",
                tuple(sorted({row.team_id for row in unique.values()})),
            )
        return Resolution(
            source_system,
            source_name,
            None,
            None,
            None,
            None,
            None,
            "unmapped",
        )

    def resolve_pair(
        self, source_system: str, home_name: str, away_name: str
    ) -> tuple[Resolution, Resolution]:
        """Resolve a pair when its reviewed aliases identify one common pool."""
        home_rows = self._unscoped_rows(source_system, home_name)
        away_rows = self._unscoped_rows(source_system, away_name)
        common_pools = {row.rating_pool for row in home_rows} & {
            row.rating_pool for row in away_rows
        }
        if len(common_pools) == 1:
            rating_pool = next(iter(common_pools))
            return (
                self.resolve(source_system, home_name, rating_pool),
                self.resolve(source_system, away_name, rating_pool),
            )
        return (
            self.resolve_without_pool(source_system, home_name),
            self.resolve_without_pool(source_system, away_name),
        )

    def fuzzy_candidates(
        self, source_name: str, rating_pool: str, *, limit: int = 5
    ) -> tuple[str, ...]:
        normalized = normalize_team_name(source_name)
        names: dict[str, str] = {}
        for (pool, canonical_name), rows in self._canonical.items():
            if pool == rating_pool and len(rows) == 1:
                names[canonical_name] = rows[0].team_id
        matches = difflib.get_close_matches(normalized, names, n=limit, cutoff=0.78)
        return tuple(names[name] for name in matches)


@dataclass(frozen=True, slots=True)
class CanonicalResult:
    source_match_id: str
    match_date: object
    home_team_id: str
    away_team_id: str
    home_score: int
    away_score: int
    competition: str
    rating_pool: str
    neutral: bool
    friendly: bool
    source: str
    snapshot_id: str
    provenance: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SourceConflict:
    rating_pool: str
    match_date: object
    home_team_id: str
    away_team_id: str
    scores: tuple[tuple[int, int], ...]
    source_match_ids: tuple[str, ...]


def canonicalize_and_deduplicate(
    raw_rows: Iterable[object], registry: IdentityRegistry
) -> tuple[
    tuple[CanonicalResult, ...], tuple[SourceConflict, ...], tuple[Resolution, ...]
]:
    """Resolve result teams, collapse identical rows, and quarantine conflicts."""
    grouped: dict[tuple[str, object, str, str], list[object]] = defaultdict(list)
    unresolved: list[Resolution] = []
    for row in raw_rows:
        home = registry.resolve(row.source, row.home_name, row.rating_pool)
        away = registry.resolve(row.source, row.away_name, row.rating_pool)
        if home.team_id is None or away.team_id is None:
            if home.team_id is None:
                unresolved.append(home)
            if away.team_id is None:
                unresolved.append(away)
            continue
        key = (row.rating_pool, row.match_date, home.team_id, away.team_id)
        grouped[key].append(row)

    canonical: list[CanonicalResult] = []
    conflicts: list[SourceConflict] = []
    for key in sorted(grouped, key=lambda item: tuple(str(value) for value in item)):
        rows = grouped[key]
        scores = sorted({(row.home_score, row.away_score) for row in rows})
        if len(scores) != 1:
            conflicts.append(
                SourceConflict(
                    *key,
                    tuple(scores),
                    tuple(sorted(row.source_match_id for row in rows)),
                )
            )
            continue
        representative = min(rows, key=lambda row: row.source_match_id)
        canonical.append(
            CanonicalResult(
                source_match_id=representative.source_match_id,
                match_date=representative.match_date,
                home_team_id=key[2],
                away_team_id=key[3],
                home_score=scores[0][0],
                away_score=scores[0][1],
                competition=representative.competition,
                rating_pool=key[0],
                neutral=any(row.neutral for row in rows),
                friendly=all(row.friendly for row in rows),
                source=representative.source,
                snapshot_id=representative.snapshot_id,
                provenance=tuple(sorted(row.source_match_id for row in rows)),
            )
        )
    unique_unresolved = {
        (row.source_system, row.source_name, row.rating_pool): row for row in unresolved
    }
    return (
        tuple(canonical),
        tuple(conflicts),
        tuple(unique_unresolved[key] for key in sorted(unique_unresolved)),
    )


def rows_from_mappings(rows: Iterable[Mapping[str, object]]) -> tuple[IdentityRow, ...]:
    return tuple(
        IdentityRow(
            source_system=str(row["source_system"]),
            source_name=str(row["source_name"]),
            team_id=str(row["team_id"]),
            canonical_display_name=str(row["canonical_display_name"]),
            rating_pool=str(row["rating_pool"]),
            country=str(row["country"]) if row.get("country") else None,
            confederation=str(row["confederation"])
            if row.get("confederation")
            else None,
            mapping_status=str(row["mapping_status"]),
        )
        for row in rows
    )


__all__ = [
    "CanonicalResult",
    "IdentityContractError",
    "IdentityRegistry",
    "IdentityRow",
    "Resolution",
    "SourceConflict",
    "canonicalize_and_deduplicate",
    "normalize_team_name",
    "rows_from_mappings",
]
