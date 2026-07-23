"""Independent WC2026 Polygon market manifest loading and validation."""

from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Mapping

from oddsfox_pipeline.config.settings import DBT_PROJECT_DIR

POLYGON_CHAIN_ID = 137
STANDARD_V2_EXCHANGE = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_V2_EXCHANGE = "0xe2222d279d744050d28e00520010520000310F59"
OPENFOOTBALL_REVISION = "bd46a148289f9930da66c140d4d7d2325e95d387"
DEFAULT_POLYGON_MARKET_SEED_PATH = (
    DBT_PROJECT_DIR / "seeds" / "polymarket_wc2026_polygon_settlement_markets.csv"
)

EXPECTED_GAMES = 104
EXPECTED_GROUP_PROPOSITIONS = 216
EXPECTED_KNOCKOUT_PROPOSITIONS = 32
EXPECTED_PROPOSITIONS = 248
EXPECTED_TOKENS = 496

SEED_COLUMNS = (
    "proposition_id",
    "fifa_match_id",
    "stage",
    "group_label",
    "home_team",
    "away_team",
    "kickoff_at_utc",
    "window_start_at_utc",
    "window_end_at_utc",
    "proposition_type",
    "yes_represents",
    "no_represents",
    "condition_id",
    "yes_token_id",
    "no_token_id",
    "market_structure",
    "exchange_address",
    "openfootball_revision",
    "openfootball_path",
    "openfootball_source_lines",
    "openfootball_line_hash",
    "condition_init_tx_hash",
    "condition_init_log_index",
    "question_init_tx_hash",
    "question_init_log_index",
    "ancillary_data_sha256",
    "token_verification_block_number",
    "token_verification_block_hash",
    "manifest_sha256",
    "manifest_version",
    "reviewed_at_utc",
)

_HEX_32 = re.compile(r"0x[0-9a-f]{64}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_UINT256 = re.compile(r"(?:0|[1-9][0-9]{0,77})\Z")
_REVISION = re.compile(r"[0-9a-f]{40}\Z")
_SOURCE_LINES = re.compile(r"[1-9][0-9]*(?:-[1-9][0-9]*)?\Z")
_SEMVER = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\Z")
_PROPOSITION_ID = re.compile(r"[a-z0-9][a-z0-9_-]*\Z")
_MAX_BIGINT = 2**63 - 1
_MAX_UINT256 = 2**256 - 1

_EXPECTED_STAGE_BY_MATCH = {
    **{match_id: "group_stage" for match_id in range(1, 73)},
    **{match_id: "round_of_32" for match_id in range(73, 89)},
    **{match_id: "round_of_16" for match_id in range(89, 97)},
    **{match_id: "quarterfinal" for match_id in range(97, 101)},
    **{match_id: "semifinal" for match_id in range(101, 103)},
    103: "third_place",
    104: "final",
}
_EXPECTED_STAGE_COUNTS = Counter(
    {
        "group_stage": 72,
        "round_of_32": 16,
        "round_of_16": 8,
        "quarterfinal": 4,
        "semifinal": 2,
        "third_place": 1,
        "final": 1,
    }
)
_GROUP_TYPES = {"home_win", "draw", "away_win"}
_SOURCE_PATHS = {"2026--usa/cup.txt", "2026--usa/cup_finals.txt"}


@dataclass(frozen=True)
class PolygonMarket:
    proposition_id: str
    fifa_match_id: int
    stage: str
    group_label: str | None
    home_team: str
    away_team: str
    kickoff_at_utc: datetime
    window_start_at_utc: datetime
    window_end_at_utc: datetime
    proposition_type: str
    yes_represents: str
    no_represents: str
    condition_id: str
    yes_token_id: str
    no_token_id: str
    market_structure: str
    exchange_address: str
    openfootball_revision: str
    openfootball_path: str
    openfootball_source_lines: str
    openfootball_line_hash: str
    condition_init_tx_hash: str
    condition_init_log_index: int
    question_init_tx_hash: str
    question_init_log_index: int
    ancillary_data_sha256: str
    token_verification_block_number: int
    token_verification_block_hash: str
    manifest_sha256: str
    manifest_version: str
    reviewed_at_utc: datetime


@dataclass(frozen=True)
class PolygonMarketManifest:
    markets: tuple[PolygonMarket, ...]
    sha256: str
    version: str

    @property
    def by_token(self) -> dict[str, tuple[PolygonMarket, str]]:
        return {
            token_id: (market, outcome)
            for market in self.markets
            for token_id, outcome in (
                (market.yes_token_id, "yes"),
                (market.no_token_id, "no"),
            )
        }


def _text(row: Mapping[str, str], field: str) -> str:
    value = str(row.get(field, "")).strip()
    if not value:
        raise ValueError(f"Seed field {field!r} must be non-empty")
    return value


def _integer(row: Mapping[str, str], field: str) -> int:
    value = _text(row, field)
    if not value.isascii() or not value.isdecimal():
        raise ValueError(f"Seed field {field!r} must be a non-negative integer")
    return int(value)


def _utc_datetime(row: Mapping[str, str], field: str) -> datetime:
    value = _text(row, field)
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"Seed field {field!r} is not an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Seed field {field!r} must be explicitly UTC")
    parsed = parsed.astimezone(timezone.utc)
    if parsed.second or parsed.microsecond:
        raise ValueError(f"Seed field {field!r} must be minute-aligned")
    return parsed


def parse_polygon_market(row: Mapping[str, str]) -> PolygonMarket:
    """Parse one strict, independently authored manifest row."""
    proposition_id = _text(row, "proposition_id")
    if not _PROPOSITION_ID.fullmatch(proposition_id):
        raise ValueError(f"Invalid proposition_id {proposition_id!r}")
    fifa_match_id = _integer(row, "fifa_match_id")
    stage = _text(row, "stage")
    expected_stage = _EXPECTED_STAGE_BY_MATCH.get(fifa_match_id)
    if stage != expected_stage:
        raise ValueError(
            f"FIFA match {fifa_match_id} expected stage {expected_stage!r}, got {stage!r}"
        )

    kickoff = _utc_datetime(row, "kickoff_at_utc")
    window_start = _utc_datetime(row, "window_start_at_utc")
    window_end = _utc_datetime(row, "window_end_at_utc")
    expected_minutes = 150 if stage == "group_stage" else 210
    if window_start != kickoff or window_end != kickoff + timedelta(
        minutes=expected_minutes
    ):
        raise ValueError(
            f"FIFA match {fifa_match_id} must use its fixed {expected_minutes}-minute window"
        )

    condition_id = _text(row, "condition_id")
    token_ids = (_text(row, "yes_token_id"), _text(row, "no_token_id"))
    if not _HEX_32.fullmatch(condition_id):
        raise ValueError(f"Invalid condition_id for {proposition_id}")
    if any(
        not _UINT256.fullmatch(token_id) or not 0 < int(token_id) <= _MAX_UINT256
        for token_id in token_ids
    ):
        raise ValueError(f"Invalid token ID for {proposition_id}")
    if token_ids[0] == token_ids[1]:
        raise ValueError(f"Yes/No token IDs must differ for {proposition_id}")

    market_structure = _text(row, "market_structure")
    exchange_address = _text(row, "exchange_address")
    exchanges = {
        "standard": STANDARD_V2_EXCHANGE,
        "neg_risk": NEG_RISK_V2_EXCHANGE,
    }
    expected_exchange = exchanges.get(market_structure)
    if (
        expected_exchange is None
        or exchange_address.casefold() != expected_exchange.casefold()
    ):
        raise ValueError(f"Invalid V2 exchange/market structure for {proposition_id}")

    hashes = (
        "condition_init_tx_hash",
        "question_init_tx_hash",
        "token_verification_block_hash",
    )
    for field in hashes:
        if not _HEX_32.fullmatch(_text(row, field)):
            raise ValueError(f"Seed field {field!r} must be canonical 32-byte hex")
    for field in ("openfootball_line_hash", "ancillary_data_sha256"):
        if not _SHA256.fullmatch(_text(row, field)):
            raise ValueError(f"Seed field {field!r} must be lowercase SHA-256")
    revision = _text(row, "openfootball_revision")
    source_path = _text(row, "openfootball_path")
    source_lines = _text(row, "openfootball_source_lines")
    if not _REVISION.fullmatch(revision) or source_path not in _SOURCE_PATHS:
        raise ValueError(f"Invalid pinned OpenFootball source for {proposition_id}")
    if not _SOURCE_LINES.fullmatch(source_lines):
        raise ValueError(f"Invalid OpenFootball source lines for {proposition_id}")
    source_bounds = tuple(int(value) for value in source_lines.split("-"))
    if len(source_bounds) == 2 and source_bounds[0] > source_bounds[1]:
        raise ValueError(f"OpenFootball source lines are reversed for {proposition_id}")
    version = _text(row, "manifest_version")
    if not _SEMVER.fullmatch(version):
        raise ValueError("manifest_version must be plain SemVer (major.minor.patch)")

    home_team = _text(row, "home_team")
    away_team = _text(row, "away_team")
    yes_represents = _text(row, "yes_represents")
    no_represents = _text(row, "no_represents")
    if home_team.casefold() == away_team.casefold():
        raise ValueError(f"Home and away teams must differ for {proposition_id}")
    if yes_represents.casefold() == no_represents.casefold():
        raise ValueError(f"Yes/No semantics must differ for {proposition_id}")

    condition_log_index = _integer(row, "condition_init_log_index")
    question_log_index = _integer(row, "question_init_log_index")
    verification_block = _integer(row, "token_verification_block_number")
    if condition_log_index > _MAX_BIGINT or question_log_index > _MAX_BIGINT:
        raise ValueError(f"Event log index exceeds BIGINT for {proposition_id}")
    if not 0 < verification_block <= _MAX_BIGINT:
        raise ValueError(f"Token verification block is invalid for {proposition_id}")

    return PolygonMarket(
        proposition_id=proposition_id,
        fifa_match_id=fifa_match_id,
        stage=stage,
        group_label=(str(row.get("group_label", "")).strip() or None),
        home_team=home_team,
        away_team=away_team,
        kickoff_at_utc=kickoff,
        window_start_at_utc=window_start,
        window_end_at_utc=window_end,
        proposition_type=_text(row, "proposition_type"),
        yes_represents=yes_represents,
        no_represents=no_represents,
        condition_id=condition_id,
        yes_token_id=token_ids[0],
        no_token_id=token_ids[1],
        market_structure=market_structure,
        exchange_address=expected_exchange,
        openfootball_revision=revision,
        openfootball_path=source_path,
        openfootball_source_lines=source_lines,
        openfootball_line_hash=_text(row, "openfootball_line_hash"),
        condition_init_tx_hash=_text(row, "condition_init_tx_hash"),
        condition_init_log_index=condition_log_index,
        question_init_tx_hash=_text(row, "question_init_tx_hash"),
        question_init_log_index=question_log_index,
        ancillary_data_sha256=_text(row, "ancillary_data_sha256"),
        token_verification_block_number=verification_block,
        token_verification_block_hash=_text(row, "token_verification_block_hash"),
        manifest_sha256=_text(row, "manifest_sha256"),
        manifest_version=version,
        reviewed_at_utc=_utc_datetime(row, "reviewed_at_utc"),
    )


def validate_polygon_market_manifest(
    markets: Iterable[PolygonMarket],
) -> tuple[PolygonMarket, ...]:
    """Fail closed unless the manifest is the complete 104-match inventory."""
    rows = tuple(markets)
    if len(rows) != EXPECTED_PROPOSITIONS:
        raise ValueError(
            f"Expected {EXPECTED_PROPOSITIONS} propositions; found {len(rows)}"
        )
    proposition_ids = {row.proposition_id for row in rows}
    condition_ids = {row.condition_id for row in rows}
    token_ids = {token for row in rows for token in (row.yes_token_id, row.no_token_id)}
    if len(proposition_ids) != EXPECTED_PROPOSITIONS:
        raise ValueError("proposition_id values must be unique")
    if len(condition_ids) != EXPECTED_PROPOSITIONS:
        raise ValueError("condition_id values must be unique")
    if len(token_ids) != EXPECTED_TOKENS:
        raise ValueError(f"Expected {EXPECTED_TOKENS} unique outcome tokens")
    condition_locators = {
        (row.condition_init_tx_hash, row.condition_init_log_index) for row in rows
    }
    question_locators = {
        (row.question_init_tx_hash, row.question_init_log_index) for row in rows
    }
    if len(condition_locators) != EXPECTED_PROPOSITIONS:
        raise ValueError("Condition initialization locators must be unique")
    if len(question_locators) != EXPECTED_PROPOSITIONS:
        raise ValueError("Question initialization locators must be unique")

    by_match: dict[int, list[PolygonMarket]] = defaultdict(list)
    for row in rows:
        by_match[row.fifa_match_id].append(row)
    if set(by_match) != set(range(1, EXPECTED_GAMES + 1)):
        raise ValueError("FIFA match IDs must be exactly 1..104")
    if Counter(rows[0].stage for rows in by_match.values()) != _EXPECTED_STAGE_COUNTS:
        raise ValueError("WC2026 stage distribution does not match 72/16/8/4/2/1/1")
    if {row.openfootball_revision for row in rows} != {OPENFOOTBALL_REVISION}:
        raise ValueError("Manifest must use the pinned OpenFootball revision")
    if len({row.reviewed_at_utc for row in rows}) != 1:
        raise ValueError("All manifest rows must use one review timestamp")
    if any(
        row.market_structure != "neg_risk"
        or row.exchange_address.casefold() != NEG_RISK_V2_EXCHANGE.casefold()
        or row.openfootball_path != "2026--usa/cup.txt"
        for row in rows
        if row.stage == "group_stage"
    ):
        raise ValueError("All group propositions must use the neg-risk V2 exchange")
    if any(
        row.market_structure != "standard"
        or row.exchange_address.casefold() != STANDARD_V2_EXCHANGE.casefold()
        or row.openfootball_path != "2026--usa/cup_finals.txt"
        for row in rows
        if row.stage != "group_stage"
    ):
        raise ValueError("All knockout propositions must use the standard V2 exchange")

    fixture_fields = (
        "stage",
        "group_label",
        "home_team",
        "away_team",
        "kickoff_at_utc",
        "window_start_at_utc",
        "window_end_at_utc",
        "openfootball_revision",
        "openfootball_path",
        "openfootball_source_lines",
        "openfootball_line_hash",
    )
    for match_id, match_rows in by_match.items():
        first = match_rows[0]
        if any(
            getattr(row, field) != getattr(first, field)
            for row in match_rows[1:]
            for field in fixture_fields
        ):
            raise ValueError(
                f"Inconsistent repeated fixture facts for match {match_id}"
            )
        types = {row.proposition_type for row in match_rows}
        if match_id <= 72:
            if len(match_rows) != 3 or types != _GROUP_TYPES or not first.group_label:
                raise ValueError(
                    f"Group match {match_id} must have three canonical propositions"
                )
        else:
            expected_type = (
                "home_win_third_place"
                if match_id == 103
                else "home_wins_final"
                if match_id == 104
                else "home_advances"
            )
            if len(match_rows) != 1 or types != {expected_type} or first.group_label:
                raise ValueError(
                    f"Knockout match {match_id} must have proposition {expected_type!r}"
                )

    versions = {row.manifest_version for row in rows}
    if len(versions) != 1:
        raise ValueError("All manifest rows must use one manifest_version")
    declared_hashes = {row.manifest_sha256 for row in rows}
    if len(declared_hashes) != 1 or not _SHA256.fullmatch(next(iter(declared_hashes))):
        raise ValueError("All manifest rows must declare one lowercase manifest_sha256")
    actual_hash = polygon_manifest_content_sha256(rows)
    if declared_hashes != {actual_hash}:
        raise ValueError(
            "manifest_sha256 does not match canonical logical seed content"
        )
    return tuple(sorted(rows, key=lambda row: (row.fifa_match_id, row.proposition_id)))


def polygon_manifest_content_sha256(markets: Iterable[PolygonMarket]) -> str:
    """Hash normalized logical rows, excluding the repeated hash declaration."""
    values: list[list[str]] = []
    for row in sorted(
        markets, key=lambda item: (item.fifa_match_id, item.proposition_id)
    ):
        logical_row: list[str] = []
        for field in SEED_COLUMNS:
            if field == "manifest_sha256":
                continue
            value = getattr(row, field)
            if isinstance(value, datetime):
                normalized = (
                    value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
                )
            elif value is None:
                normalized = ""
            else:
                normalized = str(value)
            logical_row.append(normalized)
        values.append(logical_row)
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def load_polygon_market_seed(
    path: Path = DEFAULT_POLYGON_MARKET_SEED_PATH,
) -> PolygonMarketManifest:
    """Read and fully validate the committed Polygon market manifest."""
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != SEED_COLUMNS:
            raise ValueError(
                "Polygon market seed headers do not match the required audited schema"
            )
        rows = validate_polygon_market_manifest(
            parse_polygon_market(row) for row in reader
        )
    return PolygonMarketManifest(
        markets=rows,
        sha256=rows[0].manifest_sha256,
        version=rows[0].manifest_version,
    )


__all__ = [
    "DEFAULT_POLYGON_MARKET_SEED_PATH",
    "NEG_RISK_V2_EXCHANGE",
    "OPENFOOTBALL_REVISION",
    "POLYGON_CHAIN_ID",
    "SEED_COLUMNS",
    "STANDARD_V2_EXCHANGE",
    "PolygonMarket",
    "PolygonMarketManifest",
    "load_polygon_market_seed",
    "parse_polygon_market",
    "polygon_manifest_content_sha256",
    "validate_polygon_market_manifest",
]
