#!/usr/bin/env python3
"""Author the WC2026 Polygon market seed from pinned public evidence.

This developer tool deliberately writes only below ``artifacts/``.  It never
updates the reviewed dbt seed; promotion is a separate human review step.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import unicodedata
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from zoneinfo import ZoneInfo

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_src_on_path

REPO_ROOT = ensure_src_on_path()

from oddsfox_pipeline.ingestion.openfootball.schedule_fixtures import (  # noqa: E402
    _REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID,  # noqa: F401 - authoring audit surface
    FIFA_SCHEDULE_SHA256,
    FIFA_SCHEDULE_TITLE,
    FIFA_SCHEDULE_URL,
    OPENFOOTBALL_BASE,
    OPENFOOTBALL_REVISION,
    REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH,  # noqa: F401 - authoring audit surface
    Fixture,
    parse_openfootball_fixtures,
)
from oddsfox_pipeline.ingestion.openfootball.schedule_fixtures import (  # noqa: E402
    OPENFOOTBALL_FILES as SCHEDULE_OPENFOOTBALL_FILES,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (  # noqa: E402
    write_polygon_resolution_attestation,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import (  # noqa: E402
    PolygonRPC,
    PolygonRPCError,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (  # noqa: E402
    NEG_RISK_V2_EXCHANGE,
    SEED_COLUMNS,
    STANDARD_V2_EXCHANGE,
    parse_polygon_market,
    polygon_manifest_content_sha256,
    validate_polygon_market_manifest,
)
from oddsfox_pipeline.resources.outbound_url import (  # noqa: E402
    validate_outbound_https_url,
)
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (  # noqa: E402
    validate_polygon_provider_label,
)

CHAIN_ID = 137
CTF = "0x4d97dcd97ec945f40cf65f87097ace5ea0476045"
NEG_RISK_ADAPTER = "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296"
USDC_E = "0x2791bca1f2de4661ed88a30c99a7a9449aa84174"

# Audited source pins.  Only the event layouts and view selectors used below
# are hand-coded; no upstream ABI package is vendored.
CTF_REVISION = "eeefca66eb46c800a9aaab88db2064a99026fde5"
UMA_REVISION = "8b76cc9e0d46c6f7450a0adb0ddc0f5b0568c9cc"
NEG_RISK_REVISION = "f78b35b0863b4308a431ca307d06f49b2ea65e78"
V2_REVISION = "ccc0596074f4dfd62c944fbca4de252893b82b4b"

OPENFOOTBALL_FILES = {
    **SCHEDULE_OPENFOOTBALL_FILES,
    "LICENSE.md": "36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673",
}

CONDITION_PREPARATION_TOPIC = (
    "0xab3760c3bd2bb38b5bcf54dc79802ed67338b4cf29f3054ded67ed24661e4177"
)
CONDITION_RESOLUTION_TOPIC = (
    "0xb44d84d3289691f71497564b85d4233648d9dbae8cbdbb4329f301c3a0185894"
)
QUESTION_INITIALIZED_TOPIC = (
    "0xeee0897acd6893adcaf2ba5158191b3601098ab6bece35c5d57874340b64c5b7"
)
ANCILLARY_UPDATED_TOPIC = (
    "0x0059e11815211969c0c4aaf3f498b52b6c2f2d14f286275d0862d70de22a836b"
)
MARKET_PREPARED_TOPIC = (
    "0xf059ab16d1ca60e123eab60e3c02b68faf060347c701a5d14885a8e1def7b3a8"
)
ADAPTER_QUESTION_PREPARED_TOPIC = (
    "0xaac410f87d423a922a7b226ac68f0c2eaf5bf6d15e644ac0758c7f96e2c253f7"
)
OPERATOR_MARKET_PREPARED_TOPIC = (
    "0x8138c0666fe0f752ff38486f542284f127aef02642c9c8db716ee1088839eeb0"
)
OPERATOR_QUESTION_PREPARED_TOPIC = (
    "0xcdc45423ec79c60a3fe3de57272e598d71a4ec88822e822ac8e134184a8435aa"
)

GET_COLLECTION_ID = "856296f7"
GET_CTF_POSITION_ID = "39dd7530"
GET_NEG_RISK_POSITION_ID = "752b5ba5"
GET_OUTCOME_SLOT_COUNT = "d42dc0c2"
GET_OPERATOR_ORACLE = "7dc0d1d0"
GET_OPERATOR_NEG_RISK_ADAPTER = "25c0520a"
GET_UMA_CTF = "22a9339f"
GET_NEG_RISK_CTF = "22a9339f"
GET_NEG_RISK_COL = "a78695b0"

# The group inventory was initialized in one audited batch.  The broader
# knockout range begins before the first resolved bracket and ends after the
# final.  Ranges are evidence inputs, not runtime ingestion configuration.
GROUP_FROM_BLOCK = 85_196_000
GROUP_TO_BLOCK = 85_200_000
KNOCKOUT_FROM_BLOCK = 88_978_537
KNOCKOUT_TO_BLOCK = 90_591_335
SCAN_CHUNK_BLOCKS = 30_000
SEMANTIC_TITLE_ZONE = ZoneInfo("America/New_York")

# Five first-round questions were reinitialized after an earlier duplicate.
# A reviewer selected the later canonical initialization.  Keeping these
# explicit makes ambiguity fail closed instead of silently choosing "latest".
REVIEWED_QUESTION_OVERRIDES = {
    74: "0x0f01b43802e1414c21de98d7deb6276f7401778ed17c90b88f7ab8d48c4870f0",
    75: "0xdea9f4de15f52862a0ca68e23fb64bf7657ccc5c807c2bf1a02175f248be6652",
    76: "0x1b4edf8dbe4cad70e6a8e0bc1e980c6557540ba4b726acfbe4839808107c58e3",
    77: "0x3d1ef8068175b3b86e7cd7d676bad14bdc7857836142f5b269f0596b7654c4e4",
    78: "0x2828cc7f05aed0a144c5ffe948b7caa668602abbcb2366698a1e332f3d6fef5a",
}

_STAGES = {
    "Round of 32": "round_of_32",
    "Round of 16": "round_of_16",
    "Quarter-final": "quarterfinal",
    "Semi-final": "semifinal",
    "Match for third place": "third_place",
    "Final": "final",
}
_TEAM_ALIASES = {
    "south korea": ("south korea", "korea republic"),
    "czech republic": ("czech republic", "czechia"),
    "bosnia herzegovina": ("bosnia herzegovina", "bosnia and herzegovina"),
    "usa": ("usa", "united states"),
    "turkey": ("turkey", "turkiye"),
    "ivory coast": ("ivory coast", "cote d ivoire"),
    "iran": ("iran", "ir iran"),
    "cape verde": ("cape verde", "cabo verde"),
}


@dataclass(frozen=True)
class Question:
    question_id: str
    adapter: str
    creator: str
    ancillary_sha256: str
    transaction_hash: str
    log_index: int
    block_number: int
    block_hash: str
    proposition_type: str
    fixture_id: int
    semantic_title: str
    no_outcome_label: str
    yes_outcome_label: str


@dataclass(frozen=True)
class NegRiskQuestionChain:
    market_id: str
    question_id: str
    request_id: str
    operator: str
    adapter_market_log: dict[str, Any]
    operator_market_log: dict[str, Any]
    adapter_question_log: dict[str, Any]
    operator_question_log: dict[str, Any]


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.sub(r"[^a-z0-9]+", " ", value.casefold()).split())


def _team_variants(team: str) -> tuple[str, ...]:
    normalized = _normalize(team)
    return _TEAM_ALIASES.get(normalized, (normalized,))


def _contains_team(text: str, team: str) -> bool:
    normalized = _normalize(text)
    return any(variant in normalized for variant in _team_variants(team))


def _decode_dynamic_bytes(data: str, word_index: int = 0) -> bytes:
    if not re.fullmatch(r"0x[0-9a-fA-F]+", data) or (len(data) - 2) % 64:
        raise ValueError("Event data is not canonical ABI words")
    payload = data[2:]
    offset_word = payload[word_index * 64 : (word_index + 1) * 64]
    offset = int(offset_word, 16) * 2
    if offset + 64 > len(payload):
        raise ValueError("Dynamic ABI offset is outside event data")
    size = int(payload[offset : offset + 64], 16) * 2
    value = payload[offset + 64 : offset + 64 + size]
    if len(value) != size:
        raise ValueError("Dynamic ABI value is truncated")
    return bytes.fromhex(value)


def _title(ancillary: bytes) -> str:
    text = ancillary.decode("utf-8")
    return re.split(r",\s*description\s*:", text, maxsplit=1, flags=re.I)[0]


def _parse_question_semantics(ancillary: bytes) -> tuple[str, str, str]:
    """Decode the structured binary outcome mapping without retaining prose."""
    try:
        text = ancillary.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Question semantic payload is not UTF-8") from exc
    title_match = re.match(
        r"\s*q\s*:\s*title\s*:\s*(?P<title>.+?)\s*,\s*description\s*:",
        text,
        flags=re.I | re.S,
    )
    if title_match is None:
        raise ValueError("Question semantic payload lacks a structured title")
    mappings = list(
        re.finditer(
            r"\bres_data\s*:\s*p1\s*:\s*0(?:\.0+)?\s*,\s*"
            r"p2\s*:\s*1(?:\.0+)?\s*,\s*p3\s*:\s*0\.5(?:0+)?\.\s*"
            r"Where\s+p1\s+corresponds\s+to\s+(?P<no>.+?)\s*,\s*"
            r"p2\s+to\s+(?P<yes>.+?)\s*,\s*p3\s+to\s+"
            r"(?P<unknown>.+?)\.",
            text,
            flags=re.I | re.S,
        )
    )
    if len(mappings) != 1:
        raise ValueError("Question must contain one canonical binary outcome mapping")
    mapping = mappings[0]
    if _normalize(mapping.group("unknown")) not in {"unknown", "unknown 50 50"}:
        raise ValueError("Question p3 outcome is not the unknown/50-50 fallback")
    return (
        title_match.group("title").strip(),
        mapping.group("no").strip().strip('"'),
        mapping.group("yes").strip().strip('"'),
    )


def _team_position(text: str, team: str) -> int | None:
    normalized = _normalize(text)
    positions = [
        normalized.find(variant)
        for variant in _team_variants(team)
        if normalized.find(variant) >= 0
    ]
    return min(positions) if positions else None


def _outcome_names_team(value: str, team: str) -> bool:
    return _normalize(value) in _team_variants(team)


def _validate_question_orientation(
    title: str,
    no_label: str,
    yes_label: str,
    fixture: Fixture,
    proposition_type: str,
) -> None:
    """Prove that binary YES/index-set 1 is the authored proposition side."""
    home_position = _team_position(title, fixture.home_team)
    away_position = _team_position(title, fixture.away_team)
    if fixture.fifa_match_id <= 72:
        if (_normalize(no_label), _normalize(yes_label)) != ("no", "yes"):
            raise ValueError("Group question binary outcomes are not No/Yes oriented")
        if proposition_type == "home_win":
            valid_title = home_position is not None and away_position is None
        elif proposition_type == "away_win":
            valid_title = away_position is not None and home_position is None
        else:
            valid_title = (
                home_position is not None
                and away_position is not None
                and home_position < away_position
            )
    else:
        valid_title = (
            home_position is not None
            and away_position is not None
            and home_position < away_position
            and _outcome_names_team(no_label, fixture.away_team)
            and _outcome_names_team(yes_label, fixture.home_team)
        )
    if not valid_title:
        raise ValueError(
            f"Question orientation disagrees with FIFA match {fixture.fifa_match_id}"
        )


def _hex_word(value: str | int) -> str:
    integer = (
        int(value, 16)
        if isinstance(value, str) and value.startswith("0x")
        else int(value)
    )
    return f"{integer:064x}"


def _address_word(value: str) -> str:
    return value.casefold().removeprefix("0x").rjust(64, "0")


def _topic_address(value: str) -> str:
    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError("Indexed address is malformed")
    return "0x" + value[-40:].casefold()


def _int_result(value: Any) -> int:
    if not isinstance(value, str) or not re.fullmatch(r"0x[0-9a-fA-F]{64}", value):
        raise ValueError("eth_call did not return one ABI word")
    return int(value, 16)


def _result_address(value: int, field: str) -> str:
    if value <= 0 or value >= 2**160:
        raise ValueError(f"{field} did not return a nonzero address")
    return f"0x{value:040x}"


class AuthoringRPC:
    def __init__(self, url: str) -> None:
        self.rpc = PolygonRPC(url, requests_per_second=5)

    def _raw_logs(
        self,
        from_block: int,
        to_block: int,
        *,
        address: str | None = None,
        topics: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        normalized_address = self._canonical_filter_address(address)
        normalized_topics = self._canonical_topic_filters(topics)
        query: dict[str, Any] = {
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "topics": normalized_topics,
        }
        if normalized_address is not None:
            query["address"] = normalized_address
        value = self.rpc.call("eth_getLogs", [query])
        if not isinstance(value, list) or any(
            not isinstance(row, dict) for row in value
        ):
            raise PolygonRPCError("Authoring eth_getLogs result is malformed")
        for row in value:
            self._verify_requested_filter(
                row,
                address=normalized_address,
                topics=normalized_topics,
            )
        return value

    @staticmethod
    def _canonical_filter_address(address: str | None) -> str | None:
        if address is None:
            return None
        normalized = str(address).casefold()
        if not re.fullmatch(r"0x[0-9a-f]{40}", normalized):
            raise PolygonRPCError("Authoring eth_getLogs address filter is malformed")
        return normalized

    @staticmethod
    def _canonical_topic_filters(topics: Sequence[Any]) -> list[Any]:
        def topic(value: Any) -> str:
            normalized = str(value).casefold()
            if not re.fullmatch(r"0x[0-9a-f]{64}", normalized):
                raise PolygonRPCError("Authoring eth_getLogs topic filter is malformed")
            return normalized

        normalized: list[Any] = []
        for value in topics:
            if value is None:
                normalized.append(None)
            elif isinstance(value, (list, tuple)):
                if not value:
                    raise PolygonRPCError(
                        "Authoring eth_getLogs OR topic filter is empty"
                    )
                normalized.append([topic(candidate) for candidate in value])
            else:
                normalized.append(topic(value))
        return normalized

    @staticmethod
    def _verify_requested_filter(
        row: dict[str, Any],
        *,
        address: str | None,
        topics: Sequence[Any],
    ) -> None:
        actual_address = str(row.get("address", "")).casefold()
        if not re.fullmatch(r"0x[0-9a-f]{40}", actual_address):
            raise PolygonRPCError("Authoring log address is malformed")
        if address is not None and actual_address != address:
            raise PolygonRPCError(
                "Authoring eth_getLogs response violates the address filter"
            )

        actual_topics_value = row.get("topics")
        if not isinstance(actual_topics_value, list):
            raise PolygonRPCError("Authoring log topics are missing")
        actual_topics: list[str] = []
        for value in actual_topics_value:
            normalized = str(value).casefold()
            if not re.fullmatch(r"0x[0-9a-f]{64}", normalized):
                raise PolygonRPCError("Authoring log topic is malformed")
            actual_topics.append(normalized)
        if len(actual_topics) < len(topics):
            raise PolygonRPCError(
                "Authoring eth_getLogs response violates the topic filter"
            )
        for index, expected in enumerate(topics):
            if expected is None:
                continue
            candidates = expected if isinstance(expected, list) else [expected]
            if actual_topics[index] not in candidates:
                raise PolygonRPCError(
                    "Authoring eth_getLogs response violates the topic filter"
                )

    def logs(
        self,
        from_block: int,
        to_block: int,
        *,
        address: str | None = None,
        topics: Sequence[Any] = (),
    ) -> list[dict[str, Any]]:
        value = self._raw_logs(
            from_block,
            to_block,
            address=address,
            topics=topics,
        )
        verified = [
            self._canonical_log(row, from_block=from_block, to_block=to_block)
            for row in value
        ]
        verified.sort(
            key=lambda row: (
                int(str(row["blockNumber"]), 16),
                int(str(row["transactionIndex"]), 16),
                int(str(row["logIndex"]), 16),
            )
        )
        locators = [
            (
                row["blockNumber"],
                row["transactionIndex"],
                row["logIndex"],
            )
            for row in verified
        ]
        if len(locators) != len(set(locators)):
            raise PolygonRPCError("Authoring eth_getLogs returned duplicate locators")
        return verified

    def canonical_candidate_log(
        self,
        row: dict[str, Any],
        *,
        from_block: int,
        to_block: int,
    ) -> dict[str, Any]:
        return self._canonical_log(
            row,
            from_block=from_block,
            to_block=to_block,
        )

    def _canonical_log(
        self,
        row: dict[str, Any],
        *,
        from_block: int,
        to_block: int,
    ) -> dict[str, Any]:
        """Validate one event against its canonical block before using it."""

        def quantity(field: str) -> int:
            value = str(row.get(field, "")).casefold()
            if not re.fullmatch(r"0x(?:0|[1-9a-f][0-9a-f]*)", value):
                raise PolygonRPCError(
                    f"Authoring log field {field!r} is not a canonical hex quantity"
                )
            return int(value, 16)

        def hex32(field: str) -> str:
            value = str(row.get(field, "")).casefold()
            if not re.fullmatch(r"0x[0-9a-f]{64}", value):
                raise PolygonRPCError(
                    f"Authoring log field {field!r} is not 32-byte hex"
                )
            return value

        if row.get("removed") is not False:
            raise PolygonRPCError("Removed or incomplete authoring log rejected")
        address = str(row.get("address", "")).casefold()
        if not re.fullmatch(r"0x[0-9a-f]{40}", address):
            raise PolygonRPCError("Authoring log address is malformed")
        topics_value = row.get("topics")
        if not isinstance(topics_value, list) or not topics_value:
            raise PolygonRPCError("Authoring log topics are missing")
        topics = []
        for topic in topics_value:
            normalized = str(topic).casefold()
            if not re.fullmatch(r"0x[0-9a-f]{64}", normalized):
                raise PolygonRPCError("Authoring log topic is malformed")
            topics.append(normalized)
        data = str(row.get("data", "")).casefold()
        if not re.fullmatch(r"0x(?:[0-9a-f]{64})*", data):
            raise PolygonRPCError("Authoring log data is not canonical ABI words")
        block_number = quantity("blockNumber")
        if not from_block <= block_number <= to_block:
            raise PolygonRPCError("Authoring log falls outside the requested range")
        block_hash = hex32("blockHash")
        transaction_hash = hex32("transactionHash")
        transaction_index = quantity("transactionIndex")
        log_index = quantity("logIndex")
        canonical_block = self.rpc.block(block_number)
        if canonical_block.hash != block_hash:
            raise PolygonRPCError(
                "Authoring log block hash disagrees with the canonical header"
            )
        return {
            **row,
            "address": address,
            "topics": topics,
            "data": data,
            "removed": False,
            "blockNumber": hex(canonical_block.number),
            "blockHash": canonical_block.hash,
            "transactionHash": transaction_hash,
            "transactionIndex": hex(transaction_index),
            "logIndex": hex(log_index),
        }

    def scan(
        self,
        from_block: int,
        to_block: int,
        *,
        address: str | None = None,
        topics: Sequence[Any] = (),
    ) -> Iterable[dict[str, Any]]:
        for start in range(from_block, to_block + 1, SCAN_CHUNK_BLOCKS):
            end = min(to_block, start + SCAN_CHUNK_BLOCKS - 1)
            yield from self._adaptive_logs(start, end, address=address, topics=topics)

    def scan_candidates(
        self,
        from_block: int,
        to_block: int,
        *,
        address: str | None = None,
        topics: Sequence[Any] = (),
    ) -> Iterable[dict[str, Any]]:
        """Yield raw candidates; callers must canonically validate selected rows."""
        for start in range(from_block, to_block + 1, SCAN_CHUNK_BLOCKS):
            end = min(to_block, start + SCAN_CHUNK_BLOCKS - 1)
            yield from self._adaptive_candidate_logs(
                start,
                end,
                address=address,
                topics=topics,
            )

    def _adaptive_logs(
        self,
        start: int,
        end: int,
        *,
        address: str | None,
        topics: Sequence[Any],
    ) -> Iterable[dict[str, Any]]:
        try:
            yield from self.logs(start, end, address=address, topics=topics)
        except PolygonRPCError:
            if start == end:
                raise
            middle = (start + end) // 2
            yield from self._adaptive_logs(
                start, middle, address=address, topics=topics
            )
            yield from self._adaptive_logs(
                middle + 1, end, address=address, topics=topics
            )

    def _adaptive_candidate_logs(
        self,
        start: int,
        end: int,
        *,
        address: str | None,
        topics: Sequence[Any],
    ) -> Iterable[dict[str, Any]]:
        try:
            yield from self._raw_logs(
                start,
                end,
                address=address,
                topics=topics,
            )
        except PolygonRPCError:
            if start == end:
                raise
            middle = (start + end) // 2
            yield from self._adaptive_candidate_logs(
                start,
                middle,
                address=address,
                topics=topics,
            )
            yield from self._adaptive_candidate_logs(
                middle + 1,
                end,
                address=address,
                topics=topics,
            )

    def call_at(self, contract: str, data: str, block: int) -> int:
        return _int_result(
            self.rpc.call("eth_call", [{"to": contract, "data": data}, hex(block)])
        )

    def keccak_bytes(self, value: bytes) -> str:
        result = str(self.rpc.call("web3_sha3", ["0x" + value.hex()])).casefold()
        if not re.fullmatch(r"0x[0-9a-f]{64}", result):
            raise PolygonRPCError("web3_sha3 did not return a 32-byte hash")
        return result


def _question_match(
    title: str, fixtures: Sequence[Fixture]
) -> tuple[Fixture, str] | None:
    normalized = _normalize(title)
    date_match = re.search(r"2026-(0[67])-[0-9]{2}", title)
    for fixture in fixtures:
        if not _contains_team(title, fixture.home_team) and not _contains_team(
            title, fixture.away_team
        ):
            continue
        if fixture.fifa_match_id <= 72:
            if "end in a draw" in normalized:
                if _contains_team(title, fixture.home_team) and _contains_team(
                    title, fixture.away_team
                ):
                    return fixture, "draw"
            elif " win on " in f" {normalized} ":
                if (
                    not date_match
                    or date_match.group(0)
                    != fixture.kickoff_at_utc.astimezone(SEMANTIC_TITLE_ZONE)
                    .date()
                    .isoformat()
                ):
                    continue
                if _contains_team(title, fixture.home_team):
                    return fixture, "home_win"
                if _contains_team(title, fixture.away_team):
                    return fixture, "away_win"
            continue
        both = _contains_team(title, fixture.home_team) and _contains_team(
            title, fixture.away_team
        )
        expected = "team to win" if fixture.fifa_match_id == 103 else "team to advance"
        if both and expected in normalized:
            proposition_type = (
                "home_win_third_place"
                if fixture.fifa_match_id == 103
                else "home_wins_final"
                if fixture.fifa_match_id == 104
                else "home_advances"
            )
            return fixture, proposition_type
    return None


def discover_questions(
    rpc: AuthoringRPC, fixtures: Sequence[Fixture]
) -> tuple[dict[tuple[int, str], Question], dict[int, list[str]]]:
    candidates: dict[tuple[int, str], list[Question]] = {}
    ranges = (
        (GROUP_FROM_BLOCK, GROUP_TO_BLOCK, fixtures[:72]),
        (KNOCKOUT_FROM_BLOCK, KNOCKOUT_TO_BLOCK, fixtures[72:]),
    )
    for start, end, scoped_fixtures in ranges:
        for raw_log in rpc.scan_candidates(
            start,
            end,
            topics=(QUESTION_INITIALIZED_TOPIC,),
        ):
            topics = raw_log.get("topics")
            if not isinstance(topics, list) or len(topics) != 4:
                continue
            try:
                ancillary = _decode_dynamic_bytes(str(raw_log.get("data", "")))
                match = _question_match(_title(ancillary), scoped_fixtures)
            except (UnicodeDecodeError, ValueError):
                continue
            if match is None:
                continue
            log = rpc.canonical_candidate_log(
                raw_log,
                from_block=start,
                to_block=end,
            )
            topics = log["topics"]
            question_id = str(topics[1]).casefold()
            computed_question_id = rpc.keccak_bytes(ancillary)
            if computed_question_id != question_id:
                raise ValueError(
                    "QuestionInitialized ID does not match ancillary-data keccak: "
                    f"topic_question_id={question_id}, "
                    f"computed_question_id={computed_question_id}, "
                    "question_init="
                    f"{str(log.get('transactionHash', '')).casefold()}:"
                    f"{int(str(log.get('logIndex')), 16)}"
                )
            fixture, proposition_type = match
            semantic_title, no_label, yes_label = _parse_question_semantics(ancillary)
            _validate_question_orientation(
                semantic_title,
                no_label,
                yes_label,
                fixture,
                proposition_type,
            )
            question = Question(
                question_id=question_id,
                adapter=str(log.get("address", "")).casefold(),
                creator=_topic_address(str(topics[3])),
                ancillary_sha256=hashlib.sha256(ancillary).hexdigest(),
                transaction_hash=str(log.get("transactionHash", "")).casefold(),
                log_index=int(str(log.get("logIndex")), 16),
                block_number=int(str(log.get("blockNumber")), 16),
                block_hash=str(log.get("blockHash", "")).casefold(),
                proposition_type=proposition_type,
                fixture_id=fixture.fifa_match_id,
                semantic_title=semantic_title,
                no_outcome_label=no_label,
                yes_outcome_label=yes_label,
            )
            candidates.setdefault((fixture.fifa_match_id, proposition_type), []).append(
                question
            )

    selected: dict[tuple[int, str], Question] = {}
    ambiguities: dict[int, list[str]] = {}
    for key, values in candidates.items():
        if len(values) == 1:
            selected[key] = values[0]
            continue
        fixture_id = key[0]
        override = REVIEWED_QUESTION_OVERRIDES.get(fixture_id)
        matches = [value for value in values if value.question_id == override]
        if len(matches) != 1:
            raise ValueError(
                f"Ambiguous semantic questions for match {fixture_id}: "
                f"{[value.question_id for value in values]}"
            )
        selected[key] = matches[0]
        ambiguities[fixture_id] = [value.question_id for value in values]

    expected = {
        (fixture.fifa_match_id, proposition_type)
        for fixture in fixtures
        for proposition_type in (
            ("home_win", "draw", "away_win")
            if fixture.fifa_match_id <= 72
            else (
                "home_win_third_place"
                if fixture.fifa_match_id == 103
                else "home_wins_final"
                if fixture.fifa_match_id == 104
                else "home_advances",
            )
        )
    }
    if set(selected) != expected:
        missing = sorted(expected - set(selected))
        extra = sorted(set(selected) - expected)
        raise ValueError(
            f"Semantic question inventory mismatch; missing={missing}, extra={extra}"
        )
    return selected, ambiguities


def apply_creator_updates(
    rpc: AuthoringRPC,
    fixtures: Sequence[Fixture],
    questions: dict[tuple[int, str], Question],
    *,
    finalized_block: int,
) -> tuple[dict[tuple[int, str], Question], dict[str, int]]:
    """Apply only selected-adapter updates signed by the recorded creator."""
    selected = dict(questions)
    fixture_by_id = {fixture.fifa_match_id: fixture for fixture in fixtures}
    by_adapter_question: dict[tuple[str, str], tuple[int, str]] = {}
    for key, question in selected.items():
        scoped_id = (question.adapter, question.question_id)
        if scoped_id in by_adapter_question:
            raise ValueError("Selected UMA adapter/question identity is duplicated")
        by_adapter_question[scoped_id] = key

    logs = list(
        rpc.scan(
            GROUP_FROM_BLOCK,
            finalized_block,
            topics=(
                ANCILLARY_UPDATED_TOPIC,
                sorted({question.question_id for question in selected.values()}),
            ),
        )
    )
    logs.sort(
        key=lambda log: (
            int(str(log.get("blockNumber")), 16),
            int(str(log.get("transactionIndex", "0x0")), 16),
            int(str(log.get("logIndex")), 16),
        )
    )
    accepted = 0
    ignored = 0
    for log in logs:
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("AncillaryDataUpdated topics are malformed")
        scoped_id = (
            str(log.get("address", "")).casefold(),
            str(topics[1]).casefold(),
        )
        selected_key = by_adapter_question.get(scoped_id)
        if selected_key is None:
            continue
        question = selected[selected_key]
        if _topic_address(str(topics[2])) != question.creator:
            ignored += 1
            continue
        update = _decode_dynamic_bytes(str(log.get("data", "")))
        title, no_label, yes_label = _parse_question_semantics(update)
        fixture = fixture_by_id[question.fixture_id]
        semantic_match = _question_match(title, (fixture,))
        if semantic_match is None or semantic_match[1] != question.proposition_type:
            raise ValueError(
                "Creator update changes the selected proposition semantics"
            )
        _validate_question_orientation(
            title,
            no_label,
            yes_label,
            fixture,
            question.proposition_type,
        )
        selected[selected_key] = replace(
            question,
            ancillary_sha256=hashlib.sha256(update).hexdigest(),
            semantic_title=title,
            no_outcome_label=no_label,
            yes_outcome_label=yes_label,
        )
        accepted += 1
    return selected, {
        "accepted_authorized_updates": accepted,
        "ignored_third_party_updates": ignored,
    }


def _condition_events(
    rpc: AuthoringRPC,
    questions: dict[tuple[int, str], Question] | None = None,
) -> tuple[
    dict[tuple[str, str], dict[str, Any]],
    dict[str, dict[str, Any]],
    dict[str, NegRiskQuestionChain],
]:
    adapter_markets: dict[str, tuple[str, dict[str, Any]]] = {}
    for log in rpc.scan(
        GROUP_FROM_BLOCK,
        GROUP_TO_BLOCK,
        address=NEG_RISK_ADAPTER,
        topics=(MARKET_PREPARED_TOPIC,),
    ):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("NegRiskAdapter MarketPrepared topics are malformed")
        market_id = str(topics[1]).casefold()
        if market_id in adapter_markets:
            raise ValueError(f"Duplicate NegRiskAdapter market {market_id}")
        adapter_markets[market_id] = (_topic_address(str(topics[2])), log)
    if not adapter_markets:
        raise ValueError("No NegRiskAdapter markets found in the audited group batch")

    adapter_questions: dict[str, tuple[str, dict[str, Any]]] = {}
    for log in rpc.scan(
        GROUP_FROM_BLOCK,
        GROUP_TO_BLOCK,
        address=NEG_RISK_ADAPTER,
        topics=(ADAPTER_QUESTION_PREPARED_TOPIC,),
    ):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 3:
            raise ValueError("NegRiskAdapter QuestionPrepared topics are malformed")
        market_id = str(topics[1]).casefold()
        question_id = str(topics[2]).casefold()
        if market_id not in adapter_markets:
            raise ValueError("NegRiskAdapter question references an unknown market")
        if question_id in adapter_questions:
            raise ValueError(f"Duplicate NegRiskAdapter question {question_id}")
        adapter_questions[question_id] = (market_id, log)

    operator_markets: dict[tuple[str, str], dict[str, Any]] = {}
    for log in rpc.scan(
        GROUP_FROM_BLOCK,
        GROUP_TO_BLOCK,
        topics=(OPERATOR_MARKET_PREPARED_TOPIC,),
    ):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 2:
            raise ValueError("NegRiskOperator MarketPrepared topics are malformed")
        key = (
            str(log.get("address", "")).casefold(),
            str(topics[1]).casefold(),
        )
        if key in operator_markets:
            raise ValueError(f"Duplicate NegRiskOperator market {key[1]}")
        operator_markets[key] = log

    chains_by_request: dict[str, NegRiskQuestionChain] = {}
    for log in rpc.scan(
        GROUP_FROM_BLOCK,
        GROUP_TO_BLOCK,
        topics=(OPERATOR_QUESTION_PREPARED_TOPIC,),
    ):
        topics = log.get("topics")
        if not isinstance(topics, list) or len(topics) != 4:
            raise ValueError("NegRiskOperator QuestionPrepared topics are malformed")
        market_id = str(topics[1]).casefold()
        question_id = str(topics[2]).casefold()
        request_id = str(topics[3]).casefold()
        operator = str(log.get("address", "")).casefold()
        adapter_market = adapter_markets.get(market_id)
        adapter_question = adapter_questions.get(question_id)
        if adapter_market is None and adapter_question is None:
            continue
        if adapter_market is None or adapter_question is None:
            raise ValueError("Neg-risk operator event has an incomplete adapter chain")
        expected_operator, adapter_market_log = adapter_market
        adapter_question_market, adapter_question_log = adapter_question
        if operator != expected_operator or adapter_question_market != market_id:
            raise ValueError("Neg-risk adapter/operator event chain disagrees")
        operator_market_log = operator_markets.get((operator, market_id))
        if operator_market_log is None:
            raise ValueError("Neg-risk operator market event is missing")
        if request_id in chains_by_request:
            raise ValueError(f"Duplicate NegRisk request ID {request_id}")
        chains_by_request[request_id] = NegRiskQuestionChain(
            market_id=market_id,
            question_id=question_id,
            request_id=request_id,
            operator=operator,
            adapter_market_log=adapter_market_log,
            operator_market_log=operator_market_log,
            adapter_question_log=adapter_question_log,
            operator_question_log=log,
        )
    # A question ID is scoped to the UMA adapter/oracle which initialized it.
    # The same ancillary data can therefore legitimately produce the same
    # question ID on more than one adapter.  A duplicate for the same oracle
    # remains ambiguous and must fail closed.
    by_oracle_question: dict[tuple[str, str], dict[str, Any]] = {}
    by_transaction: dict[str, dict[str, Any]] = {}
    ranges = (
        (GROUP_FROM_BLOCK, GROUP_TO_BLOCK),
        (KNOCKOUT_FROM_BLOCK, KNOCKOUT_TO_BLOCK),
    )
    condition_topics: tuple[Any, ...] = (CONDITION_PREPARATION_TOPIC,)
    if questions is not None:
        selected_question_ids: set[str] = set()
        for question in questions.values():
            if question.fixture_id <= 72:
                chain = chains_by_request.get(question.question_id)
                if chain is None:
                    raise ValueError(
                        "Selected group question has no complete neg-risk chain: "
                        f"request_id={question.question_id}"
                    )
                selected_question_ids.add(chain.question_id)
            else:
                selected_question_ids.add(question.question_id)
        if not selected_question_ids:
            raise ValueError("Selected condition question-ID filter is empty")
        condition_topics = (
            CONDITION_PREPARATION_TOPIC,
            None,
            None,
            sorted(selected_question_ids),
        )
    for start, end in ranges:
        for log in rpc.scan(
            start,
            end,
            address=CTF,
            topics=condition_topics,
        ):
            topics = log.get("topics")
            if not isinstance(topics, list) or len(topics) != 4:
                raise ValueError("ConditionPreparation topics are malformed")
            question_id = str(topics[3]).casefold()
            oracle = _topic_address(str(topics[2]))
            transaction_hash = str(log.get("transactionHash", "")).casefold()
            key = (oracle, question_id)
            if key in by_oracle_question:
                raise ValueError(
                    "Duplicate condition for oracle/question pair "
                    f"{oracle}/{question_id}"
                )
            by_oracle_question[key] = log
            by_transaction.setdefault(transaction_hash, log)
    for chain in chains_by_request.values():
        condition_log = by_oracle_question.get((NEG_RISK_ADAPTER, chain.question_id))
        if condition_log is not None:
            _verify_neg_risk_atomic_event_chain(chain, condition_log)
    return by_oracle_question, by_transaction, chains_by_request


def _verify_atomic_event_sequence(
    name: str,
    logs: Sequence[dict[str, Any]],
) -> list[dict[str, Any]]:
    locators = [_evidence_locator(log) for log in logs]
    locations = {
        (
            locator["transaction_hash"],
            locator["block_number"],
            locator["block_hash"],
        )
        for locator in locators
    }
    indexes = [locator["log_index"] for locator in locators]
    if (
        len(locations) != 1
        or indexes != sorted(indexes)
        or len(set(indexes)) != len(indexes)
    ):
        raise ValueError(
            f"{name} events are not one strictly ordered transaction: "
            f"locators={locators}"
        )
    return locators


def _verify_neg_risk_atomic_event_chain(
    chain: NegRiskQuestionChain,
    condition_log: dict[str, Any],
) -> dict[str, Any]:
    """Prove the pinned adapter/operator calls emitted atomically in order."""
    market_logs = (chain.adapter_market_log, chain.operator_market_log)
    market_locators = _verify_atomic_event_sequence(
        "Neg-risk MarketPrepared",
        market_logs,
    )
    market_data = [str(log.get("data", "")).casefold() for log in market_logs]
    if market_data[0] != market_data[1]:
        raise ValueError(
            "Neg-risk MarketPrepared adapter/operator ABI data disagree: "
            f"locators={market_locators}"
        )

    question_logs = (
        condition_log,
        chain.adapter_question_log,
        chain.operator_question_log,
    )
    question_locators = _verify_atomic_event_sequence(
        "Neg-risk question preparation",
        question_logs,
    )
    adapter_data = str(chain.adapter_question_log.get("data", "")).casefold()
    operator_data = str(chain.operator_question_log.get("data", "")).casefold()
    if adapter_data != operator_data:
        raise ValueError(
            "Neg-risk QuestionPrepared adapter/operator ABI data disagree: "
            f"locators={question_locators}"
        )
    return {
        "market_preparation_order": {
            "adapter": market_locators[0],
            "operator": market_locators[1],
        },
        "question_preparation_order": {
            "condition": question_locators[0],
            "adapter": question_locators[1],
            "operator": question_locators[2],
        },
        "market_payload_sha256": hashlib.sha256(
            bytes.fromhex(market_data[0].removeprefix("0x"))
        ).hexdigest(),
        "question_payload_sha256": hashlib.sha256(
            bytes.fromhex(adapter_data.removeprefix("0x"))
        ).hexdigest(),
    }


def _verify_standard_atomic_event_join(
    question: Question,
    condition_log: dict[str, Any],
) -> dict[str, Any]:
    condition_locator = _evidence_locator(condition_log)
    question_locator = {
        "address": question.adapter,
        "transaction_hash": question.transaction_hash,
        "log_index": question.log_index,
        "block_number": question.block_number,
        "block_hash": question.block_hash,
    }
    same_location = (
        condition_locator["transaction_hash"],
        condition_locator["block_number"],
        condition_locator["block_hash"],
    ) == (
        question_locator["transaction_hash"],
        question_locator["block_number"],
        question_locator["block_hash"],
    )
    if (
        not same_location
        or condition_locator["log_index"] >= question_locator["log_index"]
    ):
        raise ValueError(
            "Standard ConditionPreparation must precede QuestionInitialized in "
            "one transaction: "
            f"condition_init={condition_locator}, "
            f"question_init={question_locator}"
        )
    return {
        "condition_preparation": condition_locator,
        "question_initialized": question_locator,
    }


def _verify_neg_risk_contract_relationship(
    rpc: AuthoringRPC,
    question: Question,
    chain: NegRiskQuestionChain,
) -> dict[str, Any]:
    """Prove the pinned bidirectional UMA adapter/operator deployment link."""
    operator_log = chain.operator_question_log
    block_number = int(str(operator_log.get("blockNumber")), 16)
    operator_oracle = _result_address(
        rpc.call_at(chain.operator, "0x" + GET_OPERATOR_ORACLE, block_number),
        "NegRiskOperator.oracle()",
    )
    operator_adapter = _result_address(
        rpc.call_at(
            chain.operator,
            "0x" + GET_OPERATOR_NEG_RISK_ADAPTER,
            block_number,
        ),
        "NegRiskOperator.nrAdapter()",
    )
    uma_ctf = _result_address(
        rpc.call_at(question.adapter, "0x" + GET_UMA_CTF, block_number),
        "UmaCtfAdapter.ctf()",
    )
    adapter_block_number = int(str(chain.adapter_market_log.get("blockNumber")), 16)
    neg_risk_ctf = _result_address(
        rpc.call_at(
            NEG_RISK_ADAPTER,
            "0x" + GET_NEG_RISK_CTF,
            adapter_block_number,
        ),
        "NegRiskAdapter.ctf()",
    )
    neg_risk_col = _result_address(
        rpc.call_at(
            NEG_RISK_ADAPTER,
            "0x" + GET_NEG_RISK_COL,
            adapter_block_number,
        ),
        "NegRiskAdapter.col()",
    )
    expected = (
        question.adapter,
        NEG_RISK_ADAPTER,
        chain.operator,
        CTF,
        USDC_E,
    )
    actual = (
        operator_oracle,
        operator_adapter,
        uma_ctf,
        neg_risk_ctf,
        neg_risk_col,
    )
    if actual != expected:
        raise ValueError(
            "Neg-risk UMA/operator relationship mismatch: "
            f"request_id={question.question_id}, "
            f"neg_question_id={chain.question_id}, "
            f"uma_adapter={question.adapter}, operator={chain.operator}, "
            f"operator_oracle={operator_oracle}, "
            f"operator_nr_adapter={operator_adapter}, uma_ctf={uma_ctf}, "
            f"neg_risk_ctf={neg_risk_ctf}, neg_risk_col={neg_risk_col}, "
            f"question_init={question.transaction_hash}:{question.log_index}, "
            "operator_question_init="
            f"{str(operator_log.get('transactionHash', '')).casefold()}:"
            f"{int(str(operator_log.get('logIndex')), 16)}, "
            f"verification_block={block_number}, "
            "neg_risk_market_init="
            f"{str(chain.adapter_market_log.get('transactionHash', '')).casefold()}:"
            f"{int(str(chain.adapter_market_log.get('logIndex')), 16)}@"
            f"{adapter_block_number}"
        )
    return {
        "operator": chain.operator,
        "operator_oracle": operator_oracle,
        "operator_neg_risk_adapter": operator_adapter,
        "uma_adapter": question.adapter,
        "uma_ctf": uma_ctf,
        "neg_risk_adapter": NEG_RISK_ADAPTER,
        "neg_risk_ctf": neg_risk_ctf,
        "neg_risk_collateral": neg_risk_col,
        "verification_block_number": block_number,
        "verification_block_hash": str(operator_log.get("blockHash", "")).casefold(),
        "neg_risk_verification_block_number": adapter_block_number,
        "neg_risk_verification_block_hash": str(
            chain.adapter_market_log.get("blockHash", "")
        ).casefold(),
    }


def _verify_standard_contract_relationship(
    rpc: AuthoringRPC,
    question: Question,
    condition_log: dict[str, Any],
) -> dict[str, Any]:
    """Prove that a dynamically discovered UMA adapter targets audited CTF."""
    _verify_standard_atomic_event_join(question, condition_log)
    block_number = int(str(condition_log.get("blockNumber")), 16)
    block_hash = str(condition_log.get("blockHash", "")).casefold()
    condition_transaction = str(condition_log.get("transactionHash", "")).casefold()
    condition_log_index = int(str(condition_log.get("logIndex")), 16)
    uma_ctf = _result_address(
        rpc.call_at(question.adapter, "0x" + GET_UMA_CTF, block_number),
        "UmaCtfAdapter.ctf()",
    )
    if uma_ctf != CTF:
        raise ValueError(
            "Standard UMA/CTF relationship mismatch: "
            f"question_id={question.question_id}, "
            f"uma_adapter={question.adapter}, expected_ctf={CTF}, "
            f"uma_ctf={uma_ctf}, "
            f"question_init={question.transaction_hash}:{question.log_index}, "
            f"condition_init={condition_transaction}:{condition_log_index}, "
            f"verification_block={block_number}/{block_hash}"
        )
    return {
        "uma_adapter": question.adapter,
        "uma_ctf": uma_ctf,
        "verification_block_number": block_number,
        "verification_block_hash": block_hash,
    }


def _standard_tokens(
    rpc: AuthoringRPC, condition_id: str, block: int
) -> tuple[str, str]:
    collections: list[int] = []
    for index_set in (1, 2):
        data = (
            "0x"
            + GET_COLLECTION_ID
            + "0" * 64
            + condition_id.removeprefix("0x")
            + _hex_word(index_set)
        )
        collections.append(rpc.call_at(CTF, data, block))
    positions = []
    for collection in collections:
        data = (
            "0x" + GET_CTF_POSITION_ID + _address_word(USDC_E) + _hex_word(collection)
        )
        positions.append(str(rpc.call_at(CTF, data, block)))
    return positions[0], positions[1]


def _neg_risk_tokens(
    rpc: AuthoringRPC, question_id: str, block: int
) -> tuple[str, str]:
    positions = []
    for outcome in (1, 0):
        data = (
            "0x"
            + GET_NEG_RISK_POSITION_ID
            + question_id.removeprefix("0x")
            + _hex_word(outcome)
        )
        positions.append(str(rpc.call_at(NEG_RISK_ADAPTER, data, block)))
    return positions[0], positions[1]


def _semantics(fixture: Fixture, proposition_type: str) -> tuple[str, str, str]:
    suffix = proposition_type.replace("_", "-")
    proposition_id = f"wc2026-m{fixture.fifa_match_id:03d}-{suffix}"
    if proposition_type == "home_win":
        return (
            proposition_id,
            f"{fixture.home_team} wins in regulation",
            f"{fixture.home_team} does not win in regulation",
        )
    if proposition_type == "away_win":
        return (
            proposition_id,
            f"{fixture.away_team} wins in regulation",
            f"{fixture.away_team} does not win in regulation",
        )
    if proposition_type == "draw":
        return (
            proposition_id,
            "match draws in regulation",
            "match does not draw in regulation",
        )
    if proposition_type == "home_win_third_place":
        return (
            proposition_id,
            f"{fixture.home_team} wins the third-place match",
            f"{fixture.away_team} wins the third-place match",
        )
    if proposition_type == "home_wins_final":
        return (
            proposition_id,
            f"{fixture.home_team} wins the final and becomes champion",
            f"{fixture.away_team} wins the final and becomes champion",
        )
    return (
        proposition_id,
        f"{fixture.home_team} advances",
        f"{fixture.away_team} advances",
    )


def _evidence_locator(log: dict[str, Any]) -> dict[str, Any]:
    """Return a prose-free locator for one semantic-chain event."""
    return {
        "address": str(log.get("address", "")).casefold(),
        "transaction_hash": str(log.get("transactionHash", "")).casefold(),
        "log_index": int(str(log.get("logIndex")), 16),
        "block_number": int(str(log.get("blockNumber")), 16),
        "block_hash": str(log.get("blockHash", "")).casefold(),
    }


def build_rows(
    rpc: AuthoringRPC,
    fixtures: Sequence[Fixture],
    questions: dict[tuple[int, str], Question],
    *,
    manifest_version: str,
    reviewed_at: datetime,
) -> tuple[list[dict[str, str]], list[dict[str, Any]]]:
    by_oracle_question, _by_transaction, chains_by_request = _condition_events(
        rpc,
        questions,
    )
    rows: list[dict[str, str]] = []
    evidence: list[dict[str, Any]] = []
    used_neg_market_ids: set[str] = set()
    used_neg_request_ids: set[str] = set()
    verified_neg_relationships: dict[tuple[str, str], dict[str, Any]] = {}
    verified_standard_relationships: dict[str, dict[str, Any]] = {}
    for fixture in fixtures:
        proposition_types = (
            ("home_win", "draw", "away_win")
            if fixture.fifa_match_id <= 72
            else (
                "home_win_third_place"
                if fixture.fifa_match_id == 103
                else "home_wins_final"
                if fixture.fifa_match_id == 104
                else "home_advances",
            )
        )
        for proposition_type in proposition_types:
            question = questions[(fixture.fifa_match_id, proposition_type)]
            _validate_question_orientation(
                question.semantic_title,
                question.no_outcome_label,
                question.yes_outcome_label,
                fixture,
                proposition_type,
            )
            standard_relationship = None
            standard_atomic_join = None
            neg_atomic_join = None
            if fixture.fifa_match_id <= 72:
                neg_chain = chains_by_request.get(question.question_id)
                if neg_chain is None:
                    raise ValueError(
                        f"No complete neg-risk chain for request {question.question_id}"
                    )
                relationship_key = (neg_chain.operator, question.adapter)
                if relationship_key not in verified_neg_relationships:
                    verified_neg_relationships[relationship_key] = (
                        _verify_neg_risk_contract_relationship(
                            rpc,
                            question,
                            neg_chain,
                        )
                    )
                neg_relationship = verified_neg_relationships[relationship_key]
                used_neg_market_ids.add(neg_chain.market_id)
                used_neg_request_ids.add(neg_chain.request_id)
                neg_question_id = neg_chain.question_id
                condition_log = by_oracle_question.get(
                    (NEG_RISK_ADAPTER, neg_question_id)
                )
                structure = "neg_risk"
            else:
                neg_chain = None
                neg_relationship = None
                neg_question_id = None
                condition_log = by_oracle_question.get(
                    (question.adapter, question.question_id)
                )
                if (
                    condition_log is not None
                    and str(condition_log.get("transactionHash", "")).casefold()
                    != question.transaction_hash
                ):
                    raise ValueError(
                        "Standard QuestionInitialized and ConditionPreparation "
                        "transactions disagree"
                    )
                structure = "standard"
            if condition_log is None:
                raise ValueError(
                    f"No condition event joined to question {question.question_id}"
                )
            if structure == "neg_risk":
                neg_atomic_join = _verify_neg_risk_atomic_event_chain(
                    neg_chain,
                    condition_log,
                )
            else:
                standard_atomic_join = _verify_standard_atomic_event_join(
                    question,
                    condition_log,
                )
            topics = condition_log["topics"]
            condition_id = str(topics[1]).casefold()
            oracle = _topic_address(str(topics[2]))
            block_number = int(str(condition_log["blockNumber"]), 16)
            block_hash = str(condition_log["blockHash"]).casefold()
            slot_count = rpc.call_at(
                CTF,
                "0x" + GET_OUTCOME_SLOT_COUNT + condition_id.removeprefix("0x"),
                block_number,
            )
            if slot_count != 2:
                raise ValueError(f"Condition {condition_id} does not have two slots")
            if structure == "neg_risk":
                if oracle != NEG_RISK_ADAPTER:
                    raise ValueError(
                        "Neg-risk condition oracle is not the audited adapter"
                    )
                yes_token, no_token = _neg_risk_tokens(
                    rpc, str(neg_question_id), block_number
                )
                exchange = NEG_RISK_V2_EXCHANGE
            else:
                if oracle != question.adapter:
                    raise ValueError(
                        "Standard condition oracle does not match UMA adapter"
                    )
                if question.adapter not in verified_standard_relationships:
                    verified_standard_relationships[question.adapter] = (
                        _verify_standard_contract_relationship(
                            rpc,
                            question,
                            condition_log,
                        )
                    )
                standard_relationship = verified_standard_relationships[
                    question.adapter
                ]
                yes_token, no_token = _standard_tokens(rpc, condition_id, block_number)
                exchange = STANDARD_V2_EXCHANGE
            proposition_id, yes_represents, no_represents = _semantics(
                fixture, proposition_type
            )
            window_minutes = 150 if fixture.fifa_match_id <= 72 else 210
            row = {
                "proposition_id": proposition_id,
                "fifa_match_id": str(fixture.fifa_match_id),
                "stage": fixture.stage,
                "group_label": fixture.group_label or "",
                "home_team": fixture.home_team,
                "away_team": fixture.away_team,
                "kickoff_at_utc": fixture.kickoff_at_utc.isoformat().replace(
                    "+00:00", "Z"
                ),
                "window_start_at_utc": fixture.kickoff_at_utc.isoformat().replace(
                    "+00:00", "Z"
                ),
                "window_end_at_utc": (
                    fixture.kickoff_at_utc + timedelta(minutes=window_minutes)
                )
                .isoformat()
                .replace("+00:00", "Z"),
                "proposition_type": proposition_type,
                "yes_represents": yes_represents,
                "no_represents": no_represents,
                "condition_id": condition_id,
                "yes_token_id": yes_token,
                "no_token_id": no_token,
                "market_structure": structure,
                "exchange_address": exchange,
                "openfootball_revision": OPENFOOTBALL_REVISION,
                "openfootball_path": fixture.source_path,
                "openfootball_source_lines": fixture.source_lines,
                "openfootball_line_hash": fixture.source_line_hash,
                "condition_init_tx_hash": str(
                    condition_log["transactionHash"]
                ).casefold(),
                "condition_init_log_index": str(
                    int(str(condition_log["logIndex"]), 16)
                ),
                "question_init_tx_hash": question.transaction_hash,
                "question_init_log_index": str(question.log_index),
                "ancillary_data_sha256": question.ancillary_sha256,
                "token_verification_block_number": str(block_number),
                "token_verification_block_hash": block_hash,
                "manifest_sha256": "0" * 64,
                "manifest_version": manifest_version,
                "reviewed_at_utc": reviewed_at.isoformat().replace("+00:00", "Z"),
            }
            parse_polygon_market(row)
            rows.append(row)
            evidence.append(
                {
                    "proposition_id": proposition_id,
                    "question_id": question.question_id,
                    "uma_adapter": question.adapter,
                    "condition_oracle": oracle,
                    "condition_id": condition_id,
                    "condition_init": {
                        "transaction_hash": row["condition_init_tx_hash"],
                        "log_index": int(row["condition_init_log_index"]),
                    },
                    "question_init": {
                        "transaction_hash": question.transaction_hash,
                        "log_index": question.log_index,
                    },
                    "ancillary_data_sha256": question.ancillary_sha256,
                    "token_orientation": {
                        "yes_index_set": 1,
                        "no_index_set": 2,
                        "standard_payout_order": "yes_no",
                        "neg_risk_true_is_yes": True,
                        "verified_source_revisions": {
                            "uma_adapter": UMA_REVISION,
                            "neg_risk_adapter": NEG_RISK_REVISION,
                        },
                    },
                    "token_pair_verified_at": {
                        "block_number": block_number,
                        "block_hash": block_hash,
                    },
                    "neg_risk_chain": (
                        {
                            "market_id": neg_chain.market_id,
                            "operator": neg_chain.operator,
                            "adapter_market": _evidence_locator(
                                neg_chain.adapter_market_log
                            ),
                            "operator_market": _evidence_locator(
                                neg_chain.operator_market_log
                            ),
                            "adapter_question": _evidence_locator(
                                neg_chain.adapter_question_log
                            ),
                            "operator_question": _evidence_locator(
                                neg_chain.operator_question_log
                            ),
                            "contract_relationship": neg_relationship,
                            "atomic_event_join": neg_atomic_join,
                        }
                        if neg_chain is not None
                        else None
                    ),
                    "standard_contract_relationship": (
                        standard_relationship if structure == "standard" else None
                    ),
                    "standard_atomic_event_join": (
                        standard_atomic_join if structure == "standard" else None
                    ),
                }
            )
    if len(used_neg_market_ids) != 72 or len(used_neg_request_ids) != 216:
        raise ValueError(
            "Selected WC2026 group inventory must traverse 72 neg-risk markets "
            "and 216 unique complete question chains"
        )
    parsed = [parse_polygon_market(row) for row in rows]
    manifest_sha256 = polygon_manifest_content_sha256(parsed)
    for row in rows:
        row["manifest_sha256"] = manifest_sha256
    validate_polygon_market_manifest(parse_polygon_market(row) for row in rows)
    return rows, evidence


def verify_updates_and_resolutions(
    rpc: AuthoringRPC,
    rows: Sequence[dict[str, str]],
    *,
    finalized: Any,
    update_summary: dict[str, int],
) -> dict[str, Any]:
    condition_ids = [row["condition_id"] for row in rows]
    resolutions: set[str] = set()
    first_window = min(
        datetime.fromisoformat(row["window_start_at_utc"].replace("Z", "+00:00"))
        for row in rows
    )
    start = rpc.rpc.first_block_at_or_after(
        first_window - timedelta(days=1), finalized_head=finalized
    )
    for log in rpc.scan(
        start,
        finalized.number,
        address=CTF,
        topics=(CONDITION_RESOLUTION_TOPIC, condition_ids),
    ):
        topics = log.get("topics")
        if isinstance(topics, list) and len(topics) >= 2:
            resolutions.add(str(topics[1]).casefold())
    missing = sorted(set(condition_ids) - resolutions)
    if missing:
        raise ValueError(f"Conditions missing resolution evidence: {missing}")
    return {
        "finalized_head": asdict(finalized),
        "resolution_count": len(resolutions),
        **update_summary,
    }


def _fetch_pinned_sources(output_dir: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    source_dir = output_dir / "openfootball"
    source_dir.mkdir(parents=True)
    for path, expected_hash in OPENFOOTBALL_FILES.items():
        url = validate_outbound_https_url(OPENFOOTBALL_BASE + path)
        response = requests.get(url, timeout=(5, 60))
        response.raise_for_status()
        payload = response.content
        actual_hash = hashlib.sha256(payload).hexdigest()
        if actual_hash != expected_hash:
            raise ValueError(f"Pinned OpenFootball hash mismatch for {path}")
        destination = source_dir / path.replace("/", "__")
        destination.write_bytes(payload)
        values[path] = payload.decode("utf-8-sig")
    return values


def _fetch_pinned_fifa_schedule(output_dir: Path) -> None:
    """Preserve the exact independent evidence used to review match numbering."""
    url = validate_outbound_https_url(FIFA_SCHEDULE_URL)
    response = requests.get(url, timeout=(5, 60))
    response.raise_for_status()
    payload = response.content
    if hashlib.sha256(payload).hexdigest() != FIFA_SCHEDULE_SHA256:
        raise ValueError("Pinned FIFA schedule hash mismatch")
    destination = output_dir / "fifa" / "FWC26-Match-Schedule_English.pdf"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(payload)


def _validate_evidence_privacy(
    value: Any,
    *,
    private_addresses: Iterable[str],
) -> None:
    """Fail closed if authoring evidence contains participant identifiers."""
    addresses = {address.casefold() for address in private_addresses}
    prohibited_key_terms = (
        "creator",
        "wallet",
        "maker",
        "taker",
        "order_hash",
        "signature",
    )

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            for key, nested in item.items():
                normalized_key = str(key).casefold()
                if any(term in normalized_key for term in prohibited_key_terms):
                    raise ValueError(
                        "Authoring evidence contains a participant identifier field"
                    )
                visit(nested)
            return
        if isinstance(item, (list, tuple)):
            for nested in item:
                visit(nested)
            return
        if isinstance(item, str):
            normalized = item.casefold()
            if any(address in normalized for address in addresses):
                raise ValueError(
                    "Authoring evidence contains a private participant address"
                )

    visit(value)


def author_seed(
    *,
    rpc_url: str,
    provider_label: str,
    output_dir: Path,
    manifest_version: str,
    reviewed_at: datetime,
) -> Path:
    provider_label = validate_polygon_provider_label(provider_label)
    output_dir = output_dir.resolve()
    artifacts_root = (REPO_ROOT / "artifacts").resolve()
    if artifacts_root not in output_dir.parents or output_dir == artifacts_root:
        raise ValueError(
            "Seed authoring output must be a new directory below artifacts/"
        )
    if output_dir.exists():
        raise FileExistsError(f"Refusing to overwrite existing evidence: {output_dir}")
    output_dir.mkdir(parents=True)
    try:
        sources = _fetch_pinned_sources(output_dir)
        _fetch_pinned_fifa_schedule(output_dir)
        fixtures = parse_openfootball_fixtures(
            sources["2026--usa/cup.txt"], sources["2026--usa/cup_finals.txt"]
        )
        rpc = AuthoringRPC(rpc_url)
        if rpc.rpc.chain_id() != CHAIN_ID:
            raise ValueError("Seed authoring RPC is not Polygon chain 137")
        finalized = rpc.rpc.finalized_head()
        questions, ambiguities = discover_questions(rpc, fixtures)
        questions, update_summary = apply_creator_updates(
            rpc,
            fixtures,
            questions,
            finalized_block=finalized.number,
        )
        rows, evidence_rows = build_rows(
            rpc,
            fixtures,
            questions,
            manifest_version=manifest_version,
            reviewed_at=reviewed_at,
        )
        chain_evidence = verify_updates_and_resolutions(
            rpc,
            rows,
            finalized=finalized,
            update_summary=update_summary,
        )
        candidate = output_dir / "polymarket_wc2026_polygon_settlement_markets.csv"
        with candidate.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=SEED_COLUMNS, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(rows)
        report = {
            "schema_version": 1,
            "provider": {
                "label": provider_label,
                "sanitized_origin": rpc.rpc.origin,
            },
            "source_revisions": {
                "conditional_tokens": CTF_REVISION,
                "uma_adapter": UMA_REVISION,
                "neg_risk_adapter": NEG_RISK_REVISION,
                "ctf_exchange_v2": V2_REVISION,
                "openfootball": OPENFOOTBALL_REVISION,
            },
            "openfootball_sha256": OPENFOOTBALL_FILES,
            "fifa_match_id_evidence": {
                "url": FIFA_SCHEDULE_URL,
                "document_title": FIFA_SCHEDULE_TITLE,
                "sha256": FIFA_SCHEDULE_SHA256,
                "mapping_grain": "openfootball_source_line_sha256",
            },
            "manifest_sha256": rows[0]["manifest_sha256"],
            "reviewed_duplicate_candidates": ambiguities,
            "chain_verification": chain_evidence,
            "rows": evidence_rows,
        }
        _validate_evidence_privacy(
            report,
            private_addresses=(question.creator for question in questions.values()),
        )
        evidence_text = json.dumps(report, indent=2, sort_keys=True, default=str) + "\n"
        (output_dir / "EVIDENCE.json").write_text(
            evidence_text,
            encoding="utf-8",
        )
        write_polygon_resolution_attestation(
            output_dir / "resolution_attestation.yml",
            manifest_version=manifest_version,
            manifest_sha256=rows[0]["manifest_sha256"],
            chain_evidence=chain_evidence,
            authoring_evidence_sha256=hashlib.sha256(
                evidence_text.encode("utf-8")
            ).hexdigest(),
        )
        return candidate
    except Exception:
        (output_dir / "FAILED").write_text(
            "Seed authoring did not complete; do not promote this directory.\n",
            encoding="utf-8",
        )
        raise


def _reviewed_at(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise argparse.ArgumentTypeError("--reviewed-at must be explicitly UTC")
    if parsed.second or parsed.microsecond:
        raise argparse.ArgumentTypeError("--reviewed-at must be minute-aligned")
    return parsed.astimezone(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rpc-url", default=os.getenv("POLYGON_RPC_URL", ""))
    parser.add_argument(
        "--provider-label", default=os.getenv("POLYGON_RPC_PROVIDER_LABEL", "")
    )
    parser.add_argument("--manifest-version", required=True)
    parser.add_argument("--reviewed-at", required=True, type=_reviewed_at)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if not args.rpc_url or not args.provider_label.strip():
        parser.error(
            "--rpc-url and --provider-label (or matching env vars) are required"
        )
    candidate = author_seed(
        rpc_url=args.rpc_url,
        provider_label=args.provider_label.strip(),
        output_dir=args.output_dir,
        manifest_version=args.manifest_version,
        reviewed_at=args.reviewed_at,
    )
    print(candidate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
