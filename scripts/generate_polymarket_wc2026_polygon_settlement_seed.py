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

OPENFOOTBALL_REVISION = "bd46a148289f9930da66c140d4d7d2325e95d387"
OPENFOOTBALL_BASE = (
    f"https://raw.githubusercontent.com/openfootball/worldcup/{OPENFOOTBALL_REVISION}/"
)
OPENFOOTBALL_FILES = {
    "2026--usa/cup.txt": "4f52c563a5d470702fedf5078fd379c8f5ddfb2192d23b6f88ce84e997c30028",
    "2026--usa/cup_finals.txt": "03631f10fff8a3a9c485d866c98fb099f8d2612e97a034c64c28c7d189dd5949",
    "LICENSE.md": "36ffd9dc085d529a7e60e1276d73ae5a030b020313e6c5408593a6ae2af39673",
}

# Match numbering is not chronological when simultaneous or same-day fixtures
# are involved.  This independently reviewed mapping keys the exact pinned
# OpenFootball source slice to the official FIFA match number; the authoring
# tool never reads the repository's existing schedule seed.
FIFA_SCHEDULE_URL = (
    "https://digitalhub.fifa.com/asset/4b5d4417-3343-4732-9cdf-14b6662af407/"
    "FWC26-Match-Schedule_English.pdf"
)
FIFA_SCHEDULE_TITLE = "FWC26 Match Schedule_v31_16072026_EN"
FIFA_SCHEDULE_SHA256 = (
    "165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811"
)
_REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID = (
    "59d5c1f0393f0d7a82172b059a59131e84eaf86eb4ed99fd74f36cf1276189bd",
    "ea2e2a61da99c76ab99497a7a0c0ce772cc17e0ac4871cb4b9e62ce5b33395ba",
    "5c52a03ad6142f6b90a1fe3d6677bef2ce5f90464710da074dc5db0d00082ec1",
    "34010c2f99636db613ac6b011a86308dfa55c94a4b38d44c83820ab1b363195a",
    "0bd92e8cf78e40529dd8c47bde8981c1a9b590a46e413353bf5ef7ba31030513",
    "d0256a00b98c134c4d26d632734c702268c47ab22e62e7161230e40887b9734b",
    "e1bfc951b2a12c6be2e5d056a31778a00d77dcbe391445707dd61a878748fcfb",
    "36ce2180d0ef6567ffb783e0ab7225e0cd10fb348dbfde9a232cf2c1642ffcb6",
    "501698ce918a5bf6a9590f428424317ee79068f2d8971a2b5a59ce7c4f5a0947",
    "69d1ca85d1ff5bfc0249d17b40ffe1fbe8a792ed8a9c80d1f95cc0184b430179",
    "6145d75ee6731c1d35d4748e10555921e7a8222b0df3b79991f2af2ff09d3de1",
    "d47afaa7465e12d1c57cbd51fdcf5222ad8658e9805b1475153565be480506a6",
    "b9ba008b3f4177eb1664cc76222c1a72e002cbb61a1607c0701ea454585a0966",
    "2c8bdcddd6b515c3dbf152aee6efc206f149d6871695f082cbc9704ab7a67170",
    "5d8ed746cbc922c3c70b1cc430c1cc8e70939a6a534268d74c70451ab0e41bfa",
    "38dba8b12efbd4132e0bf34d9ad5e0a84a1714f64baac90c10fda1f077f34af3",
    "509694fda93bbb51cc324803d8ccb21f4db9295d640be1f04faeae31225f37c0",
    "bfd64e62a6156c53152844615dd70d08df96e873b23afeb394d88257c1105472",
    "fdf1a473bdf3333b4d8c5118cfe23fbf09a367a7000acd875d863e93fa14bece",
    "04752c0a7c2ac188d12b220c38755666eb9f4f93c09f647c9e26177e297dc612",
    "b1a971090bb9999e41b9438a9e925f5e357fb75ce5fc1fca0ebbc4c6c94fbc3a",
    "8f3308ec8e467ad29b62c74b2f369df80a42cc3086dcdf206192320e820a157e",
    "9204bf4fe5230b0c10b0792dac0a5bc5a250b892c35f60a1e5bb3fa6ceddcd41",
    "cefd95eab2952ba94e0bf6d2f3594e5292117f2062656f8ce4e1e1ac17e793bd",
    "2c156ccd23b731a315516acda024326d57444a547b705389d2895c25959d6ad9",
    "979e9623f937af1a6fce8c283b72b8b6aaba150d5d753b9eec670e7118a5bb2d",
    "8463db70b42fbb5842186b9241afa2001a20d00e3159119cccf51720d0d7f222",
    "91e23caae024cf58ad482e5c98df00470a284f649a2c5da9bb82a911e3fd92b4",
    "f9c1df8429fe45d27e2c1153a1e9660c937775d96680ef5ff00359ff890cf573",
    "065ec8aad89f4456e7d9769cfc7a4fc3a0d06c826258ffaca7a57a717522787a",
    "a503f484355e0008de8d2ecf89b1ab67d89b615671a53193091553ca31339ad6",
    "40af69c338f909c835ef9b70f8b8b8892546639048209a9aafacf748f83aa8b3",
    "9c6207aba4fec6595561ba02a62cbec176acaabab9cca9f365a2dd1fa599a953",
    "829e6c0a96274d19d9135441bb1be661396d4c39cf15c86cf891b5bd4859041a",
    "2a441b5958e568e1f7bd50badc7d80ff4057d7650b1e74114c97d609baa7d903",
    "7d57631c11752ec3d9a34f31f3bf4998cf90c205ebc7c59f351ae0163c6f02b6",
    "9704971fc28c29c2458823a8daf4ae8a24c58f681d8d8c5ba07ab86cade00cef",
    "268ac33284343015fa6ad41aa8e959743c2e541840b62d435220d77bb599f766",
    "573230c4fff84b004ec3169f795e282f7cc5c7b8a9ccff2490736f0760626d67",
    "5c52bda6c2045674ce6f0a22790e36a7d7855d805e0b7951c680b6b01fe9fab8",
    "c6b0e5ab69302f876d86ca260e90dd5524b2fa1585244147029dccddd4f1948a",
    "772caa75b548480a41b5fc3e86db6ac39f56c1ee1dd089234d3d99a702e95f7b",
    "dd3d0acaa0efbeafb2742501696ac9f365f43a234f5b45097f214ca0ea79ffdb",
    "23798a7771c5fe2d3d28245c7c4d57d92001b89192db7f46186f4579ae728ba4",
    "fb876e3cd9e2d6a85c84267afa0cafb707bc7e3c09f74664357cedd09c2f1bd0",
    "91ff1a4f4e525f188ef555bc9b92459d7a03d67077f9ed7f0b75eae1da98ee51",
    "9464bd076b010964e67be13c4757b358543f00c073083ea0f8331e4e17edd79b",
    "6f126f48e6ad7c5b395e710d476d0717545c20538c2d7644ba66ab12e9067aa9",
    "612d93d8e27b988afd33054965d222e8f294a51f10a4e76b2c8888b1f9361400",
    "e09fbfcaa1fe30238784dc6cfb6ebc8d9cf8a258236f57e77b22f33516e52db8",
    "bba4c78f755a7a4af0e636c9d4676dca8353be57dbc0a9bca75b87bd886ac368",
    "e4253cb5406dd4f2ae50f307692b70dba88ac8abdbecdddea08b702dcc5d640e",
    "35f6c6c565d90bfebeb7e555500884e318757422fff22746a4188917529fa69f",
    "372227afd2d3ff274be9bbd84bcbee8dc3ffa4211a1019d0f600c7f35332338a",
    "0ce7001ad762d67fa8858c4ca7193eb7156d3b75ac89def5468c7c51538730db",
    "f92c732970b56b12e89c25c6d6c73ff22ae9c2e510a053c25cf1a6635c172b13",
    "d409264137651883019e5271652dffc930be98525aa2145bb8de4664f3e18df5",
    "1e955a6dc9dc6576bae6e933619409819a23fdd11ac0b1cb303cf331f5247f0e",
    "8cfb504fd92ed7964ec20040306932f139298790b397a65de74c6ab006085cdb",
    "1be11117a2fafa83116bfb104d8b7a7e45a9cfdef25e78a4714f4f30c0fb6b50",
    "0bd79dd87d1fbac5ceaf31bfe76273678485cbe3594a1ba512d4eaf160a3b866",
    "dc736688efdeadb3d1c1a67c15c89f4d8be51cfd9e765b6a0b4d157b9094371a",
    "edc027c187a455739ddec5993987840847b0e10d0be5f05db6337cb9841588d3",
    "12542e8e5368fedd3f4c20ef4e91d63a3c4f107c921642f553b513a8793c08d7",
    "94f47fe5e276b6212b09dc3eba8ec4dc27786054b9b97dc235eb50648c91017a",
    "ca1a314cfd17d4d126487ded1cf205d8a85cebe663f718352526e0d37afbacd8",
    "00c43c15c8b4af9bfc42e70291da214e60551c1931dd872b3ce7ee156effb94c",
    "0a7042ade145a63b3694142768248d94d2fcee3cf2fa8284cb6f601c65a45594",
    "c7899fa045002e396f718fe2223cbf92abeedd1c5555973b00aab9cb3f0fc2d6",
    "b9b15714014a2b61a0ca9d22181048cb409c47758003eab439d29332ec25c385",
    "28ddee94486d439dc81ce0fc972f23ec5eee917bbca054caadb21a479fffe336",
    "6594aa9633c6a58f5b19c50cb7b82c44ee85b7e0cdcbdb1b5aa75017f6cafb4f",
)
REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH = {
    line_hash: match_id
    for match_id, line_hash in enumerate(
        _REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID, start=1
    )
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
class Fixture:
    fifa_match_id: int
    stage: str
    group_label: str | None
    home_team: str
    away_team: str
    kickoff_at_utc: datetime
    source_path: str
    source_lines: str
    source_line_hash: str


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


def _line_hash(lines: Sequence[str], start: int, end: int) -> str:
    payload = ("\n".join(lines[start - 1 : end]) + "\n").encode()
    return hashlib.sha256(payload).hexdigest()


def _parse_date(line: str) -> datetime | None:
    match = re.fullmatch(
        r"(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (?:June|Jun|July|Jul) ([0-9]{1,2})",
        line.strip(),
    )
    if not match:
        return None
    month = 6 if "Jun" in line else 7
    return datetime(2026, month, int(match.group(1)))


def _parse_group_teams(lines: Sequence[str]) -> dict[str, tuple[str, ...]]:
    groups: dict[str, tuple[str, ...]] = {}
    for line in lines:
        match = re.match(r"Group ([A-L]) \| (.+)$", line.strip())
        if not match:
            continue
        teams = tuple(re.split(r"\s{2,}", match.group(2).strip()))
        if len(teams) != 4:
            raise ValueError(f"OpenFootball group {match.group(1)} is malformed")
        groups[match.group(1)] = teams
    if set(groups) != set("ABCDEFGHIJKL"):
        raise ValueError("OpenFootball must define exactly groups A-L")
    return groups


def _fixture_teams(line: str, teams: Iterable[str]) -> tuple[str, str]:
    normalized = _normalize(line)
    hits: list[tuple[int, str]] = []
    for team in teams:
        positions = [
            normalized.find(variant)
            for variant in _team_variants(team)
            if normalized.find(variant) >= 0
        ]
        if positions:
            hits.append((min(positions), team))
    hits.sort()
    if len(hits) != 2 or hits[0][0] == hits[1][0]:
        raise ValueError(f"Could not identify two OpenFootball teams in {line!r}")
    return hits[0][1], hits[1][1]


def _kickoff(date: datetime, line: str) -> datetime:
    match = re.search(r"([0-9]{1,2}):([0-9]{2}) UTC([+-][0-9]+)", line)
    if not match:
        raise ValueError(f"OpenFootball fixture lacks UTC offset: {line!r}")
    local = date.replace(hour=int(match.group(1)), minute=int(match.group(2)))
    local = local.replace(tzinfo=timezone(timedelta(hours=int(match.group(3)))))
    return local.astimezone(timezone.utc)


def parse_openfootball_fixtures(
    group_text: str, finals_text: str
) -> tuple[Fixture, ...]:
    """Parse only fixture identity/timing from the two pinned CC0 files."""
    group_lines = group_text.splitlines()
    groups = _parse_group_teams(group_lines)
    group_fixtures: list[tuple[str, str, str, datetime, int, int]] = []
    current_group: str | None = None
    current_date: datetime | None = None
    date_line = 0
    for number, line in enumerate(group_lines, 1):
        group_match = re.fullmatch(r"▪ Group ([A-L])", line.strip())
        if group_match:
            current_group = group_match.group(1)
            continue
        parsed_date = _parse_date(line)
        if parsed_date:
            current_date, date_line = parsed_date, number
            continue
        if " UTC" not in line or " @ " not in line:
            continue
        if current_group is None or current_date is None:
            raise ValueError("OpenFootball group fixture lacks group/date context")
        home, away = _fixture_teams(line, groups[current_group])
        group_fixtures.append(
            (
                current_group,
                home,
                away,
                _kickoff(current_date, line),
                date_line,
                number,
            )
        )
    if len(group_fixtures) != 72:
        raise ValueError(
            f"Expected 72 OpenFootball group fixtures; found {len(group_fixtures)}"
        )

    fixtures: list[Fixture] = []
    observed_group_hashes: set[str] = set()
    for values in group_fixtures:
        group, home, away, kickoff, start, end = values
        source_hash = _line_hash(group_lines, start, end)
        match_id = REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH.get(source_hash)
        if match_id is None:
            raise ValueError(
                "OpenFootball group fixture is absent from the reviewed FIFA "
                f"match-ID mapping: lines {start}-{end}"
            )
        if source_hash in observed_group_hashes:
            raise ValueError("OpenFootball group fixture evidence is duplicated")
        observed_group_hashes.add(source_hash)
        fixtures.append(
            Fixture(
                fifa_match_id=match_id,
                stage="group_stage",
                group_label=group,
                home_team=home,
                away_team=away,
                kickoff_at_utc=kickoff,
                source_path="2026--usa/cup.txt",
                source_lines=f"{start}-{end}",
                source_line_hash=source_hash,
            )
        )
    if observed_group_hashes != set(REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH):
        raise ValueError("Reviewed FIFA group match-ID mapping is incomplete")

    all_teams = tuple(team for teams in groups.values() for team in teams)
    final_lines = finals_text.splitlines()
    current_date = None
    date_line = 0
    stage: str | None = None
    for number, line in enumerate(final_lines, 1):
        stage_match = re.fullmatch(r"▪ (.+?)\s*", line.strip())
        if stage_match and stage_match.group(1) in _STAGES:
            stage = _STAGES[stage_match.group(1)]
            continue
        parsed_date = _parse_date(line)
        if parsed_date:
            current_date, date_line = parsed_date, number
            continue
        match = re.search(r"\((7[3-9]|[89][0-9]|10[0-4])\)", line)
        if not match:
            continue
        if current_date is None or stage is None:
            raise ValueError("OpenFootball knockout fixture lacks stage/date context")
        home, away = _fixture_teams(line, all_teams)
        fixtures.append(
            Fixture(
                fifa_match_id=int(match.group(1)),
                stage=stage,
                group_label=None,
                home_team=home,
                away_team=away,
                kickoff_at_utc=_kickoff(current_date, line),
                source_path="2026--usa/cup_finals.txt",
                source_lines=f"{date_line}-{number}",
                source_line_hash=_line_hash(final_lines, date_line, number),
            )
        )
    fixtures.sort(key=lambda fixture: fixture.fifa_match_id)
    if [fixture.fifa_match_id for fixture in fixtures] != list(range(1, 105)):
        raise ValueError("OpenFootball fixtures must map to FIFA match IDs 1..104")
    return tuple(fixtures)


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
