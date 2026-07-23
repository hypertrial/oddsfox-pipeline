from __future__ import annotations

import hashlib
import json
import runpy
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from oddsfox_pipeline.ingestion.polymarket.polygon_rpc import PolygonBlock

AUTHORING = runpy.run_path(
    str(
        Path(__file__).resolve().parents[3]
        / "scripts"
        / "generate_polymarket_wc2026_polygon_settlement_seed.py"
    )
)


def _word(value: int) -> str:
    return f"0x{value:064x}"


def _address_topic(value: str) -> str:
    return "0x" + value.removeprefix("0x").rjust(64, "0")


def _dynamic_bytes(value: str) -> str:
    payload = value.encode().hex()
    padded = payload.ljust(((len(payload) + 63) // 64) * 64, "0")
    return "0x" + f"{32:064x}{len(value.encode()):064x}" + padded


def _fixture(match_id: int):
    stage = "group_stage" if match_id <= 72 else "round_of_32"
    return AUTHORING["Fixture"](
        fifa_match_id=match_id,
        stage=stage,
        group_label="A" if match_id <= 72 else None,
        home_team=f"Home{match_id:03d}X",
        away_team=f"Away{match_id:03d}X",
        kickoff_at_utc=datetime(2026, 6, 11, 12, tzinfo=timezone.utc),
        source_path=(
            "2026--usa/cup.txt" if match_id <= 72 else "2026--usa/cup_finals.txt"
        ),
        source_lines=f"{match_id}-{match_id + 1}",
        source_line_hash=f"{match_id:064x}",
    )


def _question_payload(title: str, no_label: str, yes_label: str) -> str:
    return (
        f"q: title: {title}, description: synthetic. "
        "res_data: p1: 0, p2: 1, p3: 0.5. "
        f"Where p1 corresponds to {no_label}, p2 to {yes_label}, "
        "p3 to unknown/50-50."
    )


def _question_log(
    index: int,
    title: str,
    *,
    no_label: str = "No",
    yes_label: str = "Yes",
) -> dict[str, object]:
    return {
        "address": "0x" + "a" * 40,
        "topics": [
            AUTHORING["QUESTION_INITIALIZED_TOPIC"],
            _word(index),
            _word(1_000 + index),
            _address_topic("0x" + "b" * 40),
        ],
        "data": _dynamic_bytes(_question_payload(title, no_label, yes_label)),
        "transactionHash": _word(10_000 + index),
        "logIndex": hex(index),
        "blockNumber": hex(80_000_000 + index),
        "blockHash": _word(20_000 + index),
    }


def _complete_rpc_log(
    block_number: int,
    log_index: int,
    *,
    transaction_index: int = 0,
) -> dict[str, object]:
    return {
        "address": "0x" + "a" * 40,
        "topics": [_word(1)],
        "data": "0x",
        "removed": False,
        "transactionHash": _word(10_000 + block_number),
        "transactionIndex": hex(transaction_index),
        "logIndex": hex(log_index),
        "blockNumber": hex(block_number),
        "blockHash": _word(20_000 + block_number),
    }


def _authoring_rpc_with_logs(rows):
    class UnderlyingRPC:
        def call(self, method, _params):
            assert method == "eth_getLogs"
            return rows

        def block(self, number):
            return SimpleNamespace(
                number=number,
                hash=_word(20_000 + number),
            )

    rpc = AUTHORING["AuthoringRPC"].__new__(AUTHORING["AuthoringRPC"])
    rpc.rpc = UnderlyingRPC()
    return rpc


def test_authoring_logs_verify_headers_and_restore_chain_order() -> None:
    later = _complete_rpc_log(12, 3, transaction_index=2)
    earlier = _complete_rpc_log(11, 7, transaction_index=4)
    same_block_earlier = _complete_rpc_log(12, 1, transaction_index=1)
    rpc = _authoring_rpc_with_logs([later, earlier, same_block_earlier])

    logs = rpc.logs(11, 12)

    assert [
        (
            int(log["blockNumber"], 16),
            int(log["transactionIndex"], 16),
            int(log["logIndex"], 16),
        )
        for log in logs
    ] == [(11, 4, 7), (12, 1, 1), (12, 2, 3)]
    assert logs[0]["blockHash"] == _word(20_011)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda row: row.update(removed=True), "Removed or incomplete"),
        (lambda row: row.pop("transactionIndex"), "transactionIndex"),
        (lambda row: row.update(blockHash=_word(99)), "canonical header"),
    ],
)
def test_authoring_logs_reject_removed_incomplete_and_stale_rows(
    mutate, message
) -> None:
    row = _complete_rpc_log(11, 1)
    mutate(row)
    rpc = _authoring_rpc_with_logs([row])

    with pytest.raises(AUTHORING["PolygonRPCError"], match=message):
        rpc.logs(11, 11)


def test_authoring_logs_reject_response_outside_requested_address_filter() -> None:
    row = _complete_rpc_log(11, 1)
    rpc = _authoring_rpc_with_logs([row])

    with pytest.raises(AUTHORING["PolygonRPCError"], match="address filter"):
        rpc.logs(11, 11, address="0x" + "b" * 40)


@pytest.mark.parametrize(
    "topics",
    [
        (_word(2),),
        ([_word(2), _word(3)],),
        (_word(1), None),
    ],
)
def test_authoring_logs_reject_response_outside_requested_topic_filter(
    topics,
) -> None:
    row = _complete_rpc_log(11, 1)
    rpc = _authoring_rpc_with_logs([row])

    with pytest.raises(AUTHORING["PolygonRPCError"], match="topic filter"):
        rpc.logs(11, 11, topics=topics)


def test_authoring_logs_accept_canonical_null_and_or_filters() -> None:
    row = _complete_rpc_log(11, 1)
    row["topics"] = [_word(1).upper(), _word(2).upper(), _word(3).upper()]
    rpc = _authoring_rpc_with_logs([row])

    logs = rpc.logs(
        11,
        11,
        address=("0x" + "A" * 40),
        topics=(_word(1).upper(), None, [_word(3), _word(4)]),
    )

    assert logs[0]["address"] == "0x" + "a" * 40
    assert logs[0]["topics"] == [_word(1), _word(2), _word(3)]


def test_authoring_keccak_uses_web3_sha3_and_validates_response() -> None:
    class UnderlyingRPC:
        def __init__(self, result):
            self.result = result

        def call(self, method, params):
            assert (method, params) == ("web3_sha3", ["0x616263"])
            return self.result

    rpc = AUTHORING["AuthoringRPC"].__new__(AUTHORING["AuthoringRPC"])
    rpc.rpc = UnderlyingRPC(_word(123))
    assert rpc.keccak_bytes(b"abc") == _word(123)

    rpc.rpc = UnderlyingRPC("0x1234")
    with pytest.raises(AUTHORING["PolygonRPCError"], match="32-byte hash"):
        rpc.keccak_bytes(b"abc")


def _complete_question_logs(fixtures) -> tuple[list[dict], list[dict]]:
    group_logs: list[dict] = []
    knockout_logs: list[dict] = []
    index = 1
    for fixture in fixtures:
        if fixture.fifa_match_id <= 72:
            title_date = (
                fixture.kickoff_at_utc.astimezone(AUTHORING["SEMANTIC_TITLE_ZONE"])
                .date()
                .isoformat()
            )
            titles = (
                f"Will {fixture.home_team} win on {title_date}?",
                f"Will {fixture.home_team} vs. {fixture.away_team} end in a draw?",
                f"Will {fixture.away_team} win on {title_date}?",
            )
            for title in titles:
                group_logs.append(_question_log(index, title))
                index += 1
        else:
            label = "Team to Win" if fixture.fifa_match_id == 103 else "Team to Advance"
            knockout_logs.append(
                _question_log(
                    index,
                    f"{label}: {fixture.home_team} vs. {fixture.away_team}",
                    no_label=fixture.away_team,
                    yes_label=fixture.home_team,
                )
            )
            index += 1
    return group_logs, knockout_logs


def test_semantic_discovery_is_complete_and_ambiguity_fails_closed() -> None:
    fixtures = tuple(_fixture(match_id) for match_id in range(1, 105))
    group_logs, knockout_logs = _complete_question_logs(fixtures)
    irrelevant = _question_log(888, "Will Unrelated FC win on 2026-06-11?")
    irrelevant["removed"] = True
    group_logs.append(irrelevant)

    class RPC:
        def __init__(self, *, mismatch: bool = False):
            self.mismatch = mismatch
            self.canonicalized = []

        def scan(self, start, _end, **_kwargs):
            return iter(
                group_logs if start == AUTHORING["GROUP_FROM_BLOCK"] else knockout_logs
            )

        scan_candidates = scan

        def canonical_candidate_log(self, row, **_kwargs):
            self.canonicalized.append(row)
            return row

        def keccak_bytes(self, value):
            for log in (*group_logs, *knockout_logs):
                if AUTHORING["_decode_dynamic_bytes"](log["data"]) == value:
                    return _word(999_999) if self.mismatch else log["topics"][1]
            raise AssertionError("Synthetic ancillary bytes were not found")

    rpc = RPC()
    selected, ambiguities = AUTHORING["discover_questions"](rpc, fixtures)

    assert len(selected) == 248
    assert ambiguities == {}
    assert selected[(1, "draw")].fixture_id == 1
    assert len(rpc.canonicalized) == 248
    assert irrelevant not in rpc.canonicalized

    with pytest.raises(ValueError, match="does not match ancillary-data keccak"):
        AUTHORING["discover_questions"](RPC(mismatch=True), fixtures)

    knockout_logs.append(
        _question_log(
            999,
            f"Team to Advance: {fixtures[72].home_team}  vs. {fixtures[72].away_team}",
            no_label=fixtures[72].away_team,
            yes_label=fixtures[72].home_team,
        )
    )
    with pytest.raises(ValueError, match="Ambiguous semantic questions for match 73"):
        AUTHORING["discover_questions"](RPC(), fixtures)


def test_group_win_titles_use_eastern_date_but_draws_need_no_date() -> None:
    fixture = AUTHORING["Fixture"](
        **{
            **_fixture(1).__dict__,
            "kickoff_at_utc": datetime(2026, 6, 14, 1, tzinfo=timezone.utc),
        }
    )
    assert (
        AUTHORING["_question_match"](
            f"Will {fixture.home_team} win on 2026-06-13?", (fixture,)
        )[1]
        == "home_win"
    )
    assert (
        AUTHORING["_question_match"](
            f"Will {fixture.home_team} vs. {fixture.away_team} end in a draw?",
            (fixture,),
        )[1]
        == "draw"
    )
    assert (
        AUTHORING["_question_match"](
            f"Will {fixture.home_team} win on 2026-06-14?", (fixture,)
        )
        is None
    )


def test_reviewed_fifa_identity_mapping_and_orientation_fail_closed() -> None:
    mapping = AUTHORING["REVIEWED_GROUP_MATCH_ID_BY_LINE_HASH"]
    hashes = AUTHORING["_REVIEWED_GROUP_FIXTURE_HASHES_BY_MATCH_ID"]
    assert len(mapping) == 72
    assert set(mapping.values()) == set(range(1, 73))
    assert set(mapping.values()) | set(range(73, 105)) == set(range(1, 105))
    assert mapping[hashes[4]] == 5  # Haiti - Scotland
    assert mapping[hashes[7]] == 8  # Qatar - Switzerland
    assert AUTHORING["FIFA_SCHEDULE_SHA256"] == (
        "165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811"
    )

    fixture = _fixture(73)
    AUTHORING["_validate_question_orientation"](
        f"{fixture.home_team} vs. {fixture.away_team}: Team to Advance",
        fixture.away_team,
        fixture.home_team,
        fixture,
        "home_advances",
    )
    with pytest.raises(ValueError, match="orientation disagrees"):
        AUTHORING["_validate_question_orientation"](
            f"{fixture.away_team} vs. {fixture.home_team}: Team to Advance",
            fixture.away_team,
            fixture.home_team,
            fixture,
            "home_advances",
        )
    with pytest.raises(ValueError, match="orientation disagrees"):
        AUTHORING["_validate_question_orientation"](
            f"{fixture.home_team} vs. {fixture.away_team}: Team to Advance",
            fixture.home_team,
            fixture.away_team,
            fixture,
            "home_advances",
        )


def _semantic_event(
    *,
    address: str,
    topics: list[str],
    index: int,
    transaction_hash: str | None = None,
    block_number: int | None = None,
    block_hash: str | None = None,
    data: str = "0x",
) -> dict[str, object]:
    return {
        "address": address,
        "topics": topics,
        "data": data,
        "transactionHash": transaction_hash or _word(30_000 + index),
        "logIndex": hex(index),
        "blockNumber": hex(block_number or 85_196_000 + index),
        "blockHash": block_hash or _word(40_000 + index),
    }


def _chain_rpc(*, corrupt_operator: bool = False):
    market_id = _word(1)
    question_id = _word(2)
    request_id = _word(3)
    operator = "0x" + "6" * 40
    wrong_operator = "0x" + "7" * 40
    market_transaction = _word(50_000)
    market_block = 85_196_010
    market_block_hash = _word(60_000)
    market_data = "0x" + f"{7:064x}"
    question_transaction = _word(50_001)
    question_block = 85_196_011
    question_block_hash = _word(60_001)
    question_data = "0x" + f"{8:064x}"
    logs = {
        AUTHORING["MARKET_PREPARED_TOPIC"]: [
            _semantic_event(
                address=AUTHORING["NEG_RISK_ADAPTER"],
                topics=[
                    AUTHORING["MARKET_PREPARED_TOPIC"],
                    market_id,
                    _address_topic(operator),
                ],
                index=1,
                transaction_hash=market_transaction,
                block_number=market_block,
                block_hash=market_block_hash,
                data=market_data,
            )
        ],
        AUTHORING["ADAPTER_QUESTION_PREPARED_TOPIC"]: [
            _semantic_event(
                address=AUTHORING["NEG_RISK_ADAPTER"],
                topics=[
                    AUTHORING["ADAPTER_QUESTION_PREPARED_TOPIC"],
                    market_id,
                    question_id,
                ],
                index=11,
                transaction_hash=question_transaction,
                block_number=question_block,
                block_hash=question_block_hash,
                data=question_data,
            )
        ],
        AUTHORING["OPERATOR_MARKET_PREPARED_TOPIC"]: [
            _semantic_event(
                address=operator,
                topics=[AUTHORING["OPERATOR_MARKET_PREPARED_TOPIC"], market_id],
                index=2,
                transaction_hash=market_transaction,
                block_number=market_block,
                block_hash=market_block_hash,
                data=market_data,
            )
        ],
        AUTHORING["OPERATOR_QUESTION_PREPARED_TOPIC"]: [
            _semantic_event(
                address=wrong_operator if corrupt_operator else operator,
                topics=[
                    AUTHORING["OPERATOR_QUESTION_PREPARED_TOPIC"],
                    market_id,
                    question_id,
                    request_id,
                ],
                index=12,
                transaction_hash=question_transaction,
                block_number=question_block,
                block_hash=question_block_hash,
                data=question_data,
            )
        ],
        AUTHORING["CONDITION_PREPARATION_TOPIC"]: [
            _semantic_event(
                address=AUTHORING["CTF"],
                topics=[
                    AUTHORING["CONDITION_PREPARATION_TOPIC"],
                    _word(5),
                    _address_topic(AUTHORING["NEG_RISK_ADAPTER"]),
                    question_id,
                ],
                index=10,
                transaction_hash=question_transaction,
                block_number=question_block,
                block_hash=question_block_hash,
            )
        ],
    }

    class RPC:
        def __init__(self):
            self.logs = logs
            self.calls = []

        def scan(self, start, _end, *, topics=(), **_kwargs):
            self.calls.append((start, topics))
            if (
                topics[0] == AUTHORING["CONDITION_PREPARATION_TOPIC"]
                and start != AUTHORING["GROUP_FROM_BLOCK"]
            ):
                return iter(())
            return iter(self.logs.get(topics[0], ()))

    return RPC(), request_id, operator


def test_neg_risk_semantic_join_discovers_operator_and_rejects_drift() -> None:
    rpc, request_id, operator = _chain_rpc()

    by_oracle_question, _by_transaction, chains = AUTHORING["_condition_events"](rpc)

    assert len(by_oracle_question) == 1
    assert chains[request_id].operator == operator
    assert chains[request_id].question_id == _word(2)

    corrupt_rpc, _, _ = _chain_rpc(corrupt_operator=True)
    with pytest.raises(ValueError, match="adapter/operator event chain disagrees"):
        AUTHORING["_condition_events"](corrupt_rpc)


@pytest.mark.parametrize(
    ("topic", "field", "value", "message"),
    [
        (
            AUTHORING["OPERATOR_MARKET_PREPARED_TOPIC"],
            "transactionHash",
            _word(90_001),
            "MarketPrepared events are not one strictly ordered transaction",
        ),
        (
            AUTHORING["MARKET_PREPARED_TOPIC"],
            "logIndex",
            hex(3),
            "MarketPrepared events are not one strictly ordered transaction",
        ),
        (
            AUTHORING["OPERATOR_MARKET_PREPARED_TOPIC"],
            "data",
            "0x" + f"{99:064x}",
            "MarketPrepared adapter/operator ABI data disagree",
        ),
        (
            AUTHORING["OPERATOR_QUESTION_PREPARED_TOPIC"],
            "transactionHash",
            _word(90_002),
            "question preparation events are not one strictly ordered transaction",
        ),
        (
            AUTHORING["CONDITION_PREPARATION_TOPIC"],
            "logIndex",
            hex(13),
            "question preparation events are not one strictly ordered transaction",
        ),
        (
            AUTHORING["OPERATOR_QUESTION_PREPARED_TOPIC"],
            "data",
            "0x" + f"{100:064x}",
            "QuestionPrepared adapter/operator ABI data disagree",
        ),
    ],
)
def test_neg_risk_atomic_join_rejects_transaction_order_and_payload_drift(
    topic, field, value, message
) -> None:
    rpc, _, _ = _chain_rpc()
    rpc.logs[topic][0][field] = value

    with pytest.raises(ValueError, match=message) as raised:
        AUTHORING["_condition_events"](rpc)
    assert "transaction_hash" in str(raised.value)
    assert "log_index" in str(raised.value)


def test_condition_identity_is_scoped_by_oracle_and_exact_pair_duplicates_fail() -> (
    None
):
    base, _, _ = _chain_rpc()
    other_oracle = "0x" + "8" * 40

    class RPC:
        def __init__(self, duplicate_oracle: str):
            self.duplicate_oracle = duplicate_oracle

        def scan(self, start, end, *, topics=(), **kwargs):
            rows = list(base.scan(start, end, topics=topics, **kwargs))
            if (
                topics[0] == AUTHORING["CONDITION_PREPARATION_TOPIC"]
                and start == AUTHORING["GROUP_FROM_BLOCK"]
            ):
                rows.append(
                    _semantic_event(
                        address=AUTHORING["CTF"],
                        topics=[
                            AUTHORING["CONDITION_PREPARATION_TOPIC"],
                            _word(6),
                            _address_topic(self.duplicate_oracle),
                            _word(2),
                        ],
                        index=6,
                    )
                )
            return iter(rows)

    by_oracle_question, _, _ = AUTHORING["_condition_events"](RPC(other_oracle))
    assert set(by_oracle_question) == {
        (AUTHORING["NEG_RISK_ADAPTER"], _word(2)),
        (other_oracle, _word(2)),
    }

    with pytest.raises(ValueError, match="oracle/question pair"):
        AUTHORING["_condition_events"](RPC(AUTHORING["NEG_RISK_ADAPTER"]))


def test_condition_discovery_pushes_selected_question_ids_to_topic_three() -> None:
    rpc, request_id, _ = _chain_rpc()
    question = AUTHORING["Question"](
        question_id=request_id,
        adapter="0x" + "a" * 40,
        creator="0x" + "b" * 40,
        ancillary_sha256="1" * 64,
        transaction_hash=_word(100),
        log_index=10,
        block_number=85_196_001,
        block_hash=_word(101),
        proposition_type="home_win",
        fixture_id=1,
        semantic_title="Will Home001X win on 2026-06-11?",
        no_outcome_label="No",
        yes_outcome_label="Yes",
    )

    AUTHORING["_condition_events"](rpc, {(1, "home_win"): question})

    condition_calls = [
        topics
        for _, topics in rpc.calls
        if topics[0] == AUTHORING["CONDITION_PREPARATION_TOPIC"]
    ]
    assert condition_calls == [
        (
            AUTHORING["CONDITION_PREPARATION_TOPIC"],
            None,
            None,
            [_word(2)],
        ),
        (
            AUTHORING["CONDITION_PREPARATION_TOPIC"],
            None,
            None,
            [_word(2)],
        ),
    ]


def test_neg_risk_contract_link_accepts_arbitrary_creator_and_rejects_wrong_adapter() -> (
    None
):
    rpc, request_id, operator = _chain_rpc()
    _, _, chains = AUTHORING["_condition_events"](rpc)
    chain = chains[request_id]
    adapter = "0x" + "a" * 40
    creator = "0x" + "b" * 40
    assert creator != operator
    question = AUTHORING["Question"](
        question_id=request_id,
        adapter=adapter,
        creator=creator,
        ancillary_sha256="1" * 64,
        transaction_hash=_word(100),
        log_index=10,
        block_number=85_196_001,
        block_hash=_word(101),
        proposition_type="home_win",
        fixture_id=1,
        semantic_title="Will Home001X win on 2026-06-11?",
        no_outcome_label="No",
        yes_outcome_label="Yes",
    )

    class RelationshipRPC:
        def __init__(
            self,
            operator_oracle: str = adapter,
            neg_risk_ctf: str = AUTHORING["CTF"],
            neg_risk_col: str = AUTHORING["USDC_E"],
        ):
            self.operator_oracle = operator_oracle
            self.neg_risk_ctf = neg_risk_ctf
            self.neg_risk_col = neg_risk_col

        def call_at(self, contract, data, _block):
            values = {
                (operator, "0x" + AUTHORING["GET_OPERATOR_ORACLE"]): int(
                    self.operator_oracle, 16
                ),
                (
                    operator,
                    "0x" + AUTHORING["GET_OPERATOR_NEG_RISK_ADAPTER"],
                ): int(AUTHORING["NEG_RISK_ADAPTER"], 16),
                (adapter, "0x" + AUTHORING["GET_UMA_CTF"]): int(operator, 16),
                (
                    AUTHORING["NEG_RISK_ADAPTER"],
                    "0x" + AUTHORING["GET_NEG_RISK_CTF"],
                ): int(self.neg_risk_ctf, 16),
                (
                    AUTHORING["NEG_RISK_ADAPTER"],
                    "0x" + AUTHORING["GET_NEG_RISK_COL"],
                ): int(self.neg_risk_col, 16),
            }
            return values[(contract, data)]

    relationship = AUTHORING["_verify_neg_risk_contract_relationship"](
        RelationshipRPC(), question, chain
    )
    assert relationship["operator_oracle"] == adapter
    assert relationship["uma_ctf"] == operator
    assert relationship["operator_neg_risk_adapter"] == AUTHORING["NEG_RISK_ADAPTER"]
    assert relationship["neg_risk_ctf"] == AUTHORING["CTF"]
    assert relationship["neg_risk_collateral"] == AUTHORING["USDC_E"]

    wrong_adapter = "0x" + "c" * 40
    with pytest.raises(ValueError, match="relationship mismatch") as raised:
        AUTHORING["_verify_neg_risk_contract_relationship"](
            RelationshipRPC(operator_oracle=wrong_adapter), question, chain
        )
    message = str(raised.value)
    assert request_id in message
    assert creator not in message
    assert operator in message

    for kwargs, value in (
        ({"neg_risk_ctf": "0x" + "d" * 40}, "0x" + "d" * 40),
        ({"neg_risk_col": "0x" + "e" * 40}, "0x" + "e" * 40),
    ):
        with pytest.raises(ValueError, match="relationship mismatch") as raised:
            AUTHORING["_verify_neg_risk_contract_relationship"](
                RelationshipRPC(**kwargs), question, chain
            )
        assert value in str(raised.value)


def test_standard_contract_link_accepts_arbitrary_adapter_and_rejects_wrong_ctf() -> (
    None
):
    adapter = "0x" + "a" * 40
    creator = "0x" + "b" * 40
    transaction_hash = _word(100)
    block_number = 89_000_001
    block_hash = _word(101)
    question = AUTHORING["Question"](
        question_id=_word(77),
        adapter=adapter,
        creator=creator,
        ancillary_sha256="1" * 64,
        transaction_hash=transaction_hash,
        log_index=10,
        block_number=block_number,
        block_hash=block_hash,
        proposition_type="home_advances",
        fixture_id=73,
        semantic_title="Team to Advance: Home073X vs. Away073X",
        no_outcome_label="Away073X",
        yes_outcome_label="Home073X",
    )
    condition_log = {
        "transactionHash": transaction_hash,
        "logIndex": hex(9),
        "blockNumber": hex(block_number),
        "blockHash": block_hash,
        "address": AUTHORING["CTF"],
    }

    class RelationshipRPC:
        def __init__(self, ctf=AUTHORING["CTF"]):
            self.ctf = ctf

        def call_at(self, contract, data, block):
            assert (contract, data, block) == (
                adapter,
                "0x" + AUTHORING["GET_UMA_CTF"],
                block_number,
            )
            return int(self.ctf, 16)

    relationship = AUTHORING["_verify_standard_contract_relationship"](
        RelationshipRPC(), question, condition_log
    )
    assert relationship == {
        "uma_adapter": adapter,
        "uma_ctf": AUTHORING["CTF"],
        "verification_block_number": block_number,
        "verification_block_hash": block_hash,
    }

    wrong_ctf = "0x" + "c" * 40
    with pytest.raises(ValueError, match="relationship mismatch") as raised:
        AUTHORING["_verify_standard_contract_relationship"](
            RelationshipRPC(wrong_ctf), question, condition_log
        )
    message = str(raised.value)
    assert question.question_id in message
    assert adapter in message
    assert wrong_ctf in message
    assert transaction_hash in message
    assert creator not in message

    for changed in (
        {**condition_log, "transactionHash": _word(999)},
        {**condition_log, "logIndex": hex(11)},
    ):
        with pytest.raises(ValueError, match="must precede") as atomic_error:
            AUTHORING["_verify_standard_atomic_event_join"](question, changed)
        assert transaction_hash in str(atomic_error.value)
        assert "log_index" in str(atomic_error.value)


def test_evidence_privacy_gate_excludes_creator_wallet_fields_and_values() -> None:
    creator = "0x" + "b" * 40
    report = {
        "chain_verification": {
            "accepted_authorized_updates": 1,
            "ignored_third_party_updates": 2,
        },
        "rows": [{"uma_adapter": "0x" + "a" * 40}],
    }

    AUTHORING["_validate_evidence_privacy"](
        report,
        private_addresses=(creator,),
    )
    serialized = json.dumps(report, sort_keys=True).casefold()
    assert "creator" not in serialized
    assert "wallet" not in serialized
    assert creator not in serialized

    unsafe_reports = (
        {"rows": [{"question_creator": creator}]},
        {"rows": [{"wallet_address": "0x" + "c" * 40}]},
        {"rows": [{"note": f"private participant {creator}"}]},
    )
    for unsafe in unsafe_reports:
        with pytest.raises(ValueError, match="participant"):
            AUTHORING["_validate_evidence_privacy"](
                unsafe,
                private_addresses=(creator,),
            )


def test_creator_updates_replace_semantic_hash_and_spoofs_are_ignored() -> None:
    fixture = _fixture(73)
    creator = "0x" + "b" * 40
    adapter = "0x" + "a" * 40
    question_id = _word(77)
    initial = _question_payload(
        f"{fixture.home_team} vs. {fixture.away_team}: Team to Advance",
        fixture.away_team,
        fixture.home_team,
    ).encode()
    question = AUTHORING["Question"](
        question_id=question_id,
        adapter=adapter,
        creator=creator,
        ancillary_sha256=hashlib.sha256(initial).hexdigest(),
        transaction_hash=_word(1),
        log_index=1,
        block_number=1,
        block_hash=_word(2),
        proposition_type="home_advances",
        fixture_id=73,
        semantic_title=f"{fixture.home_team} vs. {fixture.away_team}: Team to Advance",
        no_outcome_label=fixture.away_team,
        yes_outcome_label=fixture.home_team,
    )
    update = _question_payload(
        f"{fixture.home_team} vs {fixture.away_team} - Team to Advance",
        fixture.away_team,
        fixture.home_team,
    ).encode()
    spoof = _semantic_event(
        address=adapter,
        topics=[
            AUTHORING["ANCILLARY_UPDATED_TOPIC"],
            question_id,
            _address_topic("0x" + "c" * 40),
        ],
        index=1,
    )
    spoof["data"] = "0xdeadbeef"
    spoof["transactionIndex"] = "0x0"
    accepted = _semantic_event(
        address=adapter,
        topics=[
            AUTHORING["ANCILLARY_UPDATED_TOPIC"],
            question_id,
            _address_topic(creator),
        ],
        index=2,
    )
    accepted["data"] = _dynamic_bytes(update.decode())
    accepted["transactionIndex"] = "0x0"

    class RPC:
        def scan(self, *_args, **_kwargs):
            return iter((spoof, accepted))

    updated, summary = AUTHORING["apply_creator_updates"](
        RPC(),
        (fixture,),
        {(73, "home_advances"): question},
        finalized_block=100,
    )
    assert summary == {
        "accepted_authorized_updates": 1,
        "ignored_third_party_updates": 1,
    }
    assert (
        updated[(73, "home_advances")].ancillary_sha256
        == hashlib.sha256(update).hexdigest()
    )

    reversed_update = _question_payload(
        f"{fixture.away_team} vs {fixture.home_team} - Team to Advance",
        fixture.away_team,
        fixture.home_team,
    )
    accepted["data"] = _dynamic_bytes(reversed_update)
    with pytest.raises(ValueError, match="orientation disagrees"):
        AUTHORING["apply_creator_updates"](
            RPC(),
            (fixture,),
            {(73, "home_advances"): question},
            finalized_block=100,
        )


def test_resolution_verification_pushes_selected_condition_ids_to_topic_one() -> None:
    condition_ids = [_word(1), _word(2)]

    class BoundaryRPC:
        def first_block_at_or_after(self, *_args, **_kwargs):
            return 50

    class RPC:
        def __init__(self):
            self.rpc = BoundaryRPC()
            self.calls = []

        def scan(self, start, end, *, address, topics):
            self.calls.append((start, end, address, topics))
            return iter(
                {"topics": [AUTHORING["CONDITION_RESOLUTION_TOPIC"], condition_id]}
                for condition_id in condition_ids
            )

    rpc = RPC()
    result = AUTHORING["verify_updates_and_resolutions"](
        rpc,
        [
            {
                "condition_id": condition_id,
                "window_start_at_utc": "2026-06-11T12:00:00Z",
            }
            for condition_id in condition_ids
        ],
        finalized=PolygonBlock(
            100,
            _word(100),
            datetime(2026, 7, 22, tzinfo=timezone.utc),
        ),
        update_summary={"accepted_authorized_updates": 0},
    )

    assert result["resolution_count"] == 2
    assert rpc.calls == [
        (
            50,
            100,
            AUTHORING["CTF"],
            (AUTHORING["CONDITION_RESOLUTION_TOPIC"], condition_ids),
        )
    ]


def test_resolution_verification_fails_when_any_condition_is_missing() -> None:
    condition_ids = [_word(1), _word(2)]

    class BoundaryRPC:
        def first_block_at_or_after(self, *_args, **_kwargs):
            return 50

    class RPC:
        rpc = BoundaryRPC()

        def scan(self, *_args, **_kwargs):
            return iter(
                (
                    {
                        "topics": [
                            AUTHORING["CONDITION_RESOLUTION_TOPIC"],
                            condition_ids[0],
                        ]
                    },
                )
            )

    with pytest.raises(ValueError, match="missing resolution evidence"):
        AUTHORING["verify_updates_and_resolutions"](
            RPC(),
            [
                {
                    "condition_id": condition_id,
                    "window_start_at_utc": "2026-06-11T12:00:00Z",
                }
                for condition_id in condition_ids
            ],
            finalized=PolygonBlock(
                100,
                _word(100),
                datetime(2026, 7, 22, tzinfo=timezone.utc),
            ),
            update_summary={"accepted_authorized_updates": 0},
        )


class _TamperedSourceResponse:
    content = b"tampered source"

    @staticmethod
    def raise_for_status() -> None:
        return None


def test_pinned_openfootball_hash_mismatch_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        AUTHORING["requests"],
        "get",
        lambda *_args, **_kwargs: _TamperedSourceResponse(),
    )

    with pytest.raises(ValueError, match="Pinned OpenFootball hash mismatch"):
        AUTHORING["_fetch_pinned_sources"](tmp_path / "candidate")


def test_pinned_fifa_schedule_hash_mismatch_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(
        AUTHORING["requests"],
        "get",
        lambda *_args, **_kwargs: _TamperedSourceResponse(),
    )

    with pytest.raises(ValueError, match="Pinned FIFA schedule hash mismatch"):
        AUTHORING["_fetch_pinned_fifa_schedule"](tmp_path / "candidate")


@pytest.mark.parametrize(
    "provider_label",
    (
        "https://rpc.example/api_key=secret",
        "provider?api_key=secret",
        "unsafe\nlabel",
    ),
)
def test_author_seed_rejects_unsafe_provider_label_before_writing(
    monkeypatch, tmp_path, provider_label
) -> None:
    globals_ = AUTHORING["author_seed"].__globals__
    monkeypatch.setitem(globals_, "REPO_ROOT", tmp_path)
    output_dir = tmp_path / "artifacts" / "candidate"

    with pytest.raises(ValueError, match="safe 1-64 character display label"):
        AUTHORING["author_seed"](
            rpc_url="https://rpc.example",
            provider_label=provider_label,
            output_dir=output_dir,
            manifest_version="1.0.0",
            reviewed_at=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )

    assert not output_dir.exists()


@pytest.mark.parametrize("relative_output", ("outside", "artifacts"))
def test_author_seed_requires_a_child_directory_below_artifacts(
    monkeypatch, tmp_path, relative_output
) -> None:
    globals_ = AUTHORING["author_seed"].__globals__
    monkeypatch.setitem(globals_, "REPO_ROOT", tmp_path)

    with pytest.raises(ValueError, match="new directory below artifacts"):
        AUTHORING["author_seed"](
            rpc_url="https://rpc.example",
            provider_label="test-provider",
            output_dir=tmp_path / relative_output,
            manifest_version="1.0.0",
            reviewed_at=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )


def test_author_seed_refuses_to_overwrite_existing_artifacts(
    monkeypatch, tmp_path
) -> None:
    globals_ = AUTHORING["author_seed"].__globals__
    monkeypatch.setitem(globals_, "REPO_ROOT", tmp_path)
    output_dir = tmp_path / "artifacts" / "candidate"
    output_dir.mkdir(parents=True)

    with pytest.raises(FileExistsError, match="Refusing to overwrite"):
        AUTHORING["author_seed"](
            rpc_url="https://rpc.example",
            provider_label="test-provider",
            output_dir=output_dir,
            manifest_version="1.0.0",
            reviewed_at=datetime(2026, 7, 22, 12, tzinfo=timezone.utc),
        )


def test_pinned_standard_and_neg_risk_token_calls_preserve_orientation() -> None:
    class RPC:
        def __init__(self, values):
            self.values = iter(values)
            self.calls = []

        def call_at(self, contract, data, block):
            self.calls.append((contract, data, block))
            return next(self.values)

    standard = RPC((101, 202, 303, 404))
    assert AUTHORING["_standard_tokens"](standard, _word(9), 123) == (
        "303",
        "404",
    )
    assert [call[1][:10] for call in standard.calls] == [
        "0x856296f7",
        "0x856296f7",
        "0x39dd7530",
        "0x39dd7530",
    ]
    assert standard.calls[0][1].endswith(f"{1:064x}")
    assert standard.calls[1][1].endswith(f"{2:064x}")

    neg_risk = RPC((505, 606))
    assert AUTHORING["_neg_risk_tokens"](neg_risk, _word(10), 456) == (
        "505",
        "606",
    )
    assert neg_risk.calls[0][1].endswith(f"{1:064x}")
    assert neg_risk.calls[1][1].endswith(f"{0:064x}")
