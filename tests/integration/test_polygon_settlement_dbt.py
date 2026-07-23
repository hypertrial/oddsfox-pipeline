"""Fixture-backed integration test for the isolated Polygon settlement graph."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest
from tests.integration.conftest import write_dbt_profile
from tests.support.distribution_fixtures import write_synthetic_distribution_inputs

import oddsfox_pipeline.storage.duckdb.connection as connection
from oddsfox_pipeline.ingestion.polymarket.polygon_resolution import (
    load_polygon_resolution_attestation,
)
from oddsfox_pipeline.ingestion.polymarket.polygon_seed import (
    load_polygon_market_seed,
)
from oddsfox_pipeline.publishing.polygon_settlement import (
    _read_market_rows,
    _validate_committed_seed,
)
from oddsfox_pipeline.storage.duckdb.connection import init_duck_db
from oddsfox_pipeline.storage.duckdb.polygon_settlement import (
    FILL_COLUMNS,
    publish_polygon_settlement_scan,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_ROOT = REPO_ROOT / "dbt"
SEED_RELATION = (
    '"polymarket_wc2026_staging"."polymarket_wc2026_polygon_settlement_markets"'
)
MART_RELATION = (
    '"polymarket_wc2026_marts"."polymarket_wc2026_polygon_settlement_minute_odds"'
)
QUALITY_RELATION = (
    '"polymarket_wc2026_observability".'
    '"polymarket_wc2026_polygon_settlement_data_quality"'
)
QUALITY_ISSUES_RELATION = (
    '"polymarket_wc2026_observability".'
    '"polymarket_wc2026_polygon_settlement_quality_issues"'
)
UNIVERSE_RELATION = (
    '"polymarket_wc2026_intermediate".'
    '"int_polymarket_wc2026_polygon_settlement_market_universe"'
)
CANDIDATE_RELATION = (
    '"polymarket_wc2026_intermediate".'
    '"int_polymarket_wc2026_polygon_settlement_minute_odds_candidate"'
)
STG_FILLS_RELATION = (
    '"polymarket_wc2026_staging"."stg_polymarket_wc2026_polygon_settlement_fills"'
)
PUBLICATION_GATE_RELATION = (
    '"polymarket_wc2026_intermediate".'
    '"int_polymarket_wc2026_polygon_settlement_publication_gate"'
)
RAW_FILLS = '"polymarket_wc2026_raw"."polygon_settlement_fills"'
OPS_RUNS = '"polymarket_wc2026_ops"."polygon_settlement_scan_runs"'
OPS_CHUNKS = '"polymarket_wc2026_ops"."polygon_settlement_scan_chunks"'
OPS_STAGE = '"polymarket_wc2026_ops"."polygon_settlement_fill_stage"'

STANDARD_EXCHANGE = "0xe111180000d2663c0091e4f400237545b87b996b"
NEG_RISK_EXCHANGE = "0xe2222d279d744050d28e00520010520000310f59"
SCAN_ID = "fixture-polygon-settlement-scan"
FROM_BLOCK_HASH = "0x" + "1" * 64
TO_BLOCK_HASH = "0x" + "2" * 64


def _run_dbt(
    args: list[str],
    *,
    project_dir: Path,
    profiles_dir: Path,
    env: dict[str, str],
) -> None:
    command = [
        sys.executable,
        "-m",
        "dbt.cli.main",
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(profiles_dir),
    ]
    completed = subprocess.run(
        command,
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def _seed_published_fixture(db_path: Path) -> None:
    target_ranges = json.dumps(
        [
            {
                "exchange_address": NEG_RISK_EXCHANGE,
                "from_block": 100,
                "to_block": 200,
                "from_block_hash": FROM_BLOCK_HASH,
                "to_block_hash": TO_BLOCK_HASH,
            },
            {
                "exchange_address": STANDARD_EXCHANGE,
                "from_block": 100,
                "to_block": 200,
                "from_block_hash": FROM_BLOCK_HASH,
                "to_block_hash": TO_BLOCK_HASH,
            },
        ],
        separators=(",", ":"),
        sort_keys=True,
    )
    with duckdb.connect(str(db_path)) as conn:
        seed_row = conn.execute(
            f"""
            select
                proposition_id,
                condition_id,
                yes_token_id,
                no_token_id,
                lower(exchange_address) as exchange_address,
                cast(window_start_at_utc as timestamp) as window_start_at_utc,
                manifest_version,
                lower(manifest_sha256) as manifest_sha256
            from {SEED_RELATION}
            order by fifa_match_id, proposition_id
            limit 1
            """
        ).fetchone()
        assert seed_row is not None
        (
            proposition_id,
            condition_id,
            yes_token_id,
            no_token_id,
            exchange_address,
            window_start_at_utc,
            manifest_version,
            manifest_sha256,
        ) = seed_row
        assert exchange_address == NEG_RISK_EXCHANGE

        conn.execute(
            f"""
            insert into {OPS_RUNS} (
                scan_id,
                manifest_version,
                manifest_sha256,
                normalizer_version,
                chain_id,
                provider_label,
                provider_origin,
                finalized_head_number,
                finalized_head_hash,
                target_ranges_json,
                boundary_blocks_sha256,
                status,
                raw_published,
                verification_status,
                started_at
            ) values (
                ?, ?, ?, 'polygon-v2-settlement-v4', 137,
                'fixture-rpc', 'https://fixture.invalid', 200, ?, ?, ?,
                'running', false, 'matched',
                timestamp '2026-08-01 00:00:00'
            )
            """,
            [
                SCAN_ID,
                manifest_version,
                manifest_sha256,
                TO_BLOCK_HASH,
                target_ranges,
                "3" * 64,
            ],
        )

        # Model an adaptive retry: a failed parent is superseded by two
        # successful leaves. Publication must remove that diagnostic failure
        # before the fail-closed dbt audit inspects the published scan.
        conn.execute(
            f"""
            insert into {OPS_CHUNKS} (
                scan_id,
                exchange_address,
                from_block,
                to_block,
                status,
                event_count,
                scoped_event_count,
                normalized_fill_count,
                completed_at,
                error_type,
                error_message
            ) values (
                ?, ?, 100, 200, 'failed', 0, 0, 0,
                timestamp '2026-08-01 00:00:30',
                'RpcError', 'fixture provider limit'
            )
            """,
            [SCAN_ID, exchange_address],
        )

        middle_to_block_hash = "0x" + "8" * 64
        middle_from_block_hash = "0x" + "9" * 64
        chunk_rows = [
            (
                SCAN_ID,
                exchange_address,
                100,
                180,
                FROM_BLOCK_HASH,
                middle_to_block_hash,
                "success",
                21,
                21,
                7,
                "4" * 64,
                100,
                3,
                1,
                7,
                2,
                0,
                0,
                0,
                0,
                21,
                0,
                1,
            ),
            (
                SCAN_ID,
                exchange_address,
                181,
                200,
                middle_from_block_hash,
                TO_BLOCK_HASH,
                "success",
                0,
                0,
                0,
                "8" * 64,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ),
            (
                SCAN_ID,
                STANDARD_EXCHANGE,
                100,
                200,
                FROM_BLOCK_HASH,
                TO_BLOCK_HASH,
                "success",
                0,
                0,
                0,
                "a" * 64,
                1,
                1,
                1,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ),
        ]
        conn.executemany(
            f"""
            insert into {OPS_CHUNKS} (
                scan_id,
                exchange_address,
                from_block,
                to_block,
                from_block_hash,
                to_block_hash,
                status,
                event_count,
                scoped_event_count,
                normalized_fill_count,
                scoped_event_sha256,
                duration_ms,
                http_request_count,
                log_rpc_call_count,
                receipt_rpc_call_count,
                header_rpc_call_count,
                discovery_count,
                eligible_discovery_count,
                filtered_discovery_count,
                receipt_transaction_count,
                receipt_log_count,
                retry_count,
                adaptive_split_count,
                completed_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                timestamp '2026-08-01 00:01:00')
            """,
            chunk_rows,
        )

        fill_rows = [
            (
                SCAN_ID,
                exchange_address,
                "0x" + "5" * 64,
                window_start_at_utc,
                "0x" + "6" * 64,
                0,
                proposition_id,
                condition_id,
                yes_token_id,
                "yes",
                yes_token_id,
                "7.000000",
                "0.070000000000000000",
                False,
                "7" * 64,
            ),
            (
                SCAN_ID,
                exchange_address,
                "0x" + "5" * 64,
                window_start_at_utc,
                "0x" + "6" * 64,
                1,
                proposition_id,
                condition_id,
                no_token_id,
                "no",
                yes_token_id,
                "93.000000",
                "0.930000000000000000",
                True,
                "7" * 64,
            ),
        ]
        conn.executemany(
            f"""
            insert into {OPS_STAGE} (
                scan_id,
                chain_id,
                exchange_address,
                chunk_from_block,
                chunk_to_block,
                block_number,
                block_hash,
                block_timestamp,
                transaction_hash,
                transaction_index,
                passive_log_index,
                active_log_index,
                matched_log_index,
                normalized_leg_ordinal,
                proposition_id,
                condition_id,
                token_id,
                outcome_side,
                order_side,
                source_token_id,
                source_maker_amount,
                source_taker_amount,
                share_volume,
                gross_collateral_volume,
                price,
                normalization_kind,
                is_derived,
                segment_sha256,
                decoder_version,
                ingested_at
            ) values (
                ?, 137, ?, 100, 180, 150, ?, ?, ?, 0, 1, 2, 3, ?,
                ?, ?, ?, ?, 'BUY', ?, '7000000', '100000000',
                100.000000, ?, ?, 'mint', ?, ?, 'polygon-v2-settlement-v4',
                timestamp '2026-08-01 00:01:00'
            )
            """,
            fill_rows,
        )

        def complementary_fill(
            *,
            token_id: str,
            outcome_side: str,
            order_side: str,
            price: str,
            minute: int,
            transaction_index: int,
            transaction_hash_character: str,
            passive_log_index: int,
        ) -> dict[str, object]:
            shares = Decimal("1.000000")
            collateral = Decimal(price).quantize(Decimal("0.000001"))
            if order_side == "BUY":
                maker_amount = str(int(collateral * 1_000_000))
                taker_amount = "1000000"
            else:
                maker_amount = "1000000"
                taker_amount = str(int(collateral * 1_000_000))
            return {
                "scan_id": SCAN_ID,
                "chain_id": 137,
                "exchange_address": exchange_address,
                "chunk_from_block": 100,
                "chunk_to_block": 180,
                "block_number": 150 + transaction_index,
                "block_hash": "0x" + transaction_hash_character * 64,
                "block_timestamp": window_start_at_utc
                + timedelta(minutes=minute, seconds=5),
                "transaction_hash": "0x" + transaction_hash_character * 64,
                "transaction_index": transaction_index,
                "passive_log_index": passive_log_index,
                "active_log_index": passive_log_index + 1,
                "matched_log_index": passive_log_index + 2,
                "normalized_leg_ordinal": 0,
                "proposition_id": proposition_id,
                "condition_id": condition_id,
                "token_id": token_id,
                "outcome_side": outcome_side,
                "order_side": order_side,
                "source_token_id": token_id,
                "source_maker_amount": maker_amount,
                "source_taker_amount": taker_amount,
                "share_volume": shares,
                "gross_collateral_volume": collateral,
                "price": Decimal(price).quantize(Decimal("0.000000000000000001")),
                "normalization_kind": "complementary",
                "is_derived": False,
                "segment_sha256": transaction_hash_character * 64,
                "decoder_version": "polygon-v2-settlement-v4",
                "ingested_at": window_start_at_utc + timedelta(days=1),
            }

        # These minute-one rows arrive in reverse chain order and share the
        # same timestamp. OHLC must use chain locators, not response order or
        # timestamp ties.
        complementary_rows = [
            complementary_fill(
                token_id=yes_token_id,
                outcome_side="yes",
                order_side="BUY",
                price="0.600000000000000000",
                minute=1,
                transaction_index=2,
                transaction_hash_character="a",
                passive_log_index=20,
            ),
            complementary_fill(
                token_id=yes_token_id,
                outcome_side="yes",
                order_side="BUY",
                price="0.400000000000000000",
                minute=1,
                transaction_index=1,
                transaction_hash_character="b",
                passive_log_index=10,
            ),
            complementary_fill(
                token_id=no_token_id,
                outcome_side="no",
                order_side="SELL",
                price="0.300000000000000000",
                minute=2,
                transaction_index=3,
                transaction_hash_character="c",
                passive_log_index=30,
            ),
            complementary_fill(
                token_id=yes_token_id,
                outcome_side="yes",
                order_side="BUY",
                price="0.200000000000000000",
                minute=3,
                transaction_index=4,
                transaction_hash_character="d",
                passive_log_index=40,
            ),
            complementary_fill(
                token_id=no_token_id,
                outcome_side="no",
                order_side="BUY",
                price="0.700000000000000000",
                minute=3,
                transaction_index=5,
                transaction_hash_character="e",
                passive_log_index=50,
            ),
        ]
        placeholders = ", ".join("?" for _ in FILL_COLUMNS)
        conn.executemany(
            f"insert into {OPS_STAGE} ({', '.join(FILL_COLUMNS)}) "
            f"values ({placeholders})",
            [
                tuple(row[column] for column in FILL_COLUMNS)
                for row in complementary_rows
            ],
        )

        assert (
            publish_polygon_settlement_scan(
                conn,
                scan_id=SCAN_ID,
                target_ranges=json.loads(target_ranges),
            )
            == 7
        )
        assert conn.execute(
            f"select count(*) from {OPS_CHUNKS} where status = 'failed'"
        ).fetchone() == (0,)


def test_polygon_settlement_graph_builds_exact_dense_mart(
    tmp_path: Path,
    monkeypatch,
    dbt_profiles_dir: Path,
) -> None:
    dbt_root = tmp_path / "dbt"
    shutil.copytree(DBT_ROOT, dbt_root)
    seed_path, attestation_path = write_synthetic_distribution_inputs(dbt_root)
    monkeypatch.setattr(
        "oddsfox_pipeline.publishing.polygon_settlement."
        "DEFAULT_POLYGON_MARKET_SEED_PATH",
        seed_path,
    )
    monkeypatch.setattr(
        "oddsfox_pipeline.publishing.polygon_settlement."
        "load_polygon_resolution_attestation",
        lambda *, manifest: load_polygon_resolution_attestation(
            attestation_path,
            manifest=manifest,
        ),
    )
    committed_manifest = load_polygon_market_seed(seed_path)
    db_path = tmp_path / "polygon_settlement.duckdb"
    monkeypatch.setenv("DUCKDB_PATH", str(db_path))
    monkeypatch.setenv("DUCKDB_NAME", str(db_path))
    write_dbt_profile(dbt_profiles_dir, db_path)
    connection.reset_duckdb_connection_state()
    init_duck_db()

    env = os.environ.copy()
    env["DUCKDB_PATH"] = str(db_path)
    env["DUCKDB_NAME"] = str(db_path)
    _run_dbt(
        [
            "seed",
            "--full-refresh",
            "--select",
            "polymarket_wc2026_polygon_settlement_markets",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    _seed_published_fixture(db_path)
    _run_dbt(
        ["build", "--full-refresh", "--select", "tag:polygon_settlement"],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    with duckdb.connect(str(db_path), read_only=True) as conn:
        _validate_committed_seed(
            _read_market_rows(conn),
            {
                "seed_sha256": committed_manifest.sha256,
                "seed_version": committed_manifest.version,
            },
        )
        inventory = conn.execute(
            f"""
            select
                count(*) as row_count,
                count(distinct proposition_id) as proposition_count,
                count(*) filter (where stage = 'group_stage') as group_rows,
                count(*) filter (where stage <> 'group_stage') as knockout_rows,
                count(*) filter (
                    where minute_status = 'both_observed'
                ) as both_observed,
                count(*) filter (where minute_status = 'yes_only') as yes_only,
                count(*) filter (where minute_status = 'no_only') as no_only,
                count(*) filter (where minute_status = 'no_fills') as no_fills
            from {MART_RELATION}
            """
        ).fetchone()
        assert inventory == (39120, 248, 32400, 6720, 2, 1, 1, 39116)

        axes = conn.execute(
            f"""
            select
                count(*) filter (
                    where stage = 'group_stage'
                    and elapsed_window_minute between 0 and 149
                ) as valid_group_rows,
                count(*) filter (
                    where stage <> 'group_stage'
                    and elapsed_window_minute between 0 and 209
                ) as valid_knockout_rows,
                count(*) filter (
                    where settlement_minute_utc >= analysis_window_end_at_utc
                ) as end_leaks
            from {MART_RELATION}
            """
        ).fetchone()
        assert axes == (32400, 6720, 0)

        publication = conn.execute(
            f"""
            select
                publication_ready,
                blocking_issue_keys,
                expected_minute_rows,
                actual_minute_rows
            from {QUALITY_RELATION}
            """
        ).fetchone()
        assert publication == (True, None, 39120, 39120)

        empty_minute = conn.execute(
            f"""
            select
                yes_open,
                no_open,
                yes_normalized_fill_count,
                no_normalized_fill_count,
                yes_share_volume,
                no_share_volume
            from {MART_RELATION}
            where minute_status = 'no_fills'
            limit 1
            """
        ).fetchone()
        assert empty_minute == (None, None, 0, 0, 0, 0)

        same_second_ohlc = conn.execute(
            f"""
            select
                yes_open,
                yes_high,
                yes_low,
                yes_close,
                yes_vwap,
                yes_normalized_fill_count,
                yes_derived_fill_count,
                yes_share_volume,
                yes_gross_collateral_volume,
                yes_first_settlement_at_utc,
                yes_last_settlement_at_utc,
                minute_status
            from {MART_RELATION}
            where elapsed_window_minute = 1 and yes_normalized_fill_count = 2
            """
        ).fetchone()
        assert same_second_ohlc == (
            Decimal("0.400000000000000000"),
            Decimal("0.600000000000000000"),
            Decimal("0.400000000000000000"),
            Decimal("0.600000000000000000"),
            Decimal("0.500000000000000000"),
            2,
            0,
            Decimal("2.000000"),
            Decimal("1.000000"),
            same_second_ohlc[9],
            same_second_ohlc[9],
            "yes_only",
        )

        warnings = {
            row[0]
            for row in conn.execute(
                f"select distinct issue_type from {QUALITY_ISSUES_RELATION} "
                "where severity = 'warn'"
            ).fetchall()
        }
        assert {
            "token_coverage",
            "minute_coverage",
            "derived_fills",
            "pair_price",
        } <= warnings
        assert "verification" not in warnings

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"update {OPS_RUNS} set verification_status = 'mismatched' "
            "where scan_id = ?",
            [SCAN_ID],
        )
    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_quality_issues",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        verification_warning = conn.execute(
            f"""
            select count(*)
            from {QUALITY_ISSUES_RELATION}
            where severity = 'warn' and issue_type = 'verification'
            """
        ).fetchone()
        assert verification_warning == (1,)
        assert conn.execute(
            f"select publication_ready from {QUALITY_RELATION}"
        ).fetchone() == (True,)
        conn.execute(
            f"update {OPS_RUNS} set verification_status = 'matched' where scan_id = ?",
            [SCAN_ID],
        )
    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_quality_issues",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )

    duplicate_scan_id = "0-fixture-duplicate-current-scan"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            f"""
            insert into {OPS_RUNS} by name
            select * replace (? as scan_id)
            from {OPS_RUNS}
            where scan_id = ?
            """,
            [duplicate_scan_id, SCAN_ID],
        )

    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        duplicate_scan_quality = conn.execute(
            f"""
            select published_scan_count, publication_ready, blocking_issue_keys
            from {QUALITY_RELATION}
            """
        ).fetchone()
        assert duplicate_scan_quality == (2, False, "scan_missing")
        conn.execute(f"delete from {OPS_RUNS} where scan_id = ?", [duplicate_scan_id])

        conn.execute(f"create table polygon_pair_baseline as select * from {RAW_FILLS}")

    pair_mutations = {
        "missing derived ordinal": f"""
            delete from {RAW_FILLS}
            where normalized_leg_ordinal = 1
        """,
        "non-complementary token and outcome": f"""
            update {RAW_FILLS}
            set token_id = source_token_id, outcome_side = 'yes'
            where normalized_leg_ordinal = 1
        """,
        "unequal shares": f"""
            update {RAW_FILLS}
            set
                share_volume = 200.000000,
                gross_collateral_volume = 193.000000,
                price = 0.965000000000000000
            where normalized_leg_ordinal = 1
        """,
        "non-conserving collateral": f"""
            update {RAW_FILLS}
            set
                gross_collateral_volume = 92.000000,
                price = 0.920000000000000000
            where normalized_leg_ordinal = 1
        """,
        "disagreeing locator": f"""
            update {RAW_FILLS}
            set block_hash = '0x{("a" * 64)}'
            where normalized_leg_ordinal = 1
        """,
        "disagreeing kind": f"""
            update {RAW_FILLS}
            set normalization_kind = 'merge'
            where normalized_leg_ordinal = 1
        """,
        "disagreeing segment hash": f"""
            update {RAW_FILLS}
            set segment_sha256 = '{("b" * 64)}'
            where normalized_leg_ordinal = 1
        """,
    }
    for mutation_name, mutation_sql in pair_mutations.items():
        with duckdb.connect(str(db_path)) as conn:
            conn.execute(f"delete from {RAW_FILLS}")
            conn.execute(
                f"insert into {RAW_FILLS} by name select * from polygon_pair_baseline"
            )
            conn.execute(mutation_sql)

        _run_dbt(
            [
                "run",
                "--select",
                "polymarket_wc2026_polygon_settlement_data_quality",
            ],
            project_dir=dbt_root,
            profiles_dir=dbt_profiles_dir,
            env=env,
        )
        with duckdb.connect(str(db_path)) as conn:
            pair_quality = conn.execute(
                f"""
                select
                    invalid_normalization_pair_grains,
                    publication_ready,
                    blocking_issue_keys
                from {QUALITY_RELATION}
                """
            ).fetchone()
            assert pair_quality is not None, mutation_name
            assert pair_quality[0] > 0, mutation_name
            assert pair_quality[1] is False, mutation_name
            assert "raw_normalization_pairs" in pair_quality[2], mutation_name
            with pytest.raises(duckdb.Error, match="raw_normalization_pairs"):
                conn.execute(
                    f"select publication_ready from {PUBLICATION_GATE_RELATION}"
                )

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"delete from {RAW_FILLS}")
        conn.execute(
            f"insert into {RAW_FILLS} by name select * from polygon_pair_baseline"
        )
        conn.execute("drop table polygon_pair_baseline")

    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        restored_quality = conn.execute(
            f"""
            select invalid_normalization_pair_grains, publication_ready
            from {QUALITY_RELATION}
            """
        ).fetchone()
        assert restored_quality == (0, True)
        assert conn.execute(f"select count(*) from {MART_RELATION}").fetchone() == (
            39120,
        )

    baseline_relations = {
        UNIVERSE_RELATION: "polygon_quality_baseline_universe",
        CANDIDATE_RELATION: "polygon_quality_baseline_candidate",
        RAW_FILLS: "polygon_quality_baseline_fills",
        OPS_RUNS: "polygon_quality_baseline_runs",
        OPS_CHUNKS: "polygon_quality_baseline_chunks",
        QUALITY_ISSUES_RELATION: "polygon_quality_baseline_issues",
    }
    with duckdb.connect(str(db_path)) as conn:
        for relation, baseline in baseline_relations.items():
            conn.execute(f"create table {baseline} as select * from {relation}")

    def restore_quality_inputs() -> None:
        with duckdb.connect(str(db_path)) as restore_conn:
            for relation, baseline in baseline_relations.items():
                restore_conn.execute(f"delete from {relation}")
                restore_conn.execute(
                    f"insert into {relation} by name select * from {baseline}"
                )

    blocker_mutations = {
        "seed_inventory": f"""
            delete from {UNIVERSE_RELATION}
            where proposition_id = (
                select min(proposition_id) from {UNIVERSE_RELATION}
            )
        """,
        "seed_stage_distribution": f"""
            update {UNIVERSE_RELATION}
            set stage = 'final'
            where fifa_match_id = 1
        """,
        "seed_proposition_shape": f"""
            update {UNIVERSE_RELATION}
            set proposition_type = 'home_advances'
            where proposition_id = (
                select min(proposition_id) from {UNIVERSE_RELATION}
            )
        """,
        "seed_unique_ids": f"""
            update {UNIVERSE_RELATION}
            set no_token_id = yes_token_id
            where proposition_id = (
                select min(proposition_id) from {UNIVERSE_RELATION}
            )
        """,
        "seed_windows": f"""
            update {UNIVERSE_RELATION}
            set analysis_window_end_at_utc =
                    analysis_window_end_at_utc + interval '1 minute',
                window_minutes = window_minutes + 1
            where proposition_id = (
                select min(proposition_id) from {UNIVERSE_RELATION}
            )
        """,
        "seed_evidence": f"""
            update {UNIVERSE_RELATION}
            set openfootball_revision = 'invalid'
            where proposition_id = (
                select min(proposition_id) from {UNIVERSE_RELATION}
            )
        """,
        "scan_missing": f"delete from {OPS_RUNS}",
        "scan_manifest": f"""
            update {OPS_RUNS}
            set manifest_sha256 = '{"0" * 64}'
        """,
        "scan_integrity": f"""
            update {OPS_RUNS}
            set normalizer_version = 'invalid-normalizer'
        """,
        "scan_chunks": f"""
            delete from {OPS_CHUNKS}
            where exchange_address = (
                select min(exchange_address) from {OPS_CHUNKS}
            )
        """,
        "raw_empty": f"delete from {RAW_FILLS}",
        "raw_scan_mismatch": f"""
            update {RAW_FILLS}
            set scan_id = 'foreign-scan'
            where transaction_hash = (
                select min(transaction_hash) from {RAW_FILLS}
            )
        """,
        "raw_mapping": f"""
            update {RAW_FILLS}
            set proposition_id = 'unknown-proposition'
            where transaction_hash = (
                select min(transaction_hash) from {RAW_FILLS}
            )
        """,
        "raw_values": f"""
            update {RAW_FILLS}
            set source_maker_amount = '1'
            where not is_derived
        """,
        "raw_chunk_coverage": f"""
            update {RAW_FILLS}
            set chunk_from_block = 101
            where transaction_hash = (
                select min(transaction_hash) from {RAW_FILLS}
            )
        """,
        "minute_inventory": f"""
            delete from {CANDIDATE_RELATION}
            where proposition_id = (
                select min(proposition_id) from {CANDIDATE_RELATION}
            )
            and elapsed_window_minute = 0
        """,
        "minute_axis": f"""
            update {CANDIDATE_RELATION}
            set elapsed_window_minute = 999
            where proposition_id = (
                select min(proposition_id) from {CANDIDATE_RELATION}
            )
            and elapsed_window_minute = 0
        """,
        "minute_values": f"""
            update {CANDIDATE_RELATION}
            set minute_complete = false
            where minute_status = 'both_observed'
        """,
        "aggregate_reconciliation": f"""
            update {CANDIDATE_RELATION}
            set yes_normalized_fill_count = yes_normalized_fill_count + 1
            where minute_status = 'yes_only'
        """,
        "quality_errors": f"""
            update {QUALITY_ISSUES_RELATION}
            set severity = 'error'
            where issue_key = (
                select min(issue_key) from {QUALITY_ISSUES_RELATION}
            )
        """,
    }
    for blocker, mutation_sql in blocker_mutations.items():
        restore_quality_inputs()
        with duckdb.connect(str(db_path)) as conn:
            conn.execute(mutation_sql)
        _run_dbt(
            [
                "run",
                "--select",
                "polymarket_wc2026_polygon_settlement_data_quality",
            ],
            project_dir=dbt_root,
            profiles_dir=dbt_profiles_dir,
            env=env,
        )
        with duckdb.connect(str(db_path)) as conn:
            publication_ready, blocker_keys = conn.execute(
                f"select publication_ready, blocking_issue_keys from {QUALITY_RELATION}"
            ).fetchone()
            assert publication_ready is False, blocker
            assert blocker in blocker_keys.split(","), (blocker, blocker_keys)
            with pytest.raises(duckdb.Error, match=blocker):
                conn.execute(
                    f"select publication_ready from {PUBLICATION_GATE_RELATION}"
                )

    restore_quality_inputs()
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(f"drop view {STG_FILLS_RELATION}")
        conn.execute(
            f"""
            create table {STG_FILLS_RELATION} as
            select * from {RAW_FILLS}
            union all
            select * from (
                select * from {RAW_FILLS}
                order by transaction_hash, passive_log_index,
                    normalized_leg_ordinal
                limit 1
            ) as duplicate
            """
        )
    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path)) as conn:
        duplicate_quality = conn.execute(
            f"select duplicate_fill_grains, publication_ready, "
            f"blocking_issue_keys from {QUALITY_RELATION}"
        ).fetchone()
        assert duplicate_quality[0] > 0
        assert duplicate_quality[1] is False
        assert "raw_duplicates" in duplicate_quality[2].split(",")

    # Restore the source-backed staging view after the deliberate duplicate
    # relation, then prove every named hard blocker has executable coverage.
    _run_dbt(
        [
            "run",
            "--select",
            "stg_polymarket_wc2026_polygon_settlement_fills",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    expected_blockers = {
        "seed_inventory",
        "seed_stage_distribution",
        "seed_proposition_shape",
        "seed_unique_ids",
        "seed_windows",
        "seed_evidence",
        "scan_missing",
        "scan_manifest",
        "scan_integrity",
        "scan_chunks",
        "raw_empty",
        "raw_scan_mismatch",
        "raw_duplicates",
        "raw_normalization_pairs",
        "raw_mapping",
        "raw_values",
        "raw_chunk_coverage",
        "minute_inventory",
        "minute_axis",
        "minute_values",
        "aggregate_reconciliation",
        "quality_errors",
    }
    assert (
        set(blocker_mutations)
        | {
            "raw_duplicates",
            "raw_normalization_pairs",
        }
        == expected_blockers
    )

    restore_quality_inputs()
    _run_dbt(
        [
            "run",
            "--select",
            "polymarket_wc2026_polygon_settlement_data_quality",
        ],
        project_dir=dbt_root,
        profiles_dir=dbt_profiles_dir,
        env=env,
    )
    with duckdb.connect(str(db_path), read_only=True) as conn:
        assert conn.execute(
            f"select publication_ready from {QUALITY_RELATION}"
        ).fetchone() == (True,)
