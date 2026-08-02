from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest


def _load_modules():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import export_polymarket_wc2026_logical_bundle as exporter
    import materialize_polymarket_wc2026_logical_fixture as fixture

    return exporter, fixture


def test_pinned_fixture_manifest_physical_schema_and_input_inventory(tmp_path):
    exporter, fixture = _load_modules()
    output = tmp_path / "bundle"
    manifest = fixture.materialize_fixture(fixture.DEFAULT_SPEC, output)

    assert re.fullmatch(r"[0-9a-f]{40}", manifest["pipeline_git_sha"])
    assert manifest["pipeline_git_sha"] == "0" * 40
    assert set(manifest["files"]) == set(exporter.BUNDLE_FILES)
    assert manifest["row_counts"] == {
        "events.parquet": 4,
        "markets.parquet": 12,
        "market_events.parquet": 13,
        "propositions.parquet": 21,
        "entities.parquet": 15,
        "proposition_entities.parquet": 86,
        "scopes.parquet": 11,
    }
    expected_input_keys = {
        "dbt/seeds/polymarket_wc2026_logical_contract.csv",
        "relation/stg_polymarket_wc2026_event_snapshots/history",
        "relation/stg_polymarket_wc2026_event_markets/history",
        "relation/stg_polymarket_wc2026_event_market_payload_latest",
        "relation/stg_openfootball_wc2026_schedule_fixtures",
        "relation/reviewed_event_membership",
        "relation/int_polymarket_wc2026_logical_team_identities",
        "relation/wc2026_player_features",
    }
    expected_input_keys.update(
        f"scan/{partition}/{kind}"
        for partition in manifest["scan_partitions"]
        for kind in ("event_ids", "memberships", "payload")
    )
    assert set(manifest["input_hashes"]) == expected_input_keys
    assert all(
        re.fullmatch(r"[0-9a-f]{64}", value)
        for value in manifest["input_hashes"].values()
    )
    assert manifest["fixture_schedule"]["row_count"] == 104
    assert manifest["fixture_schedule"]["match_id_min"] == 1
    assert manifest["fixture_schedule"]["match_id_max"] == 104
    assert manifest["fixture_schedule"]["fifa_schedule"]["sha256"] == (
        "165fb909253b746e6173a4443bdc3e5d786530f0684af6e85c1fd21fff252811"
    )
    assert manifest["reviewed_membership"]["row_count"] == 4
    assert manifest["reviewed_membership"]["source_sha256"] == (
        fixture._source_sha256(fixture.DEFAULT_SPEC)
    )
    assert manifest["as_of"] == "2026-08-01T23:00:00+00:00"
    assert manifest["schema_version"] == exporter.EXPECTED_CONTRACT_NAME
    assert manifest["contract_version"] == exporter.EXPECTED_CONTRACT_VERSION
    assert manifest["taxonomy_version"] == exporter.EXPECTED_TAXONOMY_VERSION
    assert manifest["membership_policy_version"] == (
        exporter.EXPECTED_MEMBERSHIP_POLICY_VERSION
    )

    saved = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert saved == manifest
    with duckdb.connect() as conn:
        for filename in exporter.BUNDLE_FILES:
            exporter._validate_parquet_physical_schema(
                conn, output / filename, filename
            )
        markets = str(output / "markets.parquet").replace("'", "''")
        events = str(output / "events.parquet").replace("'", "''")
        propositions = str(output / "propositions.parquet").replace("'", "''")
        links = str(output / "proposition_entities.parquet").replace("'", "''")
        market_families = {
            row[0]
            for row in conn.execute(
                f"select distinct market_family from read_parquet('{markets}')"
            ).fetchall()
        }
        assert market_families >= {
            "award_winner",
            "exact_score",
            "match_result",
            "player_prop",
            "stage_reach",
            "total_goals",
            "tournament_statistic",
            "tournament_winner",
            "unclassified",
        }
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{markets}') "
                "where is_closed and is_resolved and winning_clob_token_id is not null"
            ).fetchone()[0]
            == 1
        )
        assert conn.execute(
            f"select market_id, subject_entity_ids, participant_entity_ids, "
            "player_national_team_entity_ids, referenced_entity_ids "
            f"from read_parquet('{markets}') "
            "where market_id like 'market-invalid-%' order by market_id"
        ).fetchall() == [
            (
                "market-invalid-award-player",
                ["player:jude_bellingham"],
                [],
                ["team:england"],
                ["award:golden_ball", "tournament:fifa_world_cup_2026"],
            ),
            (
                "market-invalid-fixture-player",
                ["player:kylian_mbappe"],
                ["team:paraguay", "team:usa"],
                ["team:france"],
                [
                    "fixture:1",
                    "group:a",
                    "stage:group_stage",
                    "tournament:fifa_world_cup_2026",
                ],
            ),
        ]
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{propositions}') "
                "where market_id like 'market-invalid-%'"
            ).fetchone()[0]
            == 0
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{events}') "
                "where is_closed and not is_active"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{events}') "
                "where event_id = 'event-audit' and is_active is null "
                "and is_closed is null and is_archived is null"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{markets}') "
                "where market_id = 'market-tokenless-audit' "
                "and is_active is null and is_closed is null "
                "and is_resolved is null"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{markets}') "
                "where market_family = 'unclassified' and logical_usable"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{markets}') "
                "where not logical_usable and not tokens_usable"
            ).fetchone()[0]
            == 3
        )
        assert (
            conn.execute(
                f"select count(*) from read_parquet('{links}') "
                "where entity_id = 'team:argentina' "
                "and entity_role = 'player_national_team'"
            ).fetchone()[0]
            == 4
        )
        assert conn.execute(
            f"select list(distinct membership_policy_version order by "
            f"membership_policy_version) from read_parquet('{events}')"
        ).fetchone()[0] == [manifest["membership_policy_version"]]


def test_exporter_rejects_event_policy_version_drift_from_manifest(tmp_path):
    exporter, fixture = _load_modules()
    output = tmp_path / "bundle"
    fixture.materialize_fixture(fixture.DEFAULT_SPEC, output)

    with duckdb.connect() as conn:
        events_path = str(output / "events.parquet").replace("'", "''")
        conn.execute(
            f"create table events as select * from read_parquet('{events_path}')"
        )
        conn.execute(
            "update events set membership_policy_version = 'policy-drift' "
            "where event_id = 'event-outrights'"
        )
        contract = {
            "contract_name": exporter.EXPECTED_CONTRACT_NAME,
            "contract_version": exporter.EXPECTED_CONTRACT_VERSION,
            "taxonomy_version": exporter.EXPECTED_TAXONOMY_VERSION,
            "policy_version": exporter.EXPECTED_MEMBERSHIP_POLICY_VERSION,
        }

        with pytest.raises(RuntimeError, match="does not match manifest contract"):
            exporter._validate_contract_versions(conn, contract, "events")


def test_exporter_rejects_unknown_taxonomy_version(tmp_path):
    exporter, fixture = _load_modules()
    output = tmp_path / "bundle"
    fixture.materialize_fixture(fixture.DEFAULT_SPEC, output)

    with duckdb.connect() as conn:
        events_path = str(output / "events.parquet").replace("'", "''")
        conn.execute(
            f"create table events as select * from read_parquet('{events_path}')"
        )
        contract = {
            "contract_name": exporter.EXPECTED_CONTRACT_NAME,
            "contract_version": exporter.EXPECTED_CONTRACT_VERSION,
            "taxonomy_version": "unknown-taxonomy",
            "policy_version": exporter.EXPECTED_MEMBERSHIP_POLICY_VERSION,
        }

        with pytest.raises(RuntimeError, match="contract versions do not match"):
            exporter._validate_contract_versions(conn, contract, "events")


def test_status_and_volume_changes_do_not_change_semantic_or_topology_fingerprint(
    tmp_path,
):
    exporter, fixture = _load_modules()
    output = tmp_path / "bundle"
    fixture.materialize_fixture(fixture.DEFAULT_SPEC, output)

    with duckdb.connect() as conn:
        relations = {}
        for index, filename in enumerate(exporter.BUNDLE_FILES):
            relation = f"fixture_{index}"
            path = str(output / filename).replace("'", "''")
            conn.execute(
                f"create table {relation} as select * from read_parquet('{path}')"
            )
            relations[filename] = relation
        topology_before = exporter._relation_fingerprint(
            conn, relations, exporter.TOPOLOGY_COLUMNS
        )
        semantic_before = exporter._relation_fingerprint(
            conn, relations, exporter.SEMANTIC_COLUMNS
        )

        conn.execute(
            """
            update fixture_0
            set event_volume_usd_lifetime_reported = 999999,
                event_volume_observed_at = event_volume_observed_at
                    + interval '1 hour',
                is_active = not is_active,
                is_closed = not is_closed
            """
        )
        conn.execute(
            """
            update fixture_1
            set market_volume_usd_lifetime_reported = 999999,
                is_active = not is_active,
                is_closed = not is_closed
            """
        )

        assert (
            exporter._relation_fingerprint(conn, relations, exporter.TOPOLOGY_COLUMNS)
            == topology_before
        )
        assert (
            exporter._relation_fingerprint(conn, relations, exporter.SEMANTIC_COLUMNS)
            == semantic_before
        )


@pytest.mark.parametrize(
    ("mutation", "violation"),
    [
        (
            "update events set event_title = null where event_id = 'event-outrights'",
            "eligible_event_display_reference_count",
        ),
        (
            "update markets set question = null where market_id = 'market-winner'",
            "logical_usable_market_display_count",
        ),
        (
            "update events set event_volume_usd_lifetime_reported = -0.01 "
            "where event_id = 'event-outrights'",
            "negative_volume_count",
        ),
        (
            "update markets set market_volume_usd_lifetime_reported = -0.01 "
            "where market_id = 'market-winner'",
            "negative_volume_count",
        ),
        (
            "update markets set subject_entity_ids = ['team:missing'] "
            "where market_id = 'market-winner'",
            "market_subject_entity_ids_reference_count",
        ),
        (
            "update markets set participant_entity_ids = "
            "['team:usa', 'team:usa'] where market_id = 'market-result'",
            "market_participant_entity_ids_shape_count",
        ),
        (
            "update propositions set statement = null where "
            "source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_statement_count",
        ),
        (
            "update events set scope_id = 'scope:missing' "
            "where event_id = 'event-outrights'",
            "event_scope_reference_count",
        ),
        (
            "update markets set scope_id = 'scope:missing' "
            "where market_id = 'market-winner'",
            "market_scope_reference_count",
        ),
        (
            "update propositions set scope_id = 'scope:missing' "
            "where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_scope_reference_count",
        ),
        (
            "update scopes set parent_scope_id = 'scope:missing' "
            "where scope_id = 'scope:wc2026:final'",
            "scope_parent_reference_count",
        ),
        (
            "update propositions set market_id = 'market-missing' "
            "where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_market_reference_count",
        ),
        (
            "update propositions set predicate_subject_entity_id = 'team:missing' "
            "where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_subject_reference_count",
        ),
        (
            "update propositions set predicate_object = 'team:missing' "
            "where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_object_entity_reference_count",
        ),
        (
            "update entities set home_team_entity_id = "
            "'tournament:fifa_world_cup_2026' where entity_id = 'fixture:1'",
            "fixture_team_reference_count",
        ),
        (
            "update markets set primary_event_id = 'event-audit' "
            "where market_id = 'market-winner'",
            "market_primary_event_reference_count",
        ),
        (
            "update market_events set event_logical_eligible = false "
            "where market_id = 'market-winner' and is_primary_qualifying_event",
            "market_primary_event_reference_count",
        ),
        (
            "update proposition_entities set source_proposition_id = "
            "'proposition:missing' where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_entity_proposition_reference_count",
        ),
        (
            "update proposition_entities set entity_id = 'team:missing' "
            "where source_proposition_id = "
            "'polymarket:condition:condition-winner:outcome:0'",
            "proposition_entity_entity_reference_count",
        ),
        (
            "update market_events set event_id = 'event-missing' "
            "where market_id = 'market-winner' and is_primary_qualifying_event",
            "market_event_event_reference_count",
        ),
        (
            "update market_events set market_id = 'market-missing' "
            "where market_id = 'market-winner' and is_primary_qualifying_event",
            "market_event_market_reference_count",
        ),
    ],
)
def test_bundle_relationship_validation_rejects_corruption(
    tmp_path, mutation, violation
):
    exporter, fixture = _load_modules()
    output = tmp_path / "bundle"
    fixture.materialize_fixture(fixture.DEFAULT_SPEC, output)

    with duckdb.connect() as conn:
        relations = {}
        for filename in exporter.BUNDLE_FILES:
            relation = filename.removesuffix(".parquet")
            path = str(output / filename).replace("'", "''")
            conn.execute(
                f"create table \"{relation}\" as select * from read_parquet('{path}')"
            )
            relations[filename] = f'"{relation}"'

        exporter._validate_bundle_relationships(conn, relations)
        conn.execute(mutation)
        with pytest.raises(RuntimeError, match=violation):
            exporter._validate_bundle_relationships(conn, relations)


def test_scan_partitions_rejects_metrics_older_than_catalog_snapshot():
    exporter, _ = _load_modules()
    signatures = {
        "event_ids_sha256": "a" * 64,
        "membership_inventory_sha256": "b" * 64,
        "event_payload_inventory_sha256": "c" * 64,
    }
    partitions = {
        f"{source}:{state}": {
            "complete": True,
            "stable": True,
            "event_count": 1,
            "child_market_count": 1,
            "membership_count": 1,
            **signatures,
        }
        for source in (
            "exact_2026_tag",
            "related_2026_tag_recall",
            "broad_fifa_world_cup_tag",
            "soccer_fifwc_series",
            "wc2026_event_slug_prefix_recall",
        )
        for state in ("open", "closed")
    }
    metrics = {
        "observed_at": "2026-08-02T10:00:00+00:00",
        "scan_partitions": partitions,
    }

    with duckdb.connect() as conn:
        conn.execute("create schema polymarket_wc2026_ops")
        conn.execute(
            "create table polymarket_wc2026_ops.sync_run_metrics "
            "(task_name varchar, metrics_json varchar)"
        )
        conn.execute(
            "insert into polymarket_wc2026_ops.sync_run_metrics values (?, ?)",
            ["event_catalog", json.dumps(metrics)],
        )
        conn.execute("create schema polymarket_wc2026_staging")
        conn.execute(
            "create table polymarket_wc2026_staging."
            "stg_polymarket_wc2026_event_snapshots (observed_at timestamp)"
        )
        conn.execute(
            "insert into polymarket_wc2026_staging."
            "stg_polymarket_wc2026_event_snapshots values "
            "(timestamp '2026-08-02 10:05:00')"
        )

        with pytest.raises(RuntimeError, match="sync metrics are stale"):
            exporter._scan_partitions(conn)

        metrics["observed_at"] = "2026-08-02T10:05:00Z"
        conn.execute(
            "update polymarket_wc2026_ops.sync_run_metrics set metrics_json = ?",
            [json.dumps(metrics)],
        )
        assert exporter._scan_partitions(conn) == partitions


def test_exporter_rejects_eligible_event_without_source_creation_time():
    exporter, _ = _load_modules()
    with duckdb.connect() as conn:
        conn.execute("create schema polymarket_wc2026_marts")
        conn.execute(
            """
            create table polymarket_wc2026_marts.
                polymarket_wc2026_logical_events as
            select
                'valid'::varchar as event_id,
                'included'::varchar as membership_status,
                true as ever_eligible,
                timestamp '2026-01-01' as event_created_at,
                timestamp '2026-01-01' as eligibility_effective_from
            """
        )
        assert exporter._eligible_event_creation_gap_count(conn) == 0

        conn.execute(
            """
            insert into polymarket_wc2026_marts.
                polymarket_wc2026_logical_events
            values ('missing', 'included', true, null, null)
            """
        )
        assert exporter._eligible_event_creation_gap_count(conn) == 1


def test_exporter_pins_utc_before_casting_utc_naive_timestamps(tmp_path):
    exporter, _ = _load_modules()
    with duckdb.connect() as conn:
        conn.execute("SET TimeZone='Europe/Warsaw'")
        conn.execute(
            "create table utc_naive (observed_at timestamp); "
            "insert into utc_naive values (timestamp '2026-08-02 00:00:00')"
        )

        with pytest.raises(duckdb.CatalogException):
            exporter.export_polymarket_wc2026_logical_bundle(
                conn, tmp_path / "incomplete-bundle"
            )

        assert conn.execute("select current_setting('TimeZone')").fetchone()[0] == "UTC"
        observed = conn.execute(
            "select cast(observed_at as timestamptz) from utc_naive"
        ).fetchone()[0]
        assert observed == datetime(2026, 8, 2, tzinfo=timezone.utc)
        assert (
            exporter._utc_timestamp(observed, field="logical events as_of").isoformat()
            == "2026-08-02T00:00:00+00:00"
        )
