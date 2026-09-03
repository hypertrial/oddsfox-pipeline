"""Cumulative sports observations, isolated from the existing graph contract."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from uuid import uuid4

import pyarrow as pa

from oddsfox_pipeline.ingestion.polymarket.catalog import (
    CATALOG_PASSES,
    _json_list,
    fetch_catalog_page,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    CATALOG_CONTRACT,
    TIME,
    Segments,
    Store,
    json_bytes,
    now,
    rows_from_files,
    sha256,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.transport import PublicHTTP

OBSERVATION_SCHEMA = pa.schema(
    [
        ("kind", pa.string()),
        ("source_id", pa.string()),
        ("parent_id", pa.string()),
        ("received_at", TIME),
        ("sequence", pa.int64()),
        ("priority", pa.int32()),
        ("endpoint", pa.string()),
        ("payload_json", pa.string()),
        ("payload_sha256", pa.string()),
        ("sports_evidence", pa.list_(pa.string())),
    ]
)
RELATION_SCHEMA = pa.schema(
    [
        ("kind", pa.string()),
        ("source_id", pa.string()),
        ("parent_id", pa.string()),
        ("received_at", TIME),
        ("payload_sha256", pa.string()),
        ("payload_json", pa.string()),
        ("sports_evidence", pa.list_(pa.string())),
        ("normalized_json", pa.string()),
        ("classification_status", pa.string()),
        ("condition_id", pa.string()),
        ("token_ids", pa.list_(pa.string())),
        ("outcome_labels", pa.list_(pa.string())),
        ("admitted", pa.bool_()),
        ("identity_valid", pa.bool_()),
        ("quality_reasons", pa.list_(pa.string())),
    ]
)
OUTCOME_SCHEMA = pa.schema(
    [
        ("market_id", pa.string()),
        ("condition_id", pa.string()),
        ("token_id", pa.string()),
        ("outcome_index", pa.int32()),
        ("outcome_label", pa.string()),
        ("observed_at", TIME),
        ("source_created_at", pa.string()),
        ("admitted", pa.bool_()),
        ("identity_valid", pa.bool_()),
    ]
)
IDENTITY_SCOPE_SCHEMA = pa.schema(
    [
        ("market_id", pa.string()),
        ("condition_id", pa.string()),
        ("sports", pa.bool_()),
    ]
)
MEMBERSHIP_SCHEMA = pa.schema([("event_id", pa.string()), ("market_id", pa.string())])


def evidence(payload: dict, taxonomy: list[dict]) -> list[str]:
    tags = {str(t.get("id")) for t in payload.get("tags") or [] if isinstance(t, dict)}
    series = {
        str(t.get("id")) for t in payload.get("series") or [] if isinstance(t, dict)
    }
    result = []
    if "1" in tags:
        result.append("source_tag:1")
    if payload.get("sportsMarketType"):
        result.append("source_field:sportsMarketType")
    if not tags and not series:
        return result
    for sport in taxonomy:
        if str(sport.get("primaryTagId")) in tags:
            result.append(f"sports_primary_tag:{sport['primaryTagId']}")
        if str(sport.get("series")) in series:
            result.append(f"sports_series:{sport['series']}")
    return sorted(set(result))


def broad_family(raw) -> str | None:
    if not raw:
        return None
    text = str(raw).lower()
    for fragments, family in (
        (("spread", "handicap"), "spread"),
        (("total", "over_under"), "total"),
        (("player", "scorer"), "player_prop"),
        (("future", "outright"), "future"),
        (("moneyline", "winner", "match_result"), "winner"),
    ):
        if any(fragment in text for fragment in fragments):
            return family
    return "unclassified"


def normalize(row: dict) -> dict:
    raw = json.loads(row["payload_json"])
    reasons, tokens, labels = [], [], []
    condition = raw.get("conditionId")
    if condition is not None and (
        not isinstance(condition, str)
        or not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition)
    ):
        reasons.append("malformed_condition_id")
        condition = None
    if condition is not None:
        condition = condition.lower()
    if row["kind"] == "market":
        try:
            tokens = _json_list(raw.get("clobTokenIds"), "clobTokenIds")
            labels = _json_list(raw.get("outcomes"), "outcomes")
            if any(
                not isinstance(t, str)
                or not re.fullmatch(r"[1-9][0-9]*", t)
                or int(t) >= 2**256
                for t in tokens
            ):
                raise ValueError("invalid token")
            if (
                len(set(tokens)) != len(tokens)
                or len(tokens) != len(labels)
                or len(tokens) < 2
            ):
                raise ValueError("invalid outcomes")
            if any(not isinstance(label, str) for label in labels):
                raise ValueError("invalid labels")
        except ValueError:
            reasons.append("unusable_outcomes")
            tokens, labels = [], []
        if not condition:
            reasons.append("missing_condition")
        if raw.get("enableOrderBook") is not True:
            reasons.append("clob_not_confirmed")
        if (
            raw.get("closed") is True
            or raw.get("resolved") is True
            or raw.get("archived") is True
        ):
            reasons.append("terminal")
    fields = {
        "question": "question",
        "description": "description",
        "resolution_rules": "rules",
        "resolution_source_url": "resolutionSource",
        "source_created_at": "createdAt",
        "source_updated_at": "updatedAt",
        "source_start_date": "startDate",
        "source_end_date": "endDate",
        "listing_at": "listingTime",
        "contest_start_at": "gameStartTime",
        "event_start_at": "eventStartTime",
        "accepting_orders": "acceptingOrders",
        "accepting_orders_at": "acceptingOrdersTimestamp",
        "active": "active",
        "approved": "approved",
        "archived": "archived",
        "ready": "ready",
        "funded": "funded",
        "deploying": "deploying",
        "closed": "closed",
        "closed_at": "closedTime",
        "resolved": "resolved",
        "resolution_status": "umaResolutionStatus",
        "resolution_statuses": "umaResolutionStatuses",
        "line": "line",
        "period": "period",
        "subject": "subject",
        "raw_group_item_title": "groupItemTitle",
        "participants": "participants",
        "teams": "teams",
        "sport": "sport",
        "league": "league",
        "series": "series",
        "tags": "tags",
        "neg_risk": "negRisk",
        "neg_risk_group": "negRiskMarketID",
        "condition_contract": "conditionContract",
        "collateral_token": "denominationToken",
        "contract_version": "contractVersion",
        "source_market_version": "version",
        "min_order_size": "orderMinSize",
        "tick_size": "orderPriceMinTickSize",
        "raw_market_type": "sportsMarketType",
        "source_market_type": "marketType",
    }
    normalized = {name: raw.get(key) for name, key in fields.items()}
    normalized["broad_family"] = broad_family(raw.get("sportsMarketType"))
    normalized["missing_fields"] = {
        name: "explicit_null" if key in raw else "absent"
        for name, key in fields.items()
        if raw.get(key) is None
    }
    return {key: row.get(key) for key in RELATION_SCHEMA.names} | {
        "normalized_json": json_bytes(normalized).decode(),
        "classification_status": "direct_source_sports_evidence"
        if row.get("sports_evidence")
        else "unclassified",
        "condition_id": condition,
        "token_ids": tokens,
        "outcome_labels": labels,
        "quality_reasons": reasons,
        "admitted": row["kind"] == "market" and not reasons,
        "identity_valid": row["kind"] == "market" and bool(condition and tokens),
    }


def active_catalog(store: Store) -> dict:
    pointer = json.loads(store.path("catalog-active.json").read_bytes())
    path = store.path(pointer["path"])
    if sha256(path) != pointer["sha256"]:
        raise ValueError("active catalog manifest checksum mismatch")
    manifest = json.loads(path.read_bytes())
    if manifest["contract"] != CATALOG_CONTRACT or not manifest["complete"]:
        raise ValueError("recording requires a complete activated sports catalog")
    return pointer | manifest


def catalog_targets(store: Store, *, admitted_only: bool = True) -> dict[str, dict]:
    manifest = active_catalog(store)
    result = {}
    for row in rows_from_files(store, manifest["outcomes"]):
        if row.get("identity_valid") is False:
            continue
        if admitted_only and not row["admitted"]:
            continue
        previous = result.get(row["token_id"])
        if previous and (previous["condition_id"], previous["outcome_index"]) != (
            row["condition_id"],
            row["outcome_index"],
        ):
            raise ValueError("active catalog has conflicting token identities")
        result[row["token_id"]] = row
    return result


def condition_scope(store: Store) -> dict[str, bool]:
    result = {}
    for row in rows_from_files(store, active_catalog(store)["identity_scope"]):
        if row["condition_id"] and re.fullmatch(
            r"0x[0-9a-fA-F]{64}", row["condition_id"]
        ):
            result[row["condition_id"].lower()] = (
                result.get(row["condition_id"].lower(), False) or row["sports"]
            )
    return result


def refresh_catalog(
    store: Store,
    *,
    client=None,
    open_only: bool = False,
    cancelled=None,
    target_conditions=None,
) -> dict:
    """A failed refresh never replaces the prior activation; restart restarts passes."""
    capture = now().strftime("%Y%m%dT%H%M%S") + "-" + uuid4().hex
    prefix = f"catalog/{capture}"
    http = client or PublicHTTP("polymarket", "https://gamma-api.polymarket.com")
    observations = Segments(store, f"{prefix}/observations", OBSERVATION_SCHEMA)
    sequence = 0
    passes = []
    previous = None
    if store.path("catalog-active.json").exists():
        previous = active_catalog(store)
    if (open_only or target_conditions) and previous is None:
        raise ValueError("partial refresh requires a complete catalog")

    def observe(kind, payload, endpoint, parent_id=None, priority=2):
        nonlocal sequence
        source_id = str(payload.get("id", ""))
        if not source_id.isdecimal():
            raise ValueError(f"invalid {kind} source ID")
        raw = json_bytes(payload)
        sequence += 1
        observations.append(
            {
                "kind": kind,
                "source_id": source_id,
                "parent_id": parent_id,
                "received_at": getattr(http, "received_at", None) or now(),
                "sequence": sequence,
                "priority": priority,
                "endpoint": endpoint,
                "payload_json": raw.decode(),
                "payload_sha256": hashlib.sha256(raw).hexdigest(),
                "sports_evidence": evidence(payload, sports)
                if kind != "membership"
                else [],
            }
        )

    try:
        sports = http.get("/sports")
        sports_received = getattr(http, "received_at", None) or now()
        market_types = http.get("/sports/market-types")
        types_received = getattr(http, "received_at", None) or now()
        sports_tag = http.get("/tags/slug/sports")
        if (
            not isinstance(sports, list)
            or not sports
            or not isinstance(market_types.get("marketTypes"), list)
            or str(sports_tag.get("id")) != "1"
        ):
            raise ValueError("unusable sports taxonomy")
        taxonomy = {
            "sports": sports,
            "market_types": market_types,
            "sports_tag": sports_tag,
            "sports_received_at": sports_received,
            "types_received_at": types_received,
            "tag_received_at": getattr(http, "received_at", None) or now(),
        }
        store.atomic_json(f"{prefix}/taxonomy.json", taxonomy)
        source_passes = (
            CATALOG_PASSES
            if not target_conditions
            else [
                (f"condition:{condition}", "/markets", "markets", False)
                for condition in sorted(target_conditions)
            ]
        )
        for name, endpoint, key, closed in source_passes:
            if open_only and closed:
                continue
            pass_started = now()
            cursor, seen, page, seen_payloads = None, set(), 0, set()
            while True:
                if cancelled is not None and cancelled.is_set():
                    raise InterruptedError(
                        "catalog refresh cancelled before activation"
                    )
                store.check(1024**2)
                if target_conditions:
                    condition = name.removeprefix("condition:")
                    if not re.fullmatch(r"0x[0-9a-fA-F]{64}", condition):
                        raise ValueError("invalid targeted condition ID")
                    rows = http.get(
                        endpoint,
                        params={
                            "condition_ids": condition,
                            "limit": 100,
                            "include_tag": True,
                        },
                    )
                    if (
                        not isinstance(rows, list)
                        or len(rows) >= 100
                        or any(r.get("conditionId") != condition for r in rows)
                    ):
                        raise ValueError(
                            "targeted condition response not exhaustive or wrong identity"
                        )
                    payload = {"markets": rows, "next_cursor": None}
                else:
                    payload = fetch_catalog_page(http, endpoint, key, closed, cursor)
                payload_hash = hashlib.sha256(json_bytes(payload[key])).hexdigest()
                if payload[key] and payload_hash in seen_payloads:
                    raise ValueError("duplicate Gamma page under a different cursor")
                seen_payloads.add(payload_hash)
                for row in payload[key]:
                    observe("event" if key == "events" else "market", row, endpoint)
                    children = row.get("markets" if key == "events" else "events") or []
                    for child in children:
                        observe(
                            "market" if key == "events" else "event",
                            child,
                            endpoint,
                            str(row["id"]),
                            1,
                        )
                        # The edge is a separate observed relation, not a guessed join.
                        edge = {
                            "id": child["id"],
                            "event_id": row["id"] if key == "events" else child["id"],
                            "market_id": child["id"] if key == "events" else row["id"],
                        }
                        observe("membership", edge, endpoint, str(row["id"]))
                page += 1
                cursor = payload.get("next_cursor")
                if cursor is None:
                    break
                if cursor in seen:
                    raise ValueError("cyclic Gamma keyset cursor")
                seen.add(cursor)
                if page % 100 == 0:
                    print(
                        json.dumps(
                            {
                                "catalog_pass": name,
                                "pages": page,
                                "observations": sequence,
                            }
                        ),
                        flush=True,
                    )
            passes.append(
                {
                    "name": name,
                    "pages": page,
                    "natural_completion": True,
                    "started_at": pass_started,
                    "completed_at": now(),
                }
            )
        files = observations.close()
        acquisition = {
            "contract": CATALOG_CONTRACT,
            "capture": capture,
            "implementation_sha256": store.code_sha256,
            "code_revision": store.code_revision,
            "passes": passes,
            "complete": True,
            "open_only": open_only,
            "target_conditions": sorted(target_conditions or []),
            "observation_files": files,
            "taxonomy": {
                "path": f"{prefix}/taxonomy.json",
                "sha256": sha256(store.path(f"{prefix}/taxonomy.json")),
            },
            "previous_manifest_sha256": previous["sha256"] if previous else None,
        }
        store.atomic_json(f"{prefix}/acquisition.json", acquisition)
        print(
            json.dumps(
                {
                    "catalog_acquisition_manifest": f"{prefix}/acquisition.json",
                    "sha256": sha256(store.path(f"{prefix}/acquisition.json")),
                }
            ),
            flush=True,
        )
        return materialize_catalog(
            store,
            prefix,
            capture,
            files,
            passes,
            previous,
            open_only,
            cancelled=cancelled,
        )
    finally:
        if client is None:
            http.close()


def materialize_catalog(
    store, prefix, capture, files, passes, previous, open_only, *, cancelled=None
):
    with store.sql_connection() as conn:
        return _materialize_catalog(
            conn,
            store,
            prefix,
            capture,
            files,
            passes,
            previous,
            open_only,
            cancelled=cancelled,
        )


def _materialize_catalog(
    conn,
    store,
    prefix,
    capture,
    files,
    passes,
    previous,
    open_only,
    *,
    cancelled=None,
):
    def check_cancelled():
        if cancelled is not None and cancelled.is_set():
            raise InterruptedError(
                "catalog materialization cancelled before activation"
            )

    check_cancelled()
    all_observations = (previous["observation_files"] if previous else []) + files
    with ThreadPoolExecutor(max_workers=4) as pool:
        paths = [str(path) for path in pool.map(store.validate_file, all_observations)]
    conn.read_parquet(paths, filename=True, file_row_number=True).create_view(
        "observations"
    )
    # Native endpoint records outrank nested summaries; never fill nulls from old rows.
    # Rank narrow physical row identities, not tens of GiB of repeated JSON.
    conn.execute("""CREATE TEMP TABLE entity_keys AS
        SELECT filename,file_row_number FROM observations WHERE kind != 'membership'
        QUALIFY row_number() OVER(PARTITION BY kind,source_id ORDER BY priority DESC,
        received_at DESC,sequence DESC,filename DESC,file_row_number DESC)=1""")
    conn.execute("""CREATE TEMP TABLE entities AS
        SELECT o.filename,o.file_row_number,o.kind,o.source_id,o.sports_evidence,
        json_extract_string(payload_json,'$.conditionId') condition_id,
        json_extract_string(payload_json,'$.markets[*].id') market_ids,
        json_extract_string(payload_json,'$.events[*].id') event_ids FROM observations o
        JOIN entity_keys k USING(filename,file_row_number)""")
    conn.execute("""CREATE TEMP TABLE edges AS
        SELECT source_id event_id,unnest(market_ids) market_id FROM entities WHERE kind='event'
        UNION SELECT unnest(event_ids) event_id,source_id market_id FROM entities WHERE kind='market'""")
    conn.execute("""CREATE TEMP TABLE observed_edges AS SELECT DISTINCT
        json_extract_string(payload_json,'$.event_id') event_id,
        json_extract_string(payload_json,'$.market_id') market_id
        FROM observations WHERE kind='membership'""")
    conn.execute("""CREATE TEMP TABLE current_sports_events AS SELECT source_id FROM entities
        WHERE kind='event' AND len(sports_evidence)>0 UNION SELECT e.event_id FROM edges e
        JOIN entities m ON m.kind='market' AND m.source_id=e.market_id WHERE len(m.sports_evidence)>0""")
    conn.execute("""CREATE TEMP TABLE current_sports_markets AS SELECT source_id FROM entities
        WHERE kind='market' AND len(sports_evidence)>0 UNION SELECT e.market_id FROM edges e
        JOIN current_sports_events s ON s.source_id=e.event_id""")
    conn.execute("""CREATE TEMP TABLE historic_direct_sports_events AS SELECT DISTINCT
        source_id FROM observations WHERE kind='event' AND len(sports_evidence)>0""")
    conn.execute("""CREATE TEMP TABLE historic_direct_sports_markets AS SELECT DISTINCT
        source_id FROM observations WHERE kind='market' AND len(sports_evidence)>0""")
    conn.execute("""CREATE TEMP TABLE sports_events AS SELECT source_id FROM
        historic_direct_sports_events UNION SELECT e.event_id FROM observed_edges e
        JOIN historic_direct_sports_markets m ON m.source_id=e.market_id""")
    conn.execute("""CREATE TEMP TABLE sports_markets AS SELECT source_id FROM
        historic_direct_sports_markets UNION SELECT e.market_id FROM observed_edges e
        JOIN sports_events s ON s.source_id=e.event_id""")
    conflicts = {
        r[0]
        for r in conn.execute("""SELECT source_id FROM observations WHERE kind='market'
        GROUP BY source_id HAVING count(DISTINCT lower(json_extract_string(payload_json,'$.conditionId')))>1""").fetchall()
    }
    conn.execute("""CREATE TEMP TABLE historic_tokens AS WITH arrays AS (
        SELECT DISTINCT source_id, lower(json_extract_string(payload_json,'$.conditionId')) condition_id,
        try_cast(CASE WHEN json_type(payload_json,'$.clobTokenIds')='VARCHAR'
          THEN json_extract_string(payload_json,'$.clobTokenIds')
          ELSE json_extract(payload_json,'$.clobTokenIds') END AS VARCHAR[]) token_ids
        FROM observations WHERE kind='market')
        SELECT source_id market_id,condition_id,unnest(token_ids) token_id,
               generate_subscripts(token_ids,1)-1 outcome_index FROM arrays""")
    bad_markets = {
        r[0]
        for r in conn.execute("""SELECT DISTINCT market_id FROM historic_tokens
        WHERE token_id IN (SELECT token_id FROM historic_tokens GROUP BY token_id
                          HAVING count(DISTINCT (condition_id,outcome_index))>1)
           OR (condition_id,outcome_index) IN (
             SELECT condition_id,outcome_index FROM historic_tokens GROUP BY condition_id,outcome_index
             HAVING count(DISTINCT token_id)>1)""").fetchall()
    }
    staging_prefix = f"temporary/relation-{uuid4().hex}"
    staging = {}
    counts = Counter()
    batches = conn.execute("""WITH selected AS (SELECT filename,file_row_number,
        CASE WHEN kind='event'
        THEN source_id IN (SELECT source_id FROM current_sports_events)
        ELSE source_id IN (SELECT source_id FROM current_sports_markets) END current_sports
        FROM entities WHERE
        (kind='event' AND source_id IN (SELECT source_id FROM sports_events)) OR
        (kind='market' AND source_id IN (SELECT source_id FROM sports_markets)))
        SELECT o.* EXCLUDE(filename,file_row_number),s.current_sports FROM observations o
        JOIN selected s USING(filename,file_row_number)
        """).to_arrow_reader(batch_size=2048)
    try:
        for batch in batches:
            check_cancelled()
            for row in batch.to_pylist():
                normalized = normalize(row)
                if not normalized["sports_evidence"]:
                    normalized["sports_evidence"] = [
                        "manifest_bound_current_sports_membership"
                        if row["current_sports"]
                        else "manifest_bound_prior_sports_evidence"
                    ]
                    normalized["classification_status"] = (
                        "current_sports_membership"
                        if row["current_sports"]
                        else "historical_sports_evidence_only"
                    )
                if row["kind"] == "market" and not row["current_sports"]:
                    normalized["admitted"] = False
                    normalized["quality_reasons"].append("no_current_sports_evidence")
                if row["source_id"] in conflicts and row["kind"] == "market":
                    normalized["admitted"] = False
                    normalized["identity_valid"] = False
                    normalized["quality_reasons"].append("durable_identity_conflict")
                if row["source_id"] in bad_markets and row["kind"] == "market":
                    normalized["admitted"] = False
                    normalized["identity_valid"] = False
                    normalized["quality_reasons"].append("token_identity_conflict")
                key = row["kind"], int(row["source_id"]) // 100_000
                writer = staging.setdefault(
                    key,
                    Segments(
                        store,
                        f"{staging_prefix}/{key[0]}/{key[1]:08d}",
                        RELATION_SCHEMA,
                    ),
                )
                writer.append(normalized)
                counts[row["kind"]] += 1
                counts[normalized["classification_status"]] += 1
        staged = {key: writer.close() for key, writer in staging.items()}
        relations = Segments(store, f"{prefix}/relations", RELATION_SCHEMA)
        for key in sorted(staged):
            check_cancelled()
            relation = conn.read_parquet(
                [str(store.path(item["path"])) for item in staged[key]]
            ).order("try_cast(source_id AS UBIGINT)")
            for batch in relation.to_arrow_reader(batch_size=2048):
                check_cancelled()
                for row in batch.to_pylist():
                    relations.append(row)
        relation_files = relations.close()
    finally:
        shutil.rmtree(store.path(staging_prefix), ignore_errors=True)
        store.usage(force=True)
    if relation_files:
        conn.read_parquet(
            [str(store.path(f["path"])) for f in relation_files]
        ).create_view("relations")
    else:
        conn.register("relations", pa.Table.from_pylist([], schema=RELATION_SCHEMA))
    conn.execute("""CREATE TEMP TABLE tokens AS SELECT source_id market_id,condition_id,
        unnest(token_ids) token_id, generate_subscripts(token_ids,1)-1 outcome_index,
        unnest(outcome_labels) outcome_label, received_at observed_at,
        json_extract_string(payload_json,'$.createdAt') source_created_at,
        admitted,identity_valid FROM relations WHERE kind='market'""")
    outcomes = Segments(store, f"{prefix}/outcomes", OUTCOME_SCHEMA)
    for batch in conn.execute(
        "SELECT * FROM tokens ORDER BY market_id,outcome_index"
    ).to_arrow_reader(batch_size=2048):
        check_cancelled()
        for row in batch.to_pylist():
            row["admitted"] = row["admitted"] and row["market_id"] not in bad_markets
            row["identity_valid"] = (
                row["identity_valid"]
                and row["market_id"] not in bad_markets | conflicts
            )
            outcomes.append(row)
            counts["admitted_tokens" if row["admitted"] else "not_admitted_tokens"] += 1
    outcome_files = outcomes.close()
    counts["admitted_outcome_rows"] = counts["admitted_tokens"]
    counts["admitted_tokens"] = conn.execute(
        "SELECT count(DISTINCT token_id) FROM tokens WHERE admitted AND identity_valid"
    ).fetchone()[0]
    memberships = Segments(
        store,
        f"{prefix}/memberships",
        MEMBERSHIP_SCHEMA,
    )
    for batch in conn.execute(
        "SELECT * FROM edges WHERE market_id IN (SELECT source_id FROM sports_markets) ORDER BY event_id,market_id"
    ).to_arrow_reader(batch_size=2048):
        check_cancelled()
        for row in batch.to_pylist():
            memberships.append(row)
    membership_files = memberships.close()
    identity_scope = Segments(store, f"{prefix}/identity-scope", IDENTITY_SCOPE_SCHEMA)
    for batch in conn.execute("""SELECT source_id market_id,condition_id,
        source_id IN (SELECT source_id FROM sports_markets) sports
        FROM entities WHERE kind='market' ORDER BY source_id""").to_arrow_reader(
        batch_size=2048
    ):
        check_cancelled()
        for row in batch.to_pylist():
            identity_scope.append(row)
    identity_scope_files = identity_scope.close()
    counts["orphan_markets"] = conn.execute(
        "SELECT count(*) FROM sports_markets WHERE source_id NOT IN (SELECT market_id FROM edges)"
    ).fetchone()[0]
    counts["events_without_usable_tokens"] = (
        conn.execute("""SELECT count(*) FROM sports_events s
        WHERE NOT EXISTS (SELECT 1 FROM edges e JOIN relations r ON r.kind='market' AND r.source_id=e.market_id
        WHERE e.event_id=s.source_id AND len(r.token_ids)>0 AND r.identity_valid)""").fetchone()[
            0
        ]
    )
    counts["identity_conflict_markets"] = len(conflicts | bad_markets)
    counts["unclassified_observations"] = conn.execute(
        "SELECT count(*) FROM entities WHERE len(sports_evidence)=0"
    ).fetchone()[0]
    check_cancelled()
    conn.close()
    activation = now()
    targeted = any(p["name"].startswith("condition:") for p in passes)
    full = not open_only and not targeted
    # Source passes can be far apart in a global crawl. Activation must not
    # reset the age of open membership acquired earlier in that crawl.
    full_refreshed = max(p["completed_at"] for p in passes) if full else None
    open_refreshed = (
        min(p["completed_at"] for p in passes if p["name"].endswith("_open"))
        if not targeted
        else None
    )
    manifest = store.commit(
        "catalog",
        capture,
        files
        + relation_files
        + outcome_files
        + membership_files
        + identity_scope_files,
        complete=True,
        refresh_kind="targeted" if targeted else "full" if full else "open",
        passes=passes,
        previous_manifest_sha256=previous["sha256"] if previous else None,
        activated_at=activation,
        last_full_refresh_at=full_refreshed
        if full
        else previous["last_full_refresh_at"],
        last_open_refresh_at=open_refreshed
        if not targeted
        else previous["last_open_refresh_at"],
        observation_files=all_observations,
        relations=relation_files,
        outcomes=outcome_files,
        memberships=membership_files,
        identity_scope=identity_scope_files,
        counts=dict(counts),
        taxonomy={
            "path": f"{prefix}/taxonomy.json",
            "sha256": sha256(store.path(f"{prefix}/taxonomy.json")),
        },
        conflicting_market_ids=sorted(conflicts | bad_markets),
        completeness="naturally completed source passes, not an atomic upstream snapshot or deleted-record recovery",
        payload_encoding="canonical source-field JSON; fractional numbers are preserved as exact decimal strings",
        acquisition={
            "path": f"{prefix}/acquisition.json",
            "sha256": sha256(store.path(f"{prefix}/acquisition.json")),
        },
    )
    store.atomic_json(
        "catalog-active.json", {"path": manifest["path"], "sha256": manifest["sha256"]}
    )
    return manifest


def observed_as_of(store: Store, source_id: str, kind: str, at: datetime):
    """Actual receipt is the availability boundary; source updatedAt never substitutes."""
    candidates = (
        r
        for r in rows_from_files(store, active_catalog(store)["observation_files"])
        if r["source_id"] == source_id and r["kind"] == kind and r["received_at"] <= at
    )
    return max(
        candidates,
        key=lambda r: (r["priority"], r["received_at"], r["sequence"]),
        default=None,
    )
