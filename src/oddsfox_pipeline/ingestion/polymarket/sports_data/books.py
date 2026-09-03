"""Exact displayed-depth normalization, receive-order replay and offline quotes."""

from __future__ import annotations

import hashlib
import heapq
import json
import re
from datetime import datetime, timedelta
from decimal import Decimal, localcontext
from itertools import groupby

import pyarrow.compute as pc
import pyarrow.parquet as pq

from oddsfox_pipeline.ingestion.polymarket.match_order_book import _decimal_string
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    STATE_SCHEMA,
    UTC,
    Segments,
    Store,
    json_bytes,
    sha256,
)

ZERO = Decimal(0)
ONE = Decimal(1)


def number(value, *, price=False) -> Decimal:
    if isinstance(value, (float, bool)):
        raise ValueError("book numeric fields must not pass through binary floats")
    return Decimal(
        _decimal_string(
            value, field="book value", minimum=ZERO, maximum=ONE if price else None
        )
    )


def levels(raw, *, bids: bool) -> list[dict]:
    if not isinstance(raw, list):
        raise ValueError("snapshot depth must be an array")
    result, seen = [], set()
    for level in raw:
        if isinstance(level, dict):
            price, size = level.get("price"), level.get("size", level.get("quantity"))
        elif isinstance(level, list) and len(level) == 2:
            price, size = level
        else:
            raise ValueError("invalid snapshot level")
        price, size = number(price, price=True), number(size)
        if price in seen:
            raise ValueError("duplicate snapshot price level")
        seen.add(price)
        if size:
            result.append({"price": price, "quantity": size})
    return sorted(result, key=lambda row: row["price"], reverse=bids)


def source_time(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("source timestamp requires timezone")
        return value.astimezone(UTC)
    milliseconds = number(value)
    if milliseconds != milliseconds.to_integral_value():
        raise ValueError("source timestamp must use whole milliseconds")
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
            milliseconds=int(milliseconds)
        )
    except OverflowError as exc:
        raise ValueError("source timestamp outside supported UTC range") from exc


def normalize_message(
    payload: bytes,
    *,
    source: str,
    stream: str,
    sequence: int,
    received_at: datetime,
    identity: str,
) -> list[dict]:
    """Ancillary messages are kept in raw evidence, never guessed into book deltas."""
    if payload in (b"PONG", b"PING"):
        return []
    value = json.loads(
        payload,
        parse_float=Decimal,
        parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
    )
    messages = value if isinstance(value, list) else [value]
    result = []
    ordinal = 0
    for message in messages:
        if not isinstance(message, dict):
            raise ValueError("book message must be an object")
        kind = message.get("event_type")
        if kind not in {
            "book",
            "price_change",
            "tick_size_change",
            "last_trade_price",
            "best_bid_ask",
            "new_market",
            "market_resolved",
        }:
            raise ValueError("unsupported market-stream message type")
        changes = message.get("price_changes") if kind == "price_change" else [message]
        if kind == "market_resolved":
            assets = message.get("assets_ids", [message.get("asset_id")])
            if (
                not isinstance(assets, list)
                or not assets
                or len(set(assets)) != len(assets)
            ):
                raise ValueError("malformed resolution asset list")
            changes = [{"asset_id": token} for token in assets]
        if not isinstance(changes, list):
            raise ValueError("price_changes must be an array")
        for item in changes:
            if not isinstance(item, dict):
                raise ValueError("price-change item must be an object")
            base = {
                "source": source,
                "stream": stream,
                "sequence": sequence,
                "ordinal": ordinal,
                "received_at": received_at,
                "source_at": source_time(message.get("timestamp")),
                "condition_id": message.get("market"),
                "token_id": item.get("asset_id"),
                "kind": kind,
                "source_hash": item.get("hash", message.get("hash")),
                "raw_identity": identity,
            }
            ordinal += 1
            if kind == "book":
                base.update(
                    bids=levels(item.get("bids"), bids=True),
                    asks=levels(item.get("asks"), bids=False),
                )
                if item.get("tick_size") is not None:
                    base["tick_size"] = number(item["tick_size"], price=True)
            elif kind == "price_change":
                if item.get("side") not in ("BUY", "SELL"):
                    raise ValueError("unknown book side")
                base.update(
                    side=item["side"],
                    price=number(item.get("price"), price=True),
                    quantity=number(item.get("size")),
                )
            elif kind == "tick_size_change":
                base.update(tick_size=number(item.get("new_tick_size"), price=True))
            elif kind == "market_resolved":
                base["reason"] = "market_resolved"
            else:
                continue
            if base.get("tick_size") is not None and base["tick_size"] <= 0:
                raise ValueError("tick size must be positive")
            if (
                not isinstance(base["token_id"], str)
                or not re.fullmatch(r"[1-9][0-9]*", base["token_id"])
                or int(base["token_id"]) >= 2**256
            ):
                raise ValueError("invalid native token ID")
            if not isinstance(base["condition_id"], str) or not re.fullmatch(
                r"0x[0-9a-fA-F]{64}", base["condition_id"]
            ):
                raise ValueError("invalid condition ID")
            base["condition_id"] = base["condition_id"].lower()
            result.append(base)
    return result


def reset_rows(tokens, *, source, stream, sequence, received_at, reason):
    for ordinal, token in enumerate(sorted(tokens)):
        yield {
            "source": source,
            "stream": stream,
            "sequence": sequence,
            "ordinal": ordinal,
            "received_at": received_at,
            "token_id": token,
            "kind": "reset",
            "reason": reason,
            "raw_identity": f"{stream}:{sequence}",
        }


class Book:
    def __init__(self):
        self.bids, self.asks = {}, {}
        self.initialized = False
        self.snapshot_at = None
        self.last = None
        self.reason = "missing_initial_snapshot"
        self.tick_size = None
        self.terminal_seen = False
        self.archive_highwater = None
        self.archive_previous = None
        self.archive_uncertain = False

    def _archive_order(self, row):
        if row["source"] not in ("pmxt-v1", "pmxt-v2"):
            return row
        received = row["received_at"]
        same_time = received == self.archive_highwater
        semantic_fields = (
            "kind",
            "condition_id",
            "token_id",
            "source_at",
            "side",
            "price",
            "quantity",
            "tick_size",
            "bids",
            "asks",
            "source_hash",
        )
        conflict = self.archive_highwater is not None and (
            received < self.archive_highwater
            or same_time
            and (
                self.archive_uncertain
                or any(
                    row.get(key) != self.archive_previous.get(key)
                    for key in semantic_fields
                )
            )
        )
        if self.archive_highwater is None or received > self.archive_highwater:
            self.archive_highwater = received
            self.archive_uncertain = False
        if received == self.archive_highwater:
            self.archive_previous = row
        self.archive_uncertain |= conflict or row["kind"] == "reset"
        if conflict and row["kind"] != "reset":
            return row | {"kind": "reset", "reason": "ambiguous_archive_order"}
        return row

    @property
    def status(self):
        if self.terminal_seen:
            return "market_resolved"
        if not self.initialized:
            return self.reason
        if not self.bids or not self.asks:
            return "empty" if not self.bids and not self.asks else "one_sided"
        return "crossed" if max(self.bids) >= min(self.asks) else "valid"

    def apply(self, row):
        # This also checks ties and backwards timestamps across adjacent files.
        # Repeating a snapshot inside a conflicting timestamp group cannot heal it.
        row = self._archive_order(row)
        with localcontext() as context:
            context.prec = 80
            previous_valid = self.status == "valid"
            previous_mid = (
                (max(self.bids) + min(self.asks)) / 2 if previous_valid else None
            )
            added = removed = None
            if row["kind"] in ("reset", "market_resolved"):
                self.initialized = False
                self.reason = row.get("reason") or "gap"
                self.terminal_seen |= row["kind"] == "market_resolved"
                self.bids.clear()
                self.asks.clear()
                self.tick_size = None
            elif row["kind"] == "book":
                self.bids = {r["price"]: r["quantity"] for r in row["bids"]}
                self.asks = {r["price"]: r["quantity"] for r in row["asks"]}
                self.snapshot_at = row["received_at"]
                self.initialized = True
                if row.get("tick_size") is not None:
                    self.tick_size = row["tick_size"]
            elif row["kind"] == "tick_size_change":
                self.tick_size = row.get("tick_size")
            elif row["kind"] == "price_change" and self.initialized:
                side = self.bids if row["side"] == "BUY" else self.asks
                old = side.get(row["price"], ZERO)
                if row["quantity"] == ZERO:
                    side.pop(row["price"], None)
                else:
                    side[row["price"]] = row["quantity"]
                if previous_valid and self.status == "valid":
                    added, removed = (
                        max(ZERO, row["quantity"] - old),
                        max(ZERO, old - row["quantity"]),
                    )
            self.last = row
            bid, ask = max(self.bids, default=None), min(self.asks, default=None)
            bid_qty, ask_qty = (
                sum(self.bids.values(), ZERO),
                sum(self.asks.values(), ZERO),
            )
            valid = self.status == "valid"
            midpoint = (bid + ask) / 2 if valid else None
            state = {key: row.get(key) for key in STATE_SCHEMA.names}
            state.update(
                snapshot_at=self.snapshot_at,
                status=self.status,
                tick_size=self.tick_size,
                best_bid=bid,
                best_ask=ask,
                spread=ask - bid if valid else None,
                midpoint=midpoint,
                bid_quantity=bid_qty,
                ask_quantity=ask_qty,
                imbalance=float((bid_qty - ask_qty) / (bid_qty + ask_qty))
                if bid_qty + ask_qty
                else None,
                midpoint_change=midpoint - previous_mid
                if valid and previous_valid and row["kind"] == "price_change"
                else None,
                displayed_added=added,
                displayed_removed=removed,
            )
            return state

    def quote(self, side, quantity):
        quantity = number(quantity)
        if quantity <= 0 or side not in ("BUY", "SELL"):
            raise ValueError("positive quantity and BUY or SELL required")
        result = {
            "requested_quantity": quantity,
            "available_quantity": ZERO,
            "gross_amount": ZERO,
            "average_price": None,
            "marginal_price": None,
            "complete": False,
            "quote_status": "unavailable",
            "status": self.status,
            "state": self.last,
            "snapshot_at": self.snapshot_at,
            "limitation": "displayed amounts before fees, latency, queue and transaction costs; not guaranteed fills",
        }
        if self.status != "valid":
            return result
        with localcontext() as context:
            context.prec = 80
            depth = self.asks if side == "BUY" else self.bids
            for price in sorted(depth, reverse=side == "SELL"):
                take = min(quantity - result["available_quantity"], depth[price])
                result["available_quantity"] += take
                result["gross_amount"] += take * price
                result["marginal_price"] = price
                if result["available_quantity"] == quantity:
                    break
            if result["available_quantity"]:
                result["average_price"] = (
                    result["gross_amount"] / result["available_quantity"]
                )
            result["complete"] = result["available_quantity"] == quantity
            if result["available_quantity"]:
                result["quote_status"] = "complete" if result["complete"] else "partial"
        return result


def pair_quote(
    first: Book,
    opposite: Book,
    quantity,
    *,
    verified_collateral_unit=None,
    verified_binary_tokens=None,
):
    if verified_collateral_unit is None or verified_binary_tokens is None:
        return {
            "status": "unavailable",
            "reason": "unverified complementary collateral mechanics",
        }
    if (
        first.last is None
        or opposite.last is None
        or first.last["source"] != opposite.last["source"]
        or first.last["stream"] != opposite.last["stream"]
    ):
        return {
            "status": "unavailable",
            "reason": "inconsistent pair source/session provenance",
        }
    if (
        not first.last.get("condition_id")
        or first.last["condition_id"] != opposite.last.get("condition_id")
        or first.last["token_id"] == opposite.last["token_id"]
    ):
        return {
            "status": "unavailable",
            "reason": "not distinct native outcomes of the same condition",
        }
    if (
        len(verified_binary_tokens) != 2
        or set(verified_binary_tokens)
        != {first.last["token_id"], opposite.last["token_id"]}
        or number(verified_collateral_unit) <= 0
    ):
        return {
            "status": "unavailable",
            "reason": "binary complement or collateral unit is not verified",
        }
    left, right = first.quote("BUY", quantity), opposite.quote("BUY", quantity)
    if not left["complete"] or not right["complete"]:
        return {"status": "partial", "first": left, "opposite": right}
    with localcontext() as context:
        context.prec = 80
        return {
            "status": "complete",
            "first": left,
            "opposite": right,
            "combined_acquisition_cost": left["gross_amount"] + right["gross_amount"],
            "opposite_acquisition_cost": right["gross_amount"],
            "gross_merge_proceeds": number(quantity) * number(verified_collateral_unit),
            "net_synthetic_exit_proceeds": number(quantity)
            * number(verified_collateral_unit)
            - right["gross_amount"],
        }


def committed_updates(store: Store, source=None):
    files, pins = [], []
    for kind in ("record", "archive"):
        for path, manifest in store.manifests(kind):
            if source is None or manifest.get("source") == source:
                pins.append(
                    {"path": str(path.relative_to(store.root)), "sha256": sha256(path)}
                )
                files.extend(manifest.get("updates", []))
    return files, pins


def archive_continuity(store):
    hours, objects, endpoints = {}, {}, {}
    for _, manifest in store.manifests("archive"):
        source = manifest["source"]
        hour = datetime.fromisoformat(str(manifest["source_file"]["hour"]))
        key = source, hour
        digest = manifest["archive"]["sha256"]
        if key in objects and objects[key] != digest:
            raise ValueError("multiple archive object revisions for one source hour")
        objects[key] = digest
        if not manifest.get("counts", {}).get("source_rows"):
            continue
        hours.setdefault(source, set()).add(hour)
        endpoints[key] = [
            datetime.fromisoformat(value)
            for value in manifest.get("observed_until", {}).values()
            if value
        ]
    groups, ends = {}, {}
    for source, available in hours.items():
        previous, beginning = None, None
        for hour in sorted(available):
            if previous is None or hour - previous != timedelta(hours=1):
                beginning = hour
            group = f"{source}/{beginning}"
            groups[f"{source}/{hour}"] = group
            for end in endpoints[source, hour]:
                ends[group] = max(ends.get(group, end), end)
            previous = hour
    return groups, ends


def sorted_updates(store, files, *, token=None, source=None, end=None, statistics=None):
    with store.sql_connection() as conn:
        yield from _sorted_updates(
            conn,
            store,
            files,
            token=token,
            source=source,
            end=end,
            statistics=statistics,
        )


def _sorted_updates(
    conn, store, files, *, token=None, source=None, end=None, statistics=None
):
    paths = [str(store.validate_file(item)) for item in files]
    if not paths:
        return
    conn.read_parquet(paths).create_view("all_updates")
    conn.execute("CREATE TEMP TABLE updates AS SELECT DISTINCT * FROM all_updates")
    if conn.execute("""SELECT count(*) FROM (SELECT source,raw_identity,ordinal FROM updates
        GROUP BY source,raw_identity,ordinal HAVING count(*)>1)""").fetchone()[0]:
        raise ValueError("conflicting normalized rows for one raw source identity")
    if conn.execute("""SELECT count(*) FROM (SELECT source,stream,sequence,ordinal FROM updates
        GROUP BY source,stream,sequence,ordinal HAVING count(*)>1)""").fetchone()[0]:
        raise ValueError("conflicting normalized rows at one source position")
    if statistics is not None:
        statistics["identical_reingestion_rows"] = conn.execute(
            "SELECT (SELECT count(*) FROM all_updates)-(SELECT count(*) FROM updates)"
        ).fetchone()[0]
    stream_groups, _ = archive_continuity(store)
    conditions, params = [], []
    for field, value, operator in (
        ("token_id", token, "="),
        ("source", source, "="),
        ("received_at", end, "<="),
    ):
        if value is not None:
            conditions.append(f"{field} {operator} ?")
            params.append(value)
    where = " WHERE " + " AND ".join(conditions) if conditions else ""
    try:
        for batch in conn.execute(
            "SELECT * FROM updates"
            + where
            + " ORDER BY source,token_id,stream,sequence,ordinal",
            params,
        ).to_arrow_reader(batch_size=2048):
            for row in batch.to_pylist():
                row["stream"] = stream_groups.get(row["stream"], row["stream"])
                yield row
    finally:
        conn.close()


def build(store: Store, start, end):
    files, pins = committed_updates(store)
    identity = hashlib.sha256(
        json_bytes(
            {
                "inputs": pins,
                "start": start,
                "end": end,
                "implementation_sha256": store.code_sha256,
                "software_versions": store.software,
            }
        )
    ).hexdigest()
    existing = store.path(f"commits/build/{identity}.json")
    if existing.exists():
        manifest = json.loads(existing.read_bytes())
        for item in files + manifest["files"]:
            store.validate_file(item)
        return manifest | {
            "path": str(existing.relative_to(store.root)),
            "sha256": sha256(existing),
        }
    states = Segments(store, f"derived/{identity}/states", STATE_SCHEMA)
    key, book, counts, statistics = None, None, {}, {}
    for row in sorted_updates(store, files, end=end, statistics=statistics):
        current = row["source"], row["token_id"], row["stream"]
        if current != key:
            key, book = current, Book()
        state = book.apply(row)
        if start <= row["received_at"] < end:
            states.append(state)
            counts[state["status"]] = counts.get(state["status"], 0) + 1
    state_files = states.close()
    return store.commit(
        "build",
        identity,
        state_files,
        inputs=pins,
        states=state_files,
        start=start,
        end=end,
        counts=counts,
        row_accounting=statistics,
        order="source/token/stream/receive sequence/item ordinal",
    )


def historical_quote(store: Store, source, token, side, quantity, as_of):
    from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
        active_catalog,
        catalog_targets,
    )

    catalog = active_catalog(store)
    target = catalog_targets(store, admitted_only=False).get(token)
    if target is None:
        return {
            "status": "unavailable",
            "reason": "unknown or conflicting catalog token identity",
        }
    _, pins = committed_updates(store, source)
    groups, archive_ends = archive_continuity(store)
    manifests = []
    observed_until = dict(archive_ends)
    for kind in ("record", "archive"):
        for path, manifest in store.manifests(kind):
            if manifest.get("source") == source:
                order = str(manifest.get("source_file", {}).get("hour", path.name))
                manifests.append((order, manifest))
                for stream, value in manifest.get("observed_until", {}).items():
                    if not value:
                        continue
                    stream = groups.get(stream, stream)
                    parsed = datetime.fromisoformat(value)
                    observed_until[stream] = max(
                        observed_until.get(stream, parsed), parsed
                    )
    books = {}

    def token_rows(manifest):
        for item in manifest.get("updates", []):
            file = store.validate_file(item)
            for batch in pq.ParquetFile(file).iter_batches(batch_size=2048):
                mask = pc.and_(
                    pc.equal(batch.column("token_id"), token),
                    pc.less_equal(batch.column("received_at"), as_of),
                )
                yield from batch.filter(mask).to_pylist()

    for _, versions in groupby(
        sorted(manifests, key=lambda item: item[0]), key=lambda item: item[0]
    ):
        # Reingestion against a newer catalog may add tokens. Merge only identical
        # source positions; conflicting parser results are not silently preferred.
        merged = heapq.merge(
            *(token_rows(manifest) for _, manifest in versions),
            key=lambda row: (row["sequence"], row["ordinal"]),
        )
        previous = None
        for row in merged:
            if (
                row.get("condition_id")
                and row["condition_id"] != target["condition_id"]
            ):
                raise ValueError(
                    "committed book contradicts catalog condition identity"
                )
            key = row["stream"], row["sequence"], row["ordinal"]
            if previous and key == (
                previous["stream"],
                previous["sequence"],
                previous["ordinal"],
            ):
                if row != previous:
                    raise ValueError(
                        "conflicting normalized rows for one source position"
                    )
                continue
            previous = row.copy()
            row["stream"] = groups.get(row["stream"], row["stream"])
            book = books.setdefault(row["stream"], Book())
            if book.last and (row["sequence"], row["ordinal"]) <= (
                book.last["sequence"],
                book.last["ordinal"],
            ):
                raise ValueError("non-increasing committed source order")
            book.apply(row)
    if not books:
        return {
            "status": "unavailable",
            "reason": "no committed evidence at requested time",
        }
    covering = []
    for stream, book in books.items():
        end = observed_until.get(stream)
        if end is None or (as_of < end if stream in archive_ends else as_of <= end):
            covering.append(book)
    if len(covering) > 1:
        return {
            "status": "unavailable",
            "reason": "multiple independent source streams overlap requested time",
            "inputs": pins,
            "catalog_manifest_sha256": catalog["sha256"],
            "as_of": as_of,
            "source": source,
            "token_id": token,
            "independent_streams": len(covering),
            "continuity": "independent streams retained; no source was silently preferred",
        }
    latest = (
        covering[0]
        if covering
        else max(books.values(), key=lambda book: book.last["received_at"])
    )
    quote = latest.quote(side, quantity)
    latest_end = observed_until.get(latest.last["stream"])
    if latest_end is not None and (
        as_of >= latest_end
        if latest.last["stream"] in archive_ends
        else as_of > latest_end
    ):
        quote.update(
            status="unavailable",
            quote_status="unavailable",
            complete=False,
            available_quantity=ZERO,
            gross_amount=None,
            average_price=None,
            marginal_price=None,
            reason="requested time follows last committed connection observation",
        )
    quote.update(
        inputs=pins,
        catalog_manifest_sha256=catalog["sha256"],
        as_of=as_of,
        observed_age_seconds=(as_of - latest.last["received_at"]).total_seconds(),
        source=source,
        token_id=token,
        independent_streams=len(books),
        snapshot_age_seconds=(as_of - latest.snapshot_at).total_seconds()
        if latest.snapshot_at
        else None,
        paired_amounts={
            "status": "unavailable",
            "reason": "catalog does not prove collateral conversion mechanics",
        },
        continuity="observed session only; no universal exchange sequence guarantee",
    )
    return quote
