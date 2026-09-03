"""Free PMXT file inventory, resumable downloads and native v1/v2 readers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from decimal import Decimal
from html.parser import HTMLParser
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import requests

from oddsfox_pipeline.ingestion.polymarket.sports_data.books import (
    normalize_message,
    reset_rows,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.catalog import (
    active_catalog,
    catalog_targets,
    condition_scope,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.storage import (
    UPDATE_SCHEMA,
    UTC,
    BudgetStop,
    Segments,
    Store,
    json_bytes,
    now,
    sha256,
)
from oddsfox_pipeline.ingestion.polymarket.sports_data.transport import (
    PublicHTTP,
    public_url,
)

ATTRIBUTION = {
    "creator": "pmxt",
    "source": "https://archive.pmxt.dev",
    "license": "CC BY 4.0",
    "license_url": "https://creativecommons.org/licenses/by/4.0/",
    "changes": "sports identity filtering and normalized displayed-depth replay",
}
ARCHIVES = {"v1": "https://r2.pmxt.dev", "v2": "https://r2v2.pmxt.dev"}
FILE_PATTERN = re.compile(r"polymarket_orderbook_(\d{4}-\d{2}-\d{2}T\d{2})\.parquet$")
V1_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp_created_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("update_type", pa.string(), nullable=False),
        pa.field("data", pa.string(), nullable=False),
    ]
)
V2_SCHEMA = pa.schema(
    [
        pa.field("timestamp_received", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("timestamp", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("market", pa.binary(66), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("asset_id", pa.string(), nullable=False),
        ("bids", pa.string()),
        ("asks", pa.string()),
        ("price", pa.decimal128(9, 4)),
        ("size", pa.decimal128(18, 6)),
        ("side", pa.string()),
        ("best_bid", pa.decimal128(9, 4)),
        ("best_ask", pa.decimal128(9, 4)),
        ("fee_rate_bps", pa.uint16()),
        ("transaction_hash", pa.string()),
        ("old_tick_size", pa.decimal128(9, 4)),
        ("new_tick_size", pa.decimal128(9, 4)),
    ]
)
EXCLUSION_SCHEMA = pa.schema(
    [
        ("file_sha256", pa.string()),
        ("row_group", pa.int32()),
        ("row_ordinal", pa.int64()),
        ("reason", pa.string()),
        ("rows", pa.int64()),
    ]
)


class Listing(HTMLParser):
    def __init__(self):
        super().__init__()
        self.files, self.text, self.current, self.parts = {}, [], None, []
        self.in_index = False
        self.hidden = 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1
        if tag == "pre":
            self.in_index = True
        if tag == "a" and self.in_index:
            href = dict(attrs).get("href", "")
            if FILE_PATTERN.search(href):
                if self.current is not None:
                    raise ValueError("archive index has an unterminated file row")
                self.current = href
                self.parts = []

    def handle_data(self, data):
        if not self.hidden:
            self.text.append(data)
        if self.current is not None:
            self.parts.append(data)

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden -= 1
        if tag == "span" and self.current:
            text = " ".join(self.parts)
            match = re.search(r"([\d,.]+)\s*(KB|MB|GB|B)\b", text)
            estimated = (
                int(
                    Decimal(match[1].replace(",", ""))
                    * {"B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3}[match[2]]
                )
                if match
                else None
            )
            if self.current in self.files:
                raise ValueError("duplicate file row in archive index")
            self.files[self.current] = estimated
            self.current = None
        if tag == "pre":
            if self.current is not None:
                raise ValueError("archive index has an incomplete file row")
            self.in_index = False


def _listing_page(store, client, url):
    attempts = []
    for attempt in range(4):
        with client.request("GET", url) as response:
            response.raise_for_status()
            content = bytearray()
            for chunk in response.iter_content(65536):
                store.charge_download(len(chunk))
                content.extend(chunk)
                if len(content) > 8 * 1024**2:
                    raise ValueError("archive listing exceeds bounded page size")
        parser = Listing()
        parser.feed(content.decode())
        descriptor = {
            "url": url,
            "sha256": hashlib.sha256(content).hexdigest(),
            "received_at": now(),
        }
        if parser.files:
            return parser, descriptor, attempts
        if attempt == 3:
            raise ValueError(f"archive directory has no recognized file rows: {url}")
        reason = (
            "provider_placeholder"
            if "coming soon" in " ".join(parser.text).lower()
            else "unrecognized_directory"
        )
        attempts.append(descriptor | {"status": reason})
        print(
            json.dumps(
                {
                    "archive_listing_retry": url,
                    "attempt": attempt + 1,
                    "reason": reason,
                }
            ),
            flush=True,
        )
        time.sleep(2**attempt)
    raise RuntimeError("unreachable listing retry state")


def exclusion_runs(mask):
    """Return half-open contiguous spans for exact, compact row evidence."""
    result = []
    start = None
    for index, excluded in enumerate([*mask, False]):
        if excluded and start is None:
            start = index
        elif not excluded and start is not None:
            result.append((start, index - start))
            start = None
    return result


def inventory(store: Store, start, end, *, http=None):
    client = http or PublicHTTP("pmxt", "https://archive.pmxt.dev", rate=1)
    found, listings, listing_retries = {}, [], []
    try:
        for version in ARCHIVES:
            page, seen_pages = 1, set()
            while True:
                url = f"https://archive.pmxt.dev/Polymarket/{version}?page={page}"
                parser, descriptor, attempts = _listing_page(store, client, url)
                listing_retries.extend(attempts)
                digest = descriptor["sha256"]
                if digest in seen_pages:
                    raise ValueError("archive directory pagination repeated a page")
                seen_pages.add(digest)
                listings.append(descriptor)
                for file_url, size in parser.files.items():
                    public_url("pmxt", file_url)
                    if not file_url.startswith(ARCHIVES[version] + "/"):
                        raise ValueError("archive listing points outside version host")
                    hour = datetime.strptime(
                        FILE_PATTERN.search(file_url)[1], "%Y-%m-%dT%H"
                    ).replace(tzinfo=UTC)
                    entry = {"url": file_url, "estimated_bytes": size}
                    if (version, hour) in found and found[version, hour] != entry:
                        raise ValueError(
                            "archive listing has conflicting identities for one hour"
                        )
                    found[version, hour] = entry
                pagination = re.search(
                    r"Page\s+(\d+)\s+of\s+(\d+)", " ".join(parser.text)
                )
                if pagination is None:
                    raise ValueError("unrecognized archive directory pagination")
                if int(pagination[1]) != page:
                    raise ValueError("archive directory returned wrong page")
                if page % 10 == 0 or page == int(pagination[2]):
                    print(
                        json.dumps(
                            {
                                "archive_inventory": version,
                                "pages": page,
                                "listed_files": len(found),
                            }
                        ),
                        flush=True,
                    )
                if page == int(pagination[2]):
                    break
                if page >= 10000:
                    raise ValueError("archive directory page bound exceeded")
                page += 1
        rows = []
        hour = start.replace(minute=0, second=0, microsecond=0)
        while hour < end:
            for version, host in ARCHIVES.items():
                listed = found.get((version, hour))
                rows.append(
                    {
                        "version": version,
                        "hour": hour,
                        "url": listed["url"]
                        if listed
                        else f"{host}/polymarket_orderbook_{hour:%Y-%m-%dT%H}.parquet",
                        "availability": "listed" if listed else "not_listed",
                        "estimated_bytes": listed["estimated_bytes"]
                        if listed
                        else None,
                        "schema_status": "declared_format_not_yet_file_validated",
                    }
                )
            hour += timedelta(hours=1)
        result = {
            "start": start,
            "end": end,
            "files": rows,
            "listings": listings,
            "listing_retries": listing_retries,
            "attribution": ATTRIBUTION,
            "counts": dict(Counter(r["availability"] for r in rows)),
            "version_counts": {
                version: dict(
                    Counter(
                        row["availability"] for row in rows if row["version"] == version
                    )
                )
                for version in ARCHIVES
            },
            "schema_status_counts": dict(Counter(row["schema_status"] for row in rows)),
            "estimated_listed_bytes": sum(r["estimated_bytes"] or 0 for r in rows),
            "limitations": [
                "directory observation, not zero-activity proof",
                "v1 provider reports material missing-market coverage",
                "v2 provider documents coverage beginning 2026-04-13T19:00:00Z",
            ],
            "budget": store.check(),
        }
        identity = hashlib.sha256(json_bytes(result)).hexdigest()
        store.atomic_json(f"inventories/{identity}.json", result)
        return result | {"inventory_path": f"inventories/{identity}.json"}
    finally:
        if http is None:
            client.close()


def download(store: Store, item: dict, client) -> tuple[Path | None, dict]:
    url = public_url("pmxt", item["url"])
    if item["version"] not in ARCHIVES or not url.startswith(
        ARCHIVES[item["version"]] + "/"
    ):
        raise ValueError("archive downloads require the approved free version host")
    with client.request(
        "HEAD", url, headers={"Accept-Encoding": "identity"}
    ) as response:
        if response.status_code == 404:
            return None, {"status": "missing", "url": url}
        if response.status_code in (401, 403):
            return None, {"status": "unavailable", "url": url}
        response.raise_for_status()
        try:
            size = int(response.headers.get("Content-Length", ""))
        except ValueError as exc:
            raise ValueError("archive object lacks a valid Content-Length") from exc
        if size < 0:
            raise ValueError("archive object has a negative Content-Length")
        etag = response.headers.get("ETag")
        modified = response.headers.get("Last-Modified")
    identity = {"url": url, "bytes": size, "etag": etag, "last_modified": modified}
    key = hashlib.sha256(url.encode()).hexdigest()
    path = store.path(f"downloads/{item['version']}/{key}.parquet")
    metadata = store.path(f"downloads/{item['version']}/{key}.json")
    partial = path.with_suffix(".partial")
    existing = {
        "status": "partial",
        "identity": identity,
        "verified_bytes": 0,
        "prefix_sha256": hashlib.sha256().hexdigest(),
    }
    if metadata.exists():
        existing = json.loads(metadata.read_bytes())
        if existing["identity"] != identity:
            raise ValueError(
                "upstream archive object changed; retained download not overwritten"
            )
        if path.exists() and existing.get("sha256"):
            if not (etag or modified):
                raise ValueError(
                    "cached archive cannot be revalidated without upstream object identity"
                )
            if path.stat().st_size != size or sha256(path) != existing["sha256"]:
                raise ValueError("cached archive checksum mismatch")
            if existing["status"] != "downloaded":
                existing.update(status="downloaded", recovered_activation=True)
                store.atomic_json(str(metadata.relative_to(store.root)), existing)
            return path, existing
        if path.exists():
            raise ValueError("installed archive lacks its pre-activation checksum")
    if size == 0:
        return None, {"status": "empty", "identity": identity}
    if partial.exists() and not (etag or modified):
        raise ValueError("cannot resume archive without upstream object identity")
    verified = existing["verified_bytes"]
    prefix_hash = hashlib.sha256()
    if partial.exists():
        if not verified <= partial.stat().st_size <= size:
            raise ValueError("partial archive size contradicts its checkpoint")
        with partial.open("rb") as handle:
            remaining_prefix = verified
            while remaining_prefix:
                chunk = handle.read(min(1024**2, remaining_prefix))
                if not chunk:
                    raise ValueError(
                        "partial archive truncated during checkpoint validation"
                    )
                prefix_hash.update(chunk)
                remaining_prefix -= len(chunk)
        if prefix_hash.hexdigest() != existing["prefix_sha256"]:
            raise ValueError("partial archive prefix checksum mismatch")
    elif verified:
        raise ValueError("checkpointed partial archive is missing")
    store.atomic_json(str(metadata.relative_to(store.root)), existing)
    attempts = 0
    while verified < size:
        offset = verified
        remaining = store.limits.download_bytes - store.downloaded
        if remaining <= 0:
            raise BudgetStop("download limit reached; partial archive retained")
        length = min(8 * 1024**2, remaining, size - offset)
        store.check(length + 1024**2)
        headers = {
            "Accept-Encoding": "identity",
            "Range": f"bytes={offset}-{offset + length - 1}",
        }
        if etag or modified:
            headers["If-Range"] = etag or modified
        try:
            with client.request("GET", url, headers=headers) as response:
                response.raise_for_status()
                if (
                    response.status_code != 206
                    or response.headers.get("Content-Range")
                    != f"bytes {offset}-{offset + length - 1}/{size}"
                ):
                    raise ValueError(
                        "archive range response does not match pinned object"
                    )
                if etag and response.headers.get("ETag") != etag:
                    raise ValueError("archive object identity changed during download")
                written = 0
                range_hash = prefix_hash.copy()
                prior_size = partial.stat().st_size if partial.exists() else 0
                try:
                    with partial.open("r+b" if partial.exists() else "x+b") as handle:
                        handle.seek(offset)
                        try:
                            for chunk in response.iter_content(65536):
                                store.charge_download(len(chunk))
                                written += len(chunk)
                                if written > length:
                                    raise ValueError(
                                        "archive range exceeds declared bytes"
                                    )
                                # Re-request and compare a crash's uncheckpointed
                                # tail before appending. Never silently truncate or
                                # trust that tail.
                                retained = min(
                                    len(chunk), max(0, prior_size - handle.tell())
                                )
                                if handle.read(retained) != chunk[:retained]:
                                    raise ValueError(
                                        "uncheckpointed archive tail differs from source"
                                    )
                                handle.write(chunk[retained:])
                                range_hash.update(chunk)
                        finally:
                            handle.flush()
                            os.fsync(handle.fileno())
                finally:
                    if partial.exists():
                        store.track(partial)
                if written != length:
                    raise requests.ConnectionError("truncated archive range")
            verified += length
            prefix_hash = range_hash
            existing.update(
                verified_bytes=verified, prefix_sha256=prefix_hash.hexdigest()
            )
            store.atomic_json(str(metadata.relative_to(store.root)), existing)
            attempts = 0
        except requests.RequestException:
            attempts += 1
            if attempts >= 4:
                raise
    if partial.stat().st_size != size:
        raise ValueError("archive download size mismatch")
    pq.ParquetFile(partial)  # Footer validation before atomic cache activation.
    digest = sha256(partial)
    if digest != prefix_hash.hexdigest():
        raise ValueError("downloaded archive differs from its verified byte stream")
    metadata_value = {
        **existing,
        "identity": identity,
        "sha256": digest,
        "status": "ready_to_activate",
        "acquired_at": now(),
    }
    store.atomic_json(str(metadata.relative_to(store.root)), metadata_value)
    os.replace(partial, path)
    store.track(partial)
    store.track(path)
    metadata_value["status"] = "downloaded"
    store.atomic_json(str(metadata.relative_to(store.root)), metadata_value)
    return path, metadata_value


def archive_message(row, version):
    if version == "v1":
        data = json.loads(row["data"], parse_float=Decimal)
        if data.get("market_id", row["market_id"]) != row["market_id"]:
            raise ValueError("v1 envelope has conflicting condition identities")
        kind = data.get("update_type", row["update_type"])
        message = {
            "event_type": "book" if kind == "book_snapshot" else kind,
            "asset_id": data.get("token_id"),
            "market": row["market_id"],
            "timestamp": (
                row["timestamp_created_at"] - datetime(1970, 1, 1, tzinfo=UTC)
            )
            // timedelta(milliseconds=1),
        }
        if message["event_type"] == "book":
            message.update(bids=data.get("bids"), asks=data.get("asks"))
        elif message["event_type"] == "price_change":
            message["price_changes"] = [
                {
                    "asset_id": data.get("token_id"),
                    "price": data.get("change_price"),
                    "size": data.get("change_size"),
                    "side": data.get("change_side"),
                }
            ]
        elif message["event_type"] == "tick_size_change":
            message["new_tick_size"] = data.get("new_tick_size")
        return message
    message = {
        "event_type": row["event_type"],
        "asset_id": row["asset_id"],
        "market": row["market"].decode("ascii"),
        "timestamp": (row["timestamp"] - datetime(1970, 1, 1, tzinfo=UTC))
        // timedelta(milliseconds=1),
    }
    if row["event_type"] == "book":
        message.update(bids=json.loads(row["bids"]), asks=json.loads(row["asks"]))
    elif row["event_type"] == "price_change":
        message["price_changes"] = [
            {
                "asset_id": row["asset_id"],
                "price": row["price"],
                "size": row["size"],
                "side": row["side"],
            }
        ]
    elif row["event_type"] == "tick_size_change":
        message["new_tick_size"] = row["new_tick_size"]
    return message


def ingest_file(
    store: Store, path: Path, item: dict, download_metadata: dict, *, targets=None
):
    version, catalog = item["version"], active_catalog(store)
    targets = (
        targets if targets is not None else catalog_targets(store, admitted_only=False)
    )
    digest = sha256(path)
    if download_metadata.get("sha256", digest) != digest:
        raise ValueError("archive bytes differ from the acquisition checksum")
    identity = hashlib.sha256(
        json_bytes(
            {
                "file": digest,
                "catalog": catalog["sha256"],
                "version": version,
                "implementation_sha256": store.code_sha256,
                "software_versions": store.software,
            }
        )
    ).hexdigest()
    existing = store.path(f"commits/archive/{identity}.json")
    if existing.exists():
        manifest = json.loads(existing.read_bytes())
        for entry in manifest["files"]:
            store.validate_file(entry)
        return manifest | {
            "path": str(existing.relative_to(store.root)),
            "sha256": sha256(existing),
        }
    parquet = pq.ParquetFile(path)
    expected = V1_SCHEMA if version == "v1" else V2_SCHEMA
    if not parquet.schema_arrow.equals(expected, check_metadata=False):
        raise ValueError(f"unsupported PMXT {version} schema: {parquet.schema_arrow}")
    hour = (
        item["hour"]
        if isinstance(item["hour"], datetime)
        else datetime.fromisoformat(item["hour"])
    )
    source, stream = f"pmxt-{version}", f"pmxt-{version}/{hour}"
    prefix = f"archive/{version}/{hour:%Y-%m-%d/%H}/{identity}"
    writer = Segments(store, f"{prefix}/updates", UPDATE_SCHEMA)
    conditions = {row["condition_id"] for row in targets.values()}
    condition_column = "market_id" if version == "v1" else "market"
    values = pa.array(
        sorted(conditions)
        if version == "v1"
        else [c.encode() for c in sorted(conditions)],
        type=expected.field(condition_column).type,
    )
    counts = Counter({"source_rows": 0})
    known = condition_scope(store)
    non_sports = [condition for condition, sports in known.items() if not sports]
    non_sports_values = pa.array(
        non_sports if version == "v1" else [c.encode() for c in non_sports],
        type=expected.field(condition_column).type,
    )
    exclusions = Segments(
        store,
        f"{prefix}/exclusions",
        EXCLUSION_SCHEMA,
    )
    observed_until = None
    last = {}
    file_sequence_base = (
        int((hour - datetime(1970, 1, 1, tzinfo=UTC)).total_seconds()) // 3600 << 32
    )
    file_row_offset = 0
    for group in range(parquet.num_row_groups):
        ordinal = 0
        for batch in parquet.iter_batches(batch_size=8192, row_groups=[group]):
            if any(
                batch.column(field.name).null_count
                for field in expected
                if not field.nullable
            ):
                raise ValueError("archive contains nulls in required source fields")
            counts["source_rows"] += batch.num_rows
            mask = pc.is_in(batch.column(condition_column), value_set=values)
            sports_mask = mask.to_pylist()
            non_sports_mask = pc.is_in(
                batch.column(condition_column), value_set=non_sports_values
            ).to_pylist()
            unknown_mask = [
                not sports and not non_sports
                for sports, non_sports in zip(sports_mask, non_sports_mask, strict=True)
            ]
            for reason, excluded_mask in (
                ("non_sports_rows", non_sports_mask),
                ("unknown_condition_rows", unknown_mask),
            ):
                total = sum(excluded_mask)
                counts[reason] += total
                for start, length in exclusion_runs(excluded_mask):
                    exclusions.append(
                        {
                            "file_sha256": digest,
                            "row_group": group,
                            "row_ordinal": ordinal + start,
                            "reason": reason,
                            "rows": length,
                        }
                    )
            end_observed = pc.max(batch.column("timestamp_received")).as_py()
            if end_observed:
                observed_until = (
                    max(observed_until, end_observed)
                    if observed_until
                    else end_observed
                )
            indices = pc.indices_nonzero(mask).to_pylist()
            for index, row in zip(indices, batch.filter(mask).to_pylist(), strict=True):
                raw_identity = f"{digest}:{group}:{ordinal + index}"
                if file_row_offset + ordinal + index >= 2**32:
                    raise ValueError(
                        "archive hourly row identity exceeds int64 ordering contract"
                    )
                sequence = file_sequence_base + file_row_offset + ordinal + index
                try:
                    message = archive_message(row, version)
                    normalized = normalize_message(
                        json_bytes(message),
                        source=source,
                        stream=stream,
                        sequence=sequence,
                        received_at=row["timestamp_received"],
                        identity=raw_identity,
                    )
                    if not normalized:
                        counts["ancillary_rows"] += 1
                        continue
                    for update in normalized:
                        token = update["token_id"]
                        if (
                            token not in targets
                            or targets[token]["condition_id"] != update["condition_id"]
                        ):
                            counts["unknown_or_conflicting_token"] += 1
                            exclusions.append(
                                {
                                    "file_sha256": digest,
                                    "row_group": group,
                                    "row_ordinal": ordinal + index,
                                    "reason": "unknown_or_conflicting_token",
                                    "rows": 1,
                                }
                            )
                            affected = {
                                candidate
                                for candidate, target in targets.items()
                                if target["condition_id"] == update["condition_id"]
                            }
                            if token in targets:
                                affected.add(token)
                            for reset in reset_rows(
                                affected,
                                source=source,
                                stream=stream,
                                sequence=sequence,
                                received_at=update["received_at"],
                                reason="unknown_or_conflicting_archive_identity",
                            ):
                                reset["raw_identity"] = raw_identity
                                writer.append(reset)
                            continue
                        previous = last.get(token)
                        signature = hashlib.sha256(json_bytes(message)).hexdigest()
                        if previous and (
                            update["received_at"] < previous[0]
                            or (
                                update["received_at"] == previous[0]
                                and signature != previous[1]
                            )
                        ):
                            reset = next(
                                reset_rows(
                                    [token],
                                    source=source,
                                    stream=stream,
                                    sequence=sequence,
                                    received_at=update["received_at"],
                                    reason="ambiguous_archive_order",
                                )
                            )
                            reset["raw_identity"] = raw_identity
                            writer.append(reset)
                            counts["uncertain_order_rows"] += 1
                            exclusions.append(
                                {
                                    "file_sha256": digest,
                                    "row_group": group,
                                    "row_ordinal": ordinal + index,
                                    "reason": "ambiguous_archive_order",
                                    "rows": 1,
                                }
                            )
                            # Even a snapshot in a tied group cannot establish which conflicting update was last.
                            last[token] = update["received_at"], signature
                            continue
                        writer.append(update)
                        last[token] = update["received_at"], signature
                        counts["normalized_rows"] += 1
                except (ValueError, TypeError, KeyError, UnicodeError):
                    counts["malformed_rows"] += 1
                    exclusions.append(
                        {
                            "file_sha256": digest,
                            "row_group": group,
                            "row_ordinal": ordinal + index,
                            "reason": "malformed_row",
                            "rows": 1,
                        }
                    )
                    # A malformed row can hide a change; invalidate all outcomes in its known condition.
                    condition = row[condition_column]
                    condition = (
                        condition.decode()
                        if isinstance(condition, bytes)
                        else condition
                    )
                    affected = [
                        token
                        for token, target in targets.items()
                        if target["condition_id"] == condition
                    ]
                    for reset in reset_rows(
                        affected,
                        source=source,
                        stream=stream,
                        sequence=sequence,
                        received_at=row["timestamp_received"],
                        reason="malformed_archive_row",
                    ):
                        reset["raw_identity"] = raw_identity
                        writer.append(reset)
            ordinal += batch.num_rows
        file_row_offset += ordinal
        print(
            json.dumps(
                {
                    "archive": version,
                    "row_groups_completed": group + 1,
                    "counts": dict(counts),
                }
            ),
            flush=True,
        )
    outputs = writer.close()
    excluded_files = exclusions.close()
    counted = sum(
        counts[key]
        for key in (
            "non_sports_rows",
            "unknown_condition_rows",
            "unknown_or_conflicting_token",
            "uncertain_order_rows",
            "malformed_rows",
            "ancillary_rows",
            "normalized_rows",
        )
    )
    if counted != counts["source_rows"]:
        raise ValueError("archive source-row accounting does not reconcile")
    if counts["source_rows"]:
        # A successfully parsed hourly object observes quiet books through its
        # half-open hour; the last update is not a connection-loss boundary.
        observed_until = hour + timedelta(hours=1)
    return store.commit(
        "archive",
        identity,
        outputs + excluded_files,
        source=source,
        updates=outputs,
        exclusions=excluded_files,
        archive={
            "path": str(path.relative_to(store.root)),
            "sha256": digest,
            "bytes": path.stat().st_size,
            "rows": parquet.metadata.num_rows,
        },
        source_file=item,
        acquisition=download_metadata,
        catalog_manifest_sha256=catalog["sha256"],
        counts=dict(counts),
        empty_archive=counts["source_rows"] == 0,
        attribution=ATTRIBUTION,
        observed_until={stream: observed_until},
        continuity="separate archive version; only adjacent committed non-empty files share observed continuity",
    )


def backfill(store: Store, start, end, *, dry_run=False, max_files=None):
    if not dry_run:
        active_catalog(store)
    listing = inventory(store, start, end)
    if dry_run:
        return {key: value for key, value in listing.items() if key != "files"}
    client = PublicHTTP("pmxt", "https://archive.pmxt.dev")
    run = uuid4().hex
    results = []
    try:
        for item in listing["files"]:
            if item["availability"] != "listed":
                continue
            if (
                max_files is not None
                and sum(r["status"] in ("ingested", "empty") for r in results)
                >= max_files
            ):
                break
            try:
                path, metadata = download(store, item, client)
                if path:
                    manifest = ingest_file(store, path, item, metadata)
                    results.append(
                        {
                            "url": item["url"],
                            "status": "empty"
                            if manifest["empty_archive"]
                            else "ingested",
                            "counts": manifest["counts"],
                        }
                    )
                else:
                    results.append({"url": item["url"], **metadata})
            except BudgetStop:
                results.append({"url": item["url"], "status": "budget_stop"})
                raise
            except (ValueError, pa.ArrowException, requests.RequestException) as exc:
                results.append(
                    {"url": item["url"], "status": "failed", "reason": str(exc)}
                )
            finally:
                store.atomic_json(
                    f"backfills/{run}.json",
                    {"inventory": listing["inventory_path"], "results": results},
                )
        return {
            "inventory": listing["inventory_path"],
            "results": results,
            "budget": store.check(),
            "status": "partial_failure"
            if any(row["status"] == "failed" for row in results)
            else "bounded_sample"
            if max_files
            else "inventory_processed",
            "file_status_counts": dict(Counter(row["status"] for row in results)),
            "pending_listed_files": listing["counts"].get("listed", 0) - len(results),
            "unlisted_hours": listing["counts"].get("not_listed", 0),
        }
    finally:
        client.close()
