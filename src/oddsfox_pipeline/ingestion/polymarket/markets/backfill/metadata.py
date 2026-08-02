"""Polymarket metadata enrichment: enrich_market_metadata."""

import logging
import sys
import time
from typing import Any, Dict

from tqdm import tqdm

from oddsfox_pipeline.ingestion.polymarket.errors import GammaRequestError
from oddsfox_pipeline.ingestion.polymarket.scope_sql import DEFAULT_MARKET_SCOPE
from oddsfox_pipeline.storage.duckdb.connection import ensure_duck_db
from oddsfox_pipeline.storage.duckdb.markets import (
    get_markets_missing_any_metadata,
    mark_market_metadata_unresolved,
    save_end_dates_batch,
    save_event_slugs_batch,
    save_slugs_batch,
    save_tokens_batch,
)
from oddsfox_pipeline.storage.duckdb.metadata import (
    get_backfill_fully_checked,
    set_backfill_fully_checked,
)

from ._events_fallback import _fill_from_events_endpoint
from ._extract import (
    _extract_end_date_record,
    _extract_event_slug_record,
    _extract_tokens_record,
)
from ._gamma import (
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    ProgressCallback,
    _chunk_market_ids,
    _duration_since,
    _fetch_markets_batch,
    _gamma_client,
)

logger = logging.getLogger(__name__)


def _error_metadata(failed_batches: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "errors": len(failed_batches),
        "failed_batches": list(failed_batches),
        "has_errors": bool(failed_batches),
    }


def _enrich_ledger_keys(
    *,
    include_tokens: bool,
    include_slugs: bool,
    include_event_slugs: bool,
    include_end_dates: bool,
) -> list[str]:
    ledger_keys = []
    if include_tokens:
        ledger_keys.append("tokens")
    if include_slugs:
        ledger_keys.append("slugs")
    if include_event_slugs:
        ledger_keys.append("event_slugs")
    if include_end_dates:
        ledger_keys.append("end_dates")
    return ledger_keys


def _enrich_skip_response(
    t0: float,
    *,
    reason: str,
    failed_batches: list[dict[str, Any]] | None = None,
) -> Dict[str, Any]:
    batches = failed_batches if failed_batches is not None else []
    return {
        "task": "enrich_market_metadata",
        "skipped": True,
        "reason": reason,
        "duration_seconds": _duration_since(t0),
        "api_requests": 0,
        **_error_metadata(batches),
    }


def _extract_metadata_batch_rows(
    chunk: list[str],
    markets: list[dict[str, Any]] | None,
    *,
    include_tokens: bool,
    include_slugs: bool,
    include_event_slugs: bool,
    include_end_dates: bool,
    remaining_event_slug_ids: set[str],
) -> tuple[
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[tuple[str, str]],
    int,
]:
    token_rows: list[tuple[str, str]] = []
    slug_rows: list[tuple[str, str]] = []
    event_slug_rows: list[tuple[str, str]] = []
    end_date_rows: list[tuple[str, str]] = []
    processed = 0
    returned_map = {str(market.get("id")): market for market in markets or []}
    for market_id in chunk:
        market = returned_map.get(str(market_id))
        if not market:
            processed += 1
            continue
        if include_tokens:
            record = _extract_tokens_record(market_id, market)
            if record is not None:
                token_rows.append(record)
        if include_slugs:
            slug = market.get("slug")
            if slug:
                slug_rows.append((slug, market_id))
        if include_event_slugs:
            record = _extract_event_slug_record(market_id, market)
            if record is not None:
                event_slug_rows.append(record)
            else:
                remaining_event_slug_ids.add(str(market_id))
        if include_end_dates:
            record = _extract_end_date_record(market_id, market)
            if record is not None:
                end_date_rows.append(record)
        processed += 1
    return token_rows, slug_rows, event_slug_rows, end_date_rows, processed


def _persist_metadata_batch_rows(
    *,
    token_rows: list[tuple[str, str]],
    slug_rows: list[tuple[str, str]],
    event_slug_rows: list[tuple[str, str]],
    end_date_rows: list[tuple[str, str]],
    market_scope: str,
    saved: dict[str, int],
) -> None:
    if token_rows:
        save_tokens_batch(token_rows, scope_name=market_scope)
        saved["tokens"] += len(token_rows)
    if slug_rows:
        save_slugs_batch(slug_rows, scope_name=market_scope)
        saved["slugs"] += len(slug_rows)
    if event_slug_rows:
        save_event_slugs_batch(event_slug_rows, scope_name=market_scope)
        saved["event_slugs"] += len(event_slug_rows)
    if end_date_rows:
        save_end_dates_batch(end_date_rows, scope_name=market_scope)
        saved["end_dates"] += len(end_date_rows)


def _set_enrich_ledger_state(
    ledger_keys: list[str],
    *,
    max_markets: int | None,
    completed_all: bool,
) -> None:
    if max_markets is None:
        for key in ledger_keys:
            set_backfill_fully_checked(key, completed_all)
        return
    for key in ledger_keys:
        set_backfill_fully_checked(key, False)


def enrich_market_metadata(
    batch_size: int = 50,
    max_markets: int = None,
    force: bool = False,
    *,
    include_tokens: bool = True,
    include_slugs: bool = True,
    include_event_slugs: bool = True,
    include_end_dates: bool = True,
    progress_callback: ProgressCallback = None,
    progress_every_n_batches: int = 10,
    gamma_requests_per_second: float | None = None,
    market_scope: str = DEFAULT_MARKET_SCOPE,
    event_slug_fallback_max_pages: int | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_PAGES,
    event_slug_fallback_max_pages_without_progress: int
    | None = DEFAULT_EVENT_SLUG_FALLBACK_MAX_NO_PROGRESS_PAGES,
    event_slug_fallback_progress_every_pages: int = 25,
    event_slug_unresolved_retry_hours: int = 168,
) -> Dict[str, Any]:
    """Enrich requested market metadata with a single Gamma pass per market."""
    t0 = time.monotonic()
    if not any([include_tokens, include_slugs, include_event_slugs, include_end_dates]):
        return _enrich_skip_response(t0, reason="no_fields_enabled")

    ledger_keys = _enrich_ledger_keys(
        include_tokens=include_tokens,
        include_slugs=include_slugs,
        include_event_slugs=include_event_slugs,
        include_end_dates=include_end_dates,
    )
    if not force and all(get_backfill_fully_checked(k) for k in ledger_keys):
        return _enrich_skip_response(t0, reason="fully_checked")

    ensure_duck_db()
    market_ids = get_markets_missing_any_metadata(
        include_tokens=include_tokens,
        include_slugs=include_slugs,
        include_event_slugs=include_event_slugs,
        include_end_dates=include_end_dates,
        limit=max_markets,
        market_scope=market_scope,
    )
    total_markets = len(market_ids)
    failed_batches: list[dict[str, Any]] = []
    if total_markets == 0:
        if max_markets is None:
            for key in ledger_keys:
                set_backfill_fully_checked(key, True)
        return {
            "task": "enrich_market_metadata",
            "skipped": False,
            "eligible": 0,
            "processed": 0,
            "saved": {
                "tokens": 0,
                "slugs": 0,
                "event_slugs": 0,
                "end_dates": 0,
            },
            "fully_checked_set": max_markets is None,
            "duration_seconds": _duration_since(t0),
            "api_requests": 0,
            **_error_metadata(failed_batches),
        }

    if progress_callback:
        progress_callback(
            "enrich_market_metadata",
            {
                "stage": "start",
                "eligible": total_markets,
                "batch_size": batch_size,
                "market_scope": market_scope,
                **_error_metadata(failed_batches),
            },
        )

    client = _gamma_client(gamma_requests_per_second)
    processed = 0
    api_requests = 0
    saved = {"tokens": 0, "slugs": 0, "event_slugs": 0, "end_dates": 0}
    unresolved_event_slugs: list[tuple[str, str, str]] = []
    remaining_event_slug_ids: set[str] = set()
    events_fb_meta: dict[str, Any] = {
        "events_fallback_pages": 0,
        "events_fallback_truncated": False,
        "events_fallback_remaining_ids": 0,
    }
    chunks = _chunk_market_ids(market_ids, batch_size)
    total_batches = len(chunks)
    loop_started = time.monotonic()
    use_tqdm = sys.stderr.isatty()

    with tqdm(
        total=total_markets, desc="Enriching market metadata", disable=not use_tqdm
    ) as pbar:
        for batch_idx, chunk in enumerate(chunks, start=1):
            try:
                api_requests += 1
                markets = _fetch_markets_batch(client, chunk, include_events=True)
                (
                    token_rows,
                    slug_rows,
                    event_slug_rows,
                    end_date_rows,
                    batch_processed,
                ) = _extract_metadata_batch_rows(
                    chunk,
                    markets,
                    include_tokens=include_tokens,
                    include_slugs=include_slugs,
                    include_event_slugs=include_event_slugs,
                    include_end_dates=include_end_dates,
                    remaining_event_slug_ids=remaining_event_slug_ids,
                )
                processed += batch_processed
                pbar.update(len(chunk))
                _persist_metadata_batch_rows(
                    token_rows=token_rows,
                    slug_rows=slug_rows,
                    event_slug_rows=event_slug_rows,
                    end_date_rows=end_date_rows,
                    market_scope=market_scope,
                    saved=saved,
                )
            except (GammaRequestError, OSError) as exc:
                logger.error("Error during combined metadata enrichment batch: %s", exc)
                failed_batches.append(
                    {
                        "batch_index": batch_idx,
                        "market_ids": list(chunk),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                pbar.update(len(chunk))
            except Exception as exc:
                logger.error("Error during combined metadata enrichment batch: %s", exc)
                failed_batches.append(
                    {
                        "batch_index": batch_idx,
                        "market_ids": list(chunk),
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                )
                pbar.update(len(chunk))

            if progress_callback and (
                batch_idx % progress_every_n_batches == 0 or batch_idx == total_batches
            ):
                progress_callback(
                    "enrich_market_metadata",
                    {
                        "batch_index": batch_idx,
                        "batches_total": total_batches,
                        "processed": processed,
                        "saved": dict(saved),
                        "api_requests": api_requests,
                        "elapsed_seconds": round(time.monotonic() - loop_started, 3),
                        **_error_metadata(failed_batches),
                    },
                )

    if include_event_slugs and remaining_event_slug_ids:
        if progress_callback:
            progress_callback(
                "enrich_market_metadata",
                {
                    "stage": "events_fallback_start",
                    "remaining_before_fallback": len(remaining_event_slug_ids),
                    "max_pages": event_slug_fallback_max_pages,
                    "max_pages_without_progress": event_slug_fallback_max_pages_without_progress,
                    **_error_metadata(failed_batches),
                },
            )
        extra_saved, events_fb_meta = _fill_from_events_endpoint(
            client,
            remaining_event_slug_ids,
            max_pages=event_slug_fallback_max_pages,
            max_pages_without_progress=event_slug_fallback_max_pages_without_progress,
            progress_callback=progress_callback,
            progress_every_pages=event_slug_fallback_progress_every_pages,
            progress_phase="enrich_market_metadata_events_fallback",
        )
        saved["event_slugs"] += extra_saved
        api_requests += int(events_fb_meta.get("events_fallback_pages", 0) or 0)
        unresolved_event_slugs = [
            (market_id, "event_slug", "missing from Gamma market and events payload")
            for market_id in remaining_event_slug_ids
        ]

    if unresolved_event_slugs:
        mark_market_metadata_unresolved(
            unresolved_event_slugs,
            retry_after_hours=event_slug_unresolved_retry_hours,
            scope_name=market_scope,
        )

    completed_all = processed >= total_markets and not failed_batches
    _set_enrich_ledger_state(
        ledger_keys,
        max_markets=max_markets,
        completed_all=completed_all,
    )

    if progress_callback:
        progress_callback(
            "enrich_market_metadata",
            {
                "stage": "complete",
                "eligible": total_markets,
                "processed": processed,
                "saved": dict(saved),
                "unresolved_event_slugs": len(unresolved_event_slugs),
                "api_requests": api_requests,
                "duration_seconds": _duration_since(t0),
                **events_fb_meta,
                **_error_metadata(failed_batches),
            },
        )

    return {
        "task": "enrich_market_metadata",
        "skipped": False,
        "eligible": total_markets,
        "processed": processed,
        "saved": dict(saved),
        "unresolved_event_slugs": len(unresolved_event_slugs),
        **events_fb_meta,
        "fully_checked_set": max_markets is None and completed_all,
        "duration_seconds": _duration_since(t0),
        "api_requests": api_requests,
        **_error_metadata(failed_batches),
    }
