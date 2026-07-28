from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from oddsfox_pipeline.ingestion.polymarket.match_order_book import (
    load_order_book_manifest,
)
from oddsfox_pipeline.storage.duckdb.match_order_book import (
    acquire_scan,
    complete_window,
    next_pending_window,
    publish_scan,
    published_scan_summary,
    reserve_api_attempt,
    scan_progress_summary,
    set_scan_status,
    split_window,
)


def test_live_lease_blocks_concurrent_run_and_expired_lease_is_resumable(duck):
    manifest = load_order_book_manifest()
    now = datetime.now(timezone.utc)
    with duck.get_connection() as conn:
        scan_id, published, resumed = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner-1",
            force=False,
            lease_seconds=300,
            now=now,
        )
        with pytest.raises(RuntimeError, match="leased by another run"):
            acquire_scan(
                conn,
                manifest_version=manifest.version,
                manifest_sha256=manifest.sha256,
                targets=manifest.targets,
                lease_owner="owner-2",
                force=False,
                lease_seconds=300,
                now=now + timedelta(seconds=1),
            )
        resumed_id, resumed_published, was_resumed = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner-2",
            force=False,
            lease_seconds=300,
            now=now + timedelta(seconds=301),
        )
        with pytest.raises(RuntimeError, match="lease was lost"):
            reserve_api_attempt(
                conn,
                scan_id=scan_id,
                lease_owner="owner-1",
                token_id=manifest.targets[0].outcomes[0].clob_token_id,
                window_start_ms=manifest.targets[0].window_start_ms,
                window_end_ms=manifest.targets[0].window_end_ms,
                monthly_credit_budget=20_000,
                now=now + timedelta(seconds=302),
            )
        usage = conn.execute(
            """
            select value
            from polymarket_wc2026_ops.scrape_metadata
            where key like 'pmxt_order_book_api_attempts_%'
            """
        ).fetchone()

    assert (published, resumed) == (False, False)
    assert resumed_id == scan_id
    assert (resumed_published, was_resumed) == (False, True)
    assert usage is None


def test_force_creates_a_separate_scan_instead_of_resuming(duck):
    manifest = load_order_book_manifest()
    now = datetime.now(timezone.utc)
    with duck.get_connection() as conn:
        first, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="first",
            force=False,
            now=now,
        )
        second, published, resumed = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="second",
            force=True,
            now=now,
        )
        run_count = conn.execute(
            """
            select count(*)
            from polymarket_wc2026_ops.match_order_book_scan_runs
            """
        ).fetchone()[0]

    assert first != second
    assert (published, resumed) == (False, False)
    assert run_count == 2


def test_naive_clock_is_stored_as_utc_without_conversion(duck):
    manifest = load_order_book_manifest()
    now = datetime(2026, 7, 28, 12, 0, 0)
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
            now=now,
        )
        started_at = conn.execute(
            """
            select started_at
            from polymarket_wc2026_ops.match_order_book_scan_runs
            where scan_id = ?
            """,
            [scan_id],
        ).fetchone()[0]

    assert started_at == now


def test_attempt_reservation_requires_existing_window_and_scan(duck):
    manifest = load_order_book_manifest()
    now = datetime.now(timezone.utc)
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
            now=now,
        )
        with pytest.raises(RuntimeError, match="work window disappeared"):
            reserve_api_attempt(
                conn,
                scan_id=scan_id,
                lease_owner="owner",
                token_id="missing",
                window_start_ms=1,
                window_end_ms=2,
                monthly_credit_budget=20_000,
                now=now,
            )
        with pytest.raises(RuntimeError, match="does not exist"):
            reserve_api_attempt(
                conn,
                scan_id="missing",
                lease_owner="owner",
                token_id="missing",
                window_start_ms=1,
                window_end_ms=2,
                monthly_credit_budget=20_000,
                now=now,
            )
        usage = conn.execute(
            """
            select value
            from polymarket_wc2026_ops.scrape_metadata
            where key like 'pmxt_order_book_api_attempts_%'
            """
        ).fetchone()

    assert usage is None


def test_checkpoint_mutations_roll_back_after_lease_loss(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
        )
        window = next_pending_window(conn, scan_id)
        assert window is not None
        set_scan_status(
            conn,
            scan_id,
            "failed",
            RuntimeError("sensitive upstream detail"),
            lease_owner="owner",
        )
        with pytest.raises(RuntimeError, match="lease was lost"):
            split_window(
                conn,
                scan_id=scan_id,
                lease_owner="owner",
                window=window,
            )
        with pytest.raises(RuntimeError, match="lease was lost"):
            complete_window(
                conn,
                scan_id=scan_id,
                lease_owner="owner",
                window=window,
                snapshot_hashes=[],
            )
        run = conn.execute(
            """
            select status, error_message
            from polymarket_wc2026_ops.match_order_book_scan_runs
            where scan_id = ?
            """,
            [scan_id],
        ).fetchone()

    assert run == (
        "failed",
        "RuntimeError: PMXT order-book scan failed",
    )


def test_window_checkpoint_rejects_duplicate_or_malformed_hashes(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
        )
        window = next_pending_window(conn, scan_id)
        assert window is not None
        with pytest.raises(ValueError, match="unique SHA-256"):
            complete_window(
                conn,
                scan_id=scan_id,
                lease_owner="owner",
                window=window,
                snapshot_hashes=["a" * 64, "a" * 64],
            )
        with pytest.raises(ValueError, match="unique SHA-256"):
            complete_window(
                conn,
                scan_id=scan_id,
                lease_owner="owner",
                window=window,
                snapshot_hashes=["not-a-hash"],
            )


def test_publication_rejects_pending_windows_and_missing_progress_scan(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
        )
        with pytest.raises(RuntimeError, match="incomplete windows"):
            publish_scan(conn, scan_id, lease_owner="owner")
        with pytest.raises(RuntimeError, match="not found"):
            scan_progress_summary(conn, "missing")


def test_publication_rejects_invalid_window_checkpoint_and_missing_summary(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
        )
        conn.execute(
            """
            update polymarket_wc2026_ops.match_order_book_scan_windows
            set status = 'loaded', snapshot_count = 0, content_sha256 = null
            where scan_id = ?
            """,
            [scan_id],
        )
        with pytest.raises(RuntimeError, match="invalid window checkpoints"):
            publish_scan(conn, scan_id, lease_owner="owner")
        with pytest.raises(RuntimeError, match="not found"):
            published_scan_summary(conn, scan_id)


def test_publication_rejects_malformed_window_hash_inventory(duck):
    manifest = load_order_book_manifest()
    with duck.get_connection() as conn:
        scan_id, _, _ = acquire_scan(
            conn,
            manifest_version=manifest.version,
            manifest_sha256=manifest.sha256,
            targets=manifest.targets,
            lease_owner="owner",
            force=False,
        )
        conn.execute(
            """
            update polymarket_wc2026_ops.match_order_book_scan_windows
            set status = 'loaded',
                snapshot_count = 1,
                content_sha256 = ?,
                snapshot_hashes_json = '[1]'
            where scan_id = ?
            """,
            ["a" * 64, scan_id],
        )
        with pytest.raises(RuntimeError, match="hash inventory is malformed"):
            publish_scan(conn, scan_id, lease_owner="owner")
