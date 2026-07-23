from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from oddsfox_pipeline.ingestion.polymarket import polygon_settlement as core
from oddsfox_pipeline.orchestration import assets_polygon_settlement as assets_mod
from oddsfox_pipeline.orchestration.config import (
    PolygonSettlementReleaseConfig,
    PolygonSettlementSyncConfig,
)
from oddsfox_pipeline.publishing import polygon_settlement as publishing


def _connection(value="connection") -> MagicMock:
    connection = MagicMock()
    connection.__enter__.return_value = value
    return connection


def test_polygon_settlement_sync_bridge_passes_only_sanitized_settings(monkeypatch):
    sync = MagicMock(return_value={"scan_id": "scan-1"})
    monkeypatch.setattr(core, "sync_polygon_settlement_fills", sync)
    monkeypatch.setattr(assets_mod, "POLYGON_RPC_URL", "https://rpc.example/key")
    monkeypatch.setattr(assets_mod, "POLYGON_RPC_PROVIDER_LABEL", "provider")
    config = PolygonSettlementSyncConfig(
        requests_per_second=3,
        workers=3,
        initial_block_chunk_size=500,
        initial_receipt_batch_size=10,
        transient_retries=2,
        transient_backoff_seconds=0.25,
    )

    assert assets_mod._sync_polygon_settlement_fills(
        "connection", config, log="log"
    ) == {"scan_id": "scan-1"}
    kwargs = sync.call_args.kwargs
    assert kwargs["rpc_url"] == "https://rpc.example/key"
    assert kwargs["provider_label"] == "provider"
    assert kwargs["log"] == "log"
    assert kwargs["config"].requests_per_second == 3
    assert kwargs["config"].workers == 3
    assert kwargs["config"].initial_block_chunk_size == 500
    assert kwargs["config"].initial_receipt_batch_size == 10
    assert kwargs["config"].no_progress_hard_timeout_seconds == 2_700


def test_polygon_settlement_verification_bridge_is_optional(monkeypatch):
    verify = MagicMock(return_value={"verification_status": "matched"})
    monkeypatch.setattr(core, "verify_polygon_settlement_scan", verify)
    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_URL", "")
    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", "")
    assert assets_mod._verify_polygon_settlement_scan("connection") is None
    verify.assert_not_called()

    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_URL", "https://verify.example")
    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", "secondary")
    assert assets_mod._verify_polygon_settlement_scan("connection") == {
        "verification_status": "matched"
    }
    assert verify.call_args.kwargs["rpc_url"] == "https://verify.example"
    assert verify.call_args.kwargs["provider_label"] == "secondary"


@pytest.mark.parametrize(
    ("rpc_url", "provider_label"),
    [
        ("https://verify.example", ""),
        ("", "secondary"),
    ],
)
def test_polygon_settlement_verification_bridge_routes_partial_configuration(
    monkeypatch, rpc_url, provider_label
):
    verify = MagicMock(
        return_value={
            "scan_id": "scan-1",
            "verification_status": "error",
            "error_type": "VerificationConfigurationError",
        }
    )
    monkeypatch.setattr(core, "verify_polygon_settlement_scan", verify)
    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_URL", rpc_url)
    monkeypatch.setattr(assets_mod, "POLYGON_VERIFY_RPC_PROVIDER_LABEL", provider_label)

    result = assets_mod._verify_polygon_settlement_scan("connection")

    assert result["verification_status"] == "error"
    assert verify.call_args.kwargs["rpc_url"] == rpc_url
    assert verify.call_args.kwargs["provider_label"] == provider_label


def test_polygon_settlement_raw_asset_forwards_fixed_sync_config(monkeypatch):
    connection = _connection()
    sync = MagicMock(return_value={"scan_id": "scan-1", "fill_count": 42})
    monkeypatch.setattr(assets_mod, "get_connection", lambda: connection)
    monkeypatch.setattr(assets_mod, "_sync_polygon_settlement_fills", sync)

    context = MagicMock()
    config = PolygonSettlementSyncConfig(initial_block_chunk_size=1_000)
    result = assets_mod.polymarket_wc2026_raw_polygon_settlement_fills.op.compute_fn.decorated_fn(
        context, config
    )

    sync.assert_called_once_with("connection", config, log=context.log)
    assert result.metadata["scan_id"] == "scan-1"
    assert result.metadata["fill_count"] == 42


def test_polygon_settlement_raw_asset_checks_disposable_path_before_connecting(
    monkeypatch,
):
    calls: list[tuple[str, object]] = []
    connection = _connection()
    monkeypatch.setattr(
        assets_mod,
        "assert_disposable_duckdb_path",
        lambda path: calls.append(("guard", path)),
    )
    monkeypatch.setattr(
        assets_mod,
        "get_connection",
        lambda: calls.append(("connect", None)) or connection,
    )
    monkeypatch.setattr(
        assets_mod,
        "_sync_polygon_settlement_fills",
        lambda *_args, **_kwargs: {"scan_id": "scan-1"},
    )

    config = PolygonSettlementSyncConfig(
        expected_duckdb_path=".cache/polygon-smoke.duckdb"
    )
    assets_mod.polymarket_wc2026_raw_polygon_settlement_fills.op.compute_fn.decorated_fn(
        MagicMock(), config
    )

    assert calls[:2] == [
        ("guard", ".cache/polygon-smoke.duckdb"),
        ("connect", None),
    ]


def test_polygon_settlement_raw_asset_guard_failure_prevents_connection(monkeypatch):
    def reject(_path):
        raise RuntimeError("unsafe warehouse")

    get_connection = MagicMock()
    monkeypatch.setattr(assets_mod, "assert_disposable_duckdb_path", reject)
    monkeypatch.setattr(assets_mod, "get_connection", get_connection)

    config = PolygonSettlementSyncConfig(
        expected_duckdb_path=".cache/polygon-smoke.duckdb"
    )
    with pytest.raises(RuntimeError, match="unsafe warehouse"):
        assets_mod.polymarket_wc2026_raw_polygon_settlement_fills.op.compute_fn.decorated_fn(
            MagicMock(), config
        )
    get_connection.assert_not_called()


def test_polygon_settlement_release_builds_with_advisory_configuration_error(
    monkeypatch, tmp_path
):
    connection = _connection()
    provenance = {"scan_id": "scan-1", "verification_status": "error"}
    verification = {
        "scan_id": "scan-1",
        "verification_status": "error",
        "error_type": "VerificationConfigurationError",
    }
    build = MagicMock(
        return_value={
            "rows": 39_120,
            "release_dir": str(tmp_path / "releases" / "1.0.0"),
        }
    )
    monkeypatch.setattr(assets_mod, "get_connection", lambda: connection)
    monkeypatch.setattr(
        assets_mod, "_verify_polygon_settlement_scan", lambda _conn: verification
    )
    monkeypatch.setattr(
        assets_mod,
        "load_polygon_settlement_release_provenance",
        lambda _conn: provenance,
    )
    monkeypatch.setattr(assets_mod, "build_polygon_settlement_release", build)
    monkeypatch.setattr(assets_mod, "current_generator_commit", lambda: "f" * 40)

    config = PolygonSettlementReleaseConfig(
        dataset_version="1.0.0",
        publisher_name="Publisher",
        attribution_url="https://example.com/data",
        rpc_provider_terms_url="https://provider.example/terms",
        rpc_provider_terms_snapshot_sha256="a" * 64,
        rpc_provider_terms_snapshot_at_utc="2026-07-22T00:00:00Z",
        output_root=str(tmp_path),
    )
    result = assets_mod.polymarket_wc2026_release_polygon_settlement_odds_bundle.op.compute_fn.decorated_fn(
        MagicMock(), config
    )

    args, kwargs = build.call_args
    assert args[:2] == ("connection", tmp_path)
    assert args[2].dataset_version == "1.0.0"
    assert args[2].publisher_name == "Publisher"
    assert args[2].rpc_provider_terms_url == "https://provider.example/terms"
    assert args[2].rpc_provider_terms_snapshot_sha256 == "a" * 64
    assert kwargs == {
        "provenance": provenance,
        "generator_commit": "f" * 40,
    }
    assert result.metadata["rows"] == 39_120
    assert result.metadata["verification"] == verification


def test_polygon_settlement_release_keeps_verification_advisory(monkeypatch, tmp_path):
    connection = _connection()
    context = MagicMock()
    provenance = iter(
        (
            {"scan_id": "scan-1", "verification_status": "matched"},
            {"scan_id": "scan-1", "verification_status": "error"},
        )
    )
    set_verification = MagicMock()
    bundle_issues = []

    def build_bundle(*_args, provenance, **_kwargs):
        _, issues = publishing._reconcile_verification_quality(
            [{"warning_issue_count": 0, "error_issue_count": 0}],
            [],
            {
                **provenance,
                "scan_published_at_utc": "2026-07-22T00:00:00Z",
            },
        )
        bundle_issues.extend(issues)
        return {"rows": 39_120, "release_dir": str(tmp_path)}

    build = MagicMock(side_effect=build_bundle)
    monkeypatch.setattr(assets_mod, "get_connection", lambda: connection)
    monkeypatch.setattr(
        assets_mod,
        "_verify_polygon_settlement_scan",
        MagicMock(side_effect=RuntimeError("provider failed")),
    )
    monkeypatch.setattr(
        assets_mod,
        "load_polygon_settlement_release_provenance",
        lambda _conn: next(provenance),
    )
    monkeypatch.setattr(
        assets_mod,
        "set_polygon_verification_status",
        set_verification,
    )
    monkeypatch.setattr(assets_mod, "build_polygon_settlement_release", build)
    monkeypatch.setattr(assets_mod, "current_generator_commit", lambda: "f" * 40)

    result = assets_mod.polymarket_wc2026_release_polygon_settlement_odds_bundle.op.compute_fn.decorated_fn(
        context,
        PolygonSettlementReleaseConfig(
            dataset_version="1.0.0",
            publisher_name="Publisher",
            output_root=str(tmp_path),
        ),
    )

    assert result.metadata["rows"] == 39_120
    assert result.metadata["verification"] == {
        "scan_id": "scan-1",
        "verification_status": "error",
        "error_type": "RuntimeError",
    }
    set_verification.assert_called_once_with("connection", "scan-1", "error")
    assert build.call_args.kwargs["provenance"] == {
        "scan_id": "scan-1",
        "verification_status": "error",
    }
    assert len(bundle_issues) == 1
    assert bundle_issues[0]["issue_type"] == "verification"
    assert "(error)" in bundle_issues[0]["issue_detail"]
    context.log.warning.assert_called_once()
