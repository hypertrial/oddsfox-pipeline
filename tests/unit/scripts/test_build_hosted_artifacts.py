"""Tests for scripts/build_hosted_artifacts.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import duckdb
import pytest


def _load_builder_module():
    scripts_dir = Path(__file__).resolve().parents[3] / "scripts"
    sys.path.insert(0, str(scripts_dir))
    import build_hosted_artifacts

    return build_hosted_artifacts


def _write_parquet(path: Path, *, rows: int = 1) -> None:
    with duckdb.connect() as connection:
        connection.sql("SELECT range AS id FROM range(?)", params=[rows]).write_parquet(
            str(path)
        )


def _write_input_bundle(path: Path, *, pipeline_sha: str = "a" * 40) -> None:
    builder = _load_builder_module()
    path.mkdir(parents=True)
    (path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": builder.INPUT_CONTRACT,
                "pipeline_git_sha": pipeline_sha,
                "temporal_odds": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    for name in builder.REQUIRED_INPUT_FILES:
        if name != "manifest.json":
            _write_parquet(path / name)


def _write_graph(
    path: Path,
    *,
    markets: int = 1,
    edges: int = 1,
    input_manifest_sha256: str = "0" * 64,
    graph_sha: str = "b" * 40,
    graph_mode: str = "fast",
) -> None:
    builder = _load_builder_module()
    path.mkdir(parents=True)
    (path / "build_manifest.json").write_text(
        json.dumps(
            {
                "build_mode": graph_mode,
                "graph_git_sha": graph_sha,
                "graph_worktree_dirty": False,
                "input": {
                    "profile": builder.INPUT_CONTRACT,
                    "schema": builder.INPUT_CONTRACT,
                    "manifest_sha256": input_manifest_sha256,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    for name in ("viewer_manifest.json", "coverage_summary.json"):
        (path / name).write_text("{}\n", encoding="utf-8")
    with duckdb.connect(str(path / "oddsfox_graph.duckdb")) as connection:
        connection.execute("CREATE TABLE events AS SELECT 'event-1' AS event_id")
        connection.execute(
            "CREATE TABLE markets AS SELECT 'market-' || range AS market_id "
            "FROM range(?)",
            [markets],
        )
        connection.execute(
            "CREATE TABLE market_events AS "
            "SELECT 'event-1' AS event_id, market_id FROM markets"
        )
        connection.execute("CREATE TABLE market_summary_v AS SELECT * FROM markets")
    for name in builder.REQUIRED_GRAPH_FILES:
        if name.endswith(".parquet"):
            _write_parquet(
                path / name, rows=edges if name == "market_edges.parquet" else 1
            )


def _write_release(path: Path, *, markets: int = 1, edges: int = 1) -> None:
    builder = _load_builder_module()
    bundle = path / builder.INPUT_BUNDLE_RELATIVE
    _write_input_bundle(bundle)
    graph = path / builder.GRAPH_RELATIVE
    input_manifest_sha256 = builder.sha256_file(bundle / "manifest.json")
    _write_graph(
        graph,
        markets=markets,
        edges=edges,
        input_manifest_sha256=input_manifest_sha256,
    )
    (path / builder.RELEASE_MANIFEST_NAME).write_text(
        json.dumps(
            {
                "schema": "oddsfox-local-atlas-release-v1",
                "input_contract": builder.INPUT_CONTRACT,
                "pipeline_git_sha": "a" * 40,
                "graph_git_sha": "b" * 40,
                "graph_mode": "fast",
                "temporal_odds": False,
                "input_manifest_sha256": input_manifest_sha256,
                "graph_manifest_sha256": builder.sha256_file(
                    graph / "build_manifest.json"
                ),
                "input_files": builder.file_hashes(bundle),
                "graph_files": builder.file_hashes(graph),
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _args(tmp_path: Path, **overrides):
    values = {
        "pipeline_python": Path("/usr/bin/python3"),
        "duckdb_path": tmp_path / "warehouse.duckdb",
        "artifact_dir": tmp_path / "artifacts",
        "graph_repo": tmp_path / "graph",
        "graph_python": None,
        "pipeline_git_sha": "",
        "graph_git_sha": "",
        "graph_mode": "fast",
        "graph_cache_dir": None,
        "graph_compute_profile": None,
        "graph_automation_profile": None,
        "graph_primary_model_manifest": None,
        "graph_verifier_model_manifest": None,
        "graph_primary_base_url": "http://127.0.0.1:8080/v1",
        "graph_verifier_base_url": "http://127.0.0.1:8081/v1",
        "skip_refresh": True,
        "skip_dbt": True,
        "input_bundle": tmp_path / "fixture",
        "allow_empty_graph": False,
        "activate_release": "",
        "validate_release": "",
        "no_activate": False,
        "interval_seconds": 3600,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_validate_release_requires_non_empty_graph(tmp_path: Path) -> None:
    builder = _load_builder_module()
    release = tmp_path / "release"
    _write_release(release, markets=1, edges=0)

    with pytest.raises(RuntimeError, match="no events, markets, or logical edges"):
        builder.validate_release(release, allow_empty_graph=False)

    builder.validate_release(release, allow_empty_graph=True)


def test_activate_current_repoints_symlink(tmp_path: Path) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    _write_release(artifact_dir / "releases" / "old")
    _write_release(artifact_dir / "releases" / "new")

    builder.activate_current(artifact_dir, "old")
    assert (artifact_dir / "current" / builder.RELEASE_MANIFEST_NAME).is_file()

    builder.activate_current(artifact_dir, "new")
    assert (artifact_dir / "current").resolve() == (
        artifact_dir / "releases" / "new"
    ).resolve()
    assert (artifact_dir / builder.PREVIOUS_LINK_NAME).resolve() == (
        artifact_dir / "releases" / "old"
    ).resolve()


@pytest.mark.parametrize("release_id", ("../escape", "/absolute", ".", "bad/id"))
def test_activate_current_rejects_unsafe_release_id(
    tmp_path: Path, release_id: str
) -> None:
    builder = _load_builder_module()

    with pytest.raises(ValueError, match="invalid release ID"):
        builder.activate_current(tmp_path / "artifacts", release_id)


def test_run_forever_carries_fixture_input_bundle(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = _args(tmp_path)

    with (
        patch.object(builder.subprocess, "run") as run,
        patch.object(builder.time, "sleep", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        builder.run_forever(args)

    command = run.call_args.args[0]
    assert "--input-bundle" in command
    assert str(args.input_bundle) in command
    assert str(args.duckdb_path) in command
    assert "--graph-mode" in command
    assert "--input-parquet" not in command
    assert command.count("--graph-repo") == 1


def test_build_graph_invokes_logical_discovery(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = _args(
        tmp_path,
        graph_python=Path("/graph/python"),
        graph_repo=tmp_path / "graph-repo",
    )
    input_bundle = tmp_path / "bundle"
    graph_dir = tmp_path / "out"

    with patch.object(builder.subprocess, "run") as run:
        builder.build_graph(args, input_bundle, graph_dir)

    command = run.call_args.args[0]
    assert command[:5] == [
        "/graph/python",
        "-m",
        "oddsfox_graph.cli",
        "discover",
        "--mode",
    ]
    assert "polymarket-wc2026-logical-v1" in command
    assert str(input_bundle) in command
    assert str(graph_dir) in command


def test_validate_graph_acceptance_invokes_graph_owned_gate(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = _args(
        tmp_path,
        graph_python=Path("/graph/python"),
        graph_repo=tmp_path / "graph-repo",
    )
    completed = SimpleNamespace(
        stdout=json.dumps(
            {
                "schema_version": "wc2026-atlas-acceptance-v1",
                "passed": True,
            }
        )
    )
    with patch.object(builder.subprocess, "run", return_value=completed) as run:
        report = builder.validate_graph_acceptance(
            args,
            tmp_path / "bundle",
            tmp_path / "graph",
        )

    assert report["passed"] is True
    assert run.call_args.args[0] == [
        "/graph/python",
        "-m",
        "oddsfox_graph.cli",
        "atlas-validate",
        "--bundle-dir",
        str(tmp_path / "bundle"),
        "--graph-dir",
        str(tmp_path / "graph"),
        "--output-format",
        "json",
    ]
    assert run.call_args.kwargs["check"] is True
    assert run.call_args.kwargs["capture_output"] is True


@pytest.mark.parametrize(
    "stdout",
    (
        "not-json",
        json.dumps({"schema_version": "wrong", "passed": True}),
        json.dumps(
            {
                "schema_version": "wc2026-atlas-acceptance-v1",
                "passed": False,
            }
        ),
    ),
)
def test_validate_graph_acceptance_fails_closed_on_bad_report(
    tmp_path: Path, stdout: str
) -> None:
    builder = _load_builder_module()
    args = _args(tmp_path, graph_python=Path("/graph/python"))
    with (
        patch.object(
            builder.subprocess,
            "run",
            return_value=SimpleNamespace(stdout=stdout),
        ),
        pytest.raises(RuntimeError, match="Graph atlas acceptance"),
    ):
        builder.validate_graph_acceptance(
            args,
            tmp_path / "bundle",
            tmp_path / "graph",
        )


def test_validate_browser_smoke_receipt_invokes_graph_owned_validator(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    args = _args(
        tmp_path,
        graph_python=Path("/graph/python"),
        graph_repo=tmp_path / "graph-repo",
    )
    artifact_dir = tmp_path / "artifacts"
    release_dir = artifact_dir / "releases" / "release-1"
    (release_dir / builder.INPUT_BUNDLE_RELATIVE).mkdir(parents=True)
    (release_dir / builder.GRAPH_RELATIVE).mkdir(parents=True)
    receipt = builder.browser_smoke_receipt_path(artifact_dir, "release-1")
    receipt.parent.mkdir(parents=True)
    receipt.write_text("{}\n", encoding="utf-8")
    completed = SimpleNamespace(
        stdout=json.dumps(
            {
                "schema_version": builder.BROWSER_SMOKE_RECEIPT_SCHEMA,
                "passed": True,
                "validated": True,
            }
        )
    )

    with patch.object(builder.subprocess, "run", return_value=completed) as run:
        report = builder.validate_browser_smoke_receipt(
            args,
            artifact_dir,
            "release-1",
            release_dir,
        )

    assert report is not None and report["validated"] is True
    assert run.call_args.args[0] == [
        "/graph/python",
        "-m",
        "oddsfox_graph.cli",
        "atlas-browser-receipt-validate",
        "--graph-dir",
        str(release_dir / builder.GRAPH_RELATIVE),
        "--receipt",
        str(receipt),
        "--output-format",
        "json",
    ]
    assert run.call_args.kwargs["check"] is True


def test_validate_browser_smoke_receipt_rejects_missing_receipt(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    args = _args(tmp_path)
    artifact_dir = tmp_path / "artifacts"
    release_dir = artifact_dir / "releases" / "release-1"
    (release_dir / builder.INPUT_BUNDLE_RELATIVE).mkdir(parents=True)

    with pytest.raises(RuntimeError, match="requires a browser-smoke receipt"):
        builder.validate_browser_smoke_receipt(
            args,
            artifact_dir,
            "release-1",
            release_dir,
        )


def test_prepare_graph_input_copies_complete_bundle(tmp_path: Path) -> None:
    builder = _load_builder_module()
    source = tmp_path / "source"
    _write_input_bundle(source)
    args = _args(tmp_path, input_bundle=source)

    output = builder.prepare_graph_input(args, tmp_path / "release")

    assert output == tmp_path / "release" / builder.INPUT_BUNDLE_RELATIVE
    assert set(path.name for path in output.iterdir()) == set(
        builder.REQUIRED_INPUT_FILES
    )


def test_prepare_graph_input_exports_configured_warehouse(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = _args(tmp_path, input_bundle=None)
    output = tmp_path / "release" / builder.INPUT_BUNDLE_RELATIVE

    def export(command: list[str], **_kwargs) -> None:
        assert command[command.index("--duckdb-path") + 1] == str(args.duckdb_path)
        _write_input_bundle(output)

    with patch.object(builder.subprocess, "run", side_effect=export):
        assert builder.prepare_graph_input(args, tmp_path / "release") == output


def test_write_release_manifest_binds_both_revisions(tmp_path: Path) -> None:
    builder = _load_builder_module()
    bundle = tmp_path / "bundle"
    graph = tmp_path / "graph"
    release = tmp_path / "release"
    release.mkdir()
    _write_input_bundle(bundle)
    _write_graph(
        graph,
        input_manifest_sha256=builder.sha256_file(bundle / "manifest.json"),
    )
    args = _args(
        tmp_path,
        pipeline_git_sha="a" * 40,
        graph_git_sha="b" * 40,
    )

    with patch.object(
        builder,
        "release_git_sha",
        side_effect=("a" * 40, "b" * 40),
    ):
        builder.write_release_manifest(args, bundle, graph, release)

    payload = json.loads(
        (release / builder.RELEASE_MANIFEST_NAME).read_text(encoding="utf-8")
    )
    assert payload["pipeline_git_sha"] == "a" * 40
    assert payload["graph_git_sha"] == "b" * 40
    assert payload["input_contract"] == "polymarket-wc2026-logical-v1"
    assert payload["input_files"] == builder.file_hashes(bundle)
    assert payload["graph_files"] == builder.file_hashes(graph)


def test_write_release_manifest_rejects_invalid_revision(tmp_path: Path) -> None:
    builder = _load_builder_module()
    bundle = tmp_path / "bundle"
    graph = tmp_path / "graph"
    release = tmp_path / "release"
    bundle.mkdir()
    graph.mkdir()
    release.mkdir()
    (bundle / "manifest.json").write_text("{}\n", encoding="utf-8")
    (graph / "build_manifest.json").write_text("{}\n", encoding="utf-8")
    args = _args(
        tmp_path,
        pipeline_git_sha="not-a-sha",
        graph_git_sha="b" * 40,
    )

    with pytest.raises(RuntimeError, match="invalid Pipeline Git SHA"):
        builder.write_release_manifest(args, bundle, graph, release)


def test_write_release_manifest_rejects_input_pipeline_revision_drift(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    bundle = tmp_path / "bundle"
    graph = tmp_path / "graph"
    release = tmp_path / "release"
    release.mkdir()
    _write_input_bundle(bundle, pipeline_sha="c" * 40)
    _write_graph(
        graph,
        input_manifest_sha256=builder.sha256_file(bundle / "manifest.json"),
    )
    args = _args(
        tmp_path,
        pipeline_git_sha="a" * 40,
        graph_git_sha="b" * 40,
    )

    with (
        patch.object(
            builder,
            "release_git_sha",
            side_effect=("a" * 40, "b" * 40),
        ),
        pytest.raises(RuntimeError, match="Pipeline SHA does not match"),
    ):
        builder.write_release_manifest(args, bundle, graph, release)


def test_validate_release_rejects_dirty_graph_attestation(tmp_path: Path) -> None:
    builder = _load_builder_module()
    release = tmp_path / "release"
    _write_release(release)
    graph_manifest_path = release / builder.GRAPH_RELATIVE / "build_manifest.json"
    graph_manifest = json.loads(graph_manifest_path.read_text(encoding="utf-8"))
    graph_manifest["graph_worktree_dirty"] = True
    graph_manifest_path.write_text(
        json.dumps(graph_manifest) + "\n",
        encoding="utf-8",
    )
    release_manifest_path = release / builder.RELEASE_MANIFEST_NAME
    release_manifest = json.loads(release_manifest_path.read_text(encoding="utf-8"))
    release_manifest["graph_manifest_sha256"] = builder.sha256_file(graph_manifest_path)
    release_manifest["graph_files"] = builder.file_hashes(
        release / builder.GRAPH_RELATIVE
    )
    release_manifest_path.write_text(
        json.dumps(release_manifest) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="clean worktree"):
        builder.validate_release(release, allow_empty_graph=False)


def test_release_git_sha_rejects_dirty_or_mismatched_checkout(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    repo = tmp_path / "repo"
    (repo / ".git").mkdir(parents=True)

    with (
        patch.object(builder, "git_sha", return_value="a" * 40),
        patch.object(
            builder.subprocess,
            "run",
            return_value=SimpleNamespace(stdout=" M changed.py\n"),
        ),
        pytest.raises(RuntimeError, match="worktree must be clean"),
    ):
        builder.release_git_sha(repo, "a" * 40, label="Fixture")

    with (
        patch.object(builder, "git_sha", return_value="b" * 40),
        pytest.raises(RuntimeError, match="does not match checkout HEAD"),
    ):
        builder.release_git_sha(repo, "a" * 40, label="Fixture")


def test_validate_release_rejects_tampered_artifact(tmp_path: Path) -> None:
    builder = _load_builder_module()
    release = tmp_path / "release"
    _write_release(release)
    market_edges = release / builder.GRAPH_RELATIVE / "market_edges.parquet"
    market_edges.write_bytes(market_edges.read_bytes() + b"tampered")

    with pytest.raises(RuntimeError, match="graph output hash inventory mismatch"):
        builder.validate_release(release, allow_empty_graph=False)


def test_run_refresh_uses_fixed_dagster_command(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = SimpleNamespace(
        pipeline_python=Path("/usr/bin/python3"),
        duckdb_path=tmp_path / "warehouse.duckdb",
        skip_refresh=False,
    )

    with patch.object(builder.subprocess, "run") as run:
        builder.run_refresh(args)

    assert run.call_args.kwargs.get("shell") is not True
    assert run.call_args.kwargs["cwd"] == builder.REPO_ROOT
    assert run.call_args.kwargs["env"]["DUCKDB_PATH"] == str(args.duckdb_path.resolve())
    command = run.call_args.args[0]
    assert command == [
        "/usr/bin/python3",
        "-m",
        "dagster",
        "job",
        "execute",
        "-m",
        "oddsfox_pipeline.orchestration.definitions",
        "-j",
        "polymarket_wc2026_logical_atlas",
    ]


def test_run_dbt_is_scoped_to_logical_atlas(tmp_path: Path) -> None:
    builder = _load_builder_module()
    args = SimpleNamespace(
        pipeline_python=Path("/usr/bin/python3"),
        duckdb_path=tmp_path / "warehouse.duckdb",
        skip_dbt=False,
    )

    with patch.object(builder.subprocess, "run") as run:
        builder.run_dbt(args)

    command = run.call_args.args[0]
    assert command[command.index("--select") + 1] == "+tag:wc2026_logical_atlas"
    assert command[command.index("--exclude") + 1 :] == [
        "tag:polygon_settlement",
        "tag:pmxt_order_book",
    ]


def test_parse_args_reads_container_revision_bindings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    builder = _load_builder_module()
    monkeypatch.setenv("ODDSFOX_PIPELINE_GIT_SHA", "a" * 40)
    monkeypatch.setenv("ODDSFOX_GRAPH_GIT_SHA", "b" * 40)

    args = builder.parse_args(["--artifact-dir", str(tmp_path)])

    assert args.pipeline_git_sha == "a" * 40
    assert args.graph_git_sha == "b" * 40


def test_main_activates_only_after_revalidating_existing_release(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    release = artifact_dir / "releases" / "shadow-1"
    _write_release(release)
    order: list[str] = []
    activate_current_locked = builder._activate_current_locked

    def publish(*args, **kwargs):
        order.append("publish")
        return activate_current_locked(*args, **kwargs)

    with (
        patch.object(
            builder,
            "validate_graph_acceptance_for_release",
            side_effect=lambda *_args: order.append("graph") or {"passed": True},
        ),
        patch.object(
            builder,
            "validate_browser_smoke_receipt",
            side_effect=lambda *_args: (
                order.append("browser") or {"passed": True, "validated": True}
            ),
        ),
        patch.object(builder, "_activate_current_locked", side_effect=publish),
    ):
        assert (
            builder.main(
                [
                    "--artifact-dir",
                    str(artifact_dir),
                    "--activate-release",
                    "shadow-1",
                ]
            )
            == 0
        )

    assert (artifact_dir / "current").resolve() == release.resolve()
    assert order == ["graph", "browser", "publish"]


def test_validate_release_mode_never_repoints_current(tmp_path: Path) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    first = artifact_dir / "releases" / "first"
    second = artifact_dir / "releases" / "second"
    _write_release(first)
    _write_release(second)
    builder.activate_current(artifact_dir, "first")

    with patch.object(
        builder,
        "validate_graph_acceptance_for_release",
        return_value={"passed": True},
    ):
        assert (
            builder.main(
                [
                    "--artifact-dir",
                    str(artifact_dir),
                    "--validate-release",
                    "second",
                ]
            )
            == 0
        )
    assert (artifact_dir / "current").resolve() == first.resolve()


def test_main_can_build_shadow_without_repointing_current(tmp_path: Path) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    fixture = tmp_path / "fixture"
    _write_input_bundle(fixture)

    def build_graph(_args, _input_bundle: Path, graph_dir: Path) -> None:
        _write_graph(
            graph_dir,
            input_manifest_sha256=builder.sha256_file(_input_bundle / "manifest.json"),
        )

    with (
        patch.object(builder, "build_graph", side_effect=build_graph),
        patch.object(
            builder,
            "validate_graph_acceptance",
            return_value={"passed": True},
        ),
        patch.object(
            builder,
            "release_git_sha",
            side_effect=("a" * 40, "b" * 40, "a" * 40, "b" * 40),
        ),
    ):
        assert (
            builder.main(
                [
                    "--artifact-dir",
                    str(artifact_dir),
                    "--release-id",
                    "shadow-1",
                    "--skip-refresh",
                    "--skip-dbt",
                    "--input-bundle",
                    str(fixture),
                    "--pipeline-git-sha",
                    "a" * 40,
                    "--graph-git-sha",
                    "b" * 40,
                ]
            )
            == 0
        )

    assert (artifact_dir / "releases" / "shadow-1").is_dir()
    assert not (artifact_dir / "current").exists()


def test_main_activation_fails_closed_without_browser_smoke_receipt(
    tmp_path: Path,
) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"
    release = artifact_dir / "releases" / "shadow-1"
    _write_release(release)

    with (
        patch.object(
            builder,
            "validate_graph_acceptance_for_release",
            return_value={"passed": True},
        ),
        pytest.raises(RuntimeError, match="requires a browser-smoke receipt"),
    ):
        builder.main(
            [
                "--artifact-dir",
                str(artifact_dir),
                "--activate-release",
                "shadow-1",
            ]
        )

    assert not (artifact_dir / "current").exists()


def test_main_rejects_unbound_revision_before_refresh(tmp_path: Path) -> None:
    builder = _load_builder_module()
    artifact_dir = tmp_path / "artifacts"

    with (
        patch.object(
            builder,
            "preflight_release_revisions",
            side_effect=RuntimeError("dirty checkout"),
        ),
        patch.object(builder, "run_refresh") as refresh,
        pytest.raises(RuntimeError, match="dirty checkout"),
    ):
        builder.main(["--artifact-dir", str(artifact_dir)])

    refresh.assert_not_called()
    assert not (artifact_dir / "releases").exists()


def test_parse_args_defaults_duckdb_path_to_settings(
    tmp_path: Path, monkeypatch
) -> None:
    builder = _load_builder_module()
    root = tmp_path / "pipeline-root"
    root.mkdir()
    monkeypatch.setenv("ODDSFOX_PIPELINE_ROOT", str(root))
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    monkeypatch.setenv("DUCKDB_NAME", "custom.duckdb")

    from oddsfox_pipeline.config._reload_settings import reload_all_settings_modules

    settings = reload_all_settings_modules()
    args = builder.parse_args([])

    assert args.duckdb_path.resolve() == settings.DUCKDB_PATH.resolve()
    assert args.duckdb_path == (root / "custom.duckdb").resolve()
