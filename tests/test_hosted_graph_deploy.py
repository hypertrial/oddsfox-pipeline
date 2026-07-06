from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = REPO_ROOT / "deploy" / "hosted-graph" / "docker-compose.yml"


def test_hosted_graph_compose_uses_ssd_bind_mounts():
    compose = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))

    assert "volumes" not in compose

    live = compose["services"]["live"]
    assert "-replay-dir" in live["command"]
    assert "/replay" in live["command"]
    assert {
        "type": "bind",
        "source": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/artifacts",
        "target": "/artifacts",
        "read_only": True,
    } in live["volumes"]
    assert {
        "type": "bind",
        "source": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/replay",
        "target": "/replay",
    } in live["volumes"]

    builder = compose["services"]["artifact-builder"]
    builder_mounts = {mount["target"]: mount["source"] for mount in builder["volumes"]}
    assert builder_mounts == {
        "/artifacts": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/artifacts",
        "/warehouse": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/warehouse",
        "/exports": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/exports",
        "/dagster-home": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/dagster-home",
        "/dlt": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/dlt",
        "/logs": "${ODDSFOX_DATA_DIR:?set ODDSFOX_DATA_DIR}/logs",
    }
    assert (
        builder["environment"]["DUCKDB_PATH"]
        == "${DUCKDB_PATH:-/warehouse/oddsfox.duckdb}"
    )
    assert builder["environment"]["DAGSTER_HOME"] == "${DAGSTER_HOME:-/dagster-home}"
    assert builder["environment"]["DLT_DATA_DIR"] == "${DLT_DATA_DIR:-/dlt}"
