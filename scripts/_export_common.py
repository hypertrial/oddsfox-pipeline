from __future__ import annotations

import shutil
from pathlib import Path

import duckdb


def snapshot_duckdb_files(src: Path, dest_dir: Path) -> Path:
    """Copy ``src`` and same-directory siblings (e.g. ``.wal``) for offline use."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    files = sorted(f for f in src.parent.glob(src.name + "*") if f.is_file())
    if not files:
        raise FileNotFoundError(
            f"No DuckDB files matched {src.name!r}* under {src.parent}"
        )
    for f in files:
        shutil.copy2(f, dest_dir / f.name)
    main = dest_dir / src.name
    if not main.is_file():
        raise FileNotFoundError(f"Expected {main} after snapshot copy")
    return main


def qualified_mart_name(schema: str, mart_name: str) -> str:
    from oddsfox_pipeline.storage.duckdb.profile.discovery import qualified_name

    return qualified_name(schema, mart_name)


def mart_exists(conn: duckdb.DuckDBPyConnection, schema: str, mart_name: str) -> bool:
    row = conn.execute(
        """
        select count(*)
        from information_schema.tables
        where table_schema = ?
          and table_name = ?
        """,
        [schema, mart_name],
    ).fetchone()
    return bool(row and row[0])
