"""Immutable Parquet segments, manifest commits and bounded single-writer storage."""

from __future__ import annotations

import fcntl
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

CATALOG_CONTRACT = "oddsfox.polymarket.sports-catalog.v1"
BOOK_CONTRACT = "oddsfox.polymarket.sports-order-books.v1"
PARSER_VERSION = "1"
GIB = 1024**3
UTC = timezone.utc
DECIMAL = pa.decimal128(38, 18)
TIME = pa.timestamp("us", tz="UTC")
LEVELS = pa.list_(pa.struct([("price", DECIMAL), ("quantity", DECIMAL)]))

RAW_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("session", pa.string()),
        ("connection", pa.string()),
        ("sequence", pa.int64()),
        ("received_at", TIME),
        ("kind", pa.string()),
        ("payload", pa.binary()),
    ]
)
UPDATE_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("stream", pa.string()),
        ("sequence", pa.int64()),
        ("ordinal", pa.int32()),
        ("received_at", TIME),
        ("source_at", TIME),
        ("token_id", pa.string()),
        ("condition_id", pa.string()),
        ("kind", pa.string()),
        ("side", pa.string()),
        ("price", DECIMAL),
        ("quantity", DECIMAL),
        ("tick_size", DECIMAL),
        ("bids", LEVELS),
        ("asks", LEVELS),
        ("source_hash", pa.string()),
        ("raw_identity", pa.string()),
        ("reason", pa.string()),
    ]
)
STATE_SCHEMA = pa.schema(
    [
        ("source", pa.string()),
        ("stream", pa.string()),
        ("token_id", pa.string()),
        ("sequence", pa.int64()),
        ("ordinal", pa.int32()),
        ("received_at", TIME),
        ("source_at", TIME),
        ("snapshot_at", TIME),
        ("status", pa.string()),
        ("tick_size", DECIMAL),
        ("best_bid", DECIMAL),
        ("best_ask", DECIMAL),
        ("spread", DECIMAL),
        ("midpoint", pa.decimal256(76, 36)),
        ("bid_quantity", pa.decimal256(76, 36)),
        ("ask_quantity", pa.decimal256(76, 36)),
        ("imbalance", pa.float64()),
        ("midpoint_change", pa.decimal256(76, 36)),
        ("displayed_added", DECIMAL),
        ("displayed_removed", DECIMAL),
        ("raw_identity", pa.string()),
    ]
)


def now() -> datetime:
    return datetime.now(UTC)


def json_bytes(value) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), default=str, allow_nan=False
        )
        + "\n"
    ).encode()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def revision() -> str:
    root = Path(__file__).resolve().parents[5]
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=False,
        cwd=root,
    )
    if result.returncode:
        return "unknown"
    changed = subprocess.run(
        ["git", "status", "--porcelain"], cwd=root, capture_output=True, check=False
    )
    return result.stdout.strip() + ("+dirty" if changed.stdout else "")


class BudgetStop(RuntimeError):
    """A resumable stop, never a successful full-coverage acquisition."""


@dataclass
class Limits:
    retained_bytes: int = 100 * GIB
    download_bytes: int = 10 * GIB
    reserve_bytes: int = 50 * GIB

    def __post_init__(self):
        if min(self.retained_bytes, self.download_bytes) <= 0 or self.reserve_bytes < 0:
            raise ValueError("resource limits must be positive; reserve may be zero")


class Store:
    def __init__(self, root: Path, limits: Limits | None = None):
        self.root = root.resolve()
        ancestor = self.root
        while not ancestor.exists():
            ancestor = ancestor.parent
        repository = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=ancestor,
            capture_output=True,
            text=True,
            check=False,
        )
        if (
            repository.returncode == 0
            and subprocess.run(
                ["git", "check-ignore", "--quiet", str(self.root)],
                cwd=ancestor,
                capture_output=True,
                check=False,
            ).returncode
            != 0
        ):
            raise ValueError("managed data root inside a repository must be gitignored")
        self.limits = limits or Limits()
        self.downloaded = 0
        self._reserved = 0
        self._budget_lock = threading.RLock()
        self.root.mkdir(parents=True, exist_ok=True)
        self._known_sizes: dict[Path, int] = {}
        self._used_bytes = 0
        self.usage(force=True)
        self.code_revision = revision()
        package = Path(__file__).resolve().parents[3]
        code_files = sorted(
            list(Path(__file__).parent.glob("*.py"))
            + [
                package / name
                for name in (
                    "ingestion/polymarket/catalog.py",
                    "ingestion/polymarket/match_order_book.py",
                    "resources/http.py",
                    "resources/outbound_url.py",
                    "config/acquisition_ownership.py",
                )
            ]
        )
        digest = hashlib.sha256()
        for file in code_files:
            digest.update(str(file.relative_to(package)).encode())
            digest.update(file.read_bytes())
        self.code_sha256 = digest.hexdigest()
        self.software = {
            name: importlib.metadata.version(name)
            for name in ("pyarrow", "duckdb", "websockets")
        }

    def path(self, relative: str) -> Path:
        path = (self.root / relative).resolve()
        if path == self.root or not path.is_relative_to(self.root):
            raise ValueError("manifest path escapes managed data root")
        return path

    def usage(self, *, force=False) -> int:
        if not force:
            return self._used_bytes
        known_sizes = {}
        total = 0
        for path in self.root.rglob("*"):
            if path.is_symlink():
                raise ValueError("symlinks are not permitted in the managed data root")
            if path.is_file():
                try:
                    size = path.stat().st_size
                    known_sizes[path] = size
                    total += size
                except FileNotFoundError:
                    # Another writer thread may have atomically installed a temp
                    # file while this conservative budget scan was in progress.
                    continue
        with self._budget_lock:
            self._known_sizes = known_sizes
            self._used_bytes = total
        return total

    def track(self, path: Path):
        """Account for one managed file changed by the current writer."""
        path = path.resolve()
        if not path.is_relative_to(self.root):
            raise ValueError("tracked file escapes managed data root")
        with self._budget_lock:
            previous = self._known_sizes.get(path, 0)
            current = path.stat().st_size if path.is_file() else 0
            self._used_bytes += current - previous
            if current:
                self._known_sizes[path] = current
            else:
                self._known_sizes.pop(path, None)

    def check(self, additional: int = 0) -> dict:
        used = self.usage()
        free = shutil.disk_usage(self.root).free
        if used + self._reserved + additional > self.limits.retained_bytes:
            raise BudgetStop("retained-data limit reached; existing evidence retained")
        if free - self._reserved - additional < self.limits.reserve_bytes:
            raise BudgetStop("free-disk reserve reached; existing evidence retained")
        return {
            "retained_bytes": used,
            "free_bytes": free,
            "remaining_retained_bytes": self.limits.retained_bytes
            - used
            - self._reserved,
            "downloaded_bytes": self.downloaded,
            "remaining_download_bytes": self.limits.download_bytes - self.downloaded,
        }

    @contextmanager
    def reserve(self, additional):
        """Reserve concurrent temporary/write headroom conservatively, before I/O."""
        with self._budget_lock:
            self.check(additional)
            self._reserved += additional
        try:
            yield
        finally:
            with self._budget_lock:
                self._reserved -= additional

    @contextmanager
    def sql_connection(self):
        status = self.check()
        available = min(
            status["remaining_retained_bytes"],
            status["free_bytes"] - self.limits.reserve_bytes,
        )
        temporary_limit = min(32 * GIB, available // 3)
        if temporary_limit < 1024**2:
            raise BudgetStop("insufficient managed temporary space for replay")
        temporary_parent = self.path("temporary")
        temporary_parent.mkdir(parents=True, exist_ok=True)
        temporary = temporary_parent / uuid4().hex
        with self.reserve(temporary_limit):
            conn = duckdb.connect()
            try:
                conn.execute("SET memory_limit='4GB'")
                conn.execute("SET TimeZone='UTC'")
                conn.execute("SET threads=1")
                conn.execute("SET preserve_insertion_order=false")
                conn.execute("SET enable_external_file_cache=false")
                conn.execute("SET disable_parquet_prefetching=true")
                conn.execute("SET temp_directory=?", [str(temporary)])
                conn.execute(f"SET max_temp_directory_size='{temporary_limit}B'")
                yield conn
            finally:
                conn.close()
                shutil.rmtree(temporary, ignore_errors=True)
                self.usage(force=True)

    def charge_download(self, size: int):
        self.downloaded += size
        if self.downloaded > self.limits.download_bytes:
            raise BudgetStop("invocation download limit reached")

    @contextmanager
    def writer(self):
        with self.path(".writer.lock").open("a+b") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another writer owns this data root") from exc
            try:
                self.usage(force=True)
                yield self
            finally:
                fcntl.flock(handle, fcntl.LOCK_UN)

    def atomic_json(self, relative: str, value):
        data = json_bytes(value)
        with self.reserve(len(data) + 4096):
            self._atomic_json(relative, data)

    def _atomic_json(self, relative, data):
        path = self.path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_name(path.name + "." + uuid4().hex + ".partial")
        with temp.open("xb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
        self.track(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)

    def describe(self, path: Path) -> dict:
        return {
            "path": str(path.relative_to(self.root)),
            "sha256": sha256(path),
            "bytes": path.stat().st_size,
            "rows": pq.ParquetFile(path).metadata.num_rows,
        }

    def validate_file(self, item: dict) -> Path:
        path = self.path(item["path"])
        if (
            not path.is_file()
            or path.stat().st_size != item["bytes"]
            or sha256(path) != item["sha256"]
        ):
            raise ValueError(f"file checksum/size mismatch: {item['path']}")
        if (
            path.suffix == ".parquet"
            and pq.ParquetFile(path).metadata.num_rows != item["rows"]
        ):
            raise ValueError("Parquet row count mismatch")
        return path

    def manifests(self, kind: str):
        for path in sorted(self.path(f"commits/{kind}").glob("*.json")):
            value = json.loads(path.read_bytes())
            if value.get("contract") != (
                CATALOG_CONTRACT if kind == "catalog" else BOOK_CONTRACT
            ):
                raise ValueError("unknown data contract")
            yield path, value

    def commit(self, kind: str, identity: str, files: list[dict], **metadata) -> dict:
        value = {
            "contract": CATALOG_CONTRACT if kind == "catalog" else BOOK_CONTRACT,
            "parser_version": PARSER_VERSION,
            "code_revision": self.code_revision,
            "implementation_sha256": self.code_sha256,
            "software_versions": self.software,
            "resource_limits": asdict(self.limits),
            "files": files,
            **metadata,
        }
        relative = f"commits/{kind}/{identity}.json"
        path = self.path(relative)
        if path.exists():
            if path.read_bytes() != json_bytes(value):
                raise ValueError(
                    "immutable manifest already exists with different contents"
                )
        else:
            self.atomic_json(relative, value)
        return {"path": relative, "sha256": sha256(path), **value}


class Segments:
    """Bounded Arrow row batches; only a commit makes completed segments readable."""

    def __init__(
        self, store: Store, prefix: str, schema: pa.Schema, batch_size: int = 4096
    ):
        self.store, self.prefix, self.schema = store, prefix, schema
        self.batch_size = batch_size
        self.rows: list[dict] = []
        self.buffer_bytes = 0
        self.files: list[dict] = []

    def append(self, row: dict):
        self.rows.append(row)
        self.buffer_bytes += (
            len(row.get("payload", b""))
            + len(row.get("payload_json") or "")
            + len(row.get("normalized_json") or "")
            + 512
            + 96 * (len(row.get("bids") or []) + len(row.get("asks") or []))
        )
        if len(self.rows) >= self.batch_size or self.buffer_bytes >= 8 * 1024**2:
            self.flush()

    def flush(self):
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        with self.store.reserve(table.nbytes * 2 + 1024**2):
            self._write_table(table)
        self.rows.clear()
        self.buffer_bytes = 0

    def _write_table(self, table):
        path = self.store.path(f"{self.prefix}/{len(self.files):06d}.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(".partial")
        pq.write_table(
            table,
            temp,
            compression="zstd",
            version="2.6",
            use_dictionary=False,
            write_statistics=True,
        )
        with temp.open("rb") as handle:
            os.fsync(handle.fileno())
        if path.exists():
            if sha256(path) != sha256(temp):
                raise ValueError("immutable segment conflict")
            temp.unlink()  # Only our byte-identical temporary copy, not evidence.
        else:
            os.replace(temp, path)
            self.store.track(path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        self.files.append(self.store.describe(path))

    def close(self) -> list[dict]:
        self.flush()
        return self.files


def rows_from_files(store: Store, files: list[dict]):
    for item in files:
        path = store.validate_file(item)
        for batch in pq.ParquetFile(path).iter_batches(batch_size=2048):
            yield from batch.to_pylist()
