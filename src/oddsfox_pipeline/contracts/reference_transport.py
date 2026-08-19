"""Approved HTTPS transport for immutable Scraper reference bundles."""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import requests

from oddsfox_pipeline.contracts.reference_bundle import validate_reference_bundle
from oddsfox_pipeline.resources.outbound_url import (
    join_under_base,
    validate_outbound_https_url,
)

_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SAFE_FILE = re.compile(r"^[a-z][a-z0-9_]*\.parquet$")
_MAX_FILE_BYTES = 2 * 1024 * 1024 * 1024


def _download(url: str, path: Path) -> None:
    with requests.get(
        url,
        stream=True,
        allow_redirects=False,
        timeout=(15, 120),
    ) as response:
        response.raise_for_status()
        total = 0
        with path.open("xb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                total += len(chunk)
                if total > _MAX_FILE_BYTES:
                    raise ValueError(
                        f"reference payload exceeds {_MAX_FILE_BYTES} bytes"
                    )
                handle.write(chunk)


def materialize_reference_bundle(
    location: str,
    *,
    cache_root: Path,
    approved_hosts: frozenset[str],
) -> Path:
    """Return a local validated bundle from a path or approved HTTPS directory."""
    if "://" not in location:
        path = Path(location).expanduser().resolve()
        validate_reference_bundle(path)
        return path
    raw_base = location.rstrip("/")
    parsed = urlparse(raw_base)
    host = (parsed.hostname or "").casefold()
    if parsed.scheme.casefold() != "https" or not host:
        raise ValueError("artifact URL must be absolute HTTPS")
    if host not in approved_hosts:
        raise ValueError(f"artifact host {host!r} is not approved")
    base = validate_outbound_https_url(raw_base)

    cache_root.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix=".reference.", dir=cache_root))
    try:
        _download(join_under_base(base, "manifest.json"), temporary / "manifest.json")
        manifest = json.loads((temporary / "manifest.json").read_text(encoding="utf-8"))
        bundle_id = manifest.get("bundle_id") if isinstance(manifest, dict) else None
        if not isinstance(bundle_id, str) or not _SAFE_ID.fullmatch(bundle_id):
            raise ValueError("remote reference manifest has an unsafe bundle_id")
        target = cache_root / bundle_id
        if target.exists():
            validate_reference_bundle(target)
            if (target / "manifest.json").read_bytes() != (
                temporary / "manifest.json"
            ).read_bytes():
                raise ValueError("remote immutable bundle ID has changed")
            shutil.rmtree(temporary)
            return target
        tables = manifest.get("tables")
        if not isinstance(tables, list):
            raise ValueError("remote reference manifest has no table inventory")
        names = [entry.get("path") for entry in tables if isinstance(entry, dict)]
        if len(names) != len(tables) or any(
            not isinstance(name, str) or not _SAFE_FILE.fullmatch(name)
            for name in names
        ):
            raise ValueError("remote reference manifest contains unsafe paths")
        for name in ["checksums.sha256", *names]:
            _download(join_under_base(base, name), temporary / name)
        validate_reference_bundle(temporary, expected_bundle_id=bundle_id)
        os.replace(temporary, target)
        return target
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


__all__ = ["materialize_reference_bundle"]
