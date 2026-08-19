"""Stable Arrow schema fingerprints shared by Pipeline contracts."""

from __future__ import annotations

import hashlib
import json

import pyarrow as pa


def schema_fingerprint(schema: pa.Schema) -> str:
    fields = [
        {"name": field.name, "type": str(field.type), "nullable": field.nullable}
        for field in schema
    ]
    canonical = json.dumps(fields, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(canonical).hexdigest()


__all__ = ["schema_fingerprint"]
