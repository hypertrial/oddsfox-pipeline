from pathlib import Path

import pytest

from oddsfox_pipeline.contracts.reference_transport import materialize_reference_bundle


def test_reference_transport_rejects_unapproved_remote_host(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not approved"):
        materialize_reference_bundle(
            "https://example.com/reference/one",
            cache_root=tmp_path,
            approved_hosts=frozenset({"artifacts.example.com"}),
        )
