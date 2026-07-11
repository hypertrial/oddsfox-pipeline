import pytest

import oddsfox_pipeline.orchestration as orchestration
from oddsfox_pipeline.orchestration.definitions import defs


def test_orchestration_package_exposes_defs_lazily():
    assert orchestration.defs is defs


def test_orchestration_package_rejects_unknown_lazy_attributes():
    with pytest.raises(AttributeError, match="missing"):
        orchestration.__getattr__("missing")
