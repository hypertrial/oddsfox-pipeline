import inspect
import os
import sys
from functools import wraps
from pathlib import Path

import pytest


def _preserve_mutmut_generator_return_values() -> None:
    """Patch Mutmut 3.6's generator wrapper to forward StopIteration.value."""
    if "MUTANT_UNDER_TEST" not in os.environ:
        return

    from mutmut.mutation import trampoline as trampoline_module

    original = trampoline_module.wrap_in_trampoline

    def wrap_in_trampoline(mutants_dict, is_classmethod=False):
        decorate = original(mutants_dict, is_classmethod)

        def preserve_return_value(func):
            wrapped = decorate(func)
            if not inspect.isgeneratorfunction(func):
                return wrapped
            trampoline = inspect.getclosurevars(wrapped).nonlocals["trampoline"]

            @wraps(func)
            def generator_wrapper(*args, **kwargs):
                return (yield from trampoline(*args, **kwargs))

            return generator_wrapper

        return preserve_return_value

    trampoline_module.wrap_in_trampoline = wrap_in_trampoline


def _isolate_xdist_xdg_cache() -> None:
    # ArviZ writes ~/.cache/arviz/daily_warning at import time; under pytest-xdist
    # multiple workers can race on the atomic rename and fail collection.
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if worker:
        os.environ["XDG_CACHE_HOME"] = os.path.join(
            os.environ.get("TMPDIR", "/tmp"),
            f"pytest-xdg-{worker}",
        )


_isolate_xdist_xdg_cache()
_preserve_mutmut_generator_return_values()

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

# Paths define ownership; markers define execution properties.
_INTEGRATION_ROOTS = (
    ROOT / "tests" / "integration",
    ROOT / "tests" / "dagster",
)


def pytest_collection_modifyitems(config, items):
    """Tag heavier suites with ``pytest.mark.integration`` for optional filtering."""
    for item in items:
        try:
            rp = Path(item.path).resolve()
        except (OSError, TypeError, ValueError, AttributeError):
            continue
        for base in _INTEGRATION_ROOTS:
            try:
                rp.relative_to(base)
            except ValueError:
                continue
            item.add_marker(pytest.mark.integration)
            break
