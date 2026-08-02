"""Recording stand-in for Dagster's DbtCliResource in wiring tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any


class RecordingDbtResource:
    """Capture ``cli`` argv without invoking the dbt executable."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def cli(self, args: list[str], context: Any = None, **_kwargs: Any):
        del context
        self.calls.append(list(args))

        class _Invocation:
            process = SimpleNamespace(returncode=0)

            def stream(self):
                return iter(())

        return _Invocation()
