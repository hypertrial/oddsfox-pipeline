"""Central settings barrel for the v0.1.x WC2026 Polymarket pipeline."""

from __future__ import annotations

# Import warehouse first: it loads the repository .env before source modules read it.
# isort: off
import oddsfox_pipeline.config.settings_warehouse as _settings_warehouse
import oddsfox_pipeline.config.settings_kalshi as _settings_kalshi
import oddsfox_pipeline.config.settings_polymarket as _settings_polymarket
from oddsfox_pipeline.config._env import (  # noqa: F401
    _env_bool,
    _env_date,
    _env_float,
    _env_int,
    _optional_env_float,
    _optional_env_int,
    _optional_env_str,
)
from oddsfox_pipeline.config.settings_warehouse import *  # noqa: F403
from oddsfox_pipeline.config.settings_kalshi import *  # noqa: F403
from oddsfox_pipeline.config.settings_polymarket import *  # noqa: F403
# isort: on

__all__ = list(
    dict.fromkeys(
        (
            "_env_bool",
            "_env_date",
            "_env_float",
            "_env_int",
            "_optional_env_float",
            "_optional_env_int",
            "_optional_env_str",
            *_settings_warehouse.__all__,
            *_settings_kalshi.__all__,
            *_settings_polymarket.__all__,
        )
    )
)
