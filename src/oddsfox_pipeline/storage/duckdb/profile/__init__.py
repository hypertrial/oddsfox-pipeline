"""
DuckDB warehouse profiling: catalog discovery, row counts, and column-level stats.

Read-only by default. Designed for the shared OddsFox warehouse: optional
ingestion schemas plus dbt-owned ``{source}_{stage}`` modeled schemas.
"""

from __future__ import annotations

from .discovery import (
    discover_relations,
    fetch_column_specs,
    qualified_name,
)
from .models import (
    DEFAULT_SCHEMAS,
    ColumnProfile,
    ColumnSpec,
    ColumnStats,
    OutputFormat,
    ProfileConfig,
    RefreshStepResult,
    RelationInfo,
    RelationProfile,
    ReportMetadata,
    StatsLevel,
    WarehouseProfileReport,
)
from .report import (
    build_warehouse_profile_report,
    render_markdown_report,
    stats_present,
)
from .stats import profile_relation

__all__ = [
    "ColumnProfile",
    "ColumnSpec",
    "ColumnStats",
    "DEFAULT_SCHEMAS",
    "OutputFormat",
    "ProfileConfig",
    "RefreshStepResult",
    "RelationInfo",
    "RelationProfile",
    "ReportMetadata",
    "StatsLevel",
    "WarehouseProfileReport",
    "build_warehouse_profile_report",
    "discover_relations",
    "fetch_column_specs",
    "profile_relation",
    "qualified_name",
    "render_markdown_report",
    "stats_present",
]
