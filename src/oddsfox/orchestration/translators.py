from __future__ import annotations

from typing import Any, Mapping

from dagster_dbt import DagsterDbtTranslator

from oddsfox.orchestration.dbt_project import DBT_DAGSTER_GROUP_NAME
from oddsfox.storage.duckdb.schemas.dbt_schemas import dbt_model_asset_key


class PolymarketDagsterDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props):
        return dbt_model_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return DBT_DAGSTER_GROUP_NAME


__all__ = ["PolymarketDagsterDbtTranslator"]
