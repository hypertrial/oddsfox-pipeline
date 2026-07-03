from __future__ import annotations

from typing import Any, Mapping

from dagster_dbt import DagsterDbtTranslator, DagsterDbtTranslatorSettings

from oddsfox_pipeline.orchestration.dbt_project import DBT_DAGSTER_GROUP_NAME
from oddsfox_pipeline.storage.duckdb.schemas.dbt_schemas import dbt_model_asset_key


class PolymarketDagsterDbtTranslator(DagsterDbtTranslator):
    def __init__(self) -> None:
        super().__init__(
            settings=DagsterDbtTranslatorSettings(
                enable_duplicate_source_asset_keys=True,
            )
        )

    def get_asset_key(self, dbt_resource_props):
        if (dbt_resource_props.get("meta") or {}).get("dagster", {}).get("asset_key"):
            return super().get_asset_key(dbt_resource_props)
        return dbt_model_asset_key(dbt_resource_props)

    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return DBT_DAGSTER_GROUP_NAME


__all__ = ["PolymarketDagsterDbtTranslator"]
