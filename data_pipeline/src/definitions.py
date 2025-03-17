import dagster as dg
from src.ingestion.fact_assets import generate_events_data
from src.transformation.fact_assets import transform_events_data
from src.ingestion.dim_assets import generate_asset_data
from src.transformation.dim_assets import transform_asset_data
from src.curation.combine_fact_and_dim import merge_asset_data
from src.transformation.asset_check import asset_health_check

defs = dg.Definitions(
    assets=[generate_events_data,
            transform_events_data,
            generate_asset_data,
            transform_asset_data,
            merge_asset_data],
    asset_checks=[asset_health_check],
)
