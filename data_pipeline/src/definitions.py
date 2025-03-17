import dagster as dg
from src.ingestion.fact_assets import generate_events_data
from  src.transformation.fact_assets import transform_events_data

defs = dg.Definitions(
    assets=[generate_events_data,
            transform_events_data],
)
