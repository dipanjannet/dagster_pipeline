import dagster as dg
import sqlite3
import pandas as pd
from src.transformation.dim_assets import transform_asset_data



@dg.asset_check(asset=transform_asset_data)
def asset_health_check() -> dg.AssetCheckResult:
    
    database = 'data/events.db'
    table_name = 'pz_assets'
    
    conn = sqlite3.connect(database)
    cnt = pd.read_sql_query(f"SELECT count(asset_id) as cnt FROM {table_name}", conn)
    query_result = cnt['cnt'].values[0]

    count = int(query_result) if query_result else 0
    # As we have 10 assets in the pz_assets table, we expect the count to be 10
    return dg.AssetCheckResult(
        passed=count == 10, metadata={"missing dimensions": dg.MetadataValue.int(count)}
    )