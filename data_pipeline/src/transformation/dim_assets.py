import sqlite3
import pandas as pd
import dagster as dg
from src.ingestion.dim_assets import generate_asset_data


@dg.asset(
    compute_kind="pandas",
    group_name="dimension_transformation",
    deps=[generate_asset_data]
)
def transform_asset_data():
    
    database = 'data/events.db'
    table_name = 'pz_assets'
    
    df = pd.read_csv("data/asset.csv")
    
    conn = sqlite3.connect(database)
    # Save the dataframe to a SQL table
    df.to_sql(table_name, conn, if_exists='append', index=False)

    # Close the connection
    conn.close()
    return "Loaded asset data to SQL table"