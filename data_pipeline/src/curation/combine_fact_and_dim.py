import pandas as pd
import sqlite3
import dagster as dg
from src.transformation.dim_assets import transform_asset_data
from src.transformation.fact_assets import transform_events_data


@dg.asset(
    compute_kind="pandas",
    group_name="curation",
    deps=[transform_events_data, transform_asset_data]
)
def merge_asset_data():
    # Connect to the SQLite database
    database = 'data/events.db'
    curated_zone_db = 'data/curated_zone.db'
    process_zone_table = 'pz_fact_combined'
    
    conn = sqlite3.connect(database)
    cz_conn = sqlite3.connect(curated_zone_db)

    # Load the tables into pandas DataFrames
    pz_assets = pd.read_sql_query("SELECT asset_id, asset_name FROM pz_assets", conn)
    pz_events = pd.read_sql_query("SELECT * FROM pz_events", conn)

    # Ensure 'asset_id' columns are of the same type
    pz_assets['asset_id'] = pz_assets['asset_id'].astype(str)
    pz_events['asset_id'] = pz_events['asset_id'].astype(str)

    # Join the tables on the 'asset_id' column
    combined_df = pd.merge(pz_events, pz_assets, on='asset_id')
    
    # Save the dataframe to a SQL table
    combined_df.to_sql(process_zone_table, cz_conn, if_exists='append', index=False)

    # Close the database connection
    conn.close()
    cz_conn.close()