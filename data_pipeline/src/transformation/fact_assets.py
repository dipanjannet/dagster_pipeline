from src.ingestion.fact_assets import generate_events_data
from src.util.utility import is_full_load_pg, get_max_timestamp_from_sqlite_table
import sqlite3
import dagster as dg
import pandas as pd
from sqlalchemy import create_engine

POSTGRES_CONN_STR = "postgresql://postgres:postgres@postgres:5432/mydatabase"

@dg.asset(
    compute_kind="pandas",
    group_name="fact_transformation",
    deps=[generate_events_data]
)
def transform_events_data(use_postgres=False):
    database = "mydatabase"
    table_name = 'pz_events'
    incremental_column = 'created_ts'
    
    conn_params = {
        "host": "postgres",
        "dbname": database,
        "user": "postgres",
        "password": "postgres",
        "port": 5432
    }
    full_load = is_full_load_pg(conn_params)
    
    dg.get_dagster_logger().info(f"Full Load: {full_load}")
    
    df = pd.read_csv("data/events.csv")
    df['created_ts'] = pd.to_datetime(df['created_ts'], format='mixed')
    df['hourly_partition'] = df['created_ts'].apply(lambda x: x.strftime('%Y-%m-%d-%H:%M'))
    
    # if not full_load:
    #     max_ts = get_max_timestamp_from_sqlite_table(database, table_name, incremental_column)
    #     dg.get_dagster_logger().info(f"Max Timestamp: {max_ts}")
    #     df = df[df['created_ts'] > max_ts]

    # if df.empty:
    #     dg.get_dagster_logger().info("DataFrame is empty. No data to load.")
    #     return "No data to load"
    # else:
    #     dg.get_dagster_logger().info(f"Number of rows to load: {len(df)}")
        
    engine = create_engine(POSTGRES_CONN_STR)
    df.to_sql(table_name, engine, if_exists='append', index=False)
    engine.dispose()

