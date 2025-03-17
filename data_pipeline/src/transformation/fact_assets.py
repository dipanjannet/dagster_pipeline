from src.ingestion.fact_assets import generate_events_data
from src.util.utility import is_full_load, get_max_timestamp_from_sqlite_table
import sqlite3
import dagster as dg
import pandas as pd


@dg.asset(
    compute_kind="pandas",
    group_name="fact_transformation",
    deps=[generate_events_data]
)
def transform_events_data():
    database = 'data/events.db'
    table_name = 'pz_events'
    incremental_column = 'created_ts'
    
    full_load = is_full_load(database, table_name)
    dg.get_dagster_logger().info (f"Full Load: {full_load}")
    
    # Read data from the CSV file
    df = pd.read_csv("data/events.csv")
    df['created_ts'] = pd.to_datetime (df['created_ts'])
    df['hourly_partition'] = df['created_ts'].apply(lambda x: x.strftime('%Y-%m-%d-%H:%M'))
    
    if full_load:
        df = df
    else:
        max_ts = get_max_timestamp_from_sqlite_table(database, table_name, incremental_column)
        dg.get_dagster_logger().info (f"Max Timestamp: {max_ts}")

        df['created_ts'] = pd.to_datetime(df['created_ts'])

        df = df [df['created_ts'] > max_ts]

    if df.empty:
        dg.get_dagster_logger().info("DataFrame is empty. No data to load.")
        return "No data to load"
    else:
        dg.get_dagster_logger().info (f"Number of rows to load: {len (df)}")
        # Create a connection to the SQLite database
        conn = sqlite3.connect('data/events.db')

        # Save the dataframe to a SQL table
        df.to_sql(table_name, conn, if_exists='append', index=False)

        # Close the connection

        conn.close()
