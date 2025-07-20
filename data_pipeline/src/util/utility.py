import pandas as pd
import os
import psycopg2

def is_full_load_pg(conn_params: dict, table: str) -> bool:
    """
    Checks if the PostgreSQL table exists. Returns True if not, indicating a full load is needed.
    conn_params: dict with keys host, dbname, user, password, port
    """
    conn = psycopg2.connect(**conn_params)
    query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)"
    try:
        result = pd.read_sql_query(query, conn, params=(table,))
        return not result.iloc[0, 0]
    finally:
        conn.close()

def get_max_timestamp_from_postgres_table(conn_params: dict, table: str, column: str) -> pd.Timestamp:
    """
    Returns the maximum timestamp from a PostgreSQL table.
    """
    conn = psycopg2.connect(**conn_params)
    query = f'SELECT MAX("{column}") FROM "{table}"'
    try:
        result = pd.read_sql_query(query, conn)
        max_timestamp = result.iloc[0, 0]
        if pd.isnull(max_timestamp):
            raise ValueError(f"No maximum timestamp found in column '{column}'")
        return pd.Timestamp(max_timestamp)
    finally:
        conn.close()
