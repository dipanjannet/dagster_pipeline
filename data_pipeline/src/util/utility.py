import pandas as pd
import os
import sqlite3


def is_full_load(db_path:str, table:str) -> bool:
    if not os.path.isfile(db_path):
        return True
    
    conn = sqlite3.connect(db_path)
    query = f"SELECT name FROM sqlite_master WHERE type = 'table' AND name= '{table}'"
    
    try:
        result = pd.read_sql_query(query, conn)
        return result.empty
    finally:
        conn.close()
        
def get_max_timestamp_from_sqlite_table(db_path: str, table: str, column: str) -> pd.Timestamp:
    """
    This function returns the maximum timestamp from a SQLite table.

    Args:
        db_path (str): Database file path
        table (str): Table name
        column (str): column name

    Raises:
        ValueError: Raised when no maximum timestamp is found in the column

    Returns:
        pd.Timestamp: Returns the maximum timestamp
    """
    
    conn = sqlite3.connect(db_path)
    query = f"SELECT MAX('{column}') FROM '{table}'"
    
    try:
        result = pd.read_sql_query(query, conn)
        max_timestamp = result.iloc[0, 0]
        
        if pd.isnull(max_timestamp):
            raise ValueError(f"No maximum timestamp found in column '{column}'")

        return pd.Timestamp(max_timestamp)

    finally:
        conn.close()