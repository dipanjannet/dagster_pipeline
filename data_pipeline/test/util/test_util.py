import sqlite3
import pandas as pd
from src.util.utility import is_full_load, get_max_timestamp_from_sqlite_table

def test_is_full_load_true():
    db_path = "test_db_path"
    table = "full_load_table"
    assert is_full_load(db_path, table) == True


def test_is_full_load_none_table():
    database = 'data/events.db'
    table_name = 'pz_assets'
    assert is_full_load(database, table_name) == True
    


def test_get_max_timestamp_from_sqlite_table():
    db_path = 'data/events.db'
    table = "test_table"
    column = "timestamp"

    # Create an in-memory SQLite database and table
    conn = sqlite3.connect(db_path)
    conn.execute(f"CREATE TABLE IF NOT EXISTS {table} (id INTEGER PRIMARY KEY, {column} TEXT)")
    conn.execute(f"INSERT INTO {table} ({column}) VALUES ('2025-03-18 00:38:00.650368')")
    # conn.execute(f"INSERT INTO {table} ({column}) VALUES ('2025-03-18 00:38:00')")
    conn.commit()
    conn.close()

    # Test the function
    max_timestamp = get_max_timestamp_from_sqlite_table(db_path, table, column)
    assert max_timestamp == pd.Timestamp('2023-01-02 00:00:00')
    