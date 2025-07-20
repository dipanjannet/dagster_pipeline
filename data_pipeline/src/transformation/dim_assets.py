import pandas as pd
import dagster as dg
from sqlalchemy import create_engine
from src.ingestion.dim_assets import generate_asset_data


@dg.asset(
    compute_kind="pandas",
    group_name="dimension_transformation",
    deps=[generate_asset_data]
)
def transform_asset_data():
    # PostgreSQL connection parameters
    # engine = create_engine('postgresql://postgres:postgres@postgres:5432/mydatabase')
    # Create SQLAlchemy engine for Docker Compose PostgreSQL
    # Connection string: postgresql://<user>:<password>@<host>:<port>/<db>
    # For Docker Compose, host is usually 'localhost' if connecting from host machine,
    # or 'dagster_postgres' if connecting from another container.
    # Here, we use 'localhost' for local development.
    # Already created above:
    engine = create_engine('postgresql://postgres:postgres@postgres:5432/mydatabase')
    
    table_name = 'pz_assets'
    
    df = pd.read_csv("data/asset.csv")
    
    try:
        # Save the dataframe to PostgreSQL table
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"Successfully loaded data to {table_name}")
        return f"Loaded asset data to PostgreSQL table {table_name}"
    except Exception as e:
        print(f"Error loading data: {e}")
        raise e
    finally:
        engine.dispose()