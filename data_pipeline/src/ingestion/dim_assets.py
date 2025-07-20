import pandas as pd
import datetime as datetime
import dagster as dg


@dg.asset(
    compute_kind="Pandas",
    group_name="dimension_ingestion",
)
def generate_asset_data():
    data = [
        {'asset_id': 1, 'asset_name': 'Oil & Gas Exploration & Production', 'asset_type': 'Type 1', 'created_on': datetime.datetime.now()},
        {'asset_id': 2, 'asset_name': 'Automobile Manufacturing', 'asset_type': 'Type 2', 'created_on': datetime.datetime.now()},
        {'asset_id': 3, 'asset_name': 'Steel Manufacturing', 'asset_type': 'Type 3', 'created_on': datetime.datetime.now()},
        {'asset_id': 4, 'asset_name': 'Cement Manufacturing', 'asset_type': 'Type 4', 'created_on': datetime.datetime.now()},
        {'asset_id': 5, 'asset_name': 'Power Generation', 'asset_type': 'Type 5', 'created_on': datetime.datetime.now()},
        {'asset_id': 6, 'asset_name': 'Chemical Manufacturing', 'asset_type': 'Type 6', 'created_on': datetime.datetime.now()},
        {'asset_id': 7, 'asset_name': 'Food Processing', 'asset_type': 'Type 7', 'created_on': datetime.datetime.now()},
        {'asset_id': 8, 'asset_name': 'Textile Manufacturing', 'asset_type': 'Type 8', 'created_on': datetime.datetime.now()},
        {'asset_id': 9, 'asset_name': 'Electronics Manufacturing', 'asset_type': 'Type 9', 'created_on': datetime.datetime.now()},
        {'asset_id': 10, 'asset_name': 'Mining', 'asset_type': 'Type 10', 'created_on': datetime.datetime.now()}
    ]

    # Create the DataFrame
    df = pd.DataFrame(data)

    
    df.to_csv("data/asset.csv", mode='a', index=False)

    return "Data loaded successfully into a CSV file"