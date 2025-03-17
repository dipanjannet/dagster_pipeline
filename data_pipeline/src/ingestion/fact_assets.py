import pandas as pd
import uuid
import numpy as np
import dagster as dg
from datetime import datetime, timedelta


@dg.asset(
    compute_kind="Python",
    group_name="fact_ingeation",
)
def generate_events_data():
    num_rows = 60
    data = {
        'created_ts' : [datetime.now() - timedelta(minutes=i) for i in range(num_rows)],
        'event_id': [str(uuid.uuid4()) for _ in range(num_rows)],
        'asset_id': [np.random.randint(1, 11) for _ in range(num_rows)],
        'value': np.random.randint(1, 51, num_rows)
    }
    
    df = pd.DataFrame(data)
    df.to_csv('data/events.csv', mode='a', index=False)
    return "Data Loaded into a CSV File" 