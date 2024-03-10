transformimport io
import pandas as pd
import requests
import pyarrow.parquet as pq
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    url = 'https://storage.googleapis.com/nyc-tlc-data-analytics-project/yellow_tripdata_2023-01.parquet'
    response = requests.get(url)
    trips = pq.read_table(io.BytesIO(response.content))
    df = trips.to_pandas()
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
