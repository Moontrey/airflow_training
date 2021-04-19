import pandas as pd
from volume.get_data import get_data

def test_get_data():
    X, y, df =  get_data('/usr/local/airflow/volume/data_processed.csv')
    assert type(X) is pd.DataFrame
    assert type(y) is pd.DataFrame
    assert type(df) is pd.DataFrame