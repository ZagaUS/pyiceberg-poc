import duckdb
import pandas as pd
import pyarrow.parquet as pq

parquet_file = f'./city_data.parquet'
filter_column = 'State'
states = ['California', 'Michigan', 'New York']
ds = pq.ParquetDataset(parquet_file, filters=[('state','in', states)])
df = ds.read().to_pandas()