import pandas as pd
from pandas import DataFrame

csv = 'accumulated_data_300_million_rows_id_filter.csv'
dataset: DataFrame = pd.read_csv(csv)
dataset.to_parquet('../data/accumulated_data_300_million_rows_id_filter.parquet')
