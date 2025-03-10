import requests
import time

import polars as pl
import pandas as pd

from zipfile import ZipFile
from io import BytesIO


def datapath(url: str = None):

    if url != None:
        URL = url

        # Download the ZIP file
        response = requests.get(URL)
        # print(response.status_code)

        with ZipFile(BytesIO(response.content)) as z:
            file_list = z.namelist()
            excel_filename = next(f for f in file_list if f.endswith('.xlsx'))
            with z.open(excel_filename) as f:
                excel_bytes = f.read()

        EXCEL_FILE_URL = BytesIO(excel_bytes)
        return EXCEL_FILE_URL

    else:
        EXCEL_FILE_LOCALPATH = "online_retail_II.xlsx"
        return EXCEL_FILE_LOCALPATH


def polars_loading_time(sheet_name, url: str = None):
    if url != None:
        data_path = datapath(url)
    else:
        data_path = datapath()

    # Record start time for data loading
    start_data_load_time = time.time()

    # Load data
    df_polars = pl.read_excel(data_path, sheet_name=sheet_name)

    # Record end time for data loading
    end_data_load_time = time.time()

    # Calculate data loading time
    data_loading_time = end_data_load_time - start_data_load_time

    # Record start time for aggregation
    start_aggregation_time = time.time()

    # Perform aggregation (e.g., group by and sum)
    aggregated_data = df_polars.group_by("Country").agg(
        pl.sum("Price").alias("Country Total Price"),
        pl.count("Customer ID").alias("Country Total IDs")

        )

    # Record end time for aggregation
    end_aggregation_time = time.time()

    # Calculate aggregation time
    aggregation_time = end_aggregation_time - start_aggregation_time

    # Return results
    return df_polars, data_loading_time, aggregation_time


# Example usage
# df_polars_url, pl_data_loading_time_url, pl_aggregation_time_url = polars_loading_time(sheet_name="Year 2010-2011")
# print(df_polars_url.head(10))
# df_polars_local, pl_data_loading_time_local, pl_aggregation_time_local = polars_loading_time(
#     data_path=EXCEL_FILE_LOCALPATH,
#     sheet_name="Year 2010-2011")

# print(f'Polars Data loading time for url: {pl_data_loading_time_url:.2f} seconds')
# print(f'Polars Aggregation time for url: {plaggregation_time_url:.2f} seconds')
# print(f'Polars loading time for local: {pldata_loading_time_local:.2f} seconds')
# print(f'Polars Aggregation time for local: {plaggregation_time_local:.2f} seconds')


def pandas_loading_time(sheet_name, url: str = None):
    if url != None:
        data_path = datapath(url)
    else:
        data_path = datapath()

    # Record start time for data loading
    start_data_load_time = time.time()

    # Load data
    df_pandas = pd.read_excel(data_path, sheet_name)

    # Record end time for data loading
    end_data_load_time = time.time()

    data_loading_time = end_data_load_time - start_data_load_time

    # Record start time for aggregation
    start_aggregation_time = time.time()

    # Perform aggregation (e.g., group by and sum)
    aggregated_data = df_pandas.groupby("Country").agg(
        {"Price": ["sum", "mean"], "Customer ID": ["count"]}

        )

    # Record end time for aggregation
    end_aggregation_time = time.time()

    # Calculate aggregation time
    aggregation_time = end_aggregation_time - start_aggregation_time

    # Return results
    return df_pandas, data_loading_time, aggregation_time


# Example usage
# df_pandas_url, pddata_loading_time_url, pdaggregation_time_url = pandas_loading_time(data_path=EXCEL_FILE_URL, sheet_name="Year 2010-2011")
# df_pandas_local, pddata_loading_time_local, pdaggregation_time_local = pandas_loading_time(data_path=EXCEL_FILE_LOCALPATH, sheet_name="Year 2010-2011")


# print(f'Pandas Data loading time for url: {pddata_loading_time_url:.2f} seconds')
# print(f'Pandas Aggregation time for url: {pdaggregation_time_url:.2f} seconds')
# print(f'Pandas Data loading time for local: {pddata_loading_time_local:.2f} seconds')
# print(f'Pandas Aggregation time for local: {pdaggregation_time_local:.2f} seconds')
