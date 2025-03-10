import json
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from processor.load_data import polars_loading_time, pandas_loading_time
from processor.clean import clean_df_pl, clean_df_pd
from processor.aggregate import aggregate_pandas, aggregate_polars
import os
import pyarrow as pa
import pyarrow.parquet as pq
import fastparquet as fp
import pandas as pd
import polars as pl

app = FastAPI()

@app.get("/")
def welcome():
    return {"Hello":"World"}



@app.post("/process-data")
def process_data(use_polars: bool = False, url: str = None):
    if use_polars:
        if url != None:
            df, load_time, agg_time = polars_loading_time(sheet_name="Year 2010-2011",url=url)
            df = clean_df_pl(df_pl=df)
            result = aggregate_polars(df=df)
            return {"processed_data": result.to_dicts(), "load_time": load_time, "Aggregate_time": agg_time}

        else:
            df, load_time, agg_time = polars_loading_time(sheet_name="Year 2010-2011")
            df = clean_df_pl(df_pl=df)
            result = aggregate_polars(df=df)
            return {"processed_data": result.to_dicts(), "load_time": load_time, "Aggregate_time": agg_time}
    else:
        if url !=None:
            df, load_time, agg_time = pandas_loading_time(sheet_name="Year 2010-2011", url=url)
            df = clean_df_pd(df_pd=df)
            result = aggregate_pandas(df)
            return {"processed_data": result.to_dict(orient="records"), "load_time": load_time, "Aggregate_time": agg_time}

        else:
            df, load_time, agg_time = pandas_loading_time(sheet_name="Year 2010-2011")
            df = clean_df_pd(df_pd=df)
            result = aggregate_pandas(df)
            return {"processed_data": result.to_dict(orient="records"), "load_time": load_time,"Aggregate_time": agg_time}



PARQUET_FILE_PATH = "processed_data.parquet"
@app.get("/download-json")
def download_json():
    df, _, __ = polars_loading_time( sheet_name="Year 2010-2011")
    df = clean_df_pl(df_pl=df)
    result = aggregate_polars(df)
    result.write_parquet(PARQUET_FILE_PATH)
    with open("processed_data.json", "w") as f:
        json.dump(result.to_dicts(), f)
    return {"message": "File saved as processed_data.json"}


@app.get("/download-parquet")
async def download_parquet():
    """Endpoint to download the processed data in Parquet format."""
    if not os.path.exists(PARQUET_FILE_PATH):
        raise HTTPException(status_code=404, detail="Parquet file not found. Process data first.")
    df, _, __ = polars_loading_time(sheet_name="Year 2010-2011")
    df = clean_df_pl(df_pl=df)
    result = aggregate_polars(df)
    result.write_parquet(PARQUET_FILE_PATH)
    return {"message": "File saved as processed_data.parquet"}
