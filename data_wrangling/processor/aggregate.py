import pandas as pd
import polars as pl
from processor import clean
from processor import load_data

#df_pl = clean.clean_df_pl()
# df_pd = clean.clean_df_pd()


def aggregate_pandas(df):
    """This function aggregates by country and calculates mean. min, and max using pandas"""
    df_pd = df
    return df_pd.groupby("Country").agg(
        Quantity=("Quantity", "sum"),
        Mean_TP = ("Total_Price", "mean"),
        Min_TP=("Total_Price", "min"),
        Max_TP=("Total_Price", "max")
    )



def aggregate_polars(df):
    """This function aggregates by country and calculates mean. min, and max using polars"""
    df_pl = df
    return df_pl.group_by("Country").agg([
        pl.sum("Quantity"),
        pl.mean("Total_Price").alias("Mean_TP").round(2),
        pl.min("Total_Price").alias("Minimum_TP"),
        pl.max("Total_Price").alias("Maximum_TP")

    ])

