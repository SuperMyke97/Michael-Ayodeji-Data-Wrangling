import pandas as pd
import polars as pl
# from processor import load_data
# import matplotlib.pyplot as plt
# import seaborn as sns

# df_polars = load_data.polars_loading_time("online_retail_II.xlsx",sheet_name="Year 2010-2011")[0]
# df_pandas = load_data.pandas_loading_time("online_retail_II.xlsx", sheet_name="Year 2010-2011")[0]

def clean_df_pl(df_pl):
    df_polars = df_pl
    # Missing Data
    # missing_data_pct = df.null_count().sum()/len(df) * 100
    # missing_data_pct = missing_data_pct.select([col.round(1) for col in missing_data_pct])
    # # print(missing_data_pct)


    # Since the column with missing values are customer Id and invoice and Description, we will drop null
    df = df_polars.fill_null(0)

    # Removing duplicate rows
    df = df.unique(keep="last")

    # Add column Total_Price by multipying Price of unit item and Quantity .str.contains(r"[\d]+", strict=True)
    df_clean = df.with_columns((pl.col("Quantity") * pl.col("Price")).alias("Total_Price"))


    return df_clean



# Visualizing the missing data
# p = p.to_pandas()
# plt.figure(figsize=(10,5))
# sns.barplot(p)
#
# plt.show()


def clean_df_pd(df_pd):

    df_pandas = df_pd

    # missing_data = (df.isnull().sum()/len(df) * 100).round(2)

    df = df_pandas.dropna()

    df_clean_pd = df.drop_duplicates(keep="last")

    df_clean_pd = df_clean_pd.assign(Total_Price = (df_clean_pd["Quantity"] * df_clean_pd["Price"]))

    return df_clean_pd

