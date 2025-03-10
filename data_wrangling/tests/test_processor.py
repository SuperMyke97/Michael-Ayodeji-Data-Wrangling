from processor import aggregate, clean, load_data


def test_polars_load_data():
    df_url, load_time_url, agg_time_url = load_data.polars_loading_time(load_data.EXCEL_FILE_URL)
    df_loc, load_time_loc, agg_time_loc = load_data.polars_loading_time(load_data.EXCEL_FILE_LOCALPATH)

    assert not df_url.is_empty()
    assert load_time_url > 0
    assert agg_time_url > 0
    assert not df_loc.is_empty()
    assert load_time_loc > 0
    assert agg_time_loc > 0

def test_pandas_load_data():
    df_url, load_time_url, agg_time_url = load_data.pandas_loading_time(load_data.EXCEL_FILE_URL)
    df_loc, load_time_loc, agg_time_loc = load_data.pandas_loading_time(load_data.EXCEL_FILE_LOCALPATH)

    assert not df_url.empty
    assert load_time_url > 0
    assert agg_time_url > 0
    assert not df_loc.empty
    assert load_time_loc > 0
    assert agg_time_loc > 0

def test_polars_clean_agg():
    df_pl = clean.clean_df_pl("online_retail_II.xlsx", sheet="Year 2010-2011")
    df_pd = clean.clean_df_pd("online_retail_II.xlsx", sheet="Year 2010-2011")

    df_agg_pl = aggregate.aggregate_polars(df_pl)
    df_agg_pd = aggregate.aggregate_pandas(df_pd)

    assert not df_pl.is_empty()
    assert not df_agg_pl.is_empty()

    assert not df_pd.empty
    assert not df_agg_pd.empty



