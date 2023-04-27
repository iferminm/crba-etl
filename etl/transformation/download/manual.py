import re

import numpy as np
import pandas as pd

from etl.methology.country import country_iso_list

def download(manual_file,raw_obs_value_type):
    dataframe = pd.read_excel(
            manual_file,
            # config.manual_staging / f"{source_id}.xlsx",
            # config.download_file(latest_source_files[0]),
            sheet_name="Blueprint",
        )
    
    raw_obs_value_col = "RAW_OBS_VALUE"
    if raw_obs_value_type == "categorical":
        # Delete trailing whitespace and numbers of parentheses in raw_OBS_VALUE
        dataframe[raw_obs_value_col] = (
            dataframe[raw_obs_value_col]
            .apply(lambda x: re.sub(" \(\d+\)", "", x) if type(x) == str else x)
            .apply(lambda x: x.strip() if type(x) == str else x)
        )

        # Encode missing data as "No data"
        dataframe.loc[
            dataframe[raw_obs_value_col].isnull(), "RAW_OBS_VALUE"
        ] = "No data"

    # Section dealing with numeric variables
    elif raw_obs_value_type == "continuous":
        dataframe.loc[
            (dataframe[raw_obs_value_col] == "No data")
            | (dataframe[raw_obs_value_col] == "Insufficient data")
            | (dataframe[raw_obs_value_col] == "No legal measures ")
            | (dataframe[raw_obs_value_col] == "x"),
            raw_obs_value_col,
        ] = np.nan

    else:
        print("Must specify what type of variable it is")
    # Rename COUNTRY_NAME column, to avoid trouble of ETL pipeline down the line
    if "COUNTRY_NAME" in dataframe.columns:
        dataframe = dataframe.merge(
            right= country_iso_list,
            how="left",
            on="COUNTRY_NAME",
            suffixes=("_original", "_country_list"),
        )
        dataframe["COUNTRY_ISO_3"] = dataframe[
            "COUNTRY_ISO_3_original"
        ].combine_first(dataframe["COUNTRY_ISO_3_country_list"])

    dataframe = dataframe.rename(
        columns={"COUNTRY_NAME": "country_col_not_used"}
    )
    # print(dataframe.RAW_OBS_VALUE.unique())

    dataframe = dataframe.dropna(axis="columns", how="all")
    return dataframe