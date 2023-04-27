import pandas as pd

def download_(endpoint,country_name_column_name,raw_obs_value_column_name):

    dataframe = pd.read_excel(
        endpoint,
        sheet_name="All countries",
        header=1,
        storage_options = {'User-Agent': 'Mozilla/5.0'}
    ).drop(
        [0, 1]
    )  

    dataframe = dataframe[
        [country_name_column_name, raw_obs_value_column_name]
    ]

    # Add year column //TODO
    dataframe["TIME_PERIOD"] = 2016

    # Rename columns
    dataframe = dataframe.rename(
        columns={
            country_name_column_name: "COUNTRY_NAME",
            raw_obs_value_column_name: "RAW_OBS_VALUE",
        }
    )

    return dataframe