import pandas as pd

def download(endpoint,raw_obs_value_column_name,country_name_column_name):
    dataframe = pd.read_excel(
        endpoint,
        sheet_name="Ranking",
        header=17,
        usecols=[
            "Unnamed: 11",
            "Score",
            "Unnamed: 20",
            "Score.1",
            "Unnamed: 29",
            "Score.2",
            "Unnamed: 38",
            "Score.3",
            "Unnamed: 47",
            "Score.4",
        ],
    )

    dataframe = dataframe.loc[
        :, [raw_obs_value_column_name, country_name_column_name]
    ]

    # Rename clumns
    dataframe = dataframe.rename(
        columns={
            raw_obs_value_column_name: "RAW_OBS_VALUE",
            country_name_column_name: "COUNTRY_NAME",
        }
    )

    # Add year column
    # TODO Make dynmic?!?!
    dataframe["TIME_PERIOD"] = 2022

    return dataframe