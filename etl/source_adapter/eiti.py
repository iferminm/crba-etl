import requests
import pandas as pd

from etl.source_adapter import SourceAdapter, ManualTransformer


class EITIIndicatorBuilder(SourceAdapter):

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)
        self.raw_obs_value_column_name = kwarg.get("RAW_OBS_VALUE_COLUMN_NAME")

    _transform = ManualTransformer._transform

    def _download(self):
        try:
            # Most json data is from SDG; which return json with key "data" having the data as value
            self.dataframe = pd.json_normalize(
                requests.get(self.endpoint).json()["data"]
            )
        except:
            # However, some data is also from World Bank where the command returns list,
            # which must be subset with list index
            self.dataframe = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        # Add time period
        self.dataframe["TIME_PERIOD"] = 2020

        # Rename OBS-VALUE column
        self.dataframe = self.dataframe.rename(
            columns={
                self.raw_obs_value_column_name: "RAW_OBS_VALUE",
            }
        )
        return self.dataframe
