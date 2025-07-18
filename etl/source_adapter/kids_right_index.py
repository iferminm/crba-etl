import pandas as pd

from etl.source_adapter import SourceAdapter, ManualTransformer, read_excel_with_engine_fallback


class KidsRightsIndex(SourceAdapter):

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform
    def _download(self):
        self.dataframe = read_excel_with_engine_fallback(self.config.input_data_data / self.file_path)
        # First Row is empty
        self.dataframe = self.dataframe.drop(0)

        self.dataframe["TIME_PERIOD"] = self.time_period

        self.dataframe = self.dataframe.rename(
            columns={"Countries": "COUNTRY_NAME", self.raw_obs_value_column_name: "RAW_OBS_VALUE"}
        )
        #reduce dataframe
        self.dataframe = self.dataframe[["COUNTRY_NAME", "RAW_OBS_VALUE", "TIME_PERIOD"]].copy()

        return self.dataframe


