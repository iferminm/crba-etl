from etl.source_adapter import SourceAdapter, ManualTransformer, read_excel_with_engine_fallback


class CRIN_Treaties(SourceAdapter):

    def __init__(
            self, config, COUNTRY_NAME_COLUMN_NAME, **kwarg
    ):
        self.country_name_column_name = COUNTRY_NAME_COLUMN_NAME
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = read_excel_with_engine_fallback(
            self.endpoint,
            sheet_name="All countries",
            header=1,
            storage_options={'User-Agent': 'Mozilla/5.0'}
        ).drop(
            [0, 1]
        )

        self.dataframe = self.dataframe[
            [self.country_name_column_name, self.raw_obs_value_column_name]
        ]

        # Add year column
        self.dataframe["TIME_PERIOD"] = 2016

        # Rename columns
        self.dataframe = self.dataframe.rename(
            columns={
                self.country_name_column_name: "COUNTRY_NAME",
                self.raw_obs_value_column_name: "RAW_OBS_VALUE",
            }
        )

        return self.dataframe
