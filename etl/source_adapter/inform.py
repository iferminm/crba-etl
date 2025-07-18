import pandas as pd

from etl.source_adapter import SourceAdapter, ManualTransformer, read_excel_with_engine_fallback


class Inform_Risk_Index_Data(SourceAdapter):
    """
    S-190
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        # try:
        # Try and pull data from endpoint if possible
        self.dataframe = read_excel_with_engine_fallback(
            self.endpoint,
            header=1,
            sheet_name="INFORM Risk 2021 (a-z)",
        ).drop(0)
        # except:
        #     # Load from local file if endpoint is donw
        #     self.dataframe = pd.read_excel(
        #         self.config.data_sources_raw_manual_machine
        #         / "S-190_INFORM_Risk_2021_v050.xlsx",
        #         header=1,
        #         sheet_name="INFORM Risk 2021 (a-z)",
        #     ).drop(0)

        #     # Log
        #     print(
        #         "Data for source S-190 could not be extracted URL endpoint. Loaded data from local repository."
        #     )

        # Rename column to avoid taking both country and iso3 column. Only take iso3 ()
        self.dataframe = self.dataframe.rename(
            columns={
                "COUNTRY": "country_col_not_used",  # could be anything not indcluded in column_mapping.py
                "INFORM RISK": "RAW_OBS_VALUE",
            }
        )

        # THhs daat represents data from 2021
        self.dataframe["TIME_PERIOD"] = 2021

        # Save data
        return self.dataframe
