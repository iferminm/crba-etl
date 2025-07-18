from etl.source_adapter import SourceAdapter, ManualTransformer, read_excel_with_engine_fallback

class IDMC_Extractor(SourceAdapter):
    """
    S-180, S-181, S-189, S-230
    """

    IDMC_Extractor_Source = dict()

    avaiable_years_configurations = [2019, 2021]

    def __init__(self, config, ATTR_UNIT_MEASURE, **kwarg):
        #Just a filter in the URL to reduce the returned excel
        self.startYear = config.get("CRBA_RELEASE_YEAR") - 10
        self.endYear = config.get("CRBA_RELEASE_YEAR")

        super().__init__(config, **kwarg)
        self.attr_unit_measure = ATTR_UNIT_MEASURE

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = read_excel_with_engine_fallback(self.endpoint).drop(0)

        self.dataframe["Year"] = self.dataframe["Year"].astype("Int64")

        self.dataframe = self.config.un_pop_tot.merge(
                 right=self.dataframe,
                 how="right",
                 left_on=["COUNTRY_ISO_3", "TIME_PERIOD"],
                 right_on=["ISO3", "Year"],
        )

        self.dataframe = self.dataframe[["ISO3","Year","population",self.raw_obs_value_column_name]]

        self.dataframe["RAW_OBS_VALUE"] = (
                         self.dataframe[self.raw_obs_value_column_name] / (self.dataframe["population"]) * 100
                 )  # Pop given inthousands, we want number per 100.000 pop

        # Add unit measure
        self.dataframe["ATTR_UNIT_MEASURE"] = self.attr_unit_measure
        return self.dataframe