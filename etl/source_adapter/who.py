from io import StringIO

import pandas as pd

from etl.source_adapter import ManualTransformer
from etl.source_adapter import SourceAdapter
from etl.source_adapter.csv import DefaultCSVExtractor


class GlobalHealthObservatory(SourceAdapter):

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = DefaultCSVExtractor._transform

    def _download(self):
        csv_data = SourceAdapter.api_request(self.endpoint).text
        self.dataframe = pd.read_csv(StringIO(csv_data), sep=",")

        # There are Duplicate Obersations. Both of which are valid.
        merge_comments = lambda x: "Mean from:" + "AND".join(x)
        self.dataframe = self.dataframe.groupby(["YEAR", "COUNTRY"]).agg(
            {
                "Numeric": 'mean',
                "Display Value": lambda rows: "Mean from:" + "AND".join(rows),
                "Comments": lambda rows: "Mean from:" + "AND".join(rows),
            }
        ).reset_index()

        self.dataframe = self.dataframe.rename(
            columns={
                "Display Value": "obs_value_no_used_2",
                "Numeric": "RAW_OBS_VALUE",
            }
        )

        return self.dataframe


class S_157(SourceAdapter):
    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        csv_data = SourceAdapter.api_request(self.endpoint).text
        self.dataframe = pd.read_csv(StringIO(csv_data), sep=",")

        # We only have the population data for both sexes, so discrd other dimensionsubgroups
        self.dataframe = self.dataframe.loc[self.dataframe.SEX == "BTSX"]

        self.dataframe = self.config.un_pop_tot.merge(
            right=self.dataframe,
            how="right",
            # on="ISO3_YEAR"
            left_on=["COUNTRY_ISO_3", "TIME_PERIOD"],
            right_on=["COUNTRY", "YEAR"],
        )

        # Compute target raw observation value
        self.dataframe["RAW_OBS_VALUE"] = (
                self.dataframe["Numeric"] / (self.dataframe["population"]) * 100
        )

        # Add attribute
        self.dataframe[
            "ATTR_UNIT_MEASURE"
        ] = "The burden of disease attributable to ambient air pollution expressed as Number of deaths of children under 5 years per 100.000 children under 5 years. Note: Data about deaths drawm from WHO (https://apps.who.int/gho/data/node.imr.AIR_4?lang=en), refering to year 2016. Data about population under 5 years drawn from UNICEF (https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=GLOBAL_DATAFLOW&ver=1.0&dq=.DM_POP_U5..&startPeriod=2008&endPeriod=2018), referring to year 2018. Due to a lack of matching data, the WHO data from 2016 had to be normalized with population data from 2018."

        # Rename clumns
        self.dataframe = self.dataframe.rename(
            columns={
                "TIME_PERIOD": "time_period_not_used",
                "Display Value": "obs_value_no_used_2",
                "COUNTRY": "country_not_used"
            }
        )

        # TODO
        # self.dataframe = cleanse.extract_who_raw_data(
        #     raw_data=self.dataframe,
        #     variable_type=self.value_labels,
        #     display_value_col="Display Value",
        # )

        return self.dataframe
