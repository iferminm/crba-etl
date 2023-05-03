import re
from io import StringIO

import pandas as pd

from etl.source_adapter import SourceAdapter
from etl.source_adapter import ManualTransformer


class S_157(SourceAdapter):
    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        csv_data = SourceAdapter.api_request(self.endpoint).text
        dataframe = pd.read_csv(StringIO(csv_data), sep=",")

        # We only have the population data for both sexes, so discrd other dimensionsubgroups
        dataframe = dataframe.loc[dataframe.SEX == "BTSX"]

        s_157 = self.config.un_pop_tot.merge(
            right=dataframe,
            how="right",
            # on="ISO3_YEAR"
            left_on=["COUNTRY_ISO_3", "TIME_PERIOD"],
            right_on=["COUNTRY", "Year"],
        )

        # Join population data to raw data
        # s_157 = self.config.un_pop_tot.merge(
        #     right=dataframe, how="right", left_index=True, right_on="COUNTRY"
        # )

        # Compute target raw observation value
        s_157["RAW_OBS_VALUE"] = (
                s_157["Numeric"] / (s_157["population"]) * 100
        )

        # Add attribute
        s_157[
            "ATTR_UNIT_MEASURE"
        ] = "The burden of disease attributable to ambient air pollution expressed as Number of deaths of children under 5 years per 100.000 children under 5 years. Note: Data about deaths drawm from WHO (https://apps.who.int/gho/data/node.imr.AIR_4?lang=en), refering to year 2016. Data about population under 5 years drawn from UNICEF (https://data.unicef.org/resources/data_explorer/unicef_f/?ag=UNICEF&df=GLOBAL_DATAFLOW&ver=1.0&dq=.DM_POP_U5..&startPeriod=2008&endPeriod=2018), referring to year 2018. Due to a lack of matching data, the WHO data from 2016 had to be normalized with population data from 2018."

        # Rename clumns
        s_157 = s_157.rename(
            columns={
                "TIME_PERIOD": "time_period_not_used",
                "Display Value": "obs_value_no_used_2",
            }
        )

        return s_157