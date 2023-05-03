import re

import numpy as np
import pandas as pd

from etl.methology import country_iso_list
from etl.source_adapter import ManualTransformer
from etl.source_adapter import SourceAdapter


class HumanEnteredBlueprintIndicatorBuilder(SourceAdapter):
    def __init__(self, config, RAW_OBS_VALUE_TYPE, **kwarg):
        super().__init__(config, **kwarg)
        self.raw_obs_value_type = RAW_OBS_VALUE_TYPE

    _transform = ManualTransformer._transform

    def _download(self):
        # latest_source_files, latest_year = GoogleDriveDownloader._download(self)

        if self.file_path:
            self.dataframe = pd.read_excel(
                self.config.input_data_data / self.file_path,
                sheet_name="Blueprint",
            )
        else:
            self.dataframe = pd.read_excel(
                self.config.input_data_staging / f"{self.source_id}.xlsx",
                sheet_name="Blueprint",
            )
        raw_obs_value_col = "RAW_OBS_VALUE"
        if self.raw_obs_value_type == "categorical":
            # Delete trailing whitespace and numbers of parentheses in raw_OBS_VALUE
            self.dataframe[raw_obs_value_col] = (
                self.dataframe[raw_obs_value_col]
                .apply(lambda x: re.sub(" \(\d+\)", "", x) if type(x) == str else x)
                .apply(lambda x: x.strip() if type(x) == str else x)
            )

            # Encode missing data as "No data"
            self.dataframe.loc[
                self.dataframe[raw_obs_value_col].isnull(), "RAW_OBS_VALUE"
            ] = "No data"

        # Section dealing with numeric variables
        elif self.raw_obs_value_type == "continuous":
            self.dataframe.loc[
                (self.dataframe[raw_obs_value_col] == "No data")
                | (self.dataframe[raw_obs_value_col] == "Insufficient data")
                | (self.dataframe[raw_obs_value_col] == "No legal measures ")
                | (self.dataframe[raw_obs_value_col] == "x"),
                raw_obs_value_col,
            ] = np.nan

        else:
            print("Must specify what type of variable it is")
        # Rename COUNTRY_NAME column, to avoid trouble of ETL pipeline down the line
        if "COUNTRY_NAME" in self.dataframe.columns:
            self.dataframe = self.dataframe.merge(
                right=country_iso_list,
                how="left",
                on="COUNTRY_NAME",
                suffixes=("_original", "_country_list"),
            )
            self.dataframe["COUNTRY_ISO_3"] = self.dataframe[
                "COUNTRY_ISO_3_original"
            ].combine_first(self.dataframe["COUNTRY_ISO_3_country_list"])

        self.dataframe = self.dataframe.rename(
            columns={"COUNTRY_NAME": "country_col_not_used"}
        )
        # print(dataframe.RAW_OBS_VALUE.unique())

        self.dataframe = self.dataframe.dropna(axis="columns", how="all")
        return self.dataframe


class IDMC_Extractor(SourceAdapter):
    """
    S-180, S-181, S-189, S-230
    """

    IDMC_Extractor_Source = dict()

    avaiable_years_configurations = [2019, 2021]

    def __init__(self, config, ATTR_UNIT_MEASURE, **kwarg):
        super().__init__(config, **kwarg)
        self.attr_unit_measure = ATTR_UNIT_MEASURE

    _transform = ManualTransformer._transform

    def _download(self):
        if self.source_id not in IDMC_Extractor.IDMC_Extractor_Source.keys():
            # TODO change loop into indicator Excel...
            S_180_S_181_S189_S_230 = pd.read_excel(self.config.input_data_data / self.file_path).drop(
                0
            )  # delete first row containing strings

            # Cast year as string, required for merge command later
            S_180_S_181_S189_S_230["Year"] = S_180_S_181_S189_S_230["Year"].astype(str)

            # Join raw data and population data together
            S_180_S_181_S189_S_230_raw = self.config.un_pop_tot.merge(
                right=S_180_S_181_S189_S_230,
                how="right",
                # on="ISO3_YEAR"
                left_on=["COUNTRY_ISO_3", "TIME_PERIOD"],
                right_on=["ISO3", "Year"],
            )

            # Create list to loop through
            idmc_list = [
                ["S-162", "Conflict Stock Displacement"],
                ["S-163", "Conflict New Displacements"],
                ["S-171", "Disaster New Displacements"],
                ["S-209", "Disaster Stock Displacement"],
            ]

            # Loop through list
            for element in idmc_list:
                # Extract right columns
                dataframe = S_180_S_181_S189_S_230_raw.loc[
                            :, ["ISO3", "Year", "population", element[1]]
                            ]

                # Calculate target kpi --> Normalize to per 100.000 persons
                dataframe["RAW_OBS_VALUE"] = (
                        dataframe[element[1]] / (dataframe["population"]) * 100
                )  # Pop given inthousands, we want number per 100.000 pop

                # Add unit measure
                dataframe["ATTR_UNIT_MEASURE"] = self.attr_unit_measure
                IDMC_Extractor.IDMC_Extractor_Source[element[0]] = dataframe

        return IDMC_Extractor.IDMC_Extractor_Source[self.source_id]


class Economist_Intelligence_Unit(SourceAdapter):
    """
    S-11, S-120, S-124,  S-134 , S-229,
    """

    def __init__(
            self, config, RAW_OBS_VALUE_COLUMN_NAME, COUNTRY_NAME_COLUMN_NAME, **kwarg
    ):
        self.raw_obs_value_column_name = RAW_OBS_VALUE_COLUMN_NAME
        self.country_name_column_name = COUNTRY_NAME_COLUMN_NAME
        self.time_period = kwarg.get("TIME_PERIOD")
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.input_data_data / self.file_path,
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

        self.dataframe = self.dataframe.loc[
                         :, [self.raw_obs_value_column_name, self.country_name_column_name]
                         ]

        # Rename clumns
        self.dataframe = self.dataframe.rename(
            columns={
                self.raw_obs_value_column_name: "RAW_OBS_VALUE",
                self.country_name_column_name: "COUNTRY_NAME",
            }
        )

        # Add year column
        self.dataframe["TIME_PERIOD"] = self.time_period

        return self.dataframe


class FCTC_Data(SourceAdapter):
    """
    S-89
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.manual_raw / "S-89 Answers_v2.xlsx",
        )

        self.dataframe = self.dataframe.melt(
            id_vars="Party", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE"
        )

        return self.dataframe


class Landmark_Data(SourceAdapter):
    """
    S-167
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        # Read data
        S_167 = pd.read_excel(
            self.config.input_data_data / self.file_path,
            sheet_name="Pct_IP_CommunityLands",
        )

        # The UK is split up here in its terriroties, must aggregate the data to UK-national level again

        # Pandas interprets the percentage column sometimes as string, sometimes as number  --> Convert all to numbers (in %, i.e. between 0 - 100)
        S_167["IC_F"] = S_167["IC_F"].apply(
            lambda x: x * 100
            if type(x) != str
            else ("No data" if x == "No data" else float(re.sub("%", "", x)))
        )
        # Sub selection of all GBR terriroties
        country_land_gbr = S_167.loc[236:239, "Ctry_Land"]

        # Calculate total area size of Great Britain
        total_land_gbr = sum(country_land_gbr)

        # Obtain percentage of indigenous land of all UK areas
        land_percentage = S_167.loc[
                          236:239, "IC_F"
                          ]  # .apply(lambda x: float(re.sub('%', '', x)) if type(x)==str else x * 100)

        # Compute total sqm rather than % of total land of indigenous land
        indi_land_tot = []
        for i in range(len(land_percentage)):
            indi_land_tot += [land_percentage.iloc[i] * country_land_gbr.iloc[i] / 100]

        # Total sum of all indiegnous land of all UK territories
        total_indi_land = sum(indi_land_tot)

        # Percentage of indigenous land in ALL UK
        total_indi_land_percent = total_indi_land / total_land_gbr * 100

        # Store all of this data in a dataframe to append it to the existing dataframe
        uk_df = pd.DataFrame(
            [
                [
                    "GBR",
                    np.nan,
                    np.nan,
                    "United Kingdom",
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    total_indi_land_percent,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    "According to the Eurostats study on common lands in Europe of 2010, common lands in the UK is always permanent grassland in the form of rough grazing. Much of these lands are found in the remote upland areas, and in many instance they have at least one special designation preventing their agricultural improvement. The area of these common lands across the UK are :591 901 ha in Scotland, 427 889 ha in England, 180 305 ha in Wales, 36 438 ha in Northern Ireland. Source : Eurostat, 2010. Farm structure survey - common land. Available at : http://ec.europa.eu/eurostat/statistics-explained/index.php?title=Common_land_statistics_-_background&oldid=262743<br>Jones, Gwyn. 2015. Common Grazing in Scotland - importance, governance, issues. Presentation at the EFNCP, available at : http://www.efncp.org/download/common-ground2015/Commons10Jones.pdf. There are also some lands held in in community ownership in Scotland. The latest figure of their total area is 0.19 Mha (470 094 acres). Based on a land mass of Scotland of 7.79 Mha (19.25 M acres), this would represent 2.44% of Scotland’s land mass. This figure has been calculated using the definition of Community Ownership that was agreed by the Scottish Government Short Life Working Group on community land ownership (I million acre target group) in September 2015. <br>Source: Peter Peacock, Community Land Scotland, personal communication. 2015/09/21. The component countries of  the United Kingdom have been treated separately on LandMark.",
                ]
            ],
            columns=S_167.columns,
        )

        # Append dataframe
        S_167 = S_167.append(other=uk_df)

        # Add time period
        S_167["TIME_PERIOD"] = 2017

        # Rename country column (to have it dsiregardedby the ETL pipeline) --> ONly rely on ISO3 column
        S_167 = S_167.rename(columns={"Country": "country_name_not_used"})

        # COnvert "No Data" strings into np.nan
        S_167.loc[S_167["IC_F"] == "No data", "IC_F"] = np.nan

        return S_167


class UCW_Data(SourceAdapter):
    """
    S-21
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.input_data_data / self.file_path,
            header=1,
        )

        # Unpivot
        self.dataframe = self.dataframe.melt(
            id_vars="Unnamed: 0", var_name="TIME_PERIOD", value_name="RAW_OBS_VALUE"
        )

        # Rename column
        self.dataframe = self.dataframe.rename(columns={"Unnamed: 0": "COUNTRY_NAME"})

        # Change value '..' to NaN
        self.dataframe.loc[
            self.dataframe.RAW_OBS_VALUE == "..", "RAW_OBS_VALUE"
        ] = np.nan

        return self.dataframe


class Global_Slavery_Index(SourceAdapter):
    """
    S-60
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = pd.read_excel(
            self.config.input_data_data
            / self.file_path,
            header=2,
            sheet_name="Global prev, vuln, govt table",
        )

        # THhs daat represents the 2018 global slavery index data. Add time period
        self.dataframe["TIME_PERIOD"] = 2018

        # Add unit measure attribute
        self.dataframe[
            "ATTR_UNIT_MEASURE"
        ] = "Est. prevalence of population in modern slavery (victims per 1,000 population)"

        # rename columns
        self.dataframe = self.dataframe.rename(
            columns={
                "Country ": "COUNTRY_NAME",
                "Est. prevalence of population in modern slavery (victims per 1,000 population)": "RAW_OBS_VALUE",
            }
        )

        return self.dataframe


class Climate_Watch_Data_S_153(SourceAdapter):
    """
    S-153
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    _transform = ManualTransformer._transform

    def _download(self):
        self.dataframe = pd.read_csv(
            self.config.input_data_data / self.file_path
        )

        # Cleanse target value variable (some encoding issues)
        self.dataframe.Value = self.dataframe.Value.apply(
            lambda x: re.sub(";<br>.+", "", x)
        )

        # Cleanse target value variable (some encoding issues)
        self.dataframe["TIME_PERIOD"] = 2020

        # Rename column so that it doesn't cause error downstream
        self.dataframe = self.dataframe.rename(
            columns={
                "Sector": "sector_not_used",
                "Subsector": "sub_sector_not_used",
            }
        )

        return self.dataframe


class Climate_Watch_Data_S_159(ManualTransformer):
    """
    S-159
    """

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    def _download(self):
        self.dataframe = pd.read_csv(
            self.config.input_data_data / self.file_path,
        )

        # Unpivot the data
        self.dataframe = self.dataframe.melt(
            id_vars=["Country/Region", "unit"],
            var_name="TIME_PERIOD",
            value_name="RAW_OBS_VALUE",
        )

        return self.dataframe
