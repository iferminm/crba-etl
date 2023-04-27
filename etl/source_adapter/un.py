import re

import bs4 as bs
import pandas as pd
import requests

from etl.methology import (
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_time,
    country_crba_list,
    country_full_list,
    mapping_dict
)
from etl.source_adapter import ManualTransformer
from etl.source_adapter import SourceAdapter
from etl.transformation import cleanse, scaler


class UnTreaties(SourceAdapter):

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    def _download(self):
        # Get http request
        response = SourceAdapter.api_request(self.address.strip())

        # Soupify the actual html content
        soup = bs.BeautifulSoup(response.text, features="lxml")

        # Extract the target table as attribute
        target_table = str(
            soup.find_all(
                "table",
                {
                    "class": "table table-striped table-bordered table-hover table-condensed"
                },
            )
        )

        # Create dataframe with the data
        raw_data = pd.read_html(io=target_table, header=0)[
            0
        ]  # return is a list of DFs, specify [0] to get actual DF

        # Return result
        return raw_data

    def _transform(self):
        # Save dataframe
        # self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";")

        # Cleansing
        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id))

        # Cleansing
        self.dataframe = cleanse.rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=mapping_dict,
            final_sdmx_col_list=sdmx_df_columns_all
        )

        # UN Treaty data specific: Sometimes, countries have footnotes (numbers). These must be purged for the rest of the code to work properly
        self.dataframe['COUNTRY_NAME'] = self.dataframe['COUNTRY_NAME'].apply(
            lambda x: re.sub('\s\d+.*', '', x))  # delete everything after number (and the leading whitespace)

        self.dataframe = cleanse.add_and_discard_countries(
            grouped_data=self.dataframe,
            crba_country_list=country_crba_list,
            country_list_full=country_full_list
        )

        self.dataframe = cleanse.add_cols_fill_cells(
            grouped_data_iso_filt=self.dataframe,
            dim_cols=sdmx_df_columns_dims,
            time_cols=sdmx_df_columns_time,
            indicator_name_string=self.indicator_name_y,
            index_name_string=self.index,
            issue_name_string=self.issue,
            category_name_string=self.category,
            indicator_code_string=self.indicator_code,
            indicator_source_string=self.address,
            indicator_source_body_string=self.source_body,
            indicator_description_string=self.indicator_description,
            source_title_string=self.source_titel,
            indicator_explanation_string=self.indicator_explanation,
            indicator_data_extraction_methodology_string=self.extraction_methodology,
            source_api_link_string=self.endpoint,
            attribute_unit_string=self.unit_measure,
            target_year=self.config.get("TARGET_YEAR")
        )

        self.dataframe = cleanse.encode_ilo_un_treaty_data(
            dataframe=self.dataframe,
            treaty_source_body=self.source_body
        )

        self.dataframe = cleanse.create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

        # Append dataframe to combined dataframe
        # combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        # )

        # Save cleansed data
        # self.dataframe.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")

        # Normalizing section
        self.dataframe = scaler.normalizer(
            cleansed_data=self.dataframe,
            sql_subset_query_string=self.dimension_values_normalization,
            # dim_cols=sdmx_df_columns_dims,
            variable_type=self.value_labels,
            is_inverted=self.invert_normalization,
            whisker_factor=1.5,
            raw_data_col="RAW_OBS_VALUE",
            scaled_data_col_name="SCALED_OBS_VALUE",
            maximum_score=10,
            time_period=self.config.get("TARGET_YEAR")
        )

        return self.dataframe


class UN_SDG_UN_POP(SourceAdapter):
    """""
    S-185, S-186, S-187, S-188
    """ ""

    def __init__(self, config, ATTR_UNIT_MEASURE, **kwarg):
        super().__init__(config, **kwarg)
        self.attr_unit_measure = ATTR_UNIT_MEASURE

    _transform = ManualTransformer._transform

    def _download(self):
        try:
            # Most json data is from SDG; which deturn json with key "data" having the data as value
            raw_data = pd.json_normalize(requests.get(self.endpoint).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list,
            # which must be subset with list index
            raw_data = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        self.dataframe = raw_data

        # Obtain the ISO2 and ISO3 codes
        self.dataframe = self.dataframe.merge(
            right=country_full_list,
            how="left",
            left_on="geoAreaName",
            right_on="COUNTRY_NAME",
        )

        # Cast year column as string for join
        self.dataframe.timePeriodStart = self.dataframe.timePeriodStart.astype(int)

        # Join UN Population data to to obtain population size
        self.dataframe = self.config.un_pop_tot.merge(
            right=self.dataframe,
            how="right",
            # on="ISO3_YEAR"
            left_on=["COUNTRY_ISO_3", "year"],
            right_on=["COUNTRY_ISO_3", "timePeriodStart"],
        )

        # Calculate target KPI (number of Internally displaced people per 100.000 people)
        self.dataframe["RAW_OBS_VALUE"] = (
                self.dataframe["value"].astype(float) / (self.dataframe["population"]) * 100
        )  # Pop given inthousands, we want number per 100.000 pop

        # Add unit measure
        self.dataframe["ATTR_UNIT_MEASURE"] = self.attr_unit_measure

        # Rename columns to avoid double_naming of column, which produces error down the ETL line
        self.dataframe = self.dataframe.rename(
            columns={
                "COUNTRY": "COUNTRY_ISO_3",
                "geoAreaName": "country_col_not_used",
                "year": "year_not_used",
                "COUNTRY_NAME": "country_col_2_not_used",
                "COUNTRY_ISO_2": "country_col_3_not_used",
                "value": "raw_value_before_normalisation",
                "attributes.Units": "unit_measure_not_used",
            }
        )
        return self.dataframe
