import pandas as pd
import requests

from etl.source_adapter import SourceAdapter
from etl.transformation import cleanse, scaler
from etl.methology import (
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_country,
    sdmx_df_columns_time,
    sdmx_df_columns_attr,
    country_crba_list,
    country_full_list,
    mapping_dict,
    value_mapper
)


class DefaultJsonExtractor(SourceAdapter):

    def __init__(self, config, NA_ENCODING=None, **kwarg):
        super().__init__(config, **kwarg)

        self.na_encoding = NA_ENCODING

    def _download(self):
        # Extract data and convert to pandas dataframe
        try:
            # Most json data is from SDG; which return json with key "data" having the data as value
            raw_data = pd.json_normalize(requests.get(self.endpoint).json()["data"])
        except:
            # However, some of the data is also from World Bank where the command returns list, which must be subset with list index
            raw_data = pd.json_normalize(
                requests.get(self.endpoint).json()[1]
            )  # 0 is metadata, 1 contains actual data)

        return raw_data

    def _transform(self):
        # Cleansing in
        self.dataframe = cleanse.extract_who_raw_data(
            raw_data=self.dataframe,
            variable_type=self.value_labels,
            display_value_col="Display Value"
        )

        self.dataframe = cleanse.rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=mapping_dict,
            final_sdmx_col_list=sdmx_df_columns_all
        )

        self.dataframe = cleanse.convert_nan_strings_into_nan(
            dataframe=self.dataframe
        )

        self.dataframe = cleanse.extract_year_from_timeperiod(
            dataframe=self.dataframe,
            year_col="TIME_PERIOD",
            time_cov_col="COVERAGE_TIME"
        )

        self.dataframe = cleanse.retrieve_latest_observation(
            renamed_data=self.dataframe,
            dim_cols=sdmx_df_columns_dims,
            country_cols=sdmx_df_columns_country,
            time_cols=sdmx_df_columns_time,
            attr_cols=sdmx_df_columns_attr,
        )

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
            crba_release_year=self.config.get("CRBA_RELEASE_YEAR")
        )

        self.dataframe = cleanse.map_values(
            cleansed_data=self.dataframe,
            value_mapping_dict=value_mapper
        )

        self.dataframe = cleanse.encode_categorical_variables(
            dataframe=self.dataframe,
            encoding_string=self.value_encoding,
            encoding_labels=self.value_labels,
            na_encodings=self.na_encoding
        )

        self.dataframe = cleanse.create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

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
            time_period=self.config.get("CRBA_RELEASE_YEAR")
        )

        return self.dataframe
