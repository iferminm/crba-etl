import datetime

import requests
import pandas as pd

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
from etl.source_adapter import SourceAdapter
from etl.transformation import cleanse, scaler


class ICRC_Treaties(SourceAdapter):
    """
    S-168, S-169, S170
    """

    def __init__(self, config, ATTR_RATIFICATION_DATE_COLUMN_NAME, **kwarg):
        self.attr_ratification_date_column_name = ATTR_RATIFICATION_DATE_COLUMN_NAME
        super().__init__(config, **kwarg)

    def _transform(self):
        self.dataframe = cleanse.rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=mapping_dict,
            final_sdmx_col_list=sdmx_df_columns_all
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

        self.dataframe = cleanse.encode_ilo_un_treaty_data(
            dataframe=self.dataframe,
            treaty_source_body=self.source_body
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
            time_period=self.config.get("CRBA_RELEASE_YEAR"),
        )

        return self.dataframe

    def _download(self):
        # Try loading data from endpoint (preferred)
        self.dataframe = pd.read_excel(
            self.endpoint,
            sheet_name="IHL and other related Treaties",
            header=1,
        )
        # TODO Log warning
        # Load from local file if endpoint is donw
        self.dataframe = self.dataframe[
            ["Country", self.attr_ratification_date_column_name]
        ]

        # Convert datetime format
        self.dataframe[self.attr_ratification_date_column_name] = self.dataframe[
            self.attr_ratification_date_column_name
        ].apply(
            lambda x: f"{x.year}-{x.month}-{x.day}"
            if isinstance(x, datetime.date)
            else x
        )

        # Rename clumns
        self.dataframe = self.dataframe.rename(
            columns={self.attr_ratification_date_column_name: "ATTR_RATIFICATION_DATE"}
        )

        # Add year column
        self.dataframe["TIME_PERIOD"] = self.config.get("CRBA_RELEASE_YEAR")

        return self.dataframe
