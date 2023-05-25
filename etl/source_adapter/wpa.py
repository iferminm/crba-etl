import pandas as pd

from etl.methology import (
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_time,
    country_crba_list,
    country_full_list,
    mapping_dict
)
from etl.source_adapter import SourceAdapter
from etl.transformation import cleanse, scaler


class WPA_Extractor(SourceAdapter):

    def __init__(self, config, WPA_YEAR_COL, WPA_OBS_RAW_COL, **kwarg):
        super().__init__(config, **kwarg)

        self.wpa_year_col = WPA_YEAR_COL
        self.wpa_obs_raw_col = WPA_OBS_RAW_COL

    def _download(self):
        self.dataframe = pd.read_excel(self.config.input_data_data / self.file_path)
        self.dataframe['TIME_PERIOD'] = self.wpa_year_col

        self.dataframe = self.dataframe.rename(columns={
            self.wpa_obs_raw_col: "RAW_OBS_VALUE"
        })

        self.dataframe = self.dataframe[['country', "RAW_OBS_VALUE"]]
        # Drop Rows with no observations or nan
        self.dataframe = self.dataframe.dropna()

        return self.dataframe

    def _transform(self):
        # print(dataframe.head(30))

        # Save dataframe
        # self.dataframe.to_csv(
        #    self.config.data_sources_raw / str(self.source_id + "_raw.csv"),
        #    sep = ";")
        # except:
        # print("There was an issue with source {}".format(self.source_id ))

        # Log that we are entering cleasning
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id ))

        # Cleansing
        # print("\n - - - - - \n Cleansing source {} \n".format(self.source_id ))

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
            indicator_explanation_string=self.indicator_explanation,
            indicator_data_extraction_methodology_string=self.extraction_methodology,
            source_title_string=self.source_titel,
            source_api_link_string=self.endpoint,
            attribute_unit_string=self.unit_measure,
            crba_release_year=self.config.get("CRBA_RELEASE_YEAR")
        )

        self.dataframe_cleansed = cleanse.encode_categorical_variables(
            dataframe=self.dataframe,
            encoding_string=self.value_encoding,
            encoding_labels=self.value_labels
        )

        self.dataframe_cleansed = cleanse.create_log_report_delete_duplicates(
            cleansed_data=self.dataframe_cleansed
        )

        # Append dataframe to combined dataframe
        # combined_cleansed_csv = combined_cleansed_csv.append(
        #    other = dataframe_cleansed
        # )

        # Save cleansed data
        # self.dataframe_cleansed.to_csv(
        #    self.config.data_sources_cleansed / str(self.source_id + "_cleansed.csv"),
        #    sep = ";")

        # Normalizing
        self.dataframe_normalized = scaler.normalizer(
            cleansed_data=self.dataframe_cleansed,
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

        # self.dataframe_normalized.to_csv(
        #    self.config.data_sources_normalized / str(self.indicator_id + '_' + self.source_id + '_' +self.indicator_code + "_normalized.csv"),
        #    sep = ";")

        # Append dataframe to combined dataframe
        # combined_normalized_csv = combined_normalized_csv.append(
        #    other = dataframe_normalized
        # )
        return self.dataframe_normalized
