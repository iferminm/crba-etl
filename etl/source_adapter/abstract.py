import datetime
import logging
import re
from abc import ABC, abstractmethod

import requests

from etl.conf import Config
from etl.methology import mapping_dict
from etl.methology import (
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_country,
    sdmx_df_columns_time,
    sdmx_df_columns_attr,
    country_crba_list,
    country_full_list,
    value_mapper
)
from etl.transformation import cleanse
from etl.transformation import scaler


class ExtractionError(Exception):
    def __init__(self, message, SOURCE_ID):
        super().__init__(message)
        self.source_id = SOURCE_ID

    pass


class SourceAdapter(ABC):
    """
    Maybe subcalss from Pandas Dataframe?!?!?
    """

    match_file_suffix = re.compile("application/([a-z]*)")

    @classmethod
    def api_request(cls, address, params=None, headers=None):
        """
        Dont catch exceptions. When erros occured the extraction should faile
        """
        response = requests.get(address, params=params, headers=headers)
        # If the response was successful, no Exception will be raised
        response.raise_for_status()
        # return response object
        return response

    @abstractmethod
    def __init__(self, config: Config, **kwargs):

        self.progress_logger = logging.getLogger("etl.progress_logger")
        self.kwargs = kwargs

        self.config = config
        self.source_id = kwargs.get("SOURCE_ID")
        self.source_type = kwargs.get("SOURCE_TYPE")
        self.source_titel = kwargs.get("SOURCE_TITLE")
        self.endpoint = kwargs.get("ENDPOINT_URL")
        self.value_labels = kwargs.get("VALUE_LABELS")
        self.indicator_name_y = kwargs.get("INDICATOR_NAME")
        self.index = kwargs.get("INDEX")
        self.issue = kwargs.get("ISSUE")
        self.category = kwargs.get("CATEGORY")
        self.indicator_code = kwargs.get("INDICATOR_CODE")
        self.address = kwargs.get("ADDRESS")
        self.source_body = kwargs.get("SOURCE_BODY")
        self.indicator_description = kwargs.get("INDICATOR_DESCRIPTION")
        self.indicator_explanation = kwargs.get("INDICATOR_EXPLANATION")
        self.extraction_methodology = kwargs.get("EXTRACTION_METHODOLOGY")
        self.unit_measure = kwargs.get("UNIT_MEASURE")
        self.value_encoding = kwargs.get("VALUE_ENCODING")
        self.dimension_values_normalization = kwargs.get(
            "DIMENSION_VALUES_NORMALIZATION"
        )
        self.invert_normalization = kwargs.get("INVERT_NORMALIZATION")
        self.indicator_id = kwargs.get("INDICATOR_ID")
        self.file_path = kwargs.get("FILE_PATH")
        self.url_params = vars(self) | self.kwargs

    def build(self):
        """
        Good pattern is to only create new columns but not delete old ones.
        While the data is small enough this pattern helps debugging
        """
        try:
            self.download().transform()

            # self.run_greate_expectation_checkpoint()
            return self.dataframe
        except Exception as ex:
            # Store all Data processed until now to help debugging
            if hasattr(self, "dataframe"):
                self.dataframe.to_csv(
                    self.config.error_folder / str(self.source_id + ".csv"), sep=";"
                )
            raise ExtractionError(
                f"Source {self.source_id} failed to extract cause of: {str(ex)}",
                self.source_id,
            ) from ex

    def download(self):
        if self.endpoint:
            self.endpoint = self.endpoint.format(**self.url_params)

        self.dataframe = self._download()

        self.dataframe["ATTR_ENDPOINT"] = self.endpoint

        self.dataframe.to_csv(
            self.config.staging_output / str(self.source_id + ".csv"),
            sep=";",
            index=False,
        )

        # TODO establish Great Expectation to validate sources
        assert len(self.dataframe) > 0, "The source has not provided any data  "

        return self

    def transform(self):
        ##TODO do not reassign Dataframe but instead edit in place
        self.dataframe = self._transform()

        self.dataframe["SOURCE_ID"] = self.source_id
        self.dataframe["RAW_OBS_VALUE"] = self.dataframe["RAW_OBS_VALUE"].astype(float)

        self.dataframe.to_csv(
            self.config.indicator_output / str(self.source_id + "_" + self.indicator_id + ".csv"),
            sep=";",
            index=False,
        )

        return self

    def get(self):
        return self.dataframe


class EmptyExtractor(SourceAdapter):
    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)

    def _download(self):
        raise NotImplementedError(f"For {self.source_id} no download method defined")

    def _transform(self):
        raise NotImplementedError(
            f"For {self.source_id} no transformations are defined"
        )


class ManualTransformer(SourceAdapter):
    """
    Manually extracted data (both human and machine-generated data) + data which required pre-processing
    Normal ETL-pipeline
    """

    def _transform(self):

        # Cleansing
        self.dataframe = cleanse.extract_who_raw_data(
            raw_data=self.dataframe,
            variable_type=self.value_labels,
            display_value_col="Display Value",
        )

        # print(dataframe)
        # Exception: S-126 is a UNICEF API source, but has a different structure (repetitive columns) -->
        # rename them so they are being included in the rename_and_discard_columns function
        if self.source_id == "S-126":
            self.dataframe = self.dataframe.rename(
                columns={
                    "Geographic area": "Geographic area_unused",
                    "Sex": "Sex_unused",
                    "AGE": "AGE_unused",
                }
            )
        else:
            pass

        self.dataframe = cleanse.rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=mapping_dict,
            final_sdmx_col_list=sdmx_df_columns_all,
        )

        # Specific to data from API (NRGI) --> Only two sources
        if self.source_type == "API (NRGI)":
            self.dataframe["RAW_OBS_VALUE"] = self.dataframe["RAW_OBS_VALUE"].apply(
                lambda x: np.nan if x == "." else x
            )

        self.dataframe = cleanse.extract_year_from_timeperiod(
            dataframe=self.dataframe,
            year_col="TIME_PERIOD",
            time_cov_col="COVERAGE_TIME",
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
            country_list_full=country_full_list,
        )
        # If Time period is set by source selection
        time_period = datetime.datetime.now().year
        if hasattr(self, 'time_period'):
            time_period = self.time_period

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
            target_year=self.config.get("TARGET_YEAR"),
            time_period=time_period
        )

        self.dataframe = cleanse.map_values(
            cleansed_data=self.dataframe, value_mapping_dict=value_mapper
        )

        self.dataframe = cleanse.encode_categorical_variables(
            dataframe=self.dataframe,
            encoding_string=self.value_encoding,
            encoding_labels=self.value_labels,
        )

        self.dataframe = cleanse.create_log_report_delete_duplicates(
            cleansed_data=self.dataframe
        )

        # Normalizing
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
            time_period=self.config.get("TARGET_YEAR"),
        )
        return self.dataframe
