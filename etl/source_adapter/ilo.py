import logging

import bs4 as bs
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

from etl.methology import (
    country_crba_list,
    country_full_list,
    mapping_dict,
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_time,
)
from etl.source_adapter import SourceAdapter
from etl.transformation import cleanse, scaler


log = logging.getLogger(__name__)


class ILO_Extractor(SourceAdapter):

    ## TODO make transform backward with target year

    def __init__(self, config, **kwarg):
        super().__init__(config, **kwarg)
        options = Options()
        options.add_argument("--headless=new")  # 👈 new headless mode (use this!)
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        self.driver = webdriver.Chrome(options=options)

    def _download(self):
        self.driver.get(self.address)
        soup = bs.BeautifulSoup(self.driver.page_source, features="lxml")
        wait = WebDriverWait(self.driver, 30)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".horizontalLine")))
        target_table = str(
            soup.find_all("table", {"cellspacing": "0", "class": "horizontalLine"})
        )

        # Create dataframe with the data
        self.dataframe = pd.read_html(io=target_table, header=0)[
            0
        ]
        #self.progress_logger.info(f"Downloaded Source {self.source_id} with {self.dataframe.shape}")
        return self.dataframe

    def _transform(self):
        # Cleansing
        self.dataframe = cleanse.rename_and_discard_columns(
            raw_data=self.dataframe,
            mapping_dictionary=mapping_dict,
            final_sdmx_col_list=sdmx_df_columns_all
        )

        self.dataframe = cleanse.decompose_country_footnote_ilo_normlex(
            dataframe=self.dataframe,
            country_name_list=country_full_list["COUNTRY_NAME"]
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
            treaty_source_body='ILO NORMLEX'
        )

        # Create log info
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
            log_info=True,
            time_period=self.config.get("CRBA_RELEASE_YEAR")
        )

        return self.dataframe