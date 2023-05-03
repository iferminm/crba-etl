import csv
import io
import json
import logging
from importlib.resources import files as res_files

import pandas as pd
import pandasdmx as sdmx
import requests

from etl.methology.country import country_crba_list
from etl.methology.indicator import indicator_definitions
from etl.methology.value_type import df_value_type


class Config:
    """
    This class should hold all config and context
    Its al little bit confusing and need to be refeactored.
    But next to this class there is a config, which lies in the dynamic section akka the config folder, in the config section file which hold configurtions which are overwritten.
    Maybe rename to context
    """

    def __init__(self, **kwargs) -> None:
        self.config_path = kwargs["config_path"]
        self.build_indicators_filter = kwargs.get("build_indicators_filter", None)

    def bootstrap(self):
        try:
            self.determin_folders_files()

            self.load_global_config()

            self.build_crba_report_definition()

            self.download_un_popuplation_total()
            # Needs to be after build_crba_report_definition. Because the old log file can be used to filter
            self.set_progress_logger_file_sink()
        except Exception as ex:
            raise Exception("Failed to build Config", ex)

    def set_progress_logger_file_sink(self):

        root_logger = logging.getLogger()
        global_file_handler = logging.FileHandler(self.output_dir / "log.log", mode='w+')

        global_file_handler.setFormatter(
            fmt=logging.Formatter('%(asctime)s %(levelname)s %(funcName)s(%(lineno)d) %(message)s'))
        global_file_handler.setLevel(logging.INFO)
        root_logger.addHandler(global_file_handler)

        # Put progress logger errors in separate file

        class CsvFormatter(logging.Formatter):
            def __init__(self):
                super().__init__()
                self.output = io.StringIO()
                self.writer = csv.writer(self.output, quoting=csv.QUOTE_ALL, quotechar="'", delimiter=";")

            def format(self, record):
                self.writer.writerow([record.exc_info[1].source_id, str(record.exc_info[1])])
                data = self.output.getvalue()
                self.output.truncate(0)
                self.output.seek(0)
                return data.strip()

        progress_logger = logging.getLogger("etl.progress_logger")

        # Only store errors of the last run
        extraction_error_log_file_handler = logging.FileHandler(self.output_dir / "error.log", mode='a+')
        extraction_error_log_file_handler.setFormatter(CsvFormatter())

        extraction_error_log_filter = logging.Filter(name="ExtractionError Filter")
        from etl.source_adapter import ExtractionError
        extraction_error_log_filter.filter = lambda record: record.exc_info and isinstance(record.exc_info[1],
                                                                                           ExtractionError)
        extraction_error_log_file_handler.addFilter(extraction_error_log_filter)

        progress_logger.addHandler(extraction_error_log_file_handler)

    def determin_folders_files(self):
        self.input_data = self.config_path / "in"
        self.input_data.mkdir(parents=True, exist_ok=True)
        self.input_data_data = self.input_data / "data"
        self.input_data_staging = self.input_data / "data" / "staging"
        self.input_data_staging.mkdir(parents=True, exist_ok=True)
        self.input_data_raw = self.input_data / "data" / "raw"
        self.input_data_raw.mkdir(parents=True, exist_ok=True)

        self.output_dir = self.config_path / "out"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir_data = self.output_dir / "data"
        self.output_dir_data.mkdir(parents=True, exist_ok=True)
        self.staging_output = self.output_dir_data / "staging"
        self.staging_output.mkdir(parents=True, exist_ok=True)
        self.indicator_output = self.output_dir_data / "indicator"
        self.indicator_output.mkdir(parents=True, exist_ok=True)
        self.error_folder = self.output_dir_data / "error"
        self.error_folder.mkdir(parents=True, exist_ok=True)

        self.source_selection_file = self.input_data / "source_selection.json"
        self.global_config = self.input_data / "global_config.json"

    def load_global_config(self):
        with open(self.global_config) as f:
            self.global_config = json.load(f)

    def get(self, key):
        """
        Get Value from loaeded config dict
        //TODO Maybe select class members first?!?!
        """
        return self.global_config[key]

    def build_crba_report_definition(self):
        """
        Build the crba_report_definition.

        #TODO remove most self's
        Returns: crba_report_definition

        """

        self.source_selection = pd.read_json(
            self.source_selection_file, orient="index",
        ).reset_index(names="SOURCE_ID")

        with res_files("etl.resources") as path:
            source_definitions = pd.read_json(
                path / "source_definitions.json", orient="index"
            ).reset_index(names="SOURCE_ID")

        ## Extend the selected sources with additional informations
        ## All default values can be overwritten by the source_selection.config
        self.crba_report_definition = (
            self.source_selection.merge(
                right=source_definitions,
                on="SOURCE_ID",
                how="left",
                suffixes=(None, "_default"),
            )
            .merge(
                right=indicator_definitions,
                on="INDICATOR_ID",
                how="left",
                suffixes=(None, "_default"),
                # Via source selection there shoulb be one source selected for each indicator
                validate="one_to_one"
            )
            .merge(
                right=df_value_type,
                on="VALUE_ID",
                how="left",
                suffixes=(None, "_default"),
                # Multiple Sources can have the same Value ID means can be mapped similarly
                validate="many_to_one"
            )
        )
        # If a Value is defined in source_selction AND source_defintion
        # Take Value from source selection. If Value in Source selection in nan take Value form source_definition

        for column in self.crba_report_definition.columns:
            if column.endswith("_default"):
                continue
            if column + "_default" in self.crba_report_definition.columns:
                self.crba_report_definition[column] = self.crba_report_definition[column].combine_first(
                    self.crba_report_definition[column + "_default"])

        self.crba_report_definition.set_index("SOURCE_ID").to_json(
            self.output_dir / "crba_report_definition.json", orient="index", indent=2
        )

        self.crba_report_definition["TARGET_YEAR"] = self.get("TARGET_YEAR")

        self.crba_report_definition.to_csv(
            self.output_dir / "crba_report_definition.csv",
            sep=";",
            index=True,
            index_label="SOURCE_ID",
        )
        # Each Source Should be defined by exactly on row
        assert self.crba_report_definition.SOURCE_ID.nunique() == self.crba_report_definition.shape[0]

        logging.warning(
            f"CRBA Report definition build with {self.crba_report_definition.SOURCE_ID.nunique()} Source ID's")
        # Filter CRBA REPORT DEFINITION if needed

        if self.build_indicators_filter:
            logging.info(f"Found Source Filter:{self.build_indicators_filter}")
            if self.build_indicators_filter.endswith(".sql"):
                with open(self.build_indicators_filter, "r") as f:
                    self.build_indicators_filter = f.read()
                self.crba_report_definition = self.crba_report_definition.query(self.build_indicators_filter)
            elif self.build_indicators_filter.endswith(".csv") | self.build_indicators_filter.endswith(".log"):
                # A Series of Source ID's
                self.build_indicators_filter = list(
                    set(pd.read_csv(self.build_indicators_filter, sep=";", quotechar="'").iloc[:, 0]))
                self.crba_report_definition = self.crba_report_definition[
                    self.crba_report_definition["SOURCE_ID"].isin(self.build_indicators_filter)]
            self.crba_report_definition.to_csv(
                self.output_dir / "crba_report_definition_filtered.csv",
                sep=";",
                index=True,
                index_label="SOURCE_ID",
            )
            logging.warning(
                f"CRBA Report definition FILTERED to {self.crba_report_definition.SOURCE_ID.unique()} Source ID's")

    def __repr__(self):
        return str(vars(self))

    def download_un_popuplation_total(self):
        # TODO Use pandas sdmx to contruct adress.
        # This adress querys the Total population over all ages and sexes for the selected countrys.
        # https://sdmx.data.unicef.org/databrowser/index.html?q=UNICEF:DM(1.0)
        adress = f"https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,DM,1.0/{'+'.join(list(country_crba_list['COUNTRY_ISO_3']))}.DM_POP_TOT.._T.?format=fusion-json&endPeriod={self.get('TARGET_YEAR')}&startPeriod={self.get('TARGET_YEAR') - 10}&includeHistory=true&includeMetadata=true&dimensionAtObservation=AllDimensions&includeAllAnnotations=true"

        # TODO replace with string io wrapper
        req = requests.get(adress)
        with open(self.output_dir / "unicef_population_total.json", "w") as f:
            f.write(req.text)

        df = sdmx.read_sdmx(self.output_dir / "unicef_population_total.json").to_pandas()

        # Drop unneaded indexes. This index/dmensions have only one value:TOTAL
        df = df.reset_index(level=['INDICATOR', 'RESIDENCE', 'SEX', 'AGE'], drop=True)

        # Select only latest observation
        # TODO: Select observation of target_year
        df = df.reset_index(['TIME_PERIOD', "REF_AREA"])
        df.rename(columns={"REF_AREA": "COUNTRY_ISO_3", "value": 'population'}, inplace=True)
        self.unicef_population_total = df.groupby(level=0).last()
        self.unicef_population_total.to_csv(self.output_dir / "unicef_population_total.csv")
        self.un_pop_tot = self.unicef_population_total
