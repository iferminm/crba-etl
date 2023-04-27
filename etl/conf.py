import re
import json
from importlib.resources import files as res_files

import pandas as pd
import requests
import pandasdmx as sdmx

from etl.methology.indicator import indicator_definitions
from etl.methology.value_type import df_value_type
from etl.methology.country import country_crba_list


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

            self.get_filter_for_crba_report_definition()
        except Exception as ex:
            raise Exception("Failed to build Config", ex)

    def determin_folders_files(self):

        self.input_data = self.config_path / "in"
        self.input_data.mkdir(parents=True, exist_ok=True)
        self.input_data_data = self.input_data / "data"
        self.input_data_staging = self.input_data / "data" / "staging"
        self.input_data_staging.mkdir(parents=True, exist_ok=True)
        # legacy
        self.input_data_raw = self.input_data / "data" / "raw"
        self.input_data_raw.mkdir(parents=True, exist_ok=True)

        self.output_dir = self.config_path / "out"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.staging_output = self.output_dir / "staging"
        self.staging_output.mkdir(parents=True, exist_ok=True)
        self.indicator_output = self.output_dir / "indicator"
        self.indicator_output.mkdir(parents=True, exist_ok=True)
        self.error_folder = self.output_dir / "error"
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

        self.source_selection = pd.read_json(
            self.source_selection_file, orient="index"
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
                suffixes=(None, "_overwritten"),
            )
            .merge(
                right=indicator_definitions,
                on="INDICATOR_ID",
                how="left",
                suffixes=(None, "_overwritten"),
                # Via source selection there shoulb be one source selected for each indicator
                validate="one_to_one"
            )
            .merge(
                right=df_value_type,
                on="VALUE_ID",
                how="left",
                suffixes=(None, "_overwritten"),
                # Multiple Sources can have the same Value ID means can be mapped similarly
                validate="many_to_one"
            )
        )
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

    def __repr__(self):
        return str(vars(self))

    def download_un_popuplation_total(self):
        # TODO Use pandas sdmx to contruct adress.
        # This adress querys the Total population over all ages and sexes for the selected countrys.
        # https://sdmx.data.unicef.org/databrowser/index.html?q=UNICEF:DM(1.0)
        adress = f"https://sdmx.data.unicef.org/ws/public/sdmxapi/rest/data/UNICEF,DM,1.0/{'+'.join(list(country_crba_list['COUNTRY_ISO_3']))}.DM_POP_TOT.._T.?format=fusion-json&endPeriod={self.get('TARGET_YEAR')}&includeHistory=false&includeMetadata=true&dimensionAtObservation=AllDimensions&includeAllAnnotations=true"

        # TODO replace with string io wrapper
        req = requests.get(adress)
        with open(self.output_dir / "unicef_population_total.json", "w") as f:
            f.write(req.text)

        df = sdmx.read_sdmx(self.output_dir / "unicef_population_total.json").to_pandas()

        # Drop unneaded indexes. This index/dmensions have only one value:TOTAL
        df = df.reset_index(level=['INDICATOR', 'RESIDENCE', 'SEX', 'AGE'], drop=True)

        # Select only latest observation
        df = df.reset_index(['TIME_PERIOD'])
        self.unicef_population_total = df.groupby(level=0).last()
        self.unicef_population_total.to_csv(self.output_dir / "unicef_population_total_latest.csv")
        self.un_pop_tot = self.unicef_population_total

    def get_filter_for_crba_report_definition(self):
        if self.build_indicators_filter:
            with open(self.build_indicators_filter, "r") as f:
                self.build_indicators_filter = f.read()
