import importlib
import logging
import warnings
from typing import Type

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from etl.source_adapter import ExtractionError

run_logger = logging.getLogger("etl.progress_logger")


def dynamic_load(class_path) -> Type:
    mod = ".".join(class_path.split(".")[:-1])
    _class = class_path.split(".")[-1]
    module = importlib.import_module(mod)
    return getattr(module, _class)


def build_combined_normalized_csv(config):
    ##TODO Parallelisiere . Prozess vs Thread. Probably Thread because of heavy IO Tasks concurrent.futures
    extractors = {}
    extractions_data = []
    extraction_errors_source_ids = []
    validation_batches = []
    sucesses = 0

    run_logger.info(f"{len(config.crba_report_definition)} sources selected to process")

    with logging_redirect_tqdm():
        with warnings.catch_warnings():
            warnings.simplefilter(
                "ignore"
            )  ## TODO:Store Warnings istead of jus supressing them
            for index, row in tqdm(
                    list(config.crba_report_definition.iterrows()), dynamic_ncols=True
            ):
                try:
                    row = row.dropna().to_dict()
                    extractor = dynamic_load(row["EXTRACTOR_CLASS"])(config, **row)
                    extractors[row["SOURCE_ID"]] = extractor
                    extractor.build()
                    # More IMportant then simple INFO logs but less importend the download infos
                    df = extractor.get()
                    run_logger.info(
                        msg=f"Source {row['SOURCE_ID']} extract with {df.shape[0] if df is not None else 0} rows::: {extractor.__class__.__name__}",
                    )
                    "S-180, S-181, S-189 S-230 idmc_displacement_all_dataset.xlsm"
                    "S-180, S-181, S-189 S-230 idmc_displacement_all_dataset.xlsx"
                    extractions_data.append(df)
                    sucesses += 1
                except ExtractionError as ex:
                    extraction_errors_source_ids.append(row["SOURCE_ID"])
                    run_logger.warning(
                        f"{str(ex)}",
                        exc_info=ex)
                except ValueError as ex:
                    run_logger.exception(ex)
                except AttributeError as ex:
                    run_logger.warning(
                        f"Source {row.get('SOURCE_ID')} failed. Extractor Class: {row.get('EXTRACTOR_CLASS')}",
                        exc_info=ex)
                except ModuleNotFoundError as ex:
                    run_logger.warning(
                        f"Module for source {row['SOURCE_ID']} not found",  # exc_info=True
                    )
                except TypeError as ex:
                    run_logger.exception(ex)

    # Store Indicator for inspection
    print(f"Number of sucesses{sucesses}")
    return (pd.concat(extractions_data, axis=0, ignore_index=True), "")


def run(config):
    """
    The files for eac staged are stored. ANd each stage reads all needed files.
    This is to achieve better modular executability.The loss performace is accepted.

    """
    build_combined_normalized_csv(config)
    pass
