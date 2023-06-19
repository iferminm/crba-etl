import argparse
import logging
import pathlib
import sys

import requests_cache

import etl.run
from etl.conf import Config

log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--config-path",
        # TODO Make target configurable
        help="The path to finde the config",
        action="store",
        type=pathlib.Path,
        dest="config_path",
    )

    parser.add_argument(
        "--extract-stage",
        help="Runs the download of all sources",
        # Defaults to false if not selected
        action="store_true",
        dest="extract_stage",
    )
    parser.add_argument(
        "--combine-stage",
        help="Combines the sources to form a crba_final",
        # Defaults to false if not selected
        action="store_true",
        dest="combine_stage",
    )
    parser.add_argument(
        "--sdmx-stage",
        help="Runs the transformations to create a sdmx ready csv file",
        # Defaults to false if not selected
        action="store_true",
        dest="sdmx_stage",
    )

    parser.add_argument(
        "--build-indicator-filter",
        help="SQL which is applied to the source config. Or CSV File where the first column is the Source ID",
        action="store",
        type=str,
        dest="build_indicators_filter",
    )

    parser.add_argument(
        "--no-caching",
        help="Turns off caching to enable a clean run. \n"
             "An Alternative could be to delete the cache which is under unix in ~/.cache/crba_downloads",
        # Defaults to false if not selected
        action="store_true",
        dest="no_caching",
    )

    return parser.parse_args()


def setup_logging(loglevel=logging.INFO):
    progress_logger = logging.getLogger("etl.progress_logger")
    progress_logger.setLevel(loglevel)

    root_logger = logging.getLogger()

    class NoStackTraceFormatter(logging.Formatter):
        def format(self, record):
            return record.getMessage()

    str_handler = logging.StreamHandler(sys.stderr)
    str_handler.setFormatter(NoStackTraceFormatter())
    root_logger.addHandler(str_handler)

    if loglevel == logging.DEBUG:
        from logging_tree import printout
        printout()


def setup_caching(no_caching: bool):
    """
    Configure gloabl caching
    TODO Move to Extractor?!?!
    """
    if not no_caching:
        log.info("Use Cahcing for request lib")
        requests_cache.install_cache(
            "crba_downloads", backend="filesystem", use_cache_dir=True
        )


if __name__ == "__main__":
    args = parse_args()

    setup_logging()
    setup_caching(args.no_caching)

    config = Config(**vars(args))
    config.bootstrap()

    etl.run.run(args, config)
