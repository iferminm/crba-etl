import sys
import argparse
import pathlib
import logging

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
        "--build-indicator-filter",
        help="SQL which is applied to the source config. As file",
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


def setup_logging(logLevel=logging.INFO):
    indicator_logger = logging.getLogger("etl.progress_logger")
    indicator_logger.setLevel(logging.INFO)

    root_logger = logging.getLogger()
    str_handler = logging.StreamHandler(sys.stderr)
    root_logger.addHandler(str_handler)




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

    etl.run.run(config)
