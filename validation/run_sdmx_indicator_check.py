"""
This is a basic generated Great Expectations script that runs a Checkpoint.

Checkpoints are the primary method for validating batches of data in production and triggering any followup actions.

A Checkpoint facilitates running a validation as well as configurable Actions such as updating Data Docs, sending a
notification to team members about validation results, or storing a result in a shared cloud storage.

See also <cyan>https://docs.greatexpectations.io/en/latest/guides/how_to_guides/validation/how_to_create_a_new_checkpoint_using_test_yaml_config.html</cyan> for more information about the Checkpoints and how to configure them in your Great Expectations environment.

Checkpoints can be run directly without this script using the `great_expectations checkpoint run` command.  This script
is provided for those who wish to run Checkpoints in python.

Usage:
- Run this file: `python great_expectations/uncommitted/run_sdmx_indicator_check.py`.
- This can be run manually or via a scheduler such, as cron.
- If your pipeline runner supports python snippets, then you can paste this into your pipeline.
"""
# Get current Path
import os
import re
import sys
from pathlib import Path

from great_expectations.checkpoint.types.checkpoint_result import (
    CheckpointResult,  # noqa: TCH001
)
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context import FileDataContext  # noqa: TCH001
from great_expectations.data_context.types.base import DataContextConfig, ConcurrencyConfig
from great_expectations.types import DictDot
from great_expectations.util import get_context

pwd = os.getcwd()

configs = {
    "2020": {
        "data_path": "config/2020/out/data/indicator",
        "data_connector_name": "indicators_2020",
        "VALIDATIONS_STORE_BASE_PATH": "../../config/2020/out/validations/validations",
        "DATA_DOCS_BASE_PATH": "../../config/2020/out/validations/data_docs",
        "TIME_PERIOD_SPAN_LOWER": 2020 - 10,
        "TIME_PERIOD_SPAN_UPPER": 2020
    },
    "2023": {
        "data_path": "config/2023/out/data/indicator",
        "data_connector_name": "indicators_2023",
        "VALIDATIONS_STORE_BASE_PATH": "../../config/2023/out/validations/validations",
        "DATA_DOCS_BASE_PATH": "../../config/2023/out/validations/data_docs",
        "TIME_PERIOD_SPAN_LOWER": 2023 - 10,
        "TIME_PERIOD_SPAN_UPPER": 2023
    }
}

config = configs[sys.argv[1]]

data_context: FileDataContext = get_context(
    context_root_dir=Path(pwd) / "validation/great_expectations",
    runtime_environment={"VALIDATIONS_STORE_BASE_PATH": config["VALIDATIONS_STORE_BASE_PATH"],
                         "DATA_DOCS_BASE_PATH": config["DATA_DOCS_BASE_PATH"],
                         }
)


validation_filter_regex = sys.argv[2] if len(sys.argv) > 2 else ".*"  # The first is the program name itself
validation_filter_regex = re.compile(validation_filter_regex)
validation_batches = []
for indicator_file_name in os.listdir(config["data_path"]):
    if validation_filter_regex.match(indicator_file_name):
        validation_batches.append(
            {
                "batch_request": BatchRequest(
                    datasource_name="indicators",
                    data_connector_name=config["data_connector_name"],
                    data_asset_name=indicator_file_name,
                    batch_spec_passthrough={
                        #Because of .... To present in data docs correctly
                        "data_asset_name": indicator_file_name,
                        "reader_options": {"sep": ";"}}
                )
            }
        )

result: CheckpointResult = data_context.run_checkpoint(
    checkpoint_name="sdmx_indicator_check",
    validations=validation_batches,
    evaluation_parameters={
        "TIME_PERIOD_SPAN_LOWER": config["TIME_PERIOD_SPAN_LOWER"],
        "TIME_PERIOD_SPAN_UPPER": config["TIME_PERIOD_SPAN_UPPER"],
    },
    run_name="ManualEvaluationRun",
)

if not result["success"]:
    print("Validation failed!")
    sys.exit(1)

print("Validation succeeded!")
sys.exit(0)
