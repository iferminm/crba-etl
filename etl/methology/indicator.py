import pandas as pd

from importlib.resources import files as res_files

import re


def get_indicator_info_by_id(indicator_id):
    """
    Return indicator columns in following order
    indicator_name,index,issue,category,indicator_code,indicator_description,indicator_explanation
    """
    row = df_indicator.loc[indicator_id]
    return (
        row["INDICATOR_NAME"],
        row["INDEX"],
        row["ISSUE"],
        row["CATEGORY"],
        row["INDICATOR_CODE"],
        row["INDICATOR_DESCRIPTION"],
        row["INDICATOR_EXPLANATION"],
        row["DIMENSION_VALUES_NORMALIZATION"],
        row["INVERT_NORMALIZATION"]
    )

with res_files("etl.resources") as path:
    df_indicator = pd.read_json(path / "indicator.json")
    
    index_codes = pd.read_json(path / "index_codes.json")
    issue_codes = pd.read_json(path / "issue_codes.json")
    category_codes = pd.read_json(path / "category_codes.json")

df_indicator = (
    df_indicator.merge(
        right=index_codes,
        left_on="INDEX",
        right_on="INDEX",
    )
    .merge(
        right=issue_codes,
        left_on="ISSUE",
        right_on="ISSUE",
    )
    .merge(
        right=category_codes,
        left_on="CATEGORY",
        right_on="CATEGORY",
    )
)

df_indicator = df_indicator.assign(
    INDICATOR_CODE=df_indicator.INDEX_CODE
    + "_"
    + df_indicator.ISSUE_CODE
    + "_"
    + df_indicator.CATEGORY_CODE
    + "_"
    + df_indicator.INDICATOR_ACR
)


indicator_definitions = df_indicator.set_index("INDICATOR_ID")




