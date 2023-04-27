import pandas as pd

from importlib.resources import files as res_files
import logging


def get_value_lable_by_value_id(value_id):
    """
    Return indicator columns in following order
    indicator_name,index,issue,category,indicator_code,indicator_description,indicator_explanation
    """
    row = df_value_type.loc[value_id]
    return (
        row["LABELS"],
        row["ENCODING"]
    )


with res_files("etl.resources") as path:
    df_value_type = pd.read_json(path / "value_type.json")

df_value_type = df_value_type.set_index("VALUE_ID")
logging.getLogger(__name__).info("Succesfully build df_indicator")
