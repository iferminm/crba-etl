import pandas as pd
from importlib.resources import files as res_files


with res_files("etl.resources") as path:
    country_full_list = pd.read_json(
            path / "all_countrynames_list.json"
        ).drop_duplicates()

    country_iso_list = country_full_list.drop_duplicates(
        subset="COUNTRY_ISO_2"
    )

    country_crba_list = pd.read_json(
        path / "crba_country_list.json",
    ).merge(
        right=country_iso_list[["COUNTRY_ISO_2", "COUNTRY_ISO_3"]],
        how="left",
        on="COUNTRY_ISO_3",
        validate="one_to_one",
    )