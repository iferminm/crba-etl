#!/bin/env python
import glob
import re

import pandas as pd

# path = "docs/indicator_dictionary.xlsx"
writer = pd.ExcelWriter("docs/indicator_dictionary.xlsx", engine="xlsxwriter")
workbook = writer.book
## Design Soure Definitions Sheet
sources_columns = [
    "SOURCE_ID",
    "FORMER_SOURCE_ID",
    "INDICATOR_ID",
    "VALUE_ID",
    "SOURCE_BODY",
    "SOURCE_TITLE",
    "SOURCE_TYPE",
    "ADDRESS",
]

source_definition = pd.read_json(
    "etl/resources/source_definitions.json", orient="index"
).reset_index(names="SOURCE_ID")[sources_columns]

source_definition.to_excel(writer, sheet_name="Sources", header=True, index=False)

## Design Indicator definitions sheet
indicator_columns = [
    "INDICATOR_ID",
    "INDEX",
    "ISSUE",
    "INDICATOR_NAME",
    "INDICATOR_DESCRIPTION",
    "INDICATOR_EXPLANATION",
]
indicator_definitions = pd.read_json("etl/resources/indicator.json")[indicator_columns]
indicator_definitions.to_excel(
    writer, sheet_name="Indicators", header=True, index=False
)

value_types = pd.read_json("etl/resources/value_type.json")
value_types.to_excel(writer, sheet_name="Value Type", header=True, index=False)


for f in glob.glob("config/*/in"):
    year = re.match("config/(\d{4})/in", f).group(1)
    print(f"Config {year}")
    sheet_name = f"Snapshot_{year}"
    
    snapshot = pd.read_json(
        f"config/{year}/in/source_selection.json", orient="index"
    ).reset_index(names="SOURCE_ID")[["SOURCE_ID"]]
    
    snapshot["FORMER_SOURCE_ID"] = snapshot.index.map( lambda idx : f"=VLOOKUP($A{idx+2},$Sources.A:F,2)")
   
    snapshot["SOURCE_BODY"] = snapshot.index.map( lambda idx : f"=VLOOKUP($A{idx+2},$Sources.A:F,5)")
    snapshot["SOURCE_TITLE"] = snapshot.index.map( lambda idx : f"=VLOOKUP($A{idx+2},$Sources.A:F,6)")
    
    snapshot["INDICATOR_ID"] = snapshot.index.map( lambda idx : f"=VLOOKUP($A{idx+2},$Sources.A:F,3)")
    snapshot["INDICATOR_NAME"] = snapshot.index.map( lambda idx : f"=VLOOKUP($A{idx+2},$Indicators.A:F,4)")


    snapshot.to_excel(writer, sheet_name=sheet_name, header=True, index=False)
    worksheet = writer.sheets[sheet_name]


# source_definition.to_excel(writer, sheet_name="Source", header=True, index=False)
# indicator_definitions.to_excel(writer, sheet_name="Indicator", header=True, index=False)
# value_types.to_excel(writer, sheet_name="Value_type", header=True, index=False)


# ## This part is only computed for configuration which exits
# for f in glob.glob("../config/*/in/"):
#     config_year = re.match("../config/(\d{4})/in/data", f).group(1)
#     snapshot_exits = pd.read_json(
#         f / "source_selection.json", orient="index"
#     ).reset_index(names="SOURCE_ID")

#     snapshot_exits["INDICATOR"] = snapshot_exits.index.map(
#         lambda idx: f"=VLOOKUP($Snapshot_2020.C{idx + 2},$Indicator.$A$2:$G$200,7,0)"
#     )

#     snapshot_exits.to_excel(
#         writer, sheet_name="Snapshot_{config_year}", header=True, index=False
#     )

# # ## This part is only computed for configuration which has build
# # for f in glob.glob("../config/*/out/"):
# #     config_year = re.match("../config/(\d{4})/out/data",f).group(1)
# #     snapshot_compiled = pd.read_json("config/2020/in/source_selection.json", orient="index").reset_index(names="SOURCE_ID")


# ####Markdown Version. In Excel is done ver VSLookUP
# crba_report_definition = source_definition.merge(
#     right=indicator_definitions,
#     on="INDICATOR_ID",
#     how="left",
#     suffixes=(None, "_overwritten"),
#     # Via source selection there shoulb be one source selected for each indicator
#     validate="one_to_one",
# ).merge(
#     right=value_types,
#     on="VALUE_ID",
#     how="left",
#     suffixes=(None, "_overwritten"),
#     # Multiple Sources can have the same Value ID means can be mapped similarly
#     validate="many_to_one",
# )

# crba_report_definition.to_markdown("docs/source_definitions.md")

disclaimer = pd.DataFrame()
disclaimer["DISCLAIMER"] = [
    """This Excel is just for usebility purposes. 
The ground truth can be found in the repositorie: https://github.com/MajorDaxx/crba-etl/blob/preview/etl/resources/indicator.json
Within the repositorie in the config/<year>/out/crba_report_definition.json"""
]

disclaimer.to_excel(writer, sheet_name="Disclaimer", header=True, index=False)

writer.close()
