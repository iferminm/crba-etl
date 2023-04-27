#!/bin/env python
import pandas as pd

# path = "docs/indicator_dictionary.xlsx"
writer = pd.ExcelWriter("docs/indicator_dictionary.xlsx", engine="xlsxwriter")

source_definition = pd.read_json("etl/resources/source_definitions.json", orient="index").reset_index(names="SOURCE_ID")
indicator_definitions = pd.read_json("etl/resources/indicator_definitions.json")
value_types = pd.read_json("etl/resources/value_type.json")

source_definition.to_excel(writer, sheet_name="Source", header=True, index=False)
indicator_definitions.to_excel(writer, sheet_name="Indicator", header=True, index=False)
value_types.to_excel(writer, sheet_name="Value_type", header=True, index=False)

Snapshot_2020 = pd.read_json("config/2020/in/source_selection.json", orient="index").reset_index(names="SOURCE_ID")
Snapshot_2020["INDICATOR"] = Snapshot_2020.index.map(
    lambda idx: f"=VLOOKUP($Snapshot_2020.C{idx + 2},$Indicator.$A$2:$G$200,7,0)")
Snapshot_2020.to_excel(writer, sheet_name="Snapshot_2020", header=True, index=False)

crba_report_definition = (
    source_definition.merge(
        right=indicator_definitions,
        on="INDICATOR_ID",
        how="left",
        suffixes=(None, "_overwritten"),
        # Via source selection there shoulb be one source selected for each indicator
        validate="one_to_one"
    )
    .merge(
        right=value_types,
        on="VALUE_ID",
        how="left",
        suffixes=(None, "_overwritten"),
        # Multiple Sources can have the same Value ID means can be mapped similarly
        validate="many_to_one"
    )
)

crba_report_definition.to_markdown("docs/source_definitions.md")

writer.close()
