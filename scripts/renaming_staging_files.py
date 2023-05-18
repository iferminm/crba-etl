"""
With the renaming of Source ID the Source Id's of the staging needs to be renamed. 

RUN ONLY ONCE 
"""
import os
import json
import shutil

from pathlib import Path

path_old = "manuel_data/2023/staging"
path_new = "../config/2023/in/data/staging"


with open("../etl/resources/source_definitions.json","r") as f:
    source_def = json.load(f)

source_former_mapper = {source_id:source_def[source_id]["FROMER_SOURCE_ID"] for source_id in source_def.keys() }
former_source_mapper = {former:source for source,former in source_former_mapper.items()}
old_files = os.listdir(path_old)
for source_file in old_files:
    source_name = source_file.replace(".xlsx","")
    new_source_id = former_source_mapper.get(source_name)
    if new_source_id:
        print(f"Copy {source_name} to {new_source_id}")
        shutil.copy(f"{path_old}/{source_name}.xlsx", f"{path_new}/{new_source_id}.xlsx")
    else:
        raise Exception("source_name")