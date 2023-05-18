import pandas as pd 

issus = pd.read_csv("config/2023/out/error.log", sep=";",quotechar="'")
issus.columns = ["SOURCE_ID","ERROR"]
issus["SOURCE_ID"] = issus["SOURCE_ID"].astype(str)

know_issus = pd.read_csv("scripts/know_issus.csv")
know_issus["SOURCE_ID"] = know_issus["SOURCE_ID"].astype(str)

unknown_issus = issus.merge(right=know_issus, on="SOURCE_ID", how="left")
unknown_issus.sort_values(by="JIRA_LINK").to_csv("scripts/unknown_issus.csv")
