"""Class to compare two final .csv files to export various analyses"""

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt
import numpy as np
import os

class FinalCrbaFileComparator():
    """Class to perform an exploratory analysis of two CRBA final files"""
    def __init__(
            self, 
            df_1: pd.DataFrame, 
            df_2: pd.DataFrame,
            pk_columns: list = [
                "COUNTRY_ISO_3", 
                "INDICATOR_CODE",
                "DIM_SDG_INDICATOR",
                "DIM_AGE_GROUP",
                "DIM_SEX",
                "DIM_SECTOR",
                "DIM_AREA_TYPE",
                "DIM_EDU_LEVEL",
                "DIM_ALCOHOL_TYPE",
                "DIM_QUANTILE",
                "DIM_MATERNAL_EDU_LVL",
                "DIM_OCU_TYPE",
                # "DIM_MANAGEMENT_LEVEL", #TODO This should be uncommented after we found out why this column disappears in 2023
                "DIM_CAUSE_TYPE"
            ]
        ):
        self.df_1=df_1
        self.df_2=df_2
        self.df_1_filtered=self.filter_to_rows_with_scaled_value(self.df_1)
        self.df_2_filtered=self.filter_to_rows_with_scaled_value(self.df_2)
        self.pk_columns=pk_columns
        self.merged_filtered_df = pd.merge(self.df_1_filtered, self.df_2_filtered, on=self.pk_columns, how="inner")

    def filter_to_rows_with_scaled_value(self, df):
        return df[df["SCALED_OBS_VALUE"].notna()]

    def compare_existing_columns(self):
        columns_df_1 = self.df_1.columns
        columns_df_2 = self.df_2.columns
        col_intersection_1 = set(columns_df_1) - set(columns_df_2)
        col_intersection_2 = set(columns_df_2) - set(columns_df_1)

        print(f"Columns that are in df_1 which aren_t in df_2 {col_intersection_1}")
        print(f"Columns that are in df_2 which aren_t in df_1 {col_intersection_2}")

    def compare_number_of_rows(self):
        print(f"The number of rows in df_1 are: {len(self.df_1)}")
        print(f"The number of rows in df_2 are: {len(self.df_2)}")
    
    def compare_column_distribution(
            self, 
            column_name, 
            filter_to_subset_df_1: np.array=None,
            filter_to_subset_df_2: np.array=None
        ):
        if filter_to_subset_df_1 is not None:
            df_1_to_use = self.df_1_filtered[filter_to_subset_df_1]
            df_2_to_use = self.df_2_filtered[filter_to_subset_df_2]
        else:
            df_1_to_use = self.df_1_filtered
            df_2_to_use = self.df_2_filtered
        
        sns.set(style="whitegrid")

        plt.figure(figsize=(12, 6))
        plt.title('Distribution Comparison')
        sns.histplot(data=df_1_to_use, x=column_name, label='Crba 2020', alpha=0.5)
        sns.histplot(data=df_2_to_use, x=column_name, label='Crba 2023', alpha=0.5)

        plt.legend()
        plt.tight_layout()
        plt.show()

    def find_unique_values(self, column_name):
        unique_values_df1 = set(self.df_1[column_name].unique())
        unique_values_df2 = set(self.df_2[column_name].unique())

        unique_values_only_in_df1 = unique_values_df1 - unique_values_df2
        unique_values_only_in_df2 = unique_values_df2 - unique_values_df1

        print("Unique values only in DataFrame  1 ():")
        print(unique_values_only_in_df1)

        print("\nUnique values only in DataFrame 2:")
        print(unique_values_only_in_df2)
    
    def observations(self):
        pass

    def calculate_percentage_of_updated_observations(self):
        count_23_newer_than_20 = sum(self.merged_filtered_df["TIME_PERIOD_x"] < self.merged_filtered_df["TIME_PERIOD_y"])
        count_20_newer_than_23 = sum(self.merged_filtered_df["TIME_PERIOD_x"] > self.merged_filtered_df["TIME_PERIOD_y"])
    
        print(f"This is the total number or rows in the merged_df: {len(self.merged_filtered_df)}")
        print(f"This is the number of observations for which 2020 has a newer value than 2023: {count_20_newer_than_23}")
        print(f"Number of rows where TIME_PERIOD in df_2 is higher than in df_1: {count_23_newer_than_20}")
        print(f"Percentage of rows where TIME_PERIOD in df_2 is higher than in df_1: {round(count_23_newer_than_20/len(self.merged_filtered_df) * 100, 2)}%")

    def get_number_of_NA_per_column_value(self, df, column_for_per_category: str="INDICATOR_CODE"):
        return df.groupby(column_for_per_category).apply(lambda x: (x["OBS_STATUS"] == "O").sum()).sort_values()
    
    def get_number_of_indicators_per_column(self, df):
        unique_indicator_code = self.df_1.groupby("INDICATOR_CODE").first()
        categories=[
            "INDICATOR_INDEX",
            "INDICATOR_CATEGORY",
            "INDICATOR_ISSUE"
        ]

        for i in categories:
            print(f"\n \n This is the number of indicators per {i}")
            print(unique_indicator_code.value_counts(i))
        
        print("\n \n Lastly, this is the numer of indicators per all three of those categories")
        print(unique_indicator_code.value_counts(categories))
    
    def get_top_and_worst_countries_per_category(self, category: str="OVERALL", num: int=5):
        if category== "OVERALL":
            mean_df = self.df_2.groupby("COUNTRY_ISO_3", as_index=False).mean()
            result_top = mean_df.nlargest(num, "OVERALL_SCORE")
            result_bottom = mean_df.nsmallest(num, "OVERALL_SCORE")
            print("Top 5 countries:", result_top)
            print("Bottom 5 countries:", result_bottom)
        elif category == "INDEX":
            mean_df = self.df_2.groupby(["COUNTRY_ISO_3", "INDICATOR_INDEX"], as_index=False).mean()
            #result_top = mean_df.nlargest(num, "OVERALL_SCORE")
            #result_bottom = mean_df.nsmallest(num, "OVERALL_SCORE")

            for i in ["Workplace", "Community and Environment", "Marketplace"]:
                print("Calculating top and bottom countrie for INDICATORINDEX == ", i)
                result_top = mean_df[mean_df["INDICATOR_INDEX"] == i].nlargest(num, "INDEX_SCORE")
                result_bottom = mean_df[mean_df["INDICATOR_INDEX"] == i].nsmallest(num, "INDEX_SCORE")
                print("Top 5 countries:", result_top)
                print("Bottom 5 countries:", result_bottom)
        #elif category == "ISSUE":

        #elif category == "CATEGORY_ISSUE_SCORE":
    
    def compute_country_score_changes(self):
        mean_df_1 = self.df_1.groupby("COUNTRY_ISO_3", as_index=False).mean()
        mean_df_2 = self.df_2.groupby("COUNTRY_ISO_3", as_index=False).mean()
        joined_df_2 = mean_df_1.merge(
            mean_df_2,
            how = "inner",
            on="COUNTRY_ISO_3", 
            suffixes=('_2020', '_2023')
        )
        joined_df_2["OVERALL_SCORE_CHANGE"] = joined_df_2["OVERALL_SCORE_2023"] - joined_df_2["OVERALL_SCORE_2020"]

        result_top = joined_df_2.nlargest(5, "OVERALL_SCORE_CHANGE")
        result_bottom = joined_df_2.nsmallest(5, "OVERALL_SCORE_CHANGE")
        print("Top 5 countries which improved most: \n \n", result_top)
        print("\n \n \n Bottom 5 countries which worsened most: \n \n", result_bottom)

        sns.histplot(data=joined_df_2, x="OVERALL_SCORE_CHANGE", label='Overall score change of countries from 2023 to 2020', alpha=0.5)

        plt.show()







"""

my_comparer.get_top_and_worst_countries_per_category("INDEX")

my_comparer.compute_country_score_changes()


my_comparer.compare_existing_columns()
my_comparer.calculate_percentage_of_updated_observations()
my_comparer.merged_filtered_df[
    my_comparer.merged_filtered_df["TIME_PERIOD_x"] > my_comparer.merged_filtered_df["TIME_PERIOD_y"]
    ].to_csv("temp_crba_merged_age_observations.csv", sep=";")


my_comparer.get_number_of_NA_per_column_value(my_comparer.df_1, "COUNTRY_ISO_3")

my_comparer.df_2[my_comparer.df_2["OBS_STATUS"]=="O"].value_counts("INDICATOR_CODE")


nas_per_indicator_2020 = my_comparer.get_number_of_NA_per_indicator(my_comparer.df_1)
nas_per_indicator_2020

# DONE Distribution of age (filter out NANs)
# DONE Percentage/ list of those indicator for which we have new data
# DONE Check which indicator has been retired/ which one still exists
# SKIPPED Run the same health checks (i.e. Indonesia > Canada) as before
# DONE Percentage of observations for which we have new data
# DONE Distribution comparison of scores
    # DONE Per issues
    # DONE Per category
    # ....
# DONE Number of NAN countries per indicator
# DONE Number of indicators per category/
# DONE Which country has improved/ worsened most? 
# DONE Number of NA observations per country
# DONE Ranking of ountries (Tota and also per index)







len(crba_final_2020.columns)
len(crba_final_2023.columns)

crba_final_2020.columns
crba_final_2023.columns



crba_final_2020.__name__

my_comparer.compare_column_distribution(column_name="TIME_PERIOD")
my_comparer.compare_column_distribution(column_name="SCALED_OBS_VALUE")
my_comparer.find_unique_values("INDICATOR_CODE")
my_comparer.calculate_percentage_of_updated_observations()

# These are output we are intereted in:

"""    