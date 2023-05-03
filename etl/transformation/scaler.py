import logging

import pandas as pd
import numpy as np

log = logging.getLogger(__name__)


def normalizer(
    cleansed_data,
    sql_subset_query_string,
    # dim_cols,
    variable_type="Continuous variable",
    is_inverted="not inverted",
    whisker_factor=1.5,
    raw_data_col="RAW_OBS_VALUE",
    scaled_data_col_name="SCALED_OBS_VALUE",
    maximum_score=10,
    log_info=False,
    country_iso_3_col="COUNTRY_ISO_3",
    time_col="TIME_PERIOD",
    time_period=2020
):
    """Normalize the RAW_OBS_VALUES into indicator scores

    Take the raw observation values and convert them into the actual, normalized indicator scores.
    The score will always be between 0 and 10 (but may also be "No Data")

    There are two distinct cases:

    * Numeric (continuous) variable: Scores are calculate with a standard, statistical normalization.
    The normalization depends on the min and max values of the sample. In most of the data, a raw
    is uniquely identifid not only by the country and year, but also by the values of sveral dimensions
    (e.g. gender). In that case, normalization is only being done for a specific dimension-subgroup, which
    is taken as subsample for normalization.

    * Categorical: Scores are calculated based of encoded raw data, i.e. raw data with values
    such as "yes" or "Ratified", must previously have been encoded into discrete, numeric values.
    In a given dataset, the function will identify the number of categories automatically and
    assign the corresponding scores. The value "0" (both as string or number) will always be
    encoded into "No Data".

    Parameters:
    cleansed_data (obj): Cleansed dataFrame, should be the return of Cleanser class and methods.
    sql_subset_query_string (str): String specifying for which dimension-subgroup the normalization should
    be done. Note: Only applies to numeric, continuous raw data. The string should follow this
    structure: "<column_name_1> == <"value"> & <column_name_2> == <"value"> ...
    variable_type (str): String specifying variable type, should be "Continuous variable" or any other string if categorica
    is_inverted (str): String secifying if a continuous variabl should have inverted scores
    whisker_factor (num): Value to take as whisker factor to define outliers in a distribution
    raw_data_col (str): column name containing raw data
    scaled_data_col_name (str): Desired column name for normalized (scaled) data
    maximum_score (num): Maximum score of the normalized data, typically 10
    log_info (bool): Indicate whether you want to print logging information

    Return:
    DataFrame with a new column called scaled_data_col_name containing the normalized (scaled) data
    """
    # print("\n Calling function 'scaler'... \n")
    # Convert raw_ovs_value to numeric
    #TODO ValueError: Unable to parse string "VALUE WITHOUT MAPPING - PLEASE MAP" at position 1
    raw_obs_value = cleansed_data[raw_data_col]
    cleansed_data[raw_data_col] = pd.to_numeric(raw_obs_value)

    # Create the relevant dimension-subgroup (mostly for continuous variables. However, even though unusual, some vategorical variables also habe dimension values)
    if sql_subset_query_string:
        cleansed_data_subset = cleansed_data.query(sql_subset_query_string)
    else:
        cleansed_data_subset = cleansed_data

    # In some sources, e.g. S-161 and S-186 country_iso_3 "FSM" has two data points
    # Simply take one of the values so as to not break the pipeline
    # Calculate number of country_iso_3 codes without 'FSM', to avoid applying the below code of
    # Dropping duplicats in cases whre there is actual duplicates, and not only FSM
    # For readability: Create variables with conditions
    temp_grouped_series = (
        cleansed_data_subset.loc[
            cleansed_data_subset[country_iso_3_col] != "FSM",
            [country_iso_3_col, raw_data_col],
        ]
        .groupby(country_iso_3_col, as_index=True)
        .count()[raw_data_col]
    )

    # FSM double
    fsm_double_len = len(
        cleansed_data_subset.loc[
            cleansed_data_subset[country_iso_3_col] == "FSM", country_iso_3_col
        ]
    )

    # countries excluding FSM
    countries_not_fsm = len(
        cleansed_data_subset.loc[
            cleansed_data_subset[country_iso_3_col] != "FSM", country_iso_3_col
        ]
    )

    # Calucate average number of countries in subgroup without FSM --> Should be 1
    average_country_number = sum(temp_grouped_series) / len(temp_grouped_series)

    if ((fsm_double_len == 2) & (average_country_number == 1)) | (
        (fsm_double_len == 2) & (countries_not_fsm == 194)
    ):
        log.info(
            "Dataset contains two values for Federated States of Micronesia (ISO3 code 'FSM'). Now taking the first value for FSM to proceed with normalizer."
        )

        # Drop duplicate
        cleansed_data_subset = cleansed_data_subset.drop_duplicates(
            subset=country_iso_3_col
        )
    # End of section to check if its a double-FSM (and only FSM-double)dataset

    # Exclude observations older than 10 years
    #TODO 10 years before now
    cleansed_data_subset = cleansed_data_subset[cleansed_data_subset[time_col] >= time_period-10]

    # Check that there aren't duplicates, which would imply that the dimension-subgroups have not been properly defined and a row in the subset is not unqiuely defined by the dimensions and country
    # If to establish Breakpoint

    assert (
        sum(cleansed_data_subset[country_iso_3_col].duplicated()) == 0
    ), f"There are duplicated countries in the defined dimension-subgroup dataframe."

    if variable_type != "Continuous variable":
        # Build conditions array
        unique_values = cleansed_data_subset[raw_data_col].unique()

        conditions = []
        for value in sorted(unique_values):
            conditions += [cleansed_data_subset[raw_data_col] == value]

        # Assign variable for readability purpose
        length_unique_values = len(cleansed_data_subset[raw_data_col].unique())

        # Build norm values array, where 0 =< norm_values => 10
        norm_values = []
        divisor = 1

        # In the encoding, the value 0 means "No Data/ no reponse/ not answered/ ..."
        if (0 in list(unique_values)) | ("0" in list(unique_values)):
            norm_values += [
                np.nan
            ]  # This is into what "0" is encoded into OBS_SCALED_VALUE
            length_unique_values -= 1

        # Define the normalization type, e.g. 0-5-10, or 0 - 3.3 - 6.6 - 10
        distance = maximum_score / (length_unique_values - divisor)

        for value in range(length_unique_values):
            norm_values += [round(distance * float(value), 2)]

        # store normalized scores in SCALED_OBS_VALUE
        cleansed_data_subset[scaled_data_col_name] = np.select(conditions, norm_values)

        # join normalized data to original dataframe
        cleansed_data = cleansed_data.merge(right=cleansed_data_subset, how="outer")

        # For categorical variables, the value 0 also means No data, so update OBS_STATUS
        cleansed_data.loc[
            (cleansed_data[raw_data_col] == "0") | (cleansed_data[raw_data_col] == 0),
            "OBS_STATUS",
        ] = "O"

    elif variable_type == "Continuous variable":
        # Determine basic descriptive statistics of the distribution that are required for the normalization
        min_val = np.nanmin(cleansed_data_subset[raw_data_col].astype("float"))
        max_val = np.nanmax(cleansed_data_subset[raw_data_col].astype("float"))
        q1 = cleansed_data_subset[raw_data_col].astype("float").quantile(q=0.25)
        q3 = cleansed_data_subset[raw_data_col].astype("float").quantile(q=0.75)
        iqr = q3 - q1

        # Define what max value to use for the normalization
        if max_val > q3 + whisker_factor * iqr:
            max_to_use = q3 + whisker_factor * iqr
            if log_info == True:
                log.info(
                    "The distribution of the raw data values this subgroup contains outliers or is too skewed on the upper end. The maximum value to be used for the normalisation is: 3rd quartile or distribution + {} * IQR. It is: {} \n See histogram printed below for info. \n".format(
                        whisker_factor, max_to_use
                    )
                )
        else:
            max_to_use = max_val
            if log_info == True:
                log.info(
                    "The distribution of the raw data for this subgroup does not contain outliers on the upper end. It is also not too skewed on the upper end. The maximum value used for the normalisation is the maximum value in the dataset, which is {}. This value corresponds to country: {} \n".format(
                        max_to_use,
                        cleansed_data_subset[
                            cleansed_data_subset[raw_data_col].astype("float")
                            == max_val
                        ],
                    )
                )

        # Define what min value to use for the normalization
        if min_val < q1 - whisker_factor * iqr:
            min_to_use = q1 - whisker_factor * iqr
            if log_info == True:
                log.info(
                    "The distribution of the raw data values for this subgroup contains outliers or is too skewed on the lower end. The minimum value to be used for the normalisation is 1st quartile or distribution - {} * IQR. It is: {} \n See histogram printed below for info. \n".format(
                        whisker_factor, min_to_use
                    )
                )
        else:
            min_to_use = min_val
            if log_info == True:
                log.info(
                    "The distribution of the raw data for this subgroup does not contain outliers or is too skewed on the lower end. The minimum value used for the normalisation is the minimum value in the dataset, which is {}. This value corresponds to country: {} \n".format(
                        min_to_use,
                        cleansed_data_subset[
                            cleansed_data_subset[raw_data_col].astype("float")
                            == min_val
                        ],
                    )
                )

        # If there are outliers or a skewed distribution, print the distribution for the user.
        if log_info == True:
            if (min_val < q1 - whisker_factor * iqr) or (
                max_val > q3 + whisker_factor * iqr
            ):
                log.info(
                    f"This is the distribution of the raw data of the indicator: {pd.to_numeric(cleansed_data_subset[raw_data_col]).hist(bins=30)}"
                )

        # Define the value range that is used for the scaling (normalization)
        tot_range = max_to_use - min_to_use

        # Some distributions are so heavily right skewed (e.g. S-188), that tot_range is 0 (because max_to_use is q3 * iqr, but q3 is zero)
        # To avoid division by zero (i.e. when tot_range = 0), put in a infitisemal small value
        if tot_range == 0:
            tot_range = 0.001
            print(
                "The total range was 0. This is probably because the distribution is too heavily skewed to the right. Now setting tot_range to 0.01 to allow for the algorithm to work."
            )

        # Log info
        if log_info == True:
            # TODO make one log statement
            log.info(" \n These are the values taken for the normalization: \n \n ")
            log.info(f"Max Score: {maximum_score}")
            log.info(f"Min to use: {min_to_use}")
            log.info(f"tot range: {tot_range}")
            log.info("Trying out apply function")
            log.info(f"max value: {max_val}")
            log.info(f"min value: {min_val}")
            log.info(f"max to use: {max_to_use}")
            log.info(f"q1: {q1}")
            log.info(f"q3: {q3}")
            log.info(f"iqr: {iqr}")

        # Compute the normalized value of the raw data
        # Distinguish between indicators, whose value must be inverted
        if is_inverted == "inverted":
            # # # Create SCALED_OBS_VALUE
            cleansed_data_subset[scaled_data_col_name] = round(
                maximum_score
                - maximum_score
                * (cleansed_data_subset[raw_data_col].astype("float") - min_to_use)
                / tot_range,
                2,
            )
        elif is_inverted == "not inverted":
            # # # Create SCALED_OBS_VALUE
            cleansed_data_subset[scaled_data_col_name] = round(
                maximum_score
                * (cleansed_data_subset[raw_data_col].astype("float") - min_to_use)
                / tot_range,
                2,
            )
        else:
            raise ValueError(
                "This is a numeric indicator, so you must specify whether or not it is inverted"
            )

        # If outliers are present and max_to_use != max_value OR min_to_use != min_value
        # Scores may be out of range [0; 10], so must round them down
        cleansed_data_subset.loc[
            cleansed_data_subset[scaled_data_col_name] < 0, scaled_data_col_name
        ] = 0
        cleansed_data_subset.loc[
            cleansed_data_subset[scaled_data_col_name] > 10, scaled_data_col_name
        ] = 10

        # join normalized data to original dataframe
        cleansed_data = cleansed_data.merge(right=cleansed_data_subset, how="outer")

    ######## AAdd OBS STATUS
    cleansed_data.loc[
        (cleansed_data["RAW_OBS_VALUE"].isna())
        | (cleansed_data["RAW_OBS_VALUE"].isnull())
        | (cleansed_data["RAW_OBS_VALUE"] == "NaN")
        | (cleansed_data["RAW_OBS_VALUE"] == np.nan),
        "OBS_STATUS",
    ] = "O"

    # Return result
    return cleansed_data
