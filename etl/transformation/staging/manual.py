from etl.methology.column_mapping import (
    mapping_dict,
    sdmx_df_columns_all,
    sdmx_df_columns_dims,
    sdmx_df_columns_country,
    sdmx_df_columns_time,
    sdmx_df_columns_attr,
)
from etl.methology.country import country_crba_list, country_full_list
from etl.methology.value_mapping import value_mapper
from etl.transformation import cleanse
from etl.transformation import scaler
from etl.methology.indicator import get_indicator_info_by_id
from etl.methology.value_type import get_value_lable_by_value_id



def transform(
    dataframe,
    target_indicator,
    value_id,
    address,
    source_body,
    extraction_methodology,
    source_titel,
    endpoint,
    unit_measure,
    build_year
):

    (
    indicator_name,
    index,
    issue,
    category,
    indicator_code,
    indicator_description,
    indicator_explanation,
    dimension_values_normalization,
    invert_normalization,
    ) = get_indicator_info_by_id(target_indicator)
    value_labels, value_encoding = get_value_lable_by_value_id(value_id)

    # Cleansing
    dataframe = cleanse.extract_who_raw_data(
        raw_data=dataframe,
        variable_type=value_labels,
        display_value_col="Display Value",
    )

    dataframe = cleanse.rename_and_discard_columns(
        raw_data=dataframe,
        mapping_dictionary=mapping_dict,
        final_sdmx_col_list=sdmx_df_columns_all,
    )

    dataframe = cleanse.extract_year_from_timeperiod(
        dataframe=dataframe,
        year_col="TIME_PERIOD",
        time_cov_col="COVERAGE_TIME",
    )

    dataframe = cleanse.retrieve_latest_observation(
        renamed_data=dataframe,
        dim_cols=sdmx_df_columns_dims,
        country_cols=sdmx_df_columns_country,
        time_cols=sdmx_df_columns_time,
        attr_cols=sdmx_df_columns_attr,
    )

    dataframe = cleanse.add_and_discard_countries(
        grouped_data=dataframe,
        crba_country_list=country_crba_list,
        country_list_full=country_full_list,
    )

    dataframe = cleanse.add_cols_fill_cells(
        grouped_data_iso_filt=dataframe,
        dim_cols=sdmx_df_columns_dims,
        time_cols=sdmx_df_columns_time,
        indicator_name_string=indicator_name,
        index_name_string=index,
        issue_name_string=issue,
        category_name_string=category,
        indicator_code_string=indicator_code,
        indicator_source_string=address,
        indicator_source_body_string=source_body,
        indicator_description_string=indicator_description,
        indicator_explanation_string=indicator_explanation,
        indicator_data_extraction_methodology_string=extraction_methodology,
        source_title_string=source_titel,
        source_api_link_string=endpoint,
        attribute_unit_string=unit_measure,
        target_year=build_year
    )

    dataframe = cleanse.map_values(
        cleansed_data=dataframe, value_mapping_dict=value_mapper
    )

    dataframe = cleanse.encode_categorical_variables(
        dataframe=dataframe,
        encoding_string=value_encoding,
        encoding_labels=value_labels,
    )

    dataframe = cleanse.create_log_report_delete_duplicates(cleansed_data=dataframe)

    # Append dataframe to combined dataframe
    # dataframe = combined_cleansed_csv.append(
    #    other = dataframe
    # )

    # Save cleansed data
    # dataframe.to_csv(
    #    data_sources_cleansed / str(source_id + "_cleansed.csv"),
    #    sep = ";")

    # Normalizing
    dataframe = scaler.normalizer(
        cleansed_data=dataframe,
        sql_subset_query_string=dimension_values_normalization,
        # dim_cols=sdmx_df_columns_dims,
        variable_type=value_labels,
        is_inverted=invert_normalization,
        whisker_factor=1.5,
        raw_data_col="RAW_OBS_VALUE",
        scaled_data_col_name="SCALED_OBS_VALUE",
        maximum_score=10,
        time_period=build_year,
    )
    return dataframe
