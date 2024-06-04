#!/usr/bin/env python

from typing import Dict
from pyspark.sql import DataFrame as SDF
from pyspark.sql.functions import col, explode_outer


def rename_dataframe_cols(df: SDF, col_names: Dict[str, str]) -> SDF:
    """
    Rename columns of a DataFrame according to a given mapping.

    First, the function handles malformed column names (e.g., names with periods, spaces, or special characters)
    and then renames the columns based on the provided mapping. If a column name does not exist in the mapping,
    it is retained as is.

    Args:
        df (SDF): The DataFrame whose columns are to be renamed.
        col_names (Dict[str, str]): A dictionary mapping old column names to new names.

    Returns:
        SDF: A DataFrame with renamed columns.
    """
    df = rename_malformed_columns(df)
    return df.select(
        *[
            col(col_name).alias(col_names.get(col_name, col_name))
            for col_name in df.columns
        ]
    )


def update_column_names(df: SDF, index: int) -> SDF:
    """
    Update DataFrame column names by appending an index to each column.

    The function creates a new column name for each existing column by appending the given index.
    This is useful for differentiating columns when flattening JSON structures.

    Args:
        df (SDF): The DataFrame whose columns are to be updated.
        index (int): The index to append to each column name.

    Returns:
        SDF: A DataFrame with updated column names.
    """
    df_temp = df
    all_cols = df_temp.columns
    new_cols = dict((column, f"{column}_{index-1}") for column in all_cols)
    df_temp = df_temp.transform(lambda df_x: rename_dataframe_cols(df_x, new_cols))

    return df_temp


def rename_malformed_columns(df: SDF) -> SDF:
    """
    Rename malformed columns in a DataFrame.

    Malformed columns are those with names containing periods, spaces, or special characters.
    These are replaced with underscores or removed to ensure a valid naming convention.

    Args:
        df (SDF): The DataFrame with potentially malformed column names.

    Returns:
        SDF: A DataFrame with renamed columns.
    """
    columns_to_rename = {
        col: col.replace(".", "_")
        .replace(" ", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace("-", "")
        for col in df.columns
        if "." in col
        or " " in col
        or "/" in col
        or "(" in col
        or ")" in col
        or "-" in col
    }
    for col_to_rename in columns_to_rename:
        df = df.withColumnRenamed(col_to_rename, columns_to_rename[col_to_rename])

    return df


def flatten_json(df_arg: SDF, index: int = 1) -> SDF:
    """
    Flatten a JSON structure within a DataFrame.

    This function recursively flattens nested structures in a JSON by expanding arrays and structs.
    During each recursion, it updates and renames columns to reflect their nesting level and path.

    Args:
        df_arg (SDF): The DataFrame containing the JSON structure to be flattened.
        index (int, optional): The starting index for column renaming. Defaults to 1.

    Returns:
        SDF: A DataFrame with the flattened JSON structure.
    """
    df = update_column_names(df_arg, index) if index == 1 else df_arg
    df = rename_malformed_columns(df)
    fields = df.schema.fields

    for field in fields:
        data_type = str(field.dataType)
        column_name = field.name

        first_10_chars = data_type[0:10]

        if first_10_chars == "ArrayType(":
            df_temp = df.withColumn(column_name, explode_outer(col(column_name)))
            return flatten_json(df_temp, index + 1)

        elif first_10_chars == "StructType":
            current_col = column_name

            append_str = current_col

            data_type_str = str(df.schema[current_col].dataType)
            df_temp = (
                df.withColumnRenamed(column_name, column_name + "#1")
                if column_name in data_type_str
                else df
            )
            df_temp = rename_malformed_columns(df_temp)
            current_col = (
                current_col + "#1" if column_name in data_type_str else current_col
            )

            df_before_expanding = df_temp.select(f"{current_col}.*")
            df_before_expanding = rename_malformed_columns(df_before_expanding)
            newly_gen_cols = df_before_expanding.columns

            begin_index = append_str.rfind("_")
            end_index = len(append_str)
            level = append_str[begin_index + 1 : end_index]

            next_level = int(level) + 1
            custom_cols = dict(
                (field, f"{append_str}_{field}_{next_level}")
                for field in newly_gen_cols
            )
            df_temp2 = df_temp.select("*", f"{current_col}.*").drop(current_col)
            df_temp3 = df_temp2.transform(
                lambda df_x: rename_dataframe_cols(df_x, custom_cols)
            )
            return flatten_json(df_temp3, index + 1)

    return df