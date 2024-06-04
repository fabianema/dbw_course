import pyspark
import re

from collections import defaultdict

def rename_duplicate_columns(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Renames duplicate columns in a PySpark DataFrame by appending '_2' to the last occurrence.

    This function first groups the DataFrame's column names by their lowercase equivalents.
    Then, for each group of columns with the same lowercase name, it renames the last occurrence
    if there are duplicates. The renaming appends '_2' to the original column name.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame with potential duplicate column names.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with duplicate columns renamed.

    Example:
    >>> df = spark.createDataFrame([(1, 2, 3)], ['a', 'A', 'b'])
    >>> renamed_df = rename_duplicate_columns(df)
    The DataFrame 'renamed_df' will have the second 'a' column renamed to 'A_2'.
    """

    grouped_columns = group_columns_lowercase(df)
    df = rename_last_duplicate(df, grouped_columns)

    return df

def group_columns_lowercase(df: pyspark.sql.DataFrame) -> defaultdict(list):
    """
    Groups the column names of a pandas DataFrame by their lowercase equivalents.

    This function takes a pandas DataFrame as input and returns a dictionary where
    each key is a lowercase column name, and the corresponding value is a list of
    all column names (as they appear in the DataFrame) that match this lowercase name.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame whose columns are to be grouped.

    Returns:
        dict: A dictionary with lowercase column names as keys and lists of original
          column names as values.

    Example:
    >>> df = DataFrame(columns=['Name', 'AGE', 'Gender', 'age'])
    >>> group_columns_lowercase(df)
    {'name': ['Name'], 'age': ['AGE', 'age'], 'gender': ['Gender']}
    """
    columns_to_track = defaultdict(list)
    for col in df.columns:
        columns_to_track[col.lower()].append(col)

    return columns_to_track


def rename_last_duplicate(
    df: pyspark.sql.DataFrame, columns_to_track: defaultdict(list)
) -> None:
    """
    Renames the last occurrence of duplicate column names in a PySpark DataFrame.

    This function iterates over a dictionary of column names grouped by their lowercase
    equivalents. If a duplicate is found (based on the lowercase name), the function
    renames the last occurrence of such a column by appending '_2' to its name.

    Args:
        df (pyspark.sql.DataFrame): The DataFrame in which duplicate columns need to be renamed.
        columns_to_track (defaultdict(list)): A dictionary with lowercase column names as keys
                                          and lists of original column names as values.

    Returns:
        pyspark.sql.DataFrame: Returns the modified DataFrame.

    Example:
    >>> df = spark.createDataFrame([(1, 2, 3)], ['a', 'A', 'b'])
    >>> columns_to_track = {'a': ['a', 'A'], 'b': ['b']}
    >>> change_duplicate_column_name(df, columns_to_track)
    The DataFrame 'df' will have its second 'a' column renamed to 'A_2'.
    """
    for col_lower in columns_to_track:
        if duplicate_columns(columns_to_track, col_lower):
            column_to_rename = columns_to_track[col_lower][-1]
            column_to_rename_new_name = f"{column_to_rename}_2"

            df = df.withColumnRenamed(column_to_rename, column_to_rename_new_name)

    return df


def duplicate_columns(columns_to_track, col_lower) -> bool:
    """
    Checks if there are duplicate column names based on their lowercase form.

    Given a dictionary mapping lowercase column names to lists of original column names,
    this function determines if there is more than one original name for a given lowercase
    column name, indicating duplicates.

    Args:
        columns_to_track (defaultdict(list)): A dictionary with lowercase column names as keys
                                            and lists of original column names as values.
        col_lower (str): The lowercase column name to check for duplicates.

    Returns:
        bool: True if there are duplicates, False otherwise.

    Example:
    >>> columns_to_track = {'name': ['Name', 'name', 'NAME']}
    >>> duplicate_columns(columns_to_track, 'name')
    True
    """
    return len(columns_to_track[col_lower]) > 1
