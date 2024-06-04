# Databricks notebook source
from libraries.bronze.bronze_helper import Bronze as bronze
from libraries.common.common_functions import Common as common
from libraries.common import utils
from libraries.common import json_flatter

spark.conf.set('spark.sql.caseSensitive', True)

# COMMAND ----------

def read_ny_articles_write_to_bronze():
    """
    Read New York Times articles from the landing area, flatten the JSON structure, 
    handle duplicate column names, and write the data to the bronze layer in Delta format.

    This function performs the following steps:
    1. Reads the JSON files containing New York Times articles from the landing area.
    2. Flattens the nested JSON structure to create a tabular format.
    3. Renames any duplicate column names to ensure uniqueness.
    4. Constructs the path and table name for the bronze layer storage.
    5. Writes the flattened DataFrame to the bronze layer in Delta format, 
    overwriting any existing data and schema if necessary.

    Returns:
        None
    """
    df = bronze.read_files_from_landing(type = "json", folder_name="articles")

    df_flattened = json_flatter.flatten_json(df)
    df_flattened = utils.rename_duplicate_columns(df_flattened)

    path = f"{common.get_bronze_path()}articles"
    table_name = f"{common.get_catalog_name()}.bronze.articles"

    df_flattened.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("path", path).saveAsTable(table_name)

# COMMAND ----------

if __name__ == "__main__":
    read_ny_articles_write_to_bronze()
