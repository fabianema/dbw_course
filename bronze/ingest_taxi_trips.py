# Databricks notebook source
from libraries.bronze.bronze_helper import Bronze as bronze
from libraries.common.common_functions import Common as common

# COMMAND ----------

def read_taxi_trips_write_to_bronze():
    """
    Read taxi trip data from the landing area, process it, and write it to the bronze layer in Delta format.

    This function performs the following steps:
    1. Reads the taxi trip data from the landing area using the specified file type, folder name, and delimiter.
    2. Constructs the path to the bronze layer where the data will be stored.
    3. Constructs the table name for the bronze layer based on the catalog name and table schema.
    4. Writes the DataFrame to the bronze layer in Delta format, overwriting the existing data and schema if necessary.

    Uses the following modules and methods:
    - `bronze.read_files_from_landing`: Reads files from the landing area.
    - `common.get_bronze_path`: Retrieves the path for the bronze storage in Azure Data Lake Storage.
    - `common.get_catalog_name`: Retrieves the catalog name based on the managed resource group.
    - `df.write.format("delta")`: Writes the DataFrame in Delta format.
    - `mode("overwrite")`: Specifies that the existing data should be overwritten.
    - `option("overwriteSchema", "true")`: Specifies that the schema should be overwritten if it exists.
    - `option("path", path)`: Specifies the path where the data should be stored.
    - `saveAsTable(table_name)`: Saves the DataFrame as a table in the specified catalog and schema.

    Returns:
        None
    """
    df = bronze.read_files_from_landing(type = "csv", folder_name="taxi_trip", delimeter_csv = ",")

    path = f"{common.get_bronze_path()}taxi_trips"
    table_name = f"{common.get_catalog_name()}.bronze.taxi_trips"

    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("path", path).saveAsTable(table_name)

# COMMAND ----------

if __name__ == '__main__':
    read_taxi_trips_write_to_bronze()
