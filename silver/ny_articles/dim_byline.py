# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType

from libraries.common.common_functions import Common as common

def write_dim_byline_silver():
    """
    Extract the byline dimension from the bronze layer and write it to the silver layer.

    This function performs the following steps:
    1. Reads the raw articles data from the bronze layer.
    2. Extracts the byline-related columns to create the byline dimension.
    3. Removes duplicate records to ensure uniqueness.
    4. Writes the byline dimension to the silver layer in Delta format.

    Returns:
        None
    """
    # Step 1: Read from Bronze Layer
    bronze_table_name = f"{common.get_catalog_name()}.bronze.articles"
    bronze_df = spark.table(bronze_table_name)

    # Extract byline dimension
    byline_df = bronze_df.select(
        col("response_0_docs_1__id_2").alias("article_id"),
        col("response_0_docs_1_byline_2_original_3").alias("byline"),
        col("response_0_docs_1_byline_2_organization_3").alias("byline_organization"),
        col("response_0_docs_1_byline_2_person_3_firstname_4").alias("byline_firstname"),
        col("response_0_docs_1_byline_2_person_3_lastname_4").alias("byline_lastname"),
        col("response_0_docs_1_byline_2_person_3_middlename_4").alias("byline_middlename"),
        col("response_0_docs_1_byline_2_person_3_qualifier_4").alias("byline_qualifier"),
        col("response_0_docs_1_byline_2_person_3_role_4").alias("byline_role"),
        col("response_0_docs_1_byline_2_person_3_title_4").alias("byline_title")
    ).distinct()

    # Write byline dimension to silver layer
    byline_path = f"{common.get_silver_path()}/articles/dim_byline"
    byline_table_name = f"{common.get_catalog_name()}.silver.dim_byline"
    byline_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option('path', byline_path).saveAsTable(byline_table_name)

# COMMAND ----------

if __name__ == "__main__":
    write_dim_byline_silver()
