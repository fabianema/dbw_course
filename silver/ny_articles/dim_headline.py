# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType

from libraries.common.common_functions import Common as common

def write_dim_headline_silver():
    """
    Extract the headline dimension from the bronze layer and write it to the silver layer.

    This function performs the following steps:
    1. Reads the raw articles data from the bronze layer.
    2. Extracts the headline-related columns to create the headline dimension.
    3. Removes duplicate records to ensure uniqueness.
    4. Writes the headline dimension to the silver layer in Delta format.
    
    Returns:
        None
    """
    # Step 1: Read from Bronze Layer
    bronze_table_name = f"{common.get_catalog_name()}.bronze.articles"
    bronze_df = spark.read.format("delta").table(bronze_table_name)

    # Extract headline dimension
    headline_df = bronze_df.select(
        col("response_0_docs_1__id_2").alias("article_id"),
        col("response_0_docs_1_headline_2_content_kicker_3").alias("content_kicker"),
        col("response_0_docs_1_headline_2_kicker_3").alias("kicker"),
        col("response_0_docs_1_headline_2_main_3").alias("headline_main"),
        col("response_0_docs_1_headline_2_name_3").alias("headline_name"),
        col("response_0_docs_1_headline_2_print_headline_3").alias("print_headline"),
        col("response_0_docs_1_headline_2_seo_3").alias("seo"),
        col("response_0_docs_1_headline_2_sub_3").alias("headline_sub")
    ).distinct()

    # Write headline dimension to silver layer
    headline_path = f"{common.get_silver_path()}articles/dim_headline"
    headline_table_name = f"{common.get_catalog_name()}.silver.dim_headline"

    headline_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option('path', headline_path).saveAsTable(headline_table_name)

# COMMAND ----------

if __name__ == "__main__":
    write_dim_headline_silver()
