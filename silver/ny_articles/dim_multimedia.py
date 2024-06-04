# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType

from libraries.common.common_functions import Common as common

def write_dim_multimedia_silver():
    """
    Extract the multimedia dimension from the bronze layer and write it to the silver layer.

    This function performs the following steps:
    1. Reads the raw articles data from the bronze layer.
    2. Extracts the multimedia-related columns to create the multimedia dimension.
    3. Removes duplicate records to ensure uniqueness.
    4. Writes the multimedia dimension to the silver layer in Delta format.

    Returns:
        None
    """
    bronze_table_name = f"{common.get_catalog_name()}.bronze.articles"
    bronze_df = spark.read.format("delta").table(bronze_table_name)

    # Extract multimedia dimension
    multimedia_df = bronze_df.select(
        col("response_0_docs_1__id_2").alias("article_id"),
        col("response_0_docs_1_multimedia_2_caption_3").alias("multimedia_caption"),
        col("response_0_docs_1_multimedia_2_credit_3").alias("multimedia_credit"),
        col("response_0_docs_1_multimedia_2_crop_name_3").alias("multimedia_crop_name"),
        col("response_0_docs_1_multimedia_2_height_3").alias("multimedia_height"),
        col("response_0_docs_1_multimedia_2_rank_3").alias("multimedia_rank"),
        col("response_0_docs_1_multimedia_2_subType_3").alias("multimedia_subtype"),
        col("response_0_docs_1_multimedia_2_subtype_3_2").alias("multimedia_subtype_2"),
        col("response_0_docs_1_multimedia_2_type_3").alias("multimedia_type"),
        col("response_0_docs_1_multimedia_2_url_3").alias("multimedia_url"),
        col("response_0_docs_1_multimedia_2_width_3").alias("multimedia_width")
    ).distinct()

    # Write multimedia dimension to silver layer
    multimedia_path = f"{common.get_silver_path()}articles/dim_multimedia"
    multimedia_table_name = f"{common.get_catalog_name()}.silver.dim_multimedia"
    multimedia_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option('path', multimedia_path).saveAsTable(multimedia_table_name)

# COMMAND ----------

if __name__ == "__main__":
    write_dim_multimedia_silver()
