# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, lit
from pyspark.sql.types import IntegerType, LongType, StringType, TimestampType

from libraries.common.common_functions import Common as common

def write_fact_articles_silver():
    """
    Extract the main fact table for articles from the bronze layer and write it to the silver layer.

    This function performs the following steps:
    1. Reads the raw articles data from the bronze layer.
    2. Extracts the relevant columns to create the main fact table for articles.
    3. Writes the fact table to the silver layer in Delta format.

    Returns:
        None
    """
    bronze_table_name = f"{common.get_catalog_name()}.bronze.articles"
    bronze_df = spark.read.format("delta").table(bronze_table_name)

    # Extract main fact table
    fact_df = bronze_df.select(
        col("response_0_docs_1__id_2").alias("article_id"),
        col("response_0_docs_1_abstract_2").alias("abstract"),
        col("response_0_docs_1_document_type_2").alias("document_type"),
        col("response_0_docs_1_lead_paragraph_2").alias("lead_paragraph"),
        col("response_0_docs_1_news_desk_2").alias("news_desk"),
        col("response_0_docs_1_pub_date_2").cast(TimestampType()).alias("pub_date"),
        col("response_0_docs_1_section_name_2").alias("section_name"),
        col("response_0_docs_1_snippet_2").alias("snippet"),
        col("response_0_docs_1_source_2").alias("source"),
        col("response_0_docs_1_type_of_material_2").alias("type_of_material"),
        col("response_0_docs_1_uri_2").alias("uri"),
        col("response_0_docs_1_web_url_2").alias("web_url"),
        col("response_0_docs_1_word_count_2").cast(LongType()).alias("word_count")
    )

    # Write fact table to silver layer
    fact_path = f"{common.get_silver_path()}articles/fact_articles"
    fact_table_name = f"{common.get_catalog_name()}.silver.fact_articles"
    fact_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option('path', fact_path).saveAsTable(fact_table_name)

# COMMAND ----------

if __name__ == "__main__":
    write_fact_articles_silver()
