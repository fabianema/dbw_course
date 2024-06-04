# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format

from libraries.common.common_functions import Common as common

def create_articles_by_section():
    """
    Create the articles by section table from the fact and dimension tables.

    This function performs the following steps:
    1. Reads the fact and dimension tables from the silver layer.
    2. Joins the fact table with the headline, byline, and multimedia dimension tables.
    3. Calculates the total articles by section for each day.
    4. Writes the articles by section table to the gold layer in Delta format.

    Returns:
        None
    """
    # Step 1: Read from Silver Layer
    fact_table_name = f"{common.get_catalog_name()}.silver.fact_articles"
    headline_table_name = f"{common.get_catalog_name()}.silver.dim_headline"
    byline_table_name = f"{common.get_catalog_name()}.silver.dim_byline"
    multimedia_table_name = f"{common.get_catalog_name()}.silver.dim_multimedia"

    fact_df = spark.table(fact_table_name)
    headline_df = spark.table(headline_table_name)
    byline_df = spark.table(byline_table_name)
    multimedia_df = spark.table(multimedia_table_name)

    # Step 2: Join Fact and Dimension Tables
    fact_with_headline_df = fact_df.join(headline_df, "article_id", "left")
    fact_with_byline_df = fact_with_headline_df.join(byline_df, "article_id", "left")
    fact_with_all_dimensions_df = fact_with_byline_df.join(multimedia_df, "article_id", "left")

    # Step 3: Calculate Articles by Section
    articles_by_section = fact_with_all_dimensions_df.groupBy(
        date_format("pub_date", "yyyy-MM-dd").alias("date"),
        "section_name"
    ).agg(
        count("*").alias("total_articles")
    )

    # Step 4: Write to Gold Layer
    articles_by_section_path = f"{common.get_gold_path()}articles/articles_by_section"
    articles_by_section_table_name = f"{common.get_catalog_name()}.gold.articles_by_section"
    articles_by_section.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option('path', articles_by_section_path).saveAsTable(articles_by_section_table_name)

# COMMAND ----------

if __name__ == "__main__":
    create_articles_by_section()
