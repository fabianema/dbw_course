# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, date_format

from libraries.common.common_functions import Common as common

def create_monthly_kpis():
    """
    Create the monthly KPIs table from the fact and dimension tables.

    This function performs the following steps:
    1. Reads the fact and dimension tables from the silver layer.
    2. Joins the fact table with the headline, byline, and multimedia dimension tables.
    3. Calculates monthly KPIs such as total articles and average word count.
    4. Writes the monthly KPIs table to the gold layer in Delta format.

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

    # Step 3: Calculate KPIs
    monthly_kpis = fact_with_all_dimensions_df.groupBy(date_format("pub_date", "yyyy-MM").alias("month")) \
        .agg(
            count("*").alias("total_articles"),
            avg("word_count").alias("avg_word_count")
        )

    # Step 4: Write to Gold Layer
    monthly_kpi_path = f"{common.get_gold_path()}articles/monthly_kpis"
    monthly_kpi_table_name = f"{common.get_catalog_name()}.gold.monthly_kpis"
    monthly_kpis.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("path", monthly_kpi_path).saveAsTable(monthly_kpi_table_name)

# COMMAND ----------

if __name__ == "__main__":
    create_monthly_kpis()
