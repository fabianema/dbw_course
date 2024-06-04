# Databricks notebook source
from pyspark.sql import functions as F

from libraries.common.common_functions import Common as common

def generate_gold_kpis():
    """
    Generate a logical gold table for taxi trip KPIs from the silver layer.

    This function performs the following steps:
    1. Reads the taxi trip data from the silver layer.
    2. Calculates KPIs such as total trips, average fare, total revenue, and average trip duration.
    3. Aggregates the KPIs on a daily and monthly basis.
    4. Writes the KPI data to the gold layer in Delta format.

    Returns:
        None
    """
    # Read data from silver layer
    silver_df = spark.table(f"{common.get_catalog_name()}.silver.taxi_trips")
    
    # Calculate KPIs
    kpi_df = (silver_df
              .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
              .groupBy("trip_date")
              .agg(
                  F.count("*").alias("total_trips"),
                  F.avg("fare_amount").alias("avg_fare"),
                  F.sum("total_amount").alias("total_revenue"),
                  F.avg("trip_duration").alias("avg_trip_duration")
              )
             )
    
    # Write daily KPIs to gold layer
    daily_kpi_path = f"{common.get_gold_path()}taxi_trips/daily_taxi_kpis"
    daily_kpi_table_name = f"{common.get_catalog_name()}.gold.daily_taxi_kpis"
    
    kpi_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("path", daily_kpi_path).saveAsTable(daily_kpi_table_name)
    
    # Calculate monthly KPIs
    monthly_kpi_df = (silver_df
                      .withColumn("trip_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
                      .groupBy("trip_month")
                      .agg(
                          F.count("*").alias("total_trips"),
                          F.avg("fare_amount").alias("avg_fare"),
                          F.sum("total_amount").alias("total_revenue"),
                          F.avg("trip_duration").alias("avg_trip_duration")
                      )
                     )
    
    # Write monthly KPIs to gold layer
    monthly_kpi_path = f"{common.get_gold_path()}taxi_trips/monthly_taxi_kpis"
    monthly_kpi_table_name = f"{common.get_catalog_name()}.gold.monthly_taxi_kpis"
    
    monthly_kpi_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("path", monthly_kpi_path).saveAsTable(monthly_kpi_table_name)


# COMMAND ----------

if __name__ == "__main__":
    # Run the function to generate gold KPIs
    generate_gold_kpis()
