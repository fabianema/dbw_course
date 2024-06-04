# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType, TimestampType

from libraries.common.common_functions import Common as common

def transform_and_load_to_silver():
    """
    Transform the taxi trip data from the bronze layer and load it into the silver layer.

    This function performs the following steps:
    1. Reads the taxi trip data from the bronze layer.
    2. Applies transformations:
        - Converts string columns to appropriate data types.
        - Handles null values.
        - Adds calculated columns (e.g., trip_duration).
    3. Writes the transformed data to the silver layer in Delta format.

    Returns:
        None
    """
    # Read data from bronze layer
    bronze_df = spark.table(f"{common.get_catalog_name()}.bronze.taxi_trips")
    
    # Apply transformations
    silver_df = (bronze_df
                 .withColumn("VendorID", bronze_df["VendorID"].cast(IntegerType()))
                 .withColumn("tpep_pickup_datetime", bronze_df["tpep_pickup_datetime"].cast(TimestampType()))
                 .withColumn("tpep_dropoff_datetime", bronze_df["tpep_dropoff_datetime"].cast(TimestampType()))
                 .withColumn("passenger_count", bronze_df["passenger_count"].cast(IntegerType()))
                 .withColumn("trip_distance", bronze_df["trip_distance"].cast(FloatType()))
                 .withColumn("RatecodeID", bronze_df["RatecodeID"].cast(IntegerType()))
                 .withColumn("fare_amount", bronze_df["fare_amount"].cast(FloatType()))
                 .withColumn("extra", bronze_df["extra"].cast(FloatType()))
                 .withColumn("mta_tax", bronze_df["mta_tax"].cast(FloatType()))
                 .withColumn("tip_amount", bronze_df["tip_amount"].cast(FloatType()))
                 .withColumn("tolls_amount", bronze_df["tolls_amount"].cast(FloatType()))
                 .withColumn("improvement_surcharge", bronze_df["improvement_surcharge"].cast(FloatType()))
                 .withColumn("total_amount", bronze_df["total_amount"].cast(FloatType()))
                 .withColumn("congestion_surcharge", bronze_df["congestion_surcharge"].cast(FloatType()))
                 .withColumn("trip_duration", 
                             F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
                 )
    
    # Write to silver layer
    silver_path = f"{common.get_silver_path()}taxi_trips"
    
    silver_table_name = f"{common.get_catalog_name()}.silver.taxi_trips"

    silver_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).option("path", silver_path).saveAsTable(silver_table_name)



# COMMAND ----------

if __name__ == "__main__":
    # Run the transformation and load function
    transform_and_load_to_silver()
