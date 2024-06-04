#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *

# source table
source_table = "silver_taxi_trips"

@dlt.table(
    name="gold_taxi_trips_monthly_kpi"
)
def create_gold_table():
    # read silver table
    df = dlt.read(source_table)

    # read gold table
    df_vendor = dlt.read("gold_vendor")

    # join tables
    df = df.join(df_vendor, ["VendorID"], "left")

    # create new calc column
    df = (df.withColumn(
        "trip_duration", 
        F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
        ).withColumn(
            "trip_month", 
            F.date_format("tpep_pickup_datetime", "yyyy-MM")
        )
    )
    # Calculate monthly KPIs
    monthly_kpi_df = (
        df
        .groupBy("VendorID", "ownerID", "trip_month")
        .agg(
            F.count("*").alias("total_trips"),
            F.avg("fare_amount").alias("avg_fare"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("trip_duration").alias("avg_trip_duration")
        )
    )


    return monthly_kpi_df

create_gold_table()