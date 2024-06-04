


# import libraries
import datetime
import json
import random
import time

# pyspark import
from pyspark.sql import *
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F

# eget bibliotek 
from functions.dataframe import *

ingest_path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/event_stream_dlt/"

spark_options = {
    "cloudFiles.format": "json",
    "cloudFiles.useIncrementalListing": "auto",
    "cloudFiles.backfillInterval": "1 week",
    "cloudFiles.inferColumnTypes": "true",
}

schema = """ 
    event_id INT,
    stock_id STRING,
    price DOUBLE,
    currency STRING,
    nok_id INT,
    timestamp TIMESTAMP
"""

@dlt.table(
    name="raw_stock_price"
)
@dlt.expect_all({
    "event_id is not null": "event_id IS NOT NULL", 
    "price greater than 0": "price > 0"
})
def raw_currency():
     
    return (spark.readStream.format("cloudFiles")
    .options(**spark_options)
    .schema(schema)
    .load(ingest_path)
    ) 
    

@dlt.table(
    name="cleansed_stockprices"
)

def cleansed_stockprices():
     
    df = dlt.read("raw_stock_price")

    df = (
        df.withColumn(
            "date", F.to_date(F.col("timestamp"))
        )
    )
    
    return df
    

@dlt.table(
    name="stock_price_nok_daily_avg"
)
def stock_price_nok_daily_avg():
     
    df = dlt.read("cleansed_stockprices")

    df_currency = (
        dlt.read("cleansed_currency").select(
            F.col("currency_id").alias("nok_id"),
            F.col("value").alias("usd_nok"),
            "__START_AT",
            "__END_AT",
        )
    )

    join_clause = [(df.nok_id == df_currency.nok_id)] + [(df_currency.__START_AT <= df.timestamp) & (df_currency.__END_AT >= df.timestamp)]
    
    df = df.join(df_currency, join_clause, "left")

    df = df.withColumn(
        "avg_price", F.expr("price * usd_nok")
    )

    df = df.groupby("date", "usd_nok").agg(F.avg("avg_price").alias("avg_price_nok"))

    return df





































