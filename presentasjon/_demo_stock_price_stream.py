# Databricks notebook source

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

# initialize spark session
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


# hvor filene skal lande
path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/event_stream_dlt/"


for i in range(10,1000):
    
    # ct stores current time
    ct = datetime.datetime.now()
    ct_format = ct.strftime('%Y-%m-%d %H:%M:%S')

    stock_id = "BITCOIN"
    
    stock_price = random.uniform(500000, 600000)

    currency = "usd"

    nok_id = 1

    df = spark.createDataFrame([(i, stock_id, stock_price, currency, nok_id, ct_format)], ["event_id", "stock_id", "price", "currency", "nok_id", "timestamp"])

    print("{}-{}-{}-{}-{}-{}".format(i, stock_id, stock_price, currency, nok_id, ct_format))

    export_file(df, file_name=f"stockprice_{ct_format}", file_path=path, file_format="json")

    time.sleep(3) 





# COMMAND ----------


