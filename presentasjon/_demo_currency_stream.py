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
path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/currency/"


for i in range(1,50):
    
    # ct stores current time
    ct = datetime.datetime.now()
    ct_format = ct.strftime('%Y-%m-%d %H:%M:%S')

    currency_id = 1
    
    value = random.uniform(10.5, 11)

    currency = "NOK"

    nok_id = 1

    df = spark.createDataFrame([(i, currency_id, currency, value, ct_format)], ["event_id", "currency_id", "currency", "value", "timestamp"])

    print("{}-{}-{}-{}-{}".format(i, currency_id, currency, value, ct_format))

    export_file(df, file_name=f"currency_{ct_format}", file_path=path, file_format="csv")

    time.sleep(60) 





# COMMAND ----------


