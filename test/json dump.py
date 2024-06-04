# Databricks notebook source


# COMMAND ----------



# COMMAND ----------

def export_file(
    df,
    file_name,
    file_path,
    file_format,
    add_timestamp=False,
    compress=False,
):
    if not file_path.endswith("/"):
        file_path = file_path + "/"
    file_path_dest_temp = file_path + ".dir/"

    if add_timestamp:
        file_name = file_name + datetime.datetime.now().strftime("_%Y-%m-%d-%H%M%S")

    if file_format.lower() == "csv":
        if compress:
            file_format = "csv.gz"
            df.repartition(1).write.option("ignoreNullFields", "false").option("sep",";").option(
                "compression", "org.apache.hadoop.io.compress.GzipCodec"
            ).format("csv").save(file_path_dest_temp)
        else:
            df.repartition(1).write.option("ignoreNullFields", "false").option("sep",";").format(
                "csv"
            ).save(file_path_dest_temp, header="true")
    elif file_format.lower() == "json":
        if compress:
            file_format = "json.gz"
            df.repartition(1).write.option("ignoreNullFields", "false").option(
                "compression", "org.apache.hadoop.io.compress.GzipCodec"
            ).format("json").save(file_path_dest_temp)
        else:
            df.repartition(1).write.option("ignoreNullFields", "false").format(
                "json"
            ).save(file_path_dest_temp)
    elif file_format.lower() == "parquet":
        df.repartition(1).write.option("ignoreNullFields", "false").format("parquet").save(file_path_dest_temp)
    else:
        print("Supported formats are csv, json and parquet")

        return

    file_ext = "." + file_format.lower()

    list_files = dbutils.fs.ls(file_path_dest_temp)
    for sub_files in list_files:
        if file_ext in sub_files.name:
            dbutils.fs.cp(
                file_path_dest_temp + sub_files.name, file_path + file_name + file_ext
            )

    dbutils.fs.rm(file_path_dest_temp, recurse=True)

# COMMAND ----------


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
path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/event_stream/"


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

export_df = spark.createDataFrame([(0, "", "", 0, 0)], ["event_id", "stock_id", "price", "currency", "timestamp"])
export_file(export_df, file_name="stockprice_0000", file_path=file_path, file_format="json", add_timestamp=False)



# COMMAND ----------


checkpoint_path = "abfss://landing@stpocdbw.dfs.core.windows.net/event_stream_/_checkpoint/"
file_path = "abfss://landing@stpocdbw.dfs.core.windows.net/event_stream_/"

export_df = spark.createDataFrame([(0, "", "", 0, 0)], ["event_id", "stock_id", "price", "currency", "timestamp"])
export_file(export_df, file_name="stockprice_0000", file_path=file_path, file_format="json", add_timestamp=False)


# schema
schema = """
    event_id INT,
    stock_id STRING,
    price DOUBLE,
    currency STRING,
    timestamp TIMESTAMP
"""

df = (spark.readStream
  .format("cloudFiles") # Auto Loader
  .option("cloudFiles.format", "json") # fil type
  .option("cloudFiles.backfillInterval", "1 day") # guarantee that all files are discovered within a given SLA if data completeness is a requirement
  .option("cloudFiles.schemaLocation", checkpoint_path) # 
  .option("cloudFiles.inferSchema", "true")
  .schema(schema)
  .load(file_path) # adls2
)

display(
    df
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Dårlig data filer som skal expectation skal droppe ved innlesning

# COMMAND ----------




# using datetime module
import datetime
import json
import requests
import json
from pyspark.sql import *
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F
import random
import time


spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

# landing_path = "abfss://landing@stpocdbw.dfs.core.windows.net/"

# def create_event_stream(path, file_name, file_format,):
path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/event_stream/"

# ct stores current time
ct = datetime.datetime.now()
ct_format = ct.strftime('%Y-%m-%d %H:%M:%S')


for i in range(1,20):
    
    # ct stores current time
    ct = datetime.datetime.now()
    ct_format = ct.strftime('%Y-%m-%d %H:%M:%S')

    stock_id = "BITCOIN"
    
    stock_price = -5940403

    currency = "usd"

    nok_id = 1

    df = spark.createDataFrame([(i, stock_id, stock_price, currency, nok_id, ct_format)], ["event_id", "stock_id", "price", "currency", "nok_id", "timestamp"])

    print("{}-{}-{}-{}-{}-{}".format(i, stock_id, stock_price, currency, nok_id, ct_format))
    
    export_file(df, file_name=f"stockprice_{ct_format}", file_path=path, file_format="json")

    time.sleep(3) 







# COMMAND ----------

display(
    df
)

# COMMAND ----------


