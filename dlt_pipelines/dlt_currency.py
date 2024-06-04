


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

ingest_path = "abfss://landing@stpocdbw.dfs.core.windows.net/dlt_demo/currency/"

spark_options = {
    "cloudFiles.format": "csv",
    "cloudFiles.useIncrementalListing": "auto",
    "cloudFiles.backfillInterval": "1 week",
    "cloudFiles.inferColumnTypes": "true",
    # csv
    "mergeSchema": "true",
    "delimiter": ";",
    "header": "true",
    "encoding": "UTF-8"
}

schema = """ 
    event_id INT,
    currency_id INT,
    currency STRING,
    value DOUBLE,
    timestamp TIMESTAMP
"""

@dlt.table(
    name="raw_currency"
)
def raw_currency():
     
    return (spark.readStream.format("cloudFiles")
    .options(**spark_options)
    .schema(schema)
    .load(ingest_path)
    ) 
    

def raw_currency_scd2():

    target_table = "raw_currency_scd2"
    source_table = "raw_currency"
    business_keys = ["currency_id"]
    stored_as_scd_type = 2
     
    dlt.create_streaming_table(
        name = target_table,
    )  
    
    dlt.apply_changes(
            target = target_table,
            source = source_table,
            keys = business_keys,
            sequence_by = "timestamp",
            # apply_as_deletes = apply_as_deletes,
            # except_column_list = except_column_list,
            # track_history_except_column_list=track_history_except_column_list,
            stored_as_scd_type = stored_as_scd_type
    )
# Må kalle på funksjonen for å kjøre koden.
raw_currency_scd2()


@dlt.table(
    name="cleansed_currency"
)
def raw_currency():
     
    df = dlt.read("raw_currency_scd2")

    # clean columns etc... 

    return df
    







































