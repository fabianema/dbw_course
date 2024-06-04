# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC # Hva er Auto Loader

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - Verktøy for å prosessere nye data filer effektivt som blir lagret i skyen (cloud storage)
# MAGIC
# MAGIC - Auto Loader the recommended method for incremental data ingestion.
# MAGIC
# MAGIC - Kan laste inn filer fra AWS (S3), Azure Data Lake Storage (ADLS2), Google (GCS), DBFS
# MAGIC
# MAGIC - Autoloader håndterer fil formatene JSON, CSV, XML, PARQUET, AVRO, ORC, TEXT, BINARYFILE
# MAGIC
# MAGIC - Bruker en Structured Streaming kilde som heter cloudfiles 
# MAGIC   - Gir den lokasjonen der filene blir lagret i skyen
# MAGIC   - Prosesserer nye filer med muligheten for å prosessere eksisterende filer
# MAGIC
# MAGIC - Har støtte for både SQL og Python 
# MAGIC
# MAGIC - Metadataen til filer blir persistert i RocksDB (key-value store)
# MAGIC   - Sikrer at filer blir prosessert nøyaktig en gang 
# MAGIC   - checkpointLocation
# MAGIC   
# MAGIC As files are discovered, their metadata is persisted in a scalable key-value store (RocksDB) in the checkpoint location of your Auto Loader pipeline. This key-value store ensures that data is processed exactly once.
# MAGIC
# MAGIC In case of failures, Auto Loader can resume from where it left off by information stored in the checkpoint location and continue to provide exactly-once guarantees when writing data into Delta Lake. You don’t need to maintain or manage any state yourself to achieve fault tolerance or exactly-once semantics.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### importerer nødvendige funksjoner
# MAGIC

# COMMAND ----------

from pyspark.sql import *
from pyspark.dbutils import DBUtils
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)


# COMMAND ----------

# MAGIC %md
# MAGIC ### Demo - Les CSV fil med AutoLoader

# COMMAND ----------


file_path = "abfss://landing@stpocdbw.dfs.core.windows.net/demo/autloader1"

schema = """
userId INTEGER,
name STRING,
city STRING,
operation STRING,
sequenceNum INTEGER
"""

# Configure Auto Loader to ingest JSON data to a Delta table
df = (spark.readStream
  .format("cloudFiles") # Auto Loader
  .option("cloudFiles.format", "csv") # fil type
  .option("cloudFiles.backfillInterval", "1 day") # guarantee that all files are discovered within a given SLA if data completeness is a requirement
  # csv 
  .option("delimiter", ";")
  .option("header", "true")
  .option("encoding", "UTF-8")
  .schema(schema) # definert skjema
  .load(file_path) # fillokasjon i adls2
)

display(
    df
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Configure schema inference and evolution in Auto Loader
# MAGIC
# MAGIC - Man kan konfigurere Auto Loader til å automatisk oppdage skjema av filen man leser
# MAGIC
# MAGIC - Fordelen er at man slipper å oppdatere skjemaet manuelt hvis nye kolonner blir lagt til
# MAGIC
# MAGIC - Auto Loader kan "redde" (rescue) kolonner som ikke var forventet. Blir lagret som en JSON blob kolonne i dataframen
# MAGIC
# MAGIC - Hvis man bruker Delta Live Table (DLT) håndtere Azure Databricks skjema lokasjoner og andre checkpoints automatisk (viser i demo senere)
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Schema inference
# MAGIC
# MAGIC - Ved å spesifisere cloudfiles.schemaLocation tillater man schema inference og evolution
# MAGIC
# MAGIC - Må spesifisere checkpointLocation der skjema(ene) blir lagret
# MAGIC
# MAGIC - Hvordan inference funker
# MAGIC     - Auto Loader tar en sample av de første 50 GB eller 1000 filer den oppdater (grensen den krysser først)
# MAGIC     - Lagrer skjema informasjonen i en mappe "_schemas" i den spesifiserte schemaLocation for å følge endringer i filene over tid
# MAGIC
# MAGIC
# MAGIC
# MAGIC By default, Auto Loader schema inference seeks to avoid schema evolution issues due to type mismatches. For formats that don’t encode data types (JSON, CSV, and XML), Auto Loader infers all columns as strings (including nested fields in JSON files). For formats with typed schema (Parquet and Avro), Auto Loader samples a subset of files and merges the schemas of individual files. This behavior is summarized in the following table:
# MAGIC
# MAGIC The Apache Spark DataFrameReader uses different behavior for schema inference, selecting data types for columns in JSON, CSV, and XML sources based on sample data. To enable this behavior with Auto Loader, set the option cloudFiles.inferColumnTypes to true.

# COMMAND ----------


checkpoint_path = "abfss://landing@stpocdbw.dfs.core.windows.netdemo/autloader2/_checkpoint/"
file_path = "abfss://landing@stpocdbw.dfs.core.windows.net/demo/autloader2/"

# Configure Auto Loader to ingest csv data with inferSchema
df = (spark.readStream
  .format("cloudFiles") # Auto Loader
  .option("cloudFiles.format", "csv") # fil type
  .option("cloudFiles.schemaLocation", checkpoint_path)  
  .option("cloudFiles.inferSchema", "true")
  
  # .option("cloudFiles.inferColumnTypes", "true") 

  # csv 
  .option("delimiter", ";")
  .option("header", "true")
  .option("encoding", "UTF-8")
  .load(file_path)
  
)
display(df)

# (df.writeStream
#   .option("checkpointLocation", checkpoint_path)
#   .start("dev.bronze.autoloader_demo2")
# )


# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Schema evolution
# MAGIC
# MAGIC - Auto Loader oppdager endringer i skjemaet når den prosesser dataen
# MAGIC
# MAGIC - Når nye kolonner blir oppdager stopper strømmingen med en "UnknownFieldException" error.
# MAGIC
# MAGIC - Før feilmeldingen stopper strømmingen oppdaterer Auto Loader schemaLocation med de nye endringene ved å merge inn nye kolonner til skjemaet
# MAGIC   - Data typene til de eksisterende kolonnene forblir uendret.
# MAGIC
# MAGIC - Databricks anbefaler å konfigurere arbeidsflyter (workflows) til å automatisk restarte etter slike endringer blir oppdaget.
# MAGIC
# MAGIC - Schema Evolution modes options i cloudFiles.schemaEvolutionMode:
# MAGIC
# MAGIC   - **addNewColumns** (default): Stream fails. New columns are added to the schema. Existing columns do not evolve data types.
# MAGIC
# MAGIC   - **rescue**: Schema is never evolved and stream does not fail due to schema changes. All new columns are recorded in the rescued data column.
# MAGIC
# MAGIC   - **failOnNewColumns**: Stream fails. Stream does not restart unless the provided schema is updated, or the offending data file is removed.
# MAGIC
# MAGIC   - **none**: Does not evolve the schema, new columns are ignored, and data is not rescued unless the rescuedDataColumn option is set. Stream does not fail due to schema changes.
# MAGIC   
# MAGIC

# COMMAND ----------


checkpoint_path = "abfss://landing@stpocdbw.dfs.core.windows.net/demo/autoloader3/_checkpoint/"
file_path = "abfss://landing@stpocdbw.dfs.core.windows.net/demo/autoloader3/"

# Configure Auto Loader to ingest CSV data to a Delta table
df = (spark.readStream
  .format("cloudFiles") # Auto Loader
  .option("cloudFiles.format", "csv") # fil type
  .option("cloudFiles.schemaLocation", checkpoint_path) # 
  .option("cloudFiles.inferSchema", "true")
  .option("cloudFiles.inferColumnTypes", "true")
  .option("cloudFiles.schemaEvolutionMode", "addNewColumns") # addNewColumns, rescue
  # csv 
  .option("delimiter", ";")
  .option("header", "true")
  .option("encoding", "UTF-8")
  .load(file_path)
)

display(
    df
)

# Legg til ny fil

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## DEMO
# MAGIC

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

checkpoint_path = "abfss://landing@stpocdbw.dfs.core.windows.net/event_stream/_checkpoint/"
file_path = "abfss://landing@stpocdbw.dfs.core.windows.net/event_stream/"

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
# MAGIC ## DLT DEMO

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


