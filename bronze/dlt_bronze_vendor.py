#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *




# Table name 
table_name = "bronze_vendor"

# ingest path
ingest_path = "{}vendor/".format(landing_path)


# Auto Loader configs
spark_options = {
    # common options
    "cloudFiles.format": "csv",
    "cloudFiles.useIncrementalListing": "auto",
    "cloudFiles.backfillInterval": "1 week",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.schemaEvolutionMode": "failOnNewColumns",
    "cloudFiles.useNotifications": "false",
    "mergeSchema": "false",
    # file format (csv) options
    "header": "true",
    "delimiter": ";",
    "encoding": "UTF-8",
    "enforceSchema": "true",
    #"mode": "FAILFAST",
}

# schema
schema = """
    VendorID INT,
    name STRING,
    city STRING,
    email STRING,
    ownerID INT,
    operation STRING,
    sequenceNum INT
"""

def read_raw_data():
    
    create_bronze_streaming_table(
        table_name=table_name,
        ingest_path=ingest_path,
        spark_options=spark_options,
        schema=schema,
    )
    
read_raw_data()




