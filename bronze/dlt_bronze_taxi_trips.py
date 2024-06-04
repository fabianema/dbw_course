#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *



# Table name 
table_name = "bronze_taxi_trips"

# ingest path
ingest_path = "{}taxi_trip/".format(landing_path)


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
    "delimiter": ",",
    "encoding": "UTF-8",
    "enforceSchema": "true",
    #"mode": "FAILFAST",
}


# schema
schema = """
    VendorID STRING,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    RatecodeID INT,
    store_and_fwd_flag STRING,
    PULocationID INT,
    DOLocationID INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge STRING
"""



def read_raw_data():
    
    create_bronze_streaming_table(
        table_name=table_name,
        ingest_path=ingest_path,
        spark_options=spark_options,
        schema=schema,
    )
    
read_raw_data()



