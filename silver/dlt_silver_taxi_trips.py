
#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *

# source table
source_table = "bronze_taxi_trips"

@dlt.table(
    name="silver_taxi_trips"
)
def create_silver_table():
    # read bronze streaming table
    df = dlt.read(source_table)

    

    return df

create_silver_table()