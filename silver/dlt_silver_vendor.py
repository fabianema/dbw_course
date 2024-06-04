#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *

# read source table
source_table = "bronze_vendor_cdc"

@dlt.table(
    name="silver_vendor"
)
def create_silver_table():
    
    df = dlt.read(source_table)

    return df

create_silver_table()

