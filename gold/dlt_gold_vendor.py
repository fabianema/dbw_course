#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *

# source table
source_table = "silver_vendor"

@dlt.table(
    name="gold_vendor"
)
def create_gold_table():

    # read silver table as scd_type_1
    df = dlt.read(source_table).where("__END_AT is null")

    return df

create_gold_table()