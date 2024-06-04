#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.dataframe import *


source_table = "bronze_vendor"
target_table = "bronze_vendor_cdc"
business_keys = ["VendorID"]
except_column_list = ["operation", "sequenceNum"]

def demo_raw_cdc():

    create_streaming_table_cdc(
        # cdc parameters
        target_table=target_table,
        source_table=source_table,
        business_keys=business_keys,
        except_column_list = except_column_list,
        stored_as_scd_type = 2,
        
        # delta table parameters
        expectations=None,
        table_properties=None,
        comment=None,
    )

demo_raw_cdc()