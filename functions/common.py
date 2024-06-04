import dlt
import pyspark.sql.functions as F
from pyspark.sql import *
from pyspark.dbutils import DBUtils
import datetime

spark = SparkSession.builder.getOrCreate()

dbutils = DBUtils(spark)


# landing_path = dbutils.secrets.get(scope="dataplatform-keyvault", key="landing-path")

landing_path = "abfss://landing@stpocdbw.dfs.core.windows.net/"
