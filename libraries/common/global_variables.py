import os
import sys
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

sys.path.append(f"{os.getcwd().split('/00_Common')[0]}/cicd-scripts/python")

try:
    import spark_creator
except (ImportError, FileNotFoundError):
    spark_creator = None

def create_spark_session():
    try:
        return SparkSession.builder.master("local[1]").appName("SparkByExamples.com").getOrCreate()
    except Exception as e:
        if spark_creator is not None:
            return spark_creator.spark
        raise ImportError("Cannot import spark_creator, and unable to create a SparkSession.") from e

spark = create_spark_session()
dbutils = DBUtils(spark)