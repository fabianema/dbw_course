#Define path for functions folder and import functions and libraries:
import sys
sys.path.insert(1, '/functions')

from functions.common import *

def create_bronze_streaming_table(
    table_name=None,
    ingest_path=None,
    spark_options=None,
    schema=None,

):
    """Function that creates delta streaming table from files in ADSL using cloudfiles format with Auto Loader. """

    @dlt.table(
        name=table_name,
        # comment=comment,
        # table_properties=table_properties,
    )

    # @dlt.expect_all(expectations["keep_expectations"])
    # @dlt.expect_all_or_drop(expectations["drop_expectations"])
    # @dlt.expect_all_or_fail(expectations["fail_expectations"])

    def read_raw_data():
        if schema:
            df = (spark.readStream.format("cloudFiles")
            .options(**spark_options)
            .schema(schema)
            .load(ingest_path)
            ) 
        else:
            df = (spark.readStream.format("cloudFiles")
                .options(**spark_options)
                .load(ingest_path)
                )
        
        # if add_timestamp:
        #     df = df.withColumn(
        #             "zs_data_timestamp",
        #             F.to_timestamp(F.split(df.zx_filename, "[.]")[0][-17:17], "yyyy-MM-dd_HHmmss"),
        #         )

        return df



def create_streaming_table_cdc(
    # cdc parameters
    target_table=None,
    source_table=None,
    business_keys=None,
    sequence_by = F.col("sequenceNum"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    apply_as_truncates = F.expr("operation = 'TRUNCATE'"),
    except_column_list = None,
    track_history_except_columns=None,
    ignore_null_updates = False,
    stored_as_scd_type = 1,

    # delta table parameters
    expectations=None,
    table_properties=None,
    comment=None,
):
    """Function that creates delta streaming table from streaming tables. 

    :param 'business_keys' list of columns which spark will partition on for SCD2 changes
    :param 'comment' comment for the table
    :param 'expectations' list of expectations for data quiality checks for delt tables (keep, drop and fail expectations)
    :param 'source_table' name for table to read data from
    :param 'target_table' name for table which function will create
    :param 'track_history_except_columns' list of columns to exclude from SCD2 changes
    :param 'sequence_by_col' column to sequence by, default is zs_data_timestamp
    :param 'scd_type' type of SCD version for table, default is 2
    """
    # if not sequence_by_col:
    #     sequence_by_col = "zs_data_timestamp"
  
    dlt.create_streaming_table(
            name=target_table,
            # expect_all = expectations["keep_expectations"],
            # expect_all_or_drop = expectations["drop_expectations"],
            # expect_all_or_fail = expectations["fail_expectations"],
            table_properties = table_properties,
        )  
    
    if stored_as_scd_type == 1:
        dlt.apply_changes(
            target = target_table,
            source = source_table,
            keys = business_keys,
            sequence_by = sequence_by,
            apply_as_deletes = apply_as_deletes,
            apply_as_truncates = apply_as_truncates,
            except_column_list = except_column_list,
            stored_as_scd_type = stored_as_scd_type
        )
    else:
        dlt.apply_changes(
            target = target_table,
            source = source_table,
            keys = business_keys,
            sequence_by = sequence_by,
            apply_as_deletes = apply_as_deletes,
            except_column_list = except_column_list,
            stored_as_scd_type = stored_as_scd_type
    )



def export_file(
    df,
    file_name,
    file_path,
    file_format,
    add_timestamp=False,
    compress=False,
):
    if not file_path.endswith("/"):
        file_path = file_path + "/"
    file_path_dest_temp = file_path + ".dir/"

    if add_timestamp:
        file_name = file_name + datetime.datetime.now().strftime("_%Y-%m-%d-%H%M%S")

    if file_format.lower() == "csv":
        if compress:
            file_format = "csv.gz"
            df.repartition(1).write.option("ignoreNullFields", "false").option("sep",";").option(
                "compression", "org.apache.hadoop.io.compress.GzipCodec"
            ).format("csv").save(file_path_dest_temp)
        else:
            df.repartition(1).write.option("ignoreNullFields", "false").option("sep",";").format(
                "csv"
            ).save(file_path_dest_temp, header="true")
    elif file_format.lower() == "json":
        if compress:
            file_format = "json.gz"
            df.repartition(1).write.option("ignoreNullFields", "false").option(
                "compression", "org.apache.hadoop.io.compress.GzipCodec"
            ).format("json").save(file_path_dest_temp)
        else:
            df.repartition(1).write.option("ignoreNullFields", "false").format(
                "json"
            ).save(file_path_dest_temp)
    elif file_format.lower() == "parquet":
        df.repartition(1).write.option("ignoreNullFields", "false").format("parquet").save(file_path_dest_temp)
    else:
        print("Supported formats are csv, json and parquet")

        return

    file_ext = "." + file_format.lower()

    list_files = dbutils.fs.ls(file_path_dest_temp)
    for sub_files in list_files:
        if file_ext in sub_files.name:
            dbutils.fs.cp(
                file_path_dest_temp + sub_files.name, file_path + file_name + file_ext
            )

    dbutils.fs.rm(file_path_dest_temp, recurse=True)


