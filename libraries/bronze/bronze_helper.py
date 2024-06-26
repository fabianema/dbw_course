from libraries.common.global_variables import spark
from libraries.common.common_functions import Common as common
from libraries.common import json_flatter

class Bronze:
    @classmethod
    def read_files_from_landing(
        cls, type, folder_name, delimeter_csv=";", include_header=True
    ):
        if type == "csv":
            return cls.read_csv_files_from_landing(folder_name, delimeter_csv, include_header)
        
        if type == "json":
            return cls.read_json_files_from_landing(folder_name)

    @classmethod
    def read_csv_files_from_landing(cls, folder_name, delimeter_csv, include_header):
        return (
            spark.read.option("delimiter", delimeter_csv)
            .option("header", include_header)
            .csv(f"{common.get_landing_path()}{folder_name}")
        )

    @classmethod
    def read_json_files_from_landing(cls, folder_name):
        return(
            spark.read.json(f"{common.get_landing_path()}{folder_name}")
        )