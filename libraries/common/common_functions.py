# modules
from libraries.common.global_variables import spark


class Common:
    _managed_resource_group = spark.conf.get(
        "spark.databricks.clusterUsageTags.managedResourceGroup"
    )

    @classmethod
    def get_catalog_name(cls) -> str:
        """Get the catalog name based on the managed resource

        Returns:
            str: The catalog name.
        """
        if cls._is_environment("dbw-poc"):
            catalog_name = "dev"

        elif cls._is_environment("test"):
            catalog_name = "test"

        elif cls._is_environment("prod"):
            catalog_name = "prod"

        return catalog_name

    @classmethod
    def _is_environment(cls, env_name) -> bool:
        """Check if the managed resource group is in the specified environment.

        Args:
            managed_resource_group (str): The name of the managed resource group.
            env_name (str): The name of the environment to check for.

        Returns:
            bool: True if the managed resource group is in the specified environment, False otherwise.
        """
        return env_name in cls._managed_resource_group

    # --------- Mount Paths ----------
    # General Paths (DBFS)
    @classmethod
    def get_bronze_path(cls):
        """Gets the path for the bronze storage in Azure Data Lake Storage.

        Returns:
            str: The bronze path.
        """
        return f"abfss://bronze@{cls._get_storage_name()}.dfs.core.windows.net/"

    @classmethod
    def _get_storage_name(cls):
        """Get the storage name based on the managed resource group.

        Returns:
            str: The storage name.
        """
        if cls._is_environment("dbw-poc"):
            storage_name = "stpocdbw"

        elif cls._is_environment("test"):
            storage_name = "dlsplatformtestnoeu"

        elif cls._is_environment("prod"):
            storage_name = "dlsplatformprodnoeu"
        else:
            storage_name = "unknown"

        return storage_name

    @classmethod
    def get_silver_path(cls):
        """Gets the path for the silver storage in Azure Data Lake Storage.

        Returns:
            str: The silver path.
        """
        return f"abfss://silver@{cls._get_storage_name()}.dfs.core.windows.net/"

    @classmethod
    def get_gold_path(cls):
        """Gets the path for the gold storage in Azure Data Lake Storage.

        Returns:
            str: The gold path.
        """

        return f"abfss://gold@{cls._get_storage_name()}.dfs.core.windows.net/"

    @classmethod
    def get_landing_path(cls):
        """Gets the path for the landing storage in Azure Data Lake Storage.

        Returns:
            str: The landing path.
        """

        return f"abfss://landing@{cls._get_storage_name()}.dfs.core.windows.net/"

    @classmethod
    def get_config_path(cls):
        """Gets the path for the config storage in Azure Data Lake Storage.

        Returns:
            str: The config path.
        """

        return f"abfss://config@{cls._get_storage_name()}.dfs.core.windows.net/"

    # General Paths (ADLS)
    @classmethod
    def get_external_bronze_path(cls):
        """Gets the path for the external bronze storage in Azure Data Lake Storage.

        Returns:
            str: The external bronze path.
        """

        return f"abfss://bronze@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_external_silver_path(cls):
        """Gets the path for the external silver storage in Azure Data Lake Storage.

        Returns:
            str: The external silver path.
        """

        return f"abfss://silver@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_external_gold_path(cls):
        """Gets the path for the external gold storage in Azure Data Lake Storage.

        Returns:
            str: The external gold path.
        """

        return f"abfss://gold@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_external_landing_path(cls):
        """Gets the path for the external landing storage in Azure Data Lake Storage.

        Returns:
            str: The external landing path.
        """

        return f"abfss://landing@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_external_config_path(cls):
        """Gets the path for the external config storage in Azure Data Lake Storage.

        Returns:
            str: The external config path.
        """

        return f"abfss://config@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_external_backup_path(cls):
        """Gets the path for the external backup storage in Azure Data Lake Storage.

        Returns:
            str: The external backup path.
        """

        return f"abfss://backups@{cls._get_storage_name()}.dfs.core.windows.net"

    @classmethod
    def get_path_to_storage(cls, database_name: str, folder_path: str):
        """
        Generate a full path to a specified storage tier in Azure Data Lake Storage.

        This method constructs a full path for storage based on the database tier ('bronze',
        'silver', or 'gold') and a specified folder path. The base paths for each tier are
        retrieved using corresponding class methods.

        Parameters:
        database_name (str): The name of the database tier. It should be one of 'bronze',
                            'silver', or 'gold'.
        folder_path (str): The folder path to append to the base path of the specified database tier.

        Returns:
        str: The full path to the specified storage location. Returns None if an invalid
            database name is provided.
        """
        if database_name == "bronze":
            return f"{cls.get_bronze_path()}{folder_path}"
        if database_name == "silver":
            return f"{cls.get_silver_path()}{folder_path}"
        if database_name == "gold":
            return f"{cls.get_gold_path()}{folder_path}"
        else:
            return None

    @classmethod
    def read_config_files_from_landing(
        cls, delimeter=";", include_header=True
    ):
        return (
            spark.read.option("delimiter", delimeter)
            .option("header", include_header)
            .csv(f"{cls.get_landing_path()}taxi_trip")
        )
