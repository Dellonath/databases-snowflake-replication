import datetime
from concurrent.futures import ThreadPoolExecutor
from ..models.database_client import DatabaseClient
from ..models.file_service_client import FileServiceClient
from ..models.cloud_client import (AWSCloudClient, GCPCloudClient)
from ..models.snowflake_client import SnowflakeClient
from ..logs.logger import _log


class TaskManagerClient:

    def __init__(
        self,
        database_client: DatabaseClient,
        cloud_client: AWSCloudClient | GCPCloudClient,
        snowflake_client: SnowflakeClient,
        file_service_client: FileServiceClient
    ) -> None:

        """
        Class to orchestrate the extraction of data from a database and save it to a file

        :param DatabaseClient database_client: The client to interact with the database
        :param AWSCloudClient | GCPCloudClient cloud_client: The client to upload files to cloud
        :param SnowflakeClient snowflake_client: The client to interact with Snowflake
        :param FileServiceClient file_service_client: The client to write/delete files locally
        """

        self.__database_client = database_client
        self.__cloud_client = cloud_client
        self.__snowflake_client = snowflake_client
        self.__file_service_client = file_service_client

    def __create_query(
        self,
        table_name: str,
        ingestion_mode: str,
        incremental_column: str=None,
        where: str=None,
        **kwargs
    ) -> str:

        if ingestion_mode == 'incremental':
            query = F"""
                SELECT 
                    * 
                FROM {table_name} 
                WHERE 
                    {incremental_column} > (SELECT MAX({incremental_column}) FROM {table_name})
                    {'AND '+where if where else ''}'
            """
        elif ingestion_mode == 'full_load':
            query = f"SELECT * FROM {table_name}"

        return query

    def __task_definition(
        self,
        table_name: str,
        ingestion_mode: str,
        incremental_column: str=None,
        where: str=None,
        **kwargs
    ) -> None:

        """
        Define the task to be executed by each task in multi threading

        :param str table_name: The name of the table to extract data from
        :param str ingestion_mode: The ingestion mode of extraction ('incremental' or 'full_load')
        :param str incremental_column (optional): The column to use for incremental extraction
        :param str where (optional): Additional WHERE clause for filtering data
        """

        task_starting_time = datetime.datetime.now()

        database = self.__database_client.database

        query = self.__create_query(table_name=table_name,
                                    ingestion_mode=ingestion_mode,
                                    incremental_column=incremental_column,
                                    where=where,
                                    **kwargs)

        _log.info(f"Starting querying table '{database}.{table_name}' in mode '{ingestion_mode}'")
        data = self.__database_client.execute_query(query=query)

        if not data:
            _log.warning(f"No data extracted from table '{database}.{table_name}'. Stopping task...")
            return
        _log.info(f"Total of {len(data)} rows were extracted from '{database}.{table_name}'")

        # get table columns captured from source database
        table_columns = self.__database_client.tables_columns[table_name]

        # setting the temp directory path to store the files in a temporary location
        file_directory_path = (
            f'{self.__file_service_client.tmp_local_directory}/'
            f'{database}/'
            f'{table_name}'
        )

        # defining the full qualified file path in the temporary location
        timestamp = datetime.datetime.now().strftime(format='%Y%m%d%H%M%S')
        file_path = (
            f'{file_directory_path}/'
            f'{timestamp}.{self.__file_service_client.file_format}'
        )

        self.__file_service_client.write_file(file_path=file_path,
                                              table_data=data,
                                              table_columns=table_columns)

        # uploading file to cloud storage and deleting (or not) after completion
        self.__upload_and_delete_file(file_path=file_path)

        # uploading remaining files wheter the flag is set as true
        if self.__file_service_client.upload_remaining_files:
            self.__upload_remaining_files(files_directory_path=file_directory_path)

        # uploading data to Snowflake
        _log.info(f"Setting up and ingesting data table '{database}.{table_name}' into Snowflake")
        self.__snowflake_client.setup_table_in_snowflake(stage_path=file_directory_path,
                                                         table=table_name)

        task_ending_time = datetime.datetime.now()

        _log.info(f"Task completed for table '{database}.{table_name}'." 
                  f"Total time taken: {task_ending_time - task_starting_time}")

    def __upload_and_delete_file(
        self,
        file_path: str
    ) -> None:
        
        status = self.__cloud_client.upload_file(file_path=file_path)
        if status and self.__file_service_client.exclude_file_after_uploading:
            self.__file_service_client.delete_file(file_path=file_path)
    
    def __upload_remaining_files(
        self,
        files_directory_path: str
    ) -> None:

        """
        A utility function to upload to cloud all remaining files stored in local machine
        
        :param str files_directory_path: The path fot the remaining table files
        """

        remaining_files_paths = self.__file_service_client.list_files_in_directory(
            path=files_directory_path
        )
        for remaining_file_path in remaining_files_paths:
            _log.info(f"Uploading remaining file: '{remaining_file_path}'")
            self.__upload_and_delete_file(file_path=remaining_file_path)

    def start_replication(
        self,
        tables_configs: list,
        max_workers: int=5,
        thread_name_prefix: str='TaskManagerClientThread'
    ) -> None:

        """
        A utility function to run a job in a multithreaded environment

        :param list tables_configs: A list of dictionaries containing table configurations
        :param int max_workers: The maximum number of threads to use for parallel processing
        :param str thread_name_prefix: The prefix for the thread names
        """

        with ThreadPoolExecutor(max_workers=max_workers,
                                thread_name_prefix=thread_name_prefix) as executor:

            tasks = [
                executor.submit(self.__task_definition,
                                **table_config)
                for table_config in tables_configs
            ]

        # wait for all tasks to finish
        for task in tasks:
            task.result()
