import datetime
from concurrent.futures import ThreadPoolExecutor
from ..models.database_client import DatabaseClient
from ..models.file_service_client import FileServiceClient
from ..models.cloud_client import AWSCloudClient, GCPCloudClient
from ..models.snowflake_client import SnowflakeClient
from ..logs.logger import _log


class TaskManagerClient:

    def __init__(
        self,
        database_client: DatabaseClient,
        file_service_client: FileServiceClient,
        cloud_client: AWSCloudClient | GCPCloudClient=None,
        snowflake_client: SnowflakeClient=None
    ) -> None:

        """
        Class to orchestrate the extraction of data from a database and save it to a file

        :param DatabaseClient database_client: The client to interact with the database
        :param FileServiceClient file_service_client: The client to write/delete files locally
        :param AWSCloudClient | GCPCloudClient cloud_client: The client to upload files to cloud
        :param SnowflakeClient snowflake_client: The client to interact with Snowflake
        """

        self.__database_client = database_client
        self.__cloud_client = cloud_client
        self.__snowflake_client = snowflake_client
        self.__file_service_client = file_service_client

    def __create_query(
        self,
        table_name: str,
        where: str=None,
        **kwargs
    ) -> str:

        query = f'SELECT * FROM {table_name} {f'WHERE {where}' if where else ''}'.strip()

        return query

    def __task_definition(
        self,
        table_name: str,
        where: str=None,
        **kwargs
    ) -> None:

        """
        Define the task to be executed by each task in multi threading

        :param str table_name: The name of the table to extract data from
        :param str where (optional): Where clause for filtering data
        """

        task_starting_time = datetime.datetime.now()

        database = self.__database_client.database

        query = self.__create_query(table_name=table_name,
                                    where=where,
                                    **kwargs)

        _log.info(f"Starting querying table '{database}.{table_name}': '{query}'")
        data = self.__database_client.execute_query(query=query)

        if not data:
            _log.warning(f"No data extracted from table '{database}.{table_name}'. Stopping task...")
            return
        _log.info(f"Total of {len(data)} rows were extracted from '{database}.{table_name}'")

        # get table columns captured from source database
        table_columns = self.__database_client.tables_columns[table_name]

        # setting the temp directory path to store the files in a temporary location
        local_storage_path = (
            f'{self.__file_service_client.local_storage_directory}'
            f'{database}/'
            f'{table_name}'
        )

        cloud_storage_path = (
            f'{self.__cloud_client.cloud_storage_directory}'
            f'{database}/'
            f'{table_name}'
        ) if self.__cloud_client else None

        
        partitionate_data_in_cloud = self.__cloud_client.partitionate_data if self.__cloud_client else False
        # defining the file name
        timestamp = datetime.datetime.now().strftime(format='%Y%m%d%H%M%S')
        file_name = f'{timestamp}.{self.__file_service_client.file_format}'

        self.__file_service_client.write_file(local_storage_path=local_storage_path,
                                              file_name=file_name,
                                              table_data=data,
                                              table_columns=table_columns)

        # creating stage in snowflake
        # should be prior uploading file command, cause PUT command (if applicable) works only for existing stages
        if self.__snowflake_client:
            self.__snowflake_client.create_snowflake_stage(stage_path=cloud_storage_path,
                                                           stage_name=table_name)
        
        # uploading file to cloud storage and deleting (or not) after completion
        self.__upload_orand_put_then_delete_file(local_storage_path=local_storage_path,
                                                 cloud_storage_path=cloud_storage_path,
                                                 file_name=file_name,
                                                 table_name=table_name,
                                                 partitionate=partitionate_data_in_cloud)

        # creating snowflake table using infer schema and schema evolution enabled
        if self.__snowflake_client:
            self.__snowflake_client.create_snowflake_table(table_name=table_name)
        
        # uploading remaining files (files weren't uploaded to cloud or snowflake stages)
        # just upload in case of snowflake or cloud provider defined in configs
        if self.__snowflake_client or self.__cloud_client:
            self.__upload_remaining_files(local_storage_path=local_storage_path,
                                        cloud_storage_path=cloud_storage_path,
                                        table_name=table_name,
                                        partitionate=partitionate_data_in_cloud)

        # uploading data to Snowflake
        if self.__snowflake_client:
            self.__snowflake_client.execute_copy_command(table_name=table_name)

        task_ending_time = datetime.datetime.now()

        _log.info(f"Task completed for table '{database}.{table_name}'. " 
                  f'Total time taken: {task_ending_time - task_starting_time}')

    def __upload_orand_put_then_delete_file(
        self,
        local_storage_path: str,
        cloud_storage_path: str,
        file_name: str,
        table_name: str,
        partitionate: bool=True
    ) -> None:
        
        # partitioning files in cloud
        if cloud_storage_path and partitionate:
            datetime_now = datetime.datetime.now()
            cloud_storage_path += (
                f'/'
                f'year={datetime_now.year}/'
                f'month={datetime_now.month}/'
                f'day={datetime_now.day}'
            )

        status_put = False
        if self.__snowflake_client and self.__snowflake_client.stages_type == 'internal':
            status_put = self.__snowflake_client.execute_put(file_path=local_storage_path,
                                                             file_name=file_name,
                                                             stage_name=table_name)

        status_cloud = False
        if self.__cloud_client:
            status_cloud = self.__cloud_client.upload_file(local_storage_path=local_storage_path,
                                                           cloud_storage_path=cloud_storage_path,
                                                           file_name=file_name)

        if (status_cloud or status_put) and self.__file_service_client.exclude_file_after_uploading:
            self.__file_service_client.delete_file(path=f'{local_storage_path}/{file_name}')

    def __upload_remaining_files(
        self,
        local_storage_path: str,
        cloud_storage_path: str,
        table_name: str,
        partitionate: bool=True
    ) -> None:

        remaining_files = self.__file_service_client.list_files_in_directory(
            path=local_storage_path
        )
        for remaining_file in remaining_files:
            _log.info(f"Uploading remaining file: '{local_storage_path}/{remaining_file}'")
            self.__upload_orand_put_then_delete_file(local_storage_path=local_storage_path,
                                                     cloud_storage_path=cloud_storage_path,
                                                     file_name=remaining_file,
                                                     table_name=table_name,
                                                     partitionate=partitionate)

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
        
        if self.__snowflake_client:
            # creating snowflake's database and schema if not exists and using it
            self.__snowflake_client.setup_database_and_schema()
            # defining file formats able to deal with schema evolution
            self.__snowflake_client.setup_snowflake_file_formats()

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
