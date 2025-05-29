import datetime
from concurrent.futures import ThreadPoolExecutor
from models.database_client import DatabaseClient
from models.file_service_client import FileServiceClient
from models.cloud_client import CloudClient
from models.snowflake_client import SnowflakeClient
from logs.logger import _log

class TaskManagerClient:

    """Class to orchestrate the extraction of data from a database and save it to a file."""

    def __init__(
        self,
        database_client: DatabaseClient,
        file_service_client: FileServiceClient,
        cloud_client: CloudClient,
        snowflake_client: SnowflakeClient,
        output_path: str,
        max_workers: int,
        valid_ingestion_modes: list,
        valid_file_formats: list,
        upload_remaining_files: bool = True
    ) -> None:

        """
        Initialize the ExtractionOrchestrator with a database client and file writer.
        
        Args:
            database_client (DatabaseClient): The client to interact with the database.
            file_service_client (FileServiceClient): The client to write files.
            cloud_client (CloudClient): The client to upload files to cloud storage.
            snowflake_client (SnowflakeClient): The client to interact with Snowflake.
            output_path (str): The path where the output files will be saved.
            max_workers (int): The maximum number of threads to use for concurrent execution
            valid_ingestion_modes (list): List of valid ingestion modes ('incremental', 'full_load').
            valid_file_formats (list): List of valid file formats ('csv', 'parquet').
            upload_remaining_files (bool): Whether to upload remaining files after processing. Defaults to True.
        """

        self.__database_client = database_client
        self.__file_service_client = file_service_client
        self.__cloud_client = cloud_client
        self.__snowflake_client = snowflake_client
        self.output_path = output_path
        self.max_workers = max_workers
        self.valid_ingestion_modes = valid_ingestion_modes
        self.valid_file_formats = valid_file_formats
        self.upload_remaining_files = upload_remaining_files

    def _create_query(
        self,
        table_name: str,
        ingestion_mode: str,
        incremental_column: str=None
    ) -> str:

        if ingestion_mode == 'incremental':
            query = (
                f"SELECT * FROM {table_name} "
                f"WHERE {incremental_column} > (SELECT MAX({incremental_column}) FROM {table_name})"
            )
        elif ingestion_mode == 'full_load':
            query = f"SELECT * FROM {table_name}"

        return query
   
    def __task_definition(
        self,
        table_name: str,
        ingestion_mode: str,
        incremental_column: str=None,
        file_format: str='parquet'
    ) -> None:

        """
        Define the task to be executed by each task in multi threading.
        
        Args:
            table_name (str): The name of the table to extract data from.
            ingestion_mode (str): The ingestion mode of extraction ('incremental' or 'full_load').
            incremental_column (str, optional): The column to use for incremental extraction.
            file_format (str, optional): The format of the output file ('csv' or 'parquet').
        
        """
        
        database = self.__database_client.database
        
        query = self._create_query(table_name=table_name,
                                   ingestion_mode=ingestion_mode,
                                   incremental_column=incremental_column)

        _log.info(f"Starting querying table: table='{database}.{table_name}' ingestion_mode='{ingestion_mode}'")
        result = self.__database_client.execute_query(query=query)
        
        if result is None:
            return
        
        _log.info(f"Rows were extracted: table='{database}.{table_name}' rows={len(result)}")
    
        table_columns = self.__database_client.get_table_columns(table_name=table_name)
        
        root_file_path = f"{self.output_path}/{database}/{table_name}"
        
        timestamp = datetime.datetime.now().strftime(format='%Y%m%d%H%M%S')
        file_path = f"{root_file_path}/{timestamp}.{file_format}"
        self.__file_service_client.write(file_path=file_path,
                                         table_data=result,
                                         table_columns=table_columns,
                                         file_format=file_format)
        
        # uploading file to cloud storage
        _log.info(f"Uploading file: table='{database}.{table_name}' path='{file_path}'")
        status = self.__cloud_client.upload_file(file_path=file_path)
        # if status:
        #     self.__file_service_client.delete_file(file_path=file_path)
        
        # uploading remaining files if the flag is set
        if self.upload_remaining_files:
            _log.info(f"Uploading remaining files: table='{database}.{table_name}'")
            glob_file_path = f"{root_file_path}/*"
            self.__cloud_client.upload_all_remaining_files(directory_path=glob_file_path)
        else:
            _log.info(f"Skipping uploading remaining files: table='{database}.{table_name}'")
            
        # uploading data to Snowflake
        _log.info(f"Uploading data to Snowflake: table='{database}.{table_name}'")
        self.__snowflake_client.setup_snowflake_stage_schema_table(schema=database,
                                                                   table=table_name,
                                                                   file_format=file_format)
        
        _log.info(f"Task completed: table='{database}.{table_name}'")
        
    def multithreading_job(
            self,
            tables_configs: list,
            file_format: str='csv'
        ) -> None:
        
        """
        A utility function to run a job in a multithreaded environment.
        
        Args:
            task_function (callable): The function to run in a thread.
            tables_configs (list): A list of table configurations to process.
            upload_remaining_files (bool): Whether to upload remaining files after processing. Defaults to True.
            file_format (str): The file format to use for the output files. Defaults to 'csv'.
        """
        
        database = self.__database_client.database
        
        _log.info(f"Starting database extraction: database='{database}' total_tables: {len(tables_configs)}")
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            tasks = []
            for table_config in tables_configs:

                replicate = table_config.get('replicate', True)
                table_name = table_config.get('table_name')
                ingestion_mode = table_config.get('ingestion_mode')
                incremental_key = table_config.get('incremental_column')
                
                # table ingestion validations
                if not replicate:
                    _log.info(f"Skipping table '{database}.{table_name}': replicate=false")
                    continue
                
                if not all([table_name, ingestion_mode]):
                    _log.error(f"Invalid table configuration: {table_config}. Check if configs are set")
                    continue
                
                if ingestion_mode not in self.valid_ingestion_modes:
                    _log.error(f"Invalid ingestion mode '{ingestion_mode}' defined: table='{database}.{table_name}'. Expected {self.valid_ingestion_modes}")
                    continue
                
                if ingestion_mode == 'incremental' and not incremental_key:
                    _log.error(f"No incremental ingestion key defined: table='{database}.{table_name}'")
                    continue

                _log.info(f"Creating extraction's task for '{database}.{table_name}'")
                tasks.append(
                    executor.submit(self.__task_definition,
                                    table_name=table_name,
                                    ingestion_mode=ingestion_mode,
                                    incremental_column=incremental_key,
                                    file_format = file_format)
                )

        # wait for all tasks to finish
        for task in tasks:
            task.result()
