import os
import datetime
import yaml
from dotenv import load_dotenv
from models.database_client import DatabaseClient
from models.task_manager_client import TaskManagerClient
from models.file_service_client import FileServiceClient
from models.cloud_client import CloudClient
from models.snowflake_client import SnowflakeClient
from logs.logger import _log

load_dotenv()

CONFIG_FILES_PATH = 'config'
TMP_LOCAL_PATH = 'data'
MAX_WORKERS = 5
BUCKET_NAME = os.getenv('aws_s3_bucket')
STAGE_ROOT_PATH = f's3://{BUCKET_NAME}/{TMP_LOCAL_PATH}'
VALID_FILE_FORMATS = ['csv', 'parquet']
VALID_INGESTION_MODES = ['incremental', 'full_load']
VALID_ENGINES = ['mysql+pymysql', 'postgresql+psycopg2']
UPLOAD_REMAINING_FILES = True

def get_config_files_paths(
        path: str
    ) -> list[str]:
    
    config_files = [f'{path}/{f}' for f in os.listdir(path) if f.endswith('.yaml')]
    
    if config_files:
        _log.info(f'Found {len(config_files)} configuration files: {", ".join(config_files)}')
        return config_files
    else: 
        _log.error('No configuration files found. Exiting...')
        exit(1)

def load_config_file(
        path: str
    ) -> dict:

    with open(file=path,
              mode='r',
              encoding='utf-8') as stream:
        try:
            parsed_config = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            _log.error(e)
    return parsed_config

def check_if_config_is_disabled(
        config: str,
        config_file_name: str
    ) -> bool:
    
    config_enabled = config.get('config_enabled', True)
    if not config_enabled:
        _log.warning(f"Skipping replication defined in '{config_file_name}' (config_enabled=false)")
        return True

def check_if_extraction_file_config_is_invalid(
        extraction_file_config: dict,
        config_file_name: str
    ) -> bool:
    
    config_file_format = extraction_file_config.get('file_format', 'csv').lower()
    if config_file_format not in VALID_FILE_FORMATS:
        _log.error(f"Invalid file format defined in '{config_file_name}'. "
                   f"Expected one of {VALID_FILE_FORMATS}")
        return True

def check_if_database_connection_config_is_invalid(
        database_connection_config: dict,
        config_file_name: str
    ) -> bool:
    
    if not database_connection_config:
        _log.error(f"No database connection parameters defined in '{config_file_name}'. "
                   "Skipping replication...")
        return True

    config_db_engine = database_connection_config.get('engine').lower()
    if not config_db_engine or config_db_engine not in VALID_ENGINES:
        _log.error(f"Invalid database engine '{config_db_engine}' defined in '{config_file_name}'. "
                   f"Expected one of {VALID_ENGINES}")
        return True

    config_db_host = database_connection_config.get('host')
    config_db_port = int(database_connection_config.get('port'))
    config_db_username = database_connection_config.get('username')
    config_db_password = database_connection_config.get('password')
    config_db_database = database_connection_config.get('database')
    if not all([config_db_host,
                config_db_port,
                config_db_username,
                config_db_password,
                config_db_database]):
        _log.error(f"Database parameters connection missing for '{config_file_name}'. "
                    "Check if required host, port, username, password and database are set")

        return True
    
    config_db_schema = database_connection_config.get('schema')
    if not config_db_schema:
        _log.warning(f"No schema name defined in '{config_file_name}'. "
                     f"Schema is using database name '{config_db_database}' by default")

def check_if_table_config_is_invalid(
        table_config: list[dict],
        source_db_tables: set,
        config_file_name: str
    ) -> list[dict]:

    replicate = table_config.get('replicate', True)
    if not replicate:
        _log.info(f"Skipping table defined in '{config_file_name}' due to replicate=false")
        return True
        
    table_name = table_config.get('table_name')
    if not table_name:
        _log.error(f"Table name '{table_name}' not defined or invalid in '{config_file_name}'. "
                    "Skipping table...")
        return True
    
    file_format = table_config.get('file_format', 'csv').lower()
    if file_format not in VALID_FILE_FORMATS:
        _log.error(f"Invalid file format defined in '{config_file_name}' for table '{table_name}'. "
                   f"Expected one of {VALID_FILE_FORMATS}")
        return True
    
    if table_name not in source_db_tables:
        _log.error(f"Table '{table_name}' defined in '{config_file_name}' " 
                    "does not exist in the source database. Skipping table...")
        return True
    
    ingestion_mode = table_config.get('ingestion_mode')
    if ingestion_mode not in VALID_INGESTION_MODES:
        _log.error(f"Invalid ingestion mode '{ingestion_mode}' defined in '{config_file_name}': " 
                    f"Expected one of {VALID_INGESTION_MODES}. Skipping table...")
        return True
    
    incremental_key = table_config.get('incremental_column')
    if ingestion_mode == 'incremental' and not incremental_key:
        _log.error(f"No incremental key defined for '{table_name}' in '{config_file_name}'. " 
                    f"Skipping table...")
        return True

def main() -> None:

    starting_time = datetime.datetime.now()
    
    _log.info('Starting data extraction...')

    cloud_client = CloudClient(cloud_name='aws', bucket_name=BUCKET_NAME)
    snowflake_client = SnowflakeClient(stages_data_path=STAGE_ROOT_PATH)
    
    for config_file_path in get_config_files_paths(path=CONFIG_FILES_PATH):

        config_file_name = config_file_path.split('/')[-1]
        config = load_config_file(path=config_file_path)
        extraction_file_config = config.pop('extraction_file', {})
        database_connection_config = config.pop('database_connection', {})
        tables_config = config.pop('tables', [])
        
        if check_if_config_is_disabled(
            config=config, 
            config_file_name=config_file_name
        ): continue

        if check_if_extraction_file_config_is_invalid(
            extraction_file_config=extraction_file_config, 
            config_file_name=config_file_name
        ): continue
        
        if check_if_database_connection_config_is_invalid(
            database_connection_config=database_connection_config, 
            config_file_name=config_file_name
        ): continue
        
        file_service_client = FileServiceClient(**extraction_file_config)
        
        try:
            database_client = DatabaseClient(**database_connection_config)
        except:
            _log.error(f"Failed to establish connection. Skipping replication for '{config_file_name}'")
            continue
        
        filtered_tables_configs = []
        for table_config in tables_config:
            
            if check_if_table_config_is_invalid(
                table_config=table_config,
                source_db_tables=database_client.existing_source_tables,
                config_file_name=config_file_name
            ): continue
            filtered_tables_configs.append(table_config)
                
        _log.info(f"Starting extraction of {len(filtered_tables_configs)} tables for '{config_file_name}'")
        TaskManagerClient(
            database_client=database_client,
            file_service_client=file_service_client,
            cloud_client=cloud_client,
            snowflake_client=snowflake_client
        ).start_replication(
            tables_configs=filtered_tables_configs,
            max_workers=MAX_WORKERS
        )

    ending_time = datetime.datetime.now()
    _log.info(f"Data extraction finished! Total time taken: {ending_time - starting_time}")

if __name__ == '__main__':
    main()