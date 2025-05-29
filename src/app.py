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
DATA_OUTPUT_PATH = 'data'
MAX_WORKERS = 5
BUCKET_NAME = os.getenv('aws_s3_bucket')
STAGE_ROOT_PATH = f's3://{BUCKET_NAME}/{DATA_OUTPUT_PATH}'
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

def filter_source_db_existing_tables(
        database_client: DatabaseClient, 
        db_name: str,
        db_schema: str,
        config_tables: list
    ) -> list:
    
    source_db_tables_list = database_client.get_tables_list(schema_name=db_schema)
    config_file_tables_names = set(table['table_name'] for table in config_tables)
    missing_tables = config_file_tables_names - source_db_tables_list
    if missing_tables:
        _log.warning(f"Tables don't exist on '{db_name}' and will be skipped: {missing_tables}")
        
    return [table for table in config_tables if table['table_name'] in source_db_tables_list]

def main() -> None:

    starting_time = datetime.datetime.now()
    
    _log.info('Starting data extraction...')

    config_files_path = get_config_files_paths(path=CONFIG_FILES_PATH)
    
    cloud_client = CloudClient(bucket_name=BUCKET_NAME)
    snowflake_client = SnowflakeClient(stages_data_path=STAGE_ROOT_PATH)
    file_service_client = FileServiceClient(output_root=DATA_OUTPUT_PATH)
    
    # for each configuration file defined in config directory
    for config_file_path in config_files_path:
        
        config = load_config_file(path=config_file_path)

        config_config_enabled = config.get('config_enabled', False)
        if not config_config_enabled:
            _log.warning('Replication is set to False (config_enabled=false). Skipping...')
            continue

        config_file_format = config.get('file_format', 'csv').lower()
        if config_file_format not in VALID_FILE_FORMATS:
            _log.error(f"Invalid file format '{config_file_format}'. Expected one of {VALID_FILE_FORMATS}")
            continue
        
        config_db_connection = config.get('db_connection')
        config_db_engine = config_db_connection.get('engine').lower()
        config_db_host = config_db_connection.get('host')
        config_db_port = int(config_db_connection.get('port'))
        config_db_username = config_db_connection.get('username')
        config_db_password = os.environ.get(config_db_connection.get('password'))
        config_db_database = config_db_connection.get('database')
        if not all([config_db_engine, config_db_host, config_db_port, 
                    config_db_username, config_db_password, config_db_database]):
            _log.error(
                f"Invalid database connection parameters. Check if configs are set: database='{config_db_database}'"
            )
            continue
        
        database_client = DatabaseClient(db_engine=config_db_engine,
                                         host=config_db_host,
                                         port=config_db_port,
                                         username=config_db_username,
                                         password=config_db_password,
                                         database=config_db_database)

        if not database_client.connection_status:
            continue
        
        if config_db_engine not in VALID_ENGINES:
            _log.error(f"Invalid database engine: {config_db_engine}. Expected one of {VALID_ENGINES}")
            continue
        
        config_tables_configs = config.get('tables', [])
        if not config_tables_configs:
            _log.error(f"No tables were listed to be replicated in {config}")
            continue
        
        config_db_schema = config_db_connection.get('schema', config_db_database)
        
        config_db_schema = config_db_connection.get('schema', config_db_database)
        filtered_config_tables_configs = filter_source_db_existing_tables(
            database_client=database_client, 
            config_tables=config_tables_configs,
            db_name=config_db_database, 
            db_schema=config_db_schema
        )
        
        task_manager_client = TaskManagerClient(database_client=database_client,
                                                file_service_client=file_service_client,
                                                cloud_client=cloud_client,
                                                snowflake_client=snowflake_client,
                                                output_path=DATA_OUTPUT_PATH,
                                                max_workers=MAX_WORKERS,
                                                valid_ingestion_modes=VALID_INGESTION_MODES,
                                                valid_file_formats=VALID_FILE_FORMATS,
                                                upload_remaining_files=UPLOAD_REMAINING_FILES)
        
        task_manager_client.multithreading_job(tables_configs=filtered_config_tables_configs,
                                               file_format=config_file_format)

    ending_time = datetime.datetime.now()
    _log.info(f"Data extraction finished! Total time taken: {ending_time - starting_time}")

if __name__ == '__main__':
    main()