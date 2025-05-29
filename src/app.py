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


CONFIG_FILES_PATH = 'config'
DATA_OUTPUT_PATH = 'data'
MAX_WORKERS = 5
BUCKET_NAME = os.getenv('aws_s3_bucket')
STAGE_ROOT_PATH = f's3://{BUCKET_NAME}/{DATA_OUTPUT_PATH}'
VALID_FILE_FORMATS = ['csv', 'parquet']
VALID_INGESTION_MODES = ['incremental', 'full_load']
VALID_ENGINES = ['mysql+pymysql', 'postgresql+psycopg2']
UPLOAD_REMAINING_FILES = True

def load_extraction_configs(
        config_file_path: str
    ) -> dict:

    with open(file=config_file_path,
              mode='r',
              encoding='utf-8') as stream:
        try:
            configurations = yaml.safe_load(stream)
        except yaml.YAMLError as e:
            _log.error(e)
    
    return configurations

if __name__ == '__main__':

    starting_time = datetime.datetime.now()
    
    _log.info('Starting data extraction...')

    load_dotenv()
    
    configs_paths = [
        config
        for config in os.listdir(CONFIG_FILES_PATH) if config.endswith('.yaml')]
    if not configs_paths:
        _log.error('No configuration files found. Exiting...')
        exit(1)
    _log.info(f'Found {len(configs_paths)} configuration files: {', '. join(configs_paths)}')    
    
    cloud_client = CloudClient(bucket_name=BUCKET_NAME)
    snowflake_client = SnowflakeClient(stages_data_path=STAGE_ROOT_PATH)
    file_service_client = FileServiceClient(output_root=DATA_OUTPUT_PATH)
    
    # for each configuration file defined in config directory
    for config in configs_paths:
        
        extraction_config = load_extraction_configs(config_file_path=f'{CONFIG_FILES_PATH}/{config}')
        config_config_enabled = extraction_config.get('config_enabled', False)
        config_file_format = extraction_config.get('file_format')
        config_db_connection = extraction_config.get('db_connection')
        config_db_engine = config_db_connection.get('engine')
        config_db_host = config_db_connection.get('host')
        config_db_port = config_db_connection.get('port')
        config_db_username = config_db_connection.get('username')
        config_db_password = os.environ.get(config_db_connection.get('password'))
        config_db_database = config_db_connection.get('database')
        config_db_schema = config_db_connection.get('schema', config_db_database)
        config_tables_configs = extraction_config.get('tables', [])

        database_client = DatabaseClient(db_engine=config_db_engine,
                                         host=config_db_host,
                                         port=config_db_port,
                                         username=config_db_username,
                                         password=config_db_password,
                                         database=config_db_database)

        if not database_client.connection_status:
            continue
        
        if not config_config_enabled:
            _log.warning('Replication is set to False (config_enabled=false). Skipping...')
            continue
            
        if config_file_format not in VALID_FILE_FORMATS:
            _log.error(f"Invalid file format '{config_file_format}'. Expected one of {VALID_FILE_FORMATS}")
            continue
        
        if not all([config_db_engine, config_db_host, config_db_port, config_db_username, config_db_password, config_db_database]):
            _log.error(f"Invalid database connection parameters. Check if configs are set: database='{config_db_database}'")
            continue
        
        if config_db_engine not in VALID_ENGINES:
            _log.error(f"Invalid database engine: {config_db_engine}. Expected one of {VALID_ENGINES}")
            continue
        
        if not config_tables_configs:
            _log.error(f"No tables were listed to be replicated in {config}")
            continue

        # checking if all defined tables in config exist in the database
        db_table_list = database_client.get_tables_list(schema_name=config_db_schema)
        config_table_list = set(table_config['table_name'] for table_config in config_tables_configs)
        tables_comparison = config_table_list - db_table_list
        if tables_comparison:
            _log.warning(f"Tables doesn't exist on source and will be skipped: database='{config_db_database}' missing_tables={tables_comparison}")
        filtered_config_tables_configs = [config for config in config_tables_configs if config['table_name'] in db_table_list]
        
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
