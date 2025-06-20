import os
import datetime
import yaml
import json
from dotenv import load_dotenv
from .models.database_client import DatabaseClient
from .models.task_manager_client import TaskManagerClient
from .models.file_service_client import FileServiceClient
from .models.cloud_client import CloudClient
from .models.snowflake_client import SnowflakeClient
from .logs.logger import _log

load_dotenv()

CONFIG = json.loads(open('config.json').read())
CONFIG_FILES_PATH = CONFIG.get('configs_path', 'configs')
VALID_FILE_FORMATS = CONFIG.get('valid_values').get('file_format')
VALID_ENGINES = CONFIG.get('valid_values').get('engine')
VALID_CLOUD_PROVIDERS = CONFIG.get('valid_values').get('cloud_provider')
MAX_WORKERS = CONFIG.get('max_workers', 10)

class Main:

    def __init__(
        self,
        **kwargs
    ) -> None:

        """
        Main application class to orchestrate the data extraction process.
        It loads configuration files, validates them, and starts the data extraction
        """

        starting_time = datetime.datetime.now()
        
        _log.info('Starting data extraction for databases...')
        
        for config_file_path in self.get_config_files_paths(path=CONFIG_FILES_PATH):
            
            starting_extraction_time = datetime.datetime.now()
            
            config_file_name = config_file_path.split('/')[-1]
            config = self.load_config_file(path=config_file_path)

            if self.__validate_if_config_is_disabled(
                config=config, 
                config_file_name=config_file_name
            ): continue

            extraction_file_config = config.pop('extraction_file', {})
            if self.__validate_if_extraction_file_config_is_invalid(
                extraction_file_config=extraction_file_config, 
                config_file_name=config_file_name
            ): continue
            self.__file_service_client = FileServiceClient(**extraction_file_config)

            database_connection_config = config.pop('database_connection', {})
            if self.__validate_if_database_connection_config_is_invalid(
                database_connection_config=database_connection_config, 
                config_file_name=config_file_name
            ): continue
            try:
                self.__database_client = DatabaseClient(**database_connection_config)
            except Exception as e:
                _log.error(e)
                continue

            cloud_config = config.pop('cloud', {})
            if self.__validate_if_cloud_config_is_invalid(
                cloud_config=cloud_config, 
                config_file_name=config_file_name
            ): continue
            self.__cloud_client = CloudClient(**cloud_config) if cloud_config else None

            snowflake_connection_config = config.pop('snowflake_connection', {})
            if self.__validate_if_snowflake_connection_config_is_invalid(
                snowflake_connection_config=snowflake_connection_config, 
                config_file_name=config_file_name
            ): continue
            self.__snowflake_client = SnowflakeClient(file_service_client=self.__file_service_client,
                                                      cloud_client=self.__cloud_client, 
                                                      **snowflake_connection_config) if snowflake_connection_config else None

            tables_config = config.pop('tables', [])
            filtered_tables_configs = []
            for table_config in tables_config:

                if self.__validate_if_table_config_is_invalid(
                    table_config=table_config,
                    source_db_tables=self.__database_client.source_tables,
                    config_file_name=config_file_name
                ): continue
                filtered_tables_configs.append(table_config)

            _log.info(f"Starting extraction of {len(filtered_tables_configs)} tables for '{config_file_name}'")
            self.__task_manager_client = TaskManagerClient(
                database_client=self.__database_client,
                file_service_client=self.__file_service_client,
                cloud_client=self.__cloud_client,
                snowflake_client=self.__snowflake_client
            )
            self.__task_manager_client.start_replication(
                tables_configs=filtered_tables_configs,
                max_workers=MAX_WORKERS
            )

            ending_extraction_time = datetime.datetime.now()

            _log.info(f"Data extraction finished for '{config_file_name}'. "
                      f'Total time taken: {ending_extraction_time - starting_extraction_time}')
        
        ending_time = datetime.datetime.now()
        _log.info(f'Extraction completed for all configs! Total time taken: {ending_time - starting_time}')

    def get_config_files_paths(
        self,
        path: str
    ) -> list[str]:
        
        # ordering configuration files for efficiency
        config_files = sorted([f'{path}{f}' for f in os.listdir(path) if f.endswith('.yaml')])
        
        if config_files:
            _log.info(f'Found {len(config_files)} configuration files: {", ".join(config_files)}')
            return config_files
        else: 
            _log.error('No configuration files found. Exiting...')
            exit(1)

    def load_config_file(
        self,
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

    def __validate_if_snowflake_connection_config_is_invalid(
        self,
        snowflake_connection_config: str,
        config_file_name: str
    ) -> bool:
        
        if not snowflake_connection_config:
            _log.warning(f"No Snowflake connection parameters defined in '{config_file_name}'. "
                         'Replication will assume no Snowflake instance in replication...')
            return False

        config_sf_account = snowflake_connection_config.get('account')
        config_sf_user = snowflake_connection_config.get('user')
        config_sf_password = snowflake_connection_config.get('password')
        config_sf_role = snowflake_connection_config.get('role')
        config_sf_warehouse = snowflake_connection_config.get('warehouse')
        config_sf_database = snowflake_connection_config.get('database')
        config_sf_schema = snowflake_connection_config.get('schema')
        if not all([config_sf_account,
                    config_sf_user,
                    config_sf_password,
                    config_sf_role,
                    config_sf_warehouse,
                    config_sf_database,
                    config_sf_schema]):
            _log.error(f"Snowflake parameters connection missing for '{config_file_name}'. "
                        'Check if required account, user, password, role, warehouse, database and schema are set')
            return True

        config_stages_type = snowflake_connection_config.get('stages_type')
        if config_stages_type not in ['external', 'internal']:
            _log.error(f"Snowflake's parameter '{config_stages_type}' defined in '{config_file_name}' is invalid, "
                        "should be one of ['external', 'internal']. Skipping replication...")
            return True

        config_storage_integration = snowflake_connection_config.get('storage_integration')
        if config_stages_type == 'external' and (self.__cloud_client == None or config_storage_integration == None):
            _log.error(f"Both 'snowflake_connection.storage_integration' and 'cloud' configs are mandatory in case of using external stages. "
                       f"Adjust {config_file_name}' file for correct funcionality. Skipping replication...")
            return True

    def __validate_if_config_is_disabled(
        self,
        config: str,
        config_file_name: str
    ) -> bool:
        
        config_enabled = config.get('config_enabled', True)
        if not config_enabled:
            _log.info(f"Skipping replication defined in '{config_file_name}' due to config_enabled=false")
            return True

    def __validate_if_cloud_config_is_invalid(
        self,
        cloud_config: dict,
        config_file_name: str
    ) -> bool:
        
        if not cloud_config:
            _log.warning(f"No cloud configuration defined in '{config_file_name}'. "
                          "Replication will assume no public cloud to store data...")
            return False

        cloud_provider = cloud_config.get('provider')
        if cloud_provider not in VALID_CLOUD_PROVIDERS:
            _log.error(f"Invalid cloud provider '{cloud_provider}' defined in '{config_file_name}'. "
                       f"Expected one of {VALID_CLOUD_PROVIDERS}, tthers providers aren't supported yet. "
                       f'Create a new model.clouds.<provider>_client.py to support <provider> cloud')
            return True

    def __validate_if_extraction_file_config_is_invalid(
        self,
        extraction_file_config: dict,
        config_file_name: str
    ) -> bool:
        
        config_file_format = extraction_file_config.get('file_format', 'csv').lower()
        if config_file_format not in VALID_FILE_FORMATS:
            _log.error(f"Invalid file format defined in '{config_file_name}'. "
                       f'Expected one of {VALID_FILE_FORMATS}')
            return True

    def __validate_if_database_connection_config_is_invalid(
        self,
        database_connection_config: dict,
        config_file_name: str
    ) -> bool:
        
        if not database_connection_config:
            _log.error(f"No database connection parameters defined in '{config_file_name}'. "
                        'Skipping replication...')
            return True

        config_db_engine = database_connection_config.get('engine').lower()
        if not config_db_engine or config_db_engine not in VALID_ENGINES:
            _log.error(f"Invalid database engine '{config_db_engine}' defined in '{config_file_name}'. "
                       f'Expected one of {VALID_ENGINES}')
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
                        'Check if required host, port, username, password and database are set')
            return True
        
        config_db_schema = database_connection_config.get('schema')
        if not config_db_schema:
            _log.warning(f"No schema name defined in '{config_file_name}'. "
                         f"Schema is using database name '{config_db_database}' by default")

    def __validate_if_table_config_is_invalid(
        self,
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
                        'does not exist in the source database. Skipping table...')
            return True
