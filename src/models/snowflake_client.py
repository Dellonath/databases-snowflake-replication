import os
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.errors import DatabaseError, ProgrammingError
from .cloud_client import AWSCloudClient, GCPCloudClient
from .file_service_client import FileServiceClient
from ..logs.logger import _log

load_dotenv()

class SnowflakeClient:

    """
    Class to manage connections and execute queries on a Snowflake database
    """

    def __init__(
        self,
        authenticator: str=None,
        private_key_file: str = None,
        private_key_file_pwd: str = None,
        account: str=None,
        user: str=None,
        password: str=None,
        role: str=None,
        warehouse: str='INGESTION_WH',
        raw_database: str=None,
        dwh_database: str=None,
        schema: str=None,
        file_service_client: FileServiceClient=None,
        cloud_client: AWSCloudClient | GCPCloudClient=None,
        storage_integration: str=None,
        stages_type: str='internal',
        tables_prefix: bool=None
    ) -> None:

        """
        Initialize the SnowflakeClient and establish a connection to the Snowflake database.
        This constructor reads the connection parameters from environment variables

        :param str authenticator: Authentication type
        :param str private_key_file: Private key file path
        :param str private_key_file_pwd: Private key file password
        :param str account: Snowflake's account
        :param str user: Snowflake's username
        :param str password: User password
        :param str role: Role to be assumed during connection
        :param str warehouse: Snowflake's warehouse to be used to execute queries
        :param str raw_database: Snowflake's raw layer database name
        :param str dwh_database: Snowflake's curated layer database name
        :param str schema: The database's schema name
        :param FileServiceClient file_service_client: File service client to manipulate data files
        :param AWSCloudClient | GCPCloudClient cloud_client (optional): The cloud interface client
        :param str storage_integration (optional): Provide Snowflake's Storage Integration to be used in case of external stages
        :param str stages_type (optional): Defines which stages type use either 'internal' or 'external'. If external, cloud_client need be provided
        :param str tables_prefix (optional): Prefix to be used in all tables in Snowflake
        """
        
        self.__authenticator = authenticator.lower()
        self.__private_key_file = os.getenv(private_key_file) if private_key_file else None
        self.__private_key_file_pwd = os.getenv(private_key_file_pwd) if private_key_file_pwd else None
        self.__account = os.getenv(account)
        self.__user = os.getenv(user)
        self.__password = os.getenv(password) if password else None
        self.__role = os.getenv(role) if role else None
        self.__warehouse = warehouse.upper()
        self.__raw_database = raw_database.upper()
        self.__dwh_database = dwh_database.upper() if dwh_database else None
        self.__schema = schema.upper()

        # entities
        self.__file_service_client = file_service_client
        self.__cloud_client = cloud_client
        
        # configurations
        self.stages_type = stages_type
        self.tables_prefix = tables_prefix
        self.__storage_integration = storage_integration
        
        # snowflake objects names
        self.__full_qualified_file_format = (
            f'{self.__raw_database}.'
            f'{self.__schema}.'
            f'{self.__file_service_client.file_format.upper()}_SCHEMA_EVOLUTION'
        )
        self.__raw_database_schema = (
            f'{self.__raw_database}.'
            f'{self.__schema}'
        )
        self.__dwh_database_schema = (
            f'{self.__dwh_database}.'
            f'{self.__schema}'
        )

        self.__connection = self.__connect_to_snowflake()

    def execute_query(
        self, 
        query
    ) -> list:

        """
        Execute a query on the Snowflake database

        :param str query: The SQL query to execute
        """

        try:
            with self.__connection.cursor() as cursor:
                cursor.execute(query)
                rows = [row for row in cursor.fetchall()]
            return rows
        except ProgrammingError as e:
            _log.error(f"Error executing query '{query}' in Snowflake: {e}. "
                       f"If it's the first load and the table is empty, you can ignore this error. "
                        "Will be resolved once the table if populated")
            return []

    def execute_put(
        self, 
        file_path: str,
        file_name: str,
        stage: str
    ) -> list:

        """
        Execute PUT command to upload file from local machine to snowflake stage

        :param str file_path: The path to the file
        :param str file_name: The file name
        :param str stage: The stage name
        """

        prefixed_stage = f'{self.tables_prefix}_{stage}' if self.tables_prefix else stage
        full_qualified_stage = f'{self.__raw_database_schema}.{prefixed_stage}'.upper()
        file_to_put_path = f'file://{file_path}/{file_name}'

        try:
            _log.info(f"Uploading file '{file_path}/{file_name}' into stage '@{full_qualified_stage}' using PUT command")
            self.execute_query(f'PUT {file_to_put_path} @{full_qualified_stage}')
            return True
        except:
            _log.error(f"Failed uploading file '{file_path}/{file_name}' into stage '@{full_qualified_stage}' using PUT command. "
                       f"File will be kept in local storage...")
            return False

    def execute_copy_command(
        self,
        table: str
    ) -> None:

        prefix_table = f'{self.tables_prefix}_{table}' if self.tables_prefix else table
        full_qualified_table = f'{self.__raw_database_schema}.{prefix_table}'.upper()

        # delete command executed to 'truncate' table before copying into table
        # real snowflake's truncate command resets the stage bookmarks 
        # (files already loaded are loaded again, duplicating data when executing copy command)
        _log.info(f"Truncating table '{full_qualified_table}' in Snowflake")
        self.execute_query(f'DELETE FROM {full_qualified_table}')

        _log.info(f"Executing COPY command for table '{full_qualified_table}'")
        
        # ON_ERROR set to tolerate up to 1% failed data, otherwise will raise an error
        self.execute_query(f"""
            COPY INTO {full_qualified_table}
                FROM @{full_qualified_table}
                ON_ERROR='SKIP_FILE_1%'
                MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
                FILE_FORMAT={self.__full_qualified_file_format}
        """)

    def create_snowflake_table(
        self,
        table: str
    ) -> None:
        
        prefixed_table = f'{self.tables_prefix}_{table}' if self.tables_prefix else table
        full_qualified_table = f'{self.__raw_database_schema}.{prefixed_table}'.upper()

        _log.info(f"Creating table '{full_qualified_table}' in Snowflake")
        
        # IF NOT EXISTS prevents to copy into table the previous files uploaded to referenced stage
        # replacing the table will create duplicates
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {full_qualified_table}
            USING TEMPLATE (
                    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                    FROM TABLE(
                        INFER_SCHEMA(
                            LOCATION=>'@{self.__raw_database_schema}.{prefixed_table}',
                            FILE_FORMAT=>'{self.__full_qualified_file_format}',
                            IGNORE_CASE=>false
                        )
                    )
                ) ENABLE_SCHEMA_EVOLUTION=true
        """)

    def create_snowflake_dynamic_table(
        self,
        table: str
    ) -> None:

        prefixed_table = (f'{self.tables_prefix}_{table}' if self.tables_prefix else table).upper()
        full_qualified_table = f'{self.__dwh_database_schema}.{prefixed_table}'.upper()

        columns = ', '.join((
            f'"{column[0]}"' for column in self.__list_source_table_columns(database=self.__raw_database,
                                                                            schema=self.__schema, 
                                                                            table=prefixed_table)
        ))

        _log.info(f"Creating dynamic table '{full_qualified_table}' if not exists")
        self.execute_query(f"""
            CREATE DYNAMIC TABLE IF NOT EXISTS {full_qualified_table}
                TARGET_LAG = '30 minutes'
                REFRESH_MODE = FULL
                WAREHOUSE = {self.__warehouse}
                AS SELECT {columns} FROM {self.__raw_database_schema}.{table};
        """)

    def create_snowflake_view(
        self,
        view: str
    ) -> None:

        prefix_view = (f'{self.tables_prefix}_{view}' if self.tables_prefix else view).upper()
        full_qualified_view = f'{self.__dwh_database_schema}.{prefix_view}'.upper()

        columns = ', '.join((
            f'"{column[0]}"' for column in self.__list_source_table_columns(database=self.__raw_database,
                                                                            schema=self.__schema, 
                                                                            table=prefix_view)
        ))

        if self.__dwh_database:
            _log.info(f"Creating view '{full_qualified_view}' if not exists")
            self.execute_query(f"""
                CREATE VIEW IF NOT EXISTS {full_qualified_view} AS
                    SELECT {columns} FROM {self.__raw_database_schema}.{prefix_view};
            """)
        else:
            _log.warning(f"Parameter 'dwh_database' was not passed. Ignoring next layer...")

    def create_snowflake_stage(
        self,
        stage_path: str,
        stage: str
    ) -> None:

        predixed_stage = f'{self.tables_prefix}_{stage}' if self.tables_prefix else stage
        full_qualified_stage = f'{self.__raw_database_schema}.{predixed_stage}'.upper()

        if self.stages_type == 'external':
            stage_url = (
                f'{self.__cloud_client.cloud_storage_prefix}'
                f'{self.__cloud_client.bucket}/'
                f'{stage_path}'
            )

            _log.info(f"Creating external stage if not exists '@{full_qualified_stage}'")
            self.execute_query(f"""
                CREATE STAGE IF NOT EXISTS {full_qualified_stage}
                    STORAGE_INTEGRATION={self.__storage_integration}
                    URL='{stage_url}'
                    FILE_FORMAT={self.__full_qualified_file_format}
            """)
        elif self.stages_type == 'internal':
            _log.info(f"Creating internal stage if not exists '@{full_qualified_stage}'")
            self.execute_query(f"""
                CREATE STAGE IF NOT EXISTS {full_qualified_stage}
                    FILE_FORMAT={self.__full_qualified_file_format}
            """)

    def setup_databases_and_schemas(
        self
    ) -> None:

        self.execute_query(f'CREATE DATABASE IF NOT EXISTS {self.__raw_database}')
        self.execute_query(f'CREATE SCHEMA IF NOT EXISTS {self.__raw_database_schema}')
        if self.__dwh_database:
            self.execute_query(f'CREATE DATABASE IF NOT EXISTS {self.__dwh_database}')
            self.execute_query(f'CREATE SCHEMA IF NOT EXISTS {self.__dwh_database_schema}')

    def setup_snowflake_file_formats(
        self
    ) -> None:

        if self.__file_service_client.file_format.lower() == 'parquet':
            self.execute_query(f"""
                CREATE FILE FORMAT IF NOT EXISTS {self.__full_qualified_file_format}
                TYPE='PARQUET'
            """)
        elif self.__file_service_client.file_format.lower() == 'csv':
            self.execute_query(f"""
                CREATE FILE FORMAT IF NOT EXISTS {self.__full_qualified_file_format}
                TYPE='CSV'
                FIELD_DELIMITER=','
                PARSE_HEADER=true
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                ESCAPE_UNENCLOSED_FIELD = NONE
                ERROR_ON_COLUMN_COUNT_MISMATCH=false
            """)

    def __list_source_table_columns(
        self,
        database: str,
        schema: str,
        table: str
    ) -> list[str]:

        """
        Get columns for a specific table
        """

        columns = self.execute_query(f"""
            SELECT 
                COLUMN_NAME 
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE 
                TABLE_SCHEMA = '{schema}' AND 
                TABLE_NAME = '{table}'
        """)
        
        return columns

    def __connect_to_snowflake(
        self
    ) -> snowflake.connector:

        """
        Establish a connection to the Snowflake database using environment variables
        """

        try:
            connection = snowflake.connector.connect(
                authenticator=self.__authenticator,
                private_key_file=self.__private_key_file,
                private_key_file_pwd=self.__private_key_file_pwd,
                account=self.__account,
                user=self.__user,
                password=self.__password,
                role=self.__role,
                warehouse=self.__warehouse,
                client_session_keep_alive=True
            )
            _log.info('Connected to Snowflake successfully')

            return connection 
        except DatabaseError as e:
            _log.error(f'Failed to connect to Snowflake: {e}')

        return False

    def __del__(
        self
    ) -> None:

        """Destructor to close the Snowflake connection"""

        if self.__connection:
            try:
                self.__connection.close()
                _log.info('Snowflake connection closed successfully')
            except Exception as e:
                _log.error(f'Error closing Snowflake connection: {e}')