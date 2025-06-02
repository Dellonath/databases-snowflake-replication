import os
import snowflake.connector
from dotenv import load_dotenv
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
        account: str,
        user: str,
        password: str,
        role: str,
        warehouse: str,
        database: str,
        schema: str,
        file_service_client: FileServiceClient,
        cloud_client: AWSCloudClient | GCPCloudClient
    ) -> None:
        
        """
        Initialize the SnowflakeClient and establish a connection to the Snowflake database.
        This constructor reads the connection parameters from environment variables
        
        :param str account: Snowflake's account
        :param str user: Snowflake's username
        :param str password: User password
        :param str role: Role to be assumed during connection
        :param str warehouse: Snowflake's warehouse to be used to execute queries
        :param str database: Snowflake's database
        :param str schema: The database's schema name
        :param FileServiceClient file_service_client: File service client to manipulate data files
        :param AWSCloudClient | GCPCloudClient cloud_client: The cloud interface client
        """
        
        self.__account = os.getenv(account)
        self.__user = os.getenv(user)
        self.__password = os.getenv(password)
        self.__role = os.getenv(role)
        self.__warehouse = warehouse.upper()
        self.__database = database.upper()
        self.__schema = schema.upper()
        
        self.__cloud_client = cloud_client
        self.__file_service_client = file_service_client
        
        # snowflake;s ingestion configurations
        self.__storage_integration = 'CT_CARASSO_AWS'
        self.__snowflake_file_format = (
            f'CARASSO_{self.__file_service_client.file_format.upper()}_SCHEMA_EVOLUTION'
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
        
        except Exception as e:
            _log.error(f"Error executing query '{query}' in Snowflake: {e}")
            return []
    
    def setup_table_in_snowflake(
        self,
        stage_path: str,
        table_name: str
    ) -> None:
        
        # creating snowflake's database and schema if not exists and using it
        self.__setup_database_and_schema()
        # defining file formats able to deal with schema evolution
        self.__setup_snowflake_file_formats()
        # creating exernal stage pointing to files in the external cloud provider
        self.__create_snowflake_stage(stage_name=table_name)
        # creating snowflake table using infer schema and schema evolution enabled
        self.__create_snowflake_table(table_name=table_name)
        # copying data into the new created table
        self.__execute_copy_command(table_name=table_name)
    
    def __execute_copy_command(
        self,
        table_name: str
    ) -> None:
        
        self.execute_query(f"""
            COPY INTO {self.__database}.{self.__schema}.{table_name.upper()}
                FROM @stage_{table_name}
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                FILE_FORMAT={self.__snowflake_file_format};
        """)
        
    def __create_snowflake_table(
        self,
        table_name: str
    ) -> None:
        
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {self.__database}.{self.__schema}.{table_name.upper()}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION=>'@stage_{table_name}',
                        FILE_FORMAT=>'{self.__snowflake_file_format}'
                    )
                )) ENABLE_SCHEMA_EVOLUTION=true;
        """)
    
    def __create_snowflake_stage(
        self,
        stage_name: str
    ) -> None:

        stage_path = f'{self.__cloud_client.cloud_storage_prefix}{self.__cloud_client.bucket}/{stage_path}'

        self.execute_query(f"""
            CREATE STAGE IF NOT EXISTS stage_{stage_name}
                STORAGE_INTEGRATION={self.__storage_integration}
                URL='{stage_path}'
                FILE_FORMAT={self.__snowflake_file_format};
        """)
    
    def __setup_database_and_schema(
        self
    ) -> None:
        
        self.execute_query(f'CREATE DATABASE IF NOT EXISTS {self.__database}')
        self.execute_query(f'CREATE SCHEMA IF NOT EXISTS {self.__database}.{self.__schema}')
        
    def __setup_snowflake_file_formats(
        self
    ) -> None:
        
        if self.__file_service_client.file_format.lower() == 'parquet':
            self.execute_query(f"""
                CREATE FILE FORMAT IF NOT EXISTS {self.__database}.{self.__schema}.{self.__snowflake_file_format}
                TYPE='PARQUET';
            """)
        elif self.__file_service_client.file_format.lower() == 'csv':
            self.execute_query(f"""
                CREATE FILE FORMAT IF NOT EXISTS {self.__database}.{self.__schema}.{self.__snowflake_file_format}
                TYPE='CSV'
                FIELD_DELIMITER='|'
                PARSE_HEADER=true
                ERROR_ON_COLUMN_COUNT_MISMATCH=false;
            """)

    def __connect_to_snowflake(
        self
    ) -> snowflake.connector:
        
        """
        Establish a connection to the Snowflake database using environment variables.
        """
        
        try:
            connection = snowflake.connector.connect(
                account=self.__account,
                user=self.__user,
                password=self.__password,
                role=self.__role,
                warehouse=self.__warehouse,
                database=self.__database,
                schema=self.__schema
            )
        except Exception as e:
            _log.error(f"Failed to connect to Snowflake: {e}")
            raise e
        _log.info("Connected to Snowflake successfully.")
        
        return connection
    
    def __del__(
        self
    ) -> None:
        
        """Destructor to close the Snowflake connection."""
        
        if self.__connection:
            try:
                self.__connection.close()
                _log.info("Snowflake connection closed successfully.")
            except Exception as e:
                _log.error(f"Error closing Snowflake connection: {e}")