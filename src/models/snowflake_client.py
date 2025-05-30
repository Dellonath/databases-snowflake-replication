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
        """
        
        self.__account = os.getenv(account)
        self.__user = os.getenv(user)
        self.__password = os.getenv(password)
        self.__role = os.getenv(role)
        self.__warehouse = warehouse.upper()
        self.__database = database.upper()
        self.__schema = schema.upper()
        
        if cloud_client.cloud_name == 'aws':
            self.__stages_data_path = f's3://{cloud_client.s3_bucket}/{file_service_client.tmp_local_directory}'
        elif cloud_client.cloud_name == 'gcp':
            self.__stages_data_path = f'gs://{cloud_client.cloud_storage_name}/{file_service_client.tmp_local_directory}'

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
    
    def setup_snowflake_stage_schema_table(
        self, 
        schema: str,
        table: str,
        file_format: str
    ) -> None:
        
        schema_upper = schema.upper()
        table_upper = table.upper()
        file_format_upper = file_format.upper()
        
        self.execute_query(f"CREATE DATABASE IF NOT EXISTS {self.__database};")
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.__database}.{schema_upper};")
        self.execute_query(f"USE {self.__database}.{schema_upper};")
        
        self.execute_query(f"""
            CREATE OR REPLACE FILE FORMAT CARASSO_PARQUET_SCHEMA_EVOLUTION
            TYPE='PARQUET';
        """)
        
        self.execute_query(f"""
            CREATE OR REPLACE FILE FORMAT CARASSO_CSV_SCHEMA_EVOLUTION
            TYPE='CSV'
            FIELD_DELIMITER='|'
            PARSE_HEADER=true
            ERROR_ON_COLUMN_COUNT_MISMATCH=false;
        """)
 
        self.execute_query(f"""
            CREATE STAGE IF NOT EXISTS stage_{table}
                STORAGE_INTEGRATION=CT_CARASSO_AWS
                URL='{self.__stages_data_path}/{schema}/{table}/'
                FILE_FORMAT=CARASSO_{file_format_upper}_SCHEMA_EVOLUTION;
        """)
        
        self.execute_query(f"""
            CREATE TABLE IF NOT EXISTS {table_upper}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION=>'@stage_{table}',
                        FILE_FORMAT=>'CARASSO_{file_format_upper}_SCHEMA_EVOLUTION'
                    )
                )) ENABLE_SCHEMA_EVOLUTION=true;
        """)
        
        self.execute_query(f"""
            COPY INTO {table_upper}
                FROM @stage_{table}
                MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE
                FILE_FORMAT = CARASSO_{file_format_upper}_SCHEMA_EVOLUTION;
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