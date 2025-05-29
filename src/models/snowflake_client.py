import os
import snowflake.connector
from dotenv import load_dotenv
from logs.logger import _log

load_dotenv()

class SnowflakeClient:
    """
    Class to manage connections and execute queries on a Snowflake database.
    This class uses environment variables for configuration and provides methods to execute SQL queries.
    Attributes:
        __connection (snowflake.connector): The Snowflake connection object.
    """
    
    def __init__(self, database: str = 'CARASSO_POC_DB', stages_data_path: str = None) -> None:
        
        """
        Initialize the SnowflakeClient and establish a connection to the Snowflake database.
        This constructor reads the connection parameters from environment variables.
        """
        
        self.__connection = self.__connect_to_snowflake()
        self.__sf_database = database.upper()
        self.__stages_data_path = stages_data_path

    def execute_query(self, query) -> list:
        
        """
        Execute a query on the Snowflake database.
        
        Args:
            query (str): The SQL query to execute.
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
        
        self.execute_query(f"CREATE DATABASE IF NOT EXISTS {self.__sf_database};")
        self.execute_query(f"CREATE SCHEMA IF NOT EXISTS {self.__sf_database}.{schema_upper};")
        self.execute_query(f"USE {self.__sf_database}.{schema_upper};")
        
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
    
    def __connect_to_snowflake(self) -> snowflake.connector:
        
        """
        Establish a connection to the Snowflake database using environment variables.
        """
        
        try:
            connection = snowflake.connector.connect(
                role=os.getenv('sw_role'),
                user=os.getenv('sw_user'),
                password=os.getenv('sw_password'),
                account=os.getenv('sw_account'),
                warehouse=os.getenv('sw_warehouse'),
                database=os.getenv('sw_database'),
                schema=os.getenv('sw_schema')
            )
        except Exception as e:
            _log.error(f"Failed to connect to Snowflake: {e}")
            raise e
        _log.info("Connected to Snowflake successfully.")
        
        return connection
    
    def __del__(self):
        
        """Destructor to close the Snowflake connection."""
        
        if self.__connection:
            try:
                self.__connection.close()
                _log.info("Snowflake connection closed successfully.")
            except Exception as e:
                _log.error(f"Error closing Snowflake connection: {e}")