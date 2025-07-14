import os
from dotenv import load_dotenv
import datetime
import jaydebeapi
from sqlalchemy import create_engine, text, engine
from sqlalchemy.exc import (OperationalError, 
                            DatabaseError, 
                            ProgrammingError, 
                            NoSuchTableError)
from ..logs.logger import _log

load_dotenv()


class DatabaseClient:

    def __init__(
        self,
        engine: str,
        host: str,
        port: int,
        username: str,
        password: str,
        database: str,
        schema: str=None,
        jar_file_path: str=None,
        **kargs
    ) -> None:

        """
        Initialize the DatabaseClient with connection parameters.
        
        :param str engine: The database engine (e.g., 'mysql-pymysql', 'postgresql+psycopg2')
        :param str host: The database host
        :param int port: The port number
        :param str username: The database username
        :param str password: The database password
        :param str database: The name of the database
        :param str schema: The name of the schema to connect to. Defaults is the database name
        :param str jar_file_path: Path to the JDBC driver jar file (if applicable)
        """

        self.__engine = engine
        self.__host = os.environ.get(host)
        self.__port = os.environ.get(port)
        self.__username = os.environ.get(username)
        self.__password = os.environ.get(password)
        self.database = database
        self.schema = schema if schema is not None else database
        self.__jar_file_path = jar_file_path
        self.__db_engine = self.__create_engine()
        self.source_tables = self.__list_source_tables()
    
    def execute_query(
        self,
        query: str,
        table: str=None,
        size: int = None,
        total_records: int=None
    ):
        """
        Execute a SQL query and return the result or a batch iterator

        :param str query: The SQL query to execute
        :param int size: Batch size for fetching results
        """

        full_qualified_table = f'{self.database}.{self.schema}.{table}'
        try:
            if self.__engine in ('mysql+pymysql', 'postgresql+psycopg2'):
                engine = self.__db_engine.connect()
                connection = engine.execute(statement=text(query))
            elif self.__engine == 'com.intersys.jdbc.CacheDriver':
                connection = self.__db_engine.cursor()
                connection.execute(operation=query)
            if size:
                def batch_extraction():
                    count = 0
                    count_failed = 0
                    start_time = datetime.datetime.now()
                    while True:
                        rows=[]
                        for _ in range(size):
                            if count == total_records: break
                            try:
                                row = connection.fetchone()
                                # if fetchone returned no row (table extraction was completed)
                                if row:
                                    count += 1
                                    rows.append(row)
                                else: break
                            except Exception as e:
                                _log.error(f"Failed to extract record {count} from '{full_qualified_table}' due to: {e}")
                                count_failed += 1
                                # skip record ingestion to avoid duplications at final result
                                continue

                            if count == total_records or count % 5000 == 0 or count % size == 0:
                                _log.info(f"Extraction report for '{full_qualified_table}': "
                                          f'extracted={count} '
                                          f'total={total_records} '
                                          f'completion={round(100*count/total_records, 2)}% '
                                          f'failed={count_failed} ' 
                                          f'elapsed={datetime.datetime.now()-start_time}')

                        # if batch is not empty, return completion
                        if not rows:
                            break

                        yield rows

                    connection.close()

                return batch_extraction()
            else:
                rows = connection.fetchall()
                if not rows:
                    _log.info(f"No data extracted for table '{full_qualified_table}'")
                connection.close()

                return rows

        except (ProgrammingError, NoSuchTableError, DatabaseError, OperationalError, Exception) as e:
            _log.error(f'Failed to execute query in database: {e}')

    def get_number_of_records_for_table(
        self,
        table: str,
        where: str=None
    ) -> list[str]:

        """Get number of records for a table"""

        query = f"SELECT count(1) FROM {self.schema}.{table} {f'WHERE {where}' if where else ''}"

        _log.info(f"Getting total number of records for table '{self.database}.{self.schema}.{table}'")
        columns = self.execute_query(query=query)

        n_records = [column[0] for column in columns]
        _log.info(f"Total of {n_records[0]} records was identified in table '{self.database}.{self.schema}.{table}'")

        return n_records[0]


    def list_source_table_columns(
        self,
        table: str
    ) -> list[str]:

        """Get the columns of a table"""

        tables_columns = dict()
        query = f'''
            SELECT 
                column_name
            FROM information_schema.columns 
            WHERE 
                table_schema = '{self.schema}'
                AND table_name = '{table}'
        '''

        _log.info(f"Getting list of columns for table '{self.database}.{self.schema}.{table}'")
        columns = self.execute_query(query=query)
        tables_columns = [column[0] for column in columns]

        return tables_columns

    def __list_source_tables(
        self
    ) -> list:

        """Get the list of tables in a schema"""

        query = f'''
            SELECT 
                table_name
            FROM information_schema.tables 
            WHERE table_schema = '{self.schema}'
        '''

        _log.info(f"Getting list of tables for schema '{self.database}.{self.schema}'")
        tables = self.execute_query(query=query)
        list_of_tables_names = [table_config[0] for table_config in tables]

        return list_of_tables_names

    def __create_engine(
        self
    ) -> None:

        """Create a database connection engine"""

        try:
            if self.__engine in ('mysql+pymysql', 'postgresql+psycopg2'):
                db_engine = create_engine(url=engine.URL.create(
                        drivername=self.__engine,
                        username=self.__username,
                        password=self.__password,
                        host=self.__host,
                        port=self.__port,
                        database=self.database
                    )
                )
            elif self.__engine == 'com.intersys.jdbc.CacheDriver':
                db_engine = jaydebeapi.connect(
                    jclassname=self.__engine, 
                    url=f'jdbc:Cache://{self.__host}:{self.__port}/{self.database}', 
                    driver_args=[self.__username, self.__password], 
                    jars=self.__jar_file_path
                )

            _log.info(f"Connection to '{self.database}' database established successfully") 

            return db_engine
        except OperationalError as e:
            _log.error(f'Error creating engine for database: {e}')
