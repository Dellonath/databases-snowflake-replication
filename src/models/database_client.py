import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
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
        **kwargs
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
        """

        self.__engine = engine
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = os.environ.get(password)
        self.database = database
        self.schema = database if schema is None else schema
        self.__db_engine = self.__create_engine()
        self.source_tables = self.__list_source_tables()
        self.tables_columns = self.__list_source_tables_columns()

    def execute_query(
        self,
        query: str
    ) -> list:

        """
        Execute a SQL query and return the result

        :param str query: The SQL query to execute
        """

        try:
            with self.__db_engine.connect() as connection:
                result = connection.execute(statement=text(query))
                rows = result.fetchall()
                
            return rows
        except ProgrammingError as e:
            _log.error(f'Failed to execute query in database: {e}')
        except NoSuchTableError as e:
            _log.error(f'Failed to execute query in database: {e}')
        except DatabaseError as e:
            _log.error(f'Failed to execute query in database: {e}')
        except OperationalError as e:
            _log.error(f'Failed to execute query in database: {e}')

        return []

    def __list_source_tables_columns(
        self
    ) -> dict[str, list[str]]:

        """Get the columns of a table"""

        tables_columns = dict()
        for table_name in self.source_tables:
            try:
                inspector = inspect(subject=self.__db_engine)
                columns = inspector.get_columns(table_name=table_name)
                tables_columns[table_name] = [column['name'] for column in columns]
            except ProgrammingError as e:
                _log.error(f'Error listing tables columns: {e}')
            except DatabaseError as e:
                _log.error(f'Error listing tables columns: {e}')

        return tables_columns

    def __list_source_tables(
        self
    ) -> set:

        """Get the list of tables in a schema"""

        if self.__engine in ('postgresql+psycopg2', 'mysql+pymysql'):
            query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{self.schema}'"
        list_of_tables_names = {table_config[0] for table_config in self.execute_query(query=query)}

        return list_of_tables_names
    
    def __create_engine(
        self
    ) -> None:

        """Create a database connection"""
    
        if self.__engine == 'postgresql+psycopg2':
            url = f'{self.__engine}://{self.__username}:{self.__password}@{self.__host}/{self.database}'
        elif self.__engine == 'mysql+pymysql':
            url = f'{self.__engine}://{self.__username}:{self.__password}@{self.__host}:{self.__port}/{self.database}'

        try:
            engine = create_engine(url=url)
            _log.info(f"Connection for '{self.database}' database established successfully")       
            return engine
        except OperationalError as e:
            _log.error(f'Error creating engine for database: {e}')
