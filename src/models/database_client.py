from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import (OperationalError, 
                            DatabaseError, 
                            ProgrammingError, 
                            NoSuchTableError)
from logs.logger import _log

class DatabaseClient:

    """Class to manage database connections and execute queries."""

    def __init__(
        self,
        db_engine: str,
        host: str,
        username: str,
        password: str,
        database: str,
        port: int=3306
    ) -> None:

        """
        Initialize the DatabaseClient with connection parameters.
        
        Args:
            db_engine (str): The database engine (e.g., 'mysql-pymysql').
            host (str): The database host.
            username (str): The database username.
            password (str): The database password.
            database (str): The name of the database.
            port (int, optional): The port number. Defaults to 3306.
        """

        self.__db_engine = db_engine
        self.__host = host
        self.__username = username
        self.__password = password
        self.database = database
        self.port = port

        self.__engine = self.__database_connection()
        self.connection_status = self.__test_connection()

    def execute_query(
        self,
        query: str
    ) -> list:

        """
        Execute a SQL query and return the result.

        Args:
            query (str): The SQL query to execute.
        """

        try:
            with self.__engine.connect() as connection:
                result = connection.execute(statement=text(query))
                rows = result.fetchall()
                
            return rows

        except ProgrammingError as e:
            _log.error(e)
        except NoSuchTableError as e:
            _log.error(e)
        except DatabaseError as e:
            _log.error(e)

    def get_table_columns(
        self,
        table_name: str
    ) -> list:

        """
        Get the columns of a table.
        
        Args:
            table_name (str): The name of the table.
        """

        try:
            inspector = inspect(subject=self.__engine)
            columns = inspector.get_columns(table_name=table_name)
            
            return [column['name'] for column in columns]
        
        except ProgrammingError as e:
            _log.error(e)
        except DatabaseError as e:
            _log.error(e)

        return []
    
    def get_tables_list(
        self,
        schema_name: str='public'
    ) -> list:

        """
        Get the list of tables in a schema.
        
        Args:
            schema_name (str): The schema name.
        """

        query = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}'"
        config_table_list = set(table_config[0] for table_config in self.execute_query(query=query))
        
        return config_table_list
    
    def __database_connection(
        self
    ) -> None:

        """
        Create a database connection.
        """
    
        if self.__db_engine == 'postgresql+psycopg2':
            url = f'{self.__db_engine}://{self.__username}:{self.__password}@{self.__host}/{self.database}'
        elif self.__db_engine == 'mysql+pymysql':
            url = f'{self.__db_engine}://{self.__username}:{self.__password}@{self.__host}:{self.port}/{self.database}'
        
        try:
            engine = create_engine(url=url, 
                                   pool_size=10, 
                                   pool_recycle=1800)
            _log.info(f"Database connection established: {url}")
                        
            return engine

        except OperationalError as e:
            _log.error(f"Failed to connect to the database: {e}")
            raise e

    def __test_connection(
        self
    ) -> bool:

        """
        Test the database connection.
        
        Returns:
            bool: True if the connection is successful, False otherwise.
        """

        try:
            with self.__engine.connect() as connection:
                connection.execute(text('SELECT 1'))
            _log.info(f"Database connection successful: database='{self.database}'")

            return True
        except OperationalError as e:
            _log.error(f'Failed to connect to the database due to: {e}')
            return False
