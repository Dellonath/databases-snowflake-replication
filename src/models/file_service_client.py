import os
import csv
import glob
import pyarrow as pa
import pyarrow.parquet as pq
from ..logs.logger import _log


class FileServiceClient:

    def __init__(
        self,
        tmp_local_directory: str='data',
        file_format: str='parquet'
    ) -> None:

        """
        A class to handle writing data to files in different formats
        
        :param str tmp_local_directory (optional): The root local directory where files will be saved temporarily
        :param str file_format (optional): The format of the output files ('csv' or 'parquet')
        """

        self.tmp_local_directory = tmp_local_directory
        self.file_format = file_format

    def list_files_in_directory(
        self,
        path: str
    ) -> list[str]:
        
        """
        List all files in a directory
        
        :param str path: The path to the directory to search for files
        :return: A list of file paths matching the specified pattern
        """

        pattern = f'{path}/*'
        files = glob.glob(pattern)
        _log.info(f"Found {len(files)} files in '{path}'")
        
        return files

    def write_file(
        self,
        file_path: str,
        table_data: list,
        table_columns: list[str]
    ) -> None:

        """
        Write data to a file in the specified format
        
        :param str file_path: The full file output path
        :param list table_data: The data to write
        :param list[str] table_columns: The table columns names
        """

        # creating table directory if it does not exist in local storage
        os.makedirs(name=os.path.dirname(file_path), exist_ok=True)

        _log.info(f"Writing data to path: '{file_path}'")
        if self.file_format == 'parquet':
            self.__write_parquet(file_path=file_path,
                                table_data=table_data,
                                table_columns=table_columns)
        elif self.file_format == 'csv':
            self.__write_csv(file_path=file_path,
                            table_data=table_data,
                            table_columns=table_columns)

    def delete_file(
        self,
        file_path: str
    ) -> None:

        """
        Delete a file
        
        :param str file_path: The path to the file to delete.
        """

        try:
            os.remove(file_path)
            _log.info(f"File '{file_path}' deleted successfully")
        except OSError as e:
            _log.error(f"Error deleting file {file_path}: {e}")
        except FileNotFoundError:
            _log.error(f"File {file_path} not found for deletion")

    def __write_csv(
        self,
        file_path: str,
        table_data: list,
        table_columns: list[str]
    ) -> None:

        """
        Write data as CSV file
        
        :param str file_path: The path to the CSV file
        :param list table_data: The data to write
        :param list[str] table_columns: The column names
        """

        try:
            with open(file=file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerow(table_columns)
                writer.writerows(table_data)
        except OSError as e:
            _log.error(e)
        except csv.Error as e:
            _log.error(e)

    def __write_parquet(
        self,
        file_path: str,
        table_data: list,
        table_columns: list[str]
    ) -> None:

        """
        Write data as Parquet file
        
        :param str file_path: The path to the PARQUET file
        :param list table_data: The data to write
        :param list[str] table_columns: The column names
        """

        try:
            data_dict = {col: [row[idx] for row in table_data]
                                    for idx, col in enumerate(table_columns)}

            table = pa.table(data_dict)
            pq.write_table(table=table, where=file_path)
        except OSError as e:
            _log.error(e)
        except TypeError as e:
            _log.error(e)
