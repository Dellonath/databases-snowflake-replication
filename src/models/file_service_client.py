import os
import csv
import glob
import pyarrow as pa
import pyarrow.parquet as pq
from ..logs.logger import _log


class FileServiceClient:

    def __init__(
        self,
        local_storage_directory: str='',
        file_format: str='parquet',
        exclude_file_after_uploading: bool=True,
        upload_remaining_files: bool=True
    ) -> None:

        """
        A class to handle writing data to files in different formats

        :param str local_storage_directory (optional): The local storage directory where files will be saved temporarily
        :param str file_format (optional): The format of the output files ('csv' or 'parquet')
        :param bool exclude_file_after_uploading (optional): allow excluding files after uploading it to cloud
        :param bool upload_remaining_files (optional): Flag to allow uploading all remaining files stored in local machine
        """

        self.local_storage_directory = local_storage_directory
        self.file_format = file_format
        self.exclude_file_after_uploading = exclude_file_after_uploading
        self.upload_remaining_files = upload_remaining_files

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
        files = [file_path.split('/')[-1] for file_path in glob.glob(pattern)]

        return files

    def write_file(
        self,
        local_storage_path: str,
        file_name: str,
        table_data: list,
        table_columns: list[str]
    ) -> None:

        """
        Write data to a file in the specified format
        
        :param str local_storage_table_path: The local storage where table should be loaded
        :param str file_name: The output file name
        :param list table_data: The data content to write
        :param list[str] table_columns: The source table columns names
        """

        # creating table directory if it does not exist in local 
        os.makedirs(name=local_storage_path, exist_ok=True)

        file_path = f'{local_storage_path}/{file_name}'
        _log.info(f"Writing {self.file_format} file to path: '{file_path}'")

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
        path: str
    ) -> None:

        """
        Delete a file or directory
        
        :param str path: The local file/directory to be deleted
        """

        try:
            os.remove(path)
            _log.info(f"File '{path}' deleted successfully")
        except OSError as e:
            _log.error(e)
        except FileNotFoundError:
            _log.error(e)

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
