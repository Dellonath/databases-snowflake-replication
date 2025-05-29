import os
import csv
import pyarrow as pa
import pyarrow.parquet as pq
from logs.logger import _log


class FileServiceClient:

    """A class to handle writing data to files in different formats."""

    def __init__(
            self,
            output_root: str='data') -> None:

        """
        A class to handle writing data to files in different formats.
        
        Args:
            output_root (str): The root directory where files will be saved.
        """

        self.output_root = output_root

    def write(
            self,
            file_path: str,
            table_data: list,
            table_columns: list[str],
            file_format: str='parquet') -> None:

        """
        Write data to a file in the specified format.
        
        Args:
            file_path (str): The full file output path.
            table_data (list): The data to write.
            table_columns (list[str]): The column names.
            file_format (str): The format of the output file ('csv' or 'parquet').
        """

        directory_path, table_name, file_name, file_format = self._parse_path(file_path=file_path)
        os.makedirs(name=directory_path, exist_ok=True)

        if file_format == 'parquet':
            self._write_parquet(file_path=file_path,
                                table_data=table_data,
                                table_columns=table_columns,
                                table_name=table_name)

        elif file_format == 'csv':
            self._write_csv(file_path=file_path,
                            table_data=table_data,
                            table_columns=table_columns,
                            table_name=table_name)


    def delete_file(
        self,
        file_path: str
    ) -> None:

        """
        Delete a file.
        
        Args:
            file_path (str): The path to the file to delete.
        """

        try:
            os.remove(file_path)
            _log.info(f"File {file_path} deleted successfully.")
        except OSError as e:
            _log.error(f"Error deleting file {file_path}: {e}")
        except FileNotFoundError:
            _log.error(f"File {file_path} not found for deletion.")

    def _parse_path(
        self,
        file_path: str
    ) -> tuple:

        """
        Separate file path into components
        
        Args:
            path (str): The path.
        """

        # extract the directory path
        # assuming the file path is in the format 'directory/table_name.file_format'
        directory_path = os.path.dirname(file_path)

        # extract table name and file format from the file path
        # assuming the file name is in the format 'table_name.file_format'
        table_name = file_path.split('/')[-2]
        file_name = file_path.split('/')[-1]
        file_format = file_path.split('.')[-1]

        return directory_path, table_name, file_name, file_format

    def _write_csv(
        self,
        file_path: str,
        table_data: list,
        table_columns: list[str],
        table_name: str
    ) -> None:

        """
        Write data to a CSV file.
        
        Args:
            file_path (str): The path to the CSV file.
            table_data (list): The data to write.
            table_columns (list[str]): The column names.
            table_name (str): The name of the table.
        """

        try:
            with open(file=file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f, delimiter='|')
                writer.writerow(table_columns)
                writer.writerows(table_data)
        except OSError as e:
            _log.error(f"File system error writing CSV for '{table_name}': '{e}'")
        except csv.Error as e:
            _log.error(f"CSV error for '{table_name}': '{e}'")

    def _write_parquet(
        self,
        file_path: str,
        table_data: list,
        table_columns: list[str],
        table_name: str
    ) -> None:

        """
        Write data to a PARQUET file.
        
        Args:
            file_path (str): The path to the PARQUET file.
            table_data (list): The data to write.
            table_columns (list[str]): The column names.
            table_name (str): The name of the table.
        """

        try:
            data_dict = {col: [row[idx] for row in table_data]
                                    for idx, col in enumerate(table_columns)}

            table = pa.table(data_dict)
            pq.write_table(table=table, where=file_path)
        except OSError as e:
            _log.error(f"File system error writing PARQUET for '{table_name}': '{e}'")
        except TypeError as e:
            _log.error(f"Type error writing PARQUET for '{table_name}': '{e}'")
