import os
import datetime
import boto3
from boto3.exceptions import S3UploadFailedError
from ...logs.logger import _log

class AWSCloudClient:

    def __init__(
        self,
        bucket: str,
        cloud_storage_directory: str='',
        aws_access_key_id: str=None,
        aws_secret_access_key: str=None,
        region: str=None,
        **kwargs
    ) -> None:

        """Class to manage file uploads to an S3 bucket"""

        self.cloud_provider_name = 'aws'
        self.cloud_storage_name = 's3'
        self.cloud_storage_prefix = 's3://'
        self.cloud_storage_directory = cloud_storage_directory
        self.bucket = bucket

        self.__storage_client = boto3.client(
            self.cloud_storage_name,
            aws_access_key_id=os.getenv('aws_access_key_id'),
            aws_secret_access_key=os.getenv('aws_secret_access_key'),
            region_name=os.getenv('region')
        )

    def upload_file(
        self,
        local_storage_path: str,
        cloud_storage_path: str,
        file_name: str
    ) -> bool:

        """
        Upload a file to the S3 bucket

        :param str local_storage_path: The local storage location
        :param str cloud_storage_path: The cloud storage location
        :param str file_name: The path local file name
        """

        datetime_now = datetime.datetime.now()
        partition_name = (
            f'year={datetime_now.year}/'
            f'month={datetime_now.month}/'
            f'day={datetime_now.day}'
        )

        local_storage_file_path = f'{local_storage_path}/{file_name}'
        cloud_storage_file_path = f'{cloud_storage_path}/{partition_name}/{file_name}'

        _log.info(f"Uploading file '{local_storage_file_path}' to S3 in '{cloud_storage_file_path}'")

        try:
            self.__storage_client.upload_file(Filename=local_storage_file_path,
                                              Bucket=self.bucket, 
                                              Key=cloud_storage_file_path)
            _log.info(f"File '{local_storage_file_path}' uploaded to S3 successfully")

            return True
        except FileNotFoundError as e:
            _log.error(e)
        except S3UploadFailedError as e:
            _log.error(e)

        return False