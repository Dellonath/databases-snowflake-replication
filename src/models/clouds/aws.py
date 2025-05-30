import os
import boto3
from boto3.exceptions import S3UploadFailedError
from ...logs.logger import _log

class AWSCloudClient:
  
    """"Class to manage file uploads to an S3 bucket."""

    def __init__(
            self,
            s3_bucket: str,
            aws_access_key_id: str=None,
            aws_secret_access_key: str=None,
            region: str=None,
            **kwargs
        ) -> None:
        
        self.cloud_name = 'aws'
        self.cloud_storage_name = 's3'
        self.__aws_access_key_id = os.getenv('aws_access_key_id')
        self.__aws_secret_access_key = os.getenv('aws_secret_access_key')
        self.__region = os.getenv('region')
        self.__s3_bucket = s3_bucket
        
        self.__storage_client = boto3.client(
            self.cloud_storage_name,
            aws_access_key_id=self.__aws_access_key_id,
            aws_secret_access_key=self.__aws_secret_access_key,
            region_name=self.__region
        )

    def upload_file(
            self, 
            file_path: str
        ) -> bool:
      
        """
        Upload a file to an S3 bucket
        
        :param str file_path: The local path to the file to upload
        """

        _log.info(f"Uploading file '{file_path}' into S3")
        try:
            self.__storage_client.upload_file(Key=file_path, 
                                              Bucket=self.__s3_bucket, 
                                              Filename=file_path)
            _log.info(f"File '{file_path}' uploaded to S3 successfully")
            
            return True
        except FileNotFoundError as e:
            _log.error(e)
        except S3UploadFailedError as e:
            _log.error(e)

        return False