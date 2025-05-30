import boto3
from boto3.exceptions import S3UploadFailedError
from logs.logger import _log

class AWSCloudClient:
  
    """"Class to manage file uploads to an S3 bucket."""

    def __init__(
            self,
            **kwargs
        ) -> None:
        
        self.cloud_name = 'AWS'
        self.cloud_storage_name = 'S3'
        self.__storage_client = boto3.client('s3')
        self.__bucket_name = kwargs.get('bucket_name')

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
                                              Bucket=self.__bucket_name, 
                                              Filename=file_path)
            _log.info(f"File '{file_path}' uploaded to S3 successfully")
            
            return True
        except FileNotFoundError as e:
            _log.error(e)
        except S3UploadFailedError as e:
            _log.error(e)

        return False

    # def upload_all_remaining_files(
    #         self, 
    #         directory_path: str
    #     ) -> bool:
      
    #     """
    #     Upload all files in a directory to the S3 bucket. This method searches for files matching 
    #     the glob pattern in the specified directory and uploads them to the S3 bucket.
    #     If the upload is successful, the file is deleted from the local directory. Else, files
    #     were kept in the local directtory
        
    #     :param str directory_path: The local directory path containing files to upload
    #     """

    #     files = glob.glob(directory_path)
    #     if not files:
    #         _log.info(f"No files found in '{directory_path}' to upload.")
    #         return False
    #     else:
    #         _log.info(f"Found {len(files)} files in '{directory_path}' to upload.")

    #     for file_path in files:
    #         _log.info(f"Uploading file: '{file_path}'")
    #         upload_status = self.upload_file(file_path=file_path)

    #         # delete file in local directory if upload was successful
    #         if upload_status:
    #             os.remove(path=file_path)

    #     _log.info(f"All files from '{directory_path}' have been uploaded to Cloud Storage.")
        
    #     return True
