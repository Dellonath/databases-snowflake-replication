import os
import glob
from logs.logger import _log

class GCPCloudClient:
  
    """"Class to manage file uploads to an S3 bucket."""

    def __init__(
            self,
            **kwargs
        ) -> None:
        
        self.cloud_name = 'GCP'
        self.cloud_storage_name = 'Cloud Storage'
        
    def upload_file(
            self, 
            file_path: str
        ) -> bool:
      
        """
        Upload a file to the Cloud Storage
        
        :param str file_path: The local path to the file to upload
        """
        
        # Placeholder for actual GCP upload logic
        # Here you would implement the logic to upload the file to GCP Cloud Storage

        pass # Placeholder for actual GCP upload logic
    

    # def upload_all_remaining_files(
    #         self, 
    #         directory_path: str
    #     ) -> bool:
      
    #     """
    #     Upload all files in a directory to Cloud Storage bucket. This method searches for files matching 
    #     the glob pattern in the specified directory and uploads them.
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
