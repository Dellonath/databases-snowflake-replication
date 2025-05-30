from models.clouds.aws import AWSCloudClient
from models.clouds.gcp import GCPCloudClient


class CloudClient:

    """
    Factory class to create and return a cloud client instance based on the specified cloud provider.
    Usage:
        client = CloudClient(cloud_name='aws', bucket_name='my-bucket')
    """

    def __new__(cls, cloud_name: str, **kwargs):
        if cloud_name.lower() == 'gcp':
            return GCPCloudClient(**kwargs)
        elif cloud_name.lower() == 'aws':
            return AWSCloudClient(**kwargs)
        else:
            raise ValueError(f"Unsupported cloud provider: {cloud_name}")
