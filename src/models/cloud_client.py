from .clouds.aws import AWSCloudClient
from .clouds.gcp import GCPCloudClient


class CloudClient:

    """
    Factory class to create and return a cloud client instance based on the specified cloud provider.
    Usage:
        client = CloudClient(provider='aws', bucket_name='my-bucket')
    """

    def __new__(cls, provider: str=None, **kwargs):
        if provider.lower() == 'gcp':
            return GCPCloudClient(**kwargs)
        elif provider.lower() == 'aws':
            return AWSCloudClient(**kwargs)
