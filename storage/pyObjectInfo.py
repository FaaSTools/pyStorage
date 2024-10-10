
from storage.pyStorageProvider import pyStorageProvider

# an object to save important information about either a bucket or a file
# region is only important for amazon
# if the object is a bucket then file_name is None


class pyObjectInfo:
    def __init__(self, provider: pyStorageProvider, bucket_name: str, file_name: str, region: str = None):
        self.provider = provider
        self.bucket_name = bucket_name
        self.file_name = file_name
        self.region = region
