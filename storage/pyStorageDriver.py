from storage.pyStorageProvider import pyStorageProvider
from storage.pyObjectInfo import pyObjectInfo
from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
import json
from os.path import exists
import logging


class pyStorageDriver:

    def upload_file(self, source_file: str, object_info: pyObjectInfo):
        driver = self.retrieve_driver(object_info.provider, object_info.region)
        self.upload_with_cache(
            driver, object_info.bucket_name, object_info.file_name, source_file)
        logging.info('uploaded file %s from %s',
                     object_info.file_name, object_info.bucket_name)

    def download_file(self, object_info: pyObjectInfo, target_file: str):
        driver = self.retrieve_driver(object_info.provider, object_info.region)
        self.download_with_cache(
            driver, object_info.bucket_name, object_info.file_name, target_file)
        logging.info('downloaded file %s from %s',
                     object_info.file_name, object_info.bucket_name)

    def create_bucket(self, bucket_name: str, provider: pyStorageProvider, region: str):
        driver = self.retrieve_driver(provider, region)
        try:
            driver.create_container(bucket_name)
            logging.info('created bucket %s in region %s', bucket_name, region)
        except Exception as e:
            logging.error(e)

    def delete_bucket(self, object_to_delete: pyObjectInfo, delete_if_not_empty: bool = True):
        driver = self.retrieve_driver(
            object_to_delete.provider, object_to_delete.region)
        bucket = driver.get_container(object_to_delete.bucket_name)
        try:
            if delete_if_not_empty:
                objects = driver.list_container_objects(bucket)
                for obj in objects:
                    driver.delete_object(obj)
            if (sum(1 for _ in driver.list_container_objects(bucket)) == 0):
                driver.delete_container(bucket)
                logging.info("bucket %s was deleted",
                             object_to_delete.bucket_name)
            else:
                logging.error(
                    "Cannot delete non-empty bucket %s, consider adding \"deleteIfNotEmpty=true\" to the function call to delete objects within the bucket before deleting", object_to_delete.bucket_name)
        except:
            logging.error("bucket %s doesn't exist - cannot delete",
                          object_to_delete.bucket_name)

    def copy_file(self, source_info: pyObjectInfo, target_info: pyObjectInfo):
        source_driver = self.retrieve_driver(
            source_info.provider, source_info.region)
        source_object = source_driver.get_object(
            source_info.bucket_name, source_info.file_name)
        object_to_copy = source_driver.download_object_as_stream(source_object)

        target_driver = self.retrieve_driver(
            target_info.provider, target_info.region)
        target_bucket = target_driver.get_container(target_info.bucket_name)
        target_driver.upload_object_via_stream(
            object_to_copy, target_bucket, target_info.file_name)
        logging.info('copied file %s  from %s to %s', source_info.file_name,
                     source_info.bucket_name, target_info.bucket_name)

    def copy_bucket(self, source_info: pyObjectInfo, target_info: pyObjectInfo):
        source_driver = self.retrieve_driver(
            source_info.provider, source_info.region)
        source_bucket = source_driver.get_container(source_info.bucket_name)

        target_driver = self.retrieve_driver(
            target_info.provider, target_info.region)
        target_bucket = target_driver.get_container(target_info.bucket_name)

        all_objects = source_driver.list_container_objects(source_bucket)
        for obj in all_objects:
            object_to_copy = source_driver.download_object_as_stream(obj)
            target_driver.upload_object_via_stream(
                object_to_copy, target_bucket, obj.name)

        logging.info('copied content of bucket %s to bucket %s',
                     source_info.bucket_name, target_info.bucket_name)

    # retrieving driver that connects to a provider
    def retrieve_driver(self, provider: pyStorageProvider, region: str):
        if exists('/tmp/credentials.json'):
            credentials = json.load(open('/tmp/credentials.json'))
        elif exists('/opt/credentials.json'):
            credentials = json.load(open('/opt/credentials.json'))
        else:
            logging.error('no credentials file found')
            return None
        try:
            driver = None
            if (provider == pyStorageProvider.AWS):
                if region == None:
                    logging.error("no region was found")
                else:
                    cls = get_driver(self.translate_to_libloud_region(region))
                    session_token = credentials['amazon']['aws_session_token']

                    if not session_token:
                        driver = cls(
                            credentials['amazon']['aws_access_key_id'], credentials['amazon']['aws_secret_access_key'])
                    else:
                        driver = cls(credentials['amazon']['aws_access_key_id'],
                                     credentials['amazon']['aws_secret_access_key'], token=session_token)
            elif (provider == pyStorageProvider.GCP):
                cls = get_driver(Provider.GOOGLE_STORAGE)
                driver = cls(key=credentials['google']['client_email'], secret=credentials['google']
                             ['private_key'], project=credentials['google']['project_id'])
            else:
                logging.warning('no driver was retrieved')
                return None
            if driver == None:
                logging.warning('no driver was retrieved')
        except Exception as e:
            logging.error(
                'In function retrieve_driver the following error message was received: %s', e)

        return driver

    def upload_with_cache(self, driver, bucket_name: str, file_name: str, source_file: str):
        FILE_PATH = source_file

        try:
            container = driver.get_container(container_name=bucket_name)
        except Exception as e:
            logging.error(e)

        with open(FILE_PATH, 'rb') as iterator:
            try:
                obj = driver.upload_object_via_stream(iterator=iterator,
                                                      container=container,
                                                      object_name=file_name)
            except Exception as e:
                logging.error(
                    'in upload_with_cache the following error message was recieved: %s', e)

    def download_with_cache(self, driver, bucket_name: str, file_name: str, target_file: str):
        try:
            obj = driver.get_object(container_name=bucket_name,
                                    object_name=file_name)

            driver.download_object(
                obj=obj, destination_path=target_file, overwrite_existing=True)
        except Exception as e:
            logging.error(
                'in download_with_cache the following error message was recieved: %s', e)

    # translates amazon labels to libcloud labels for regions
    def translate_to_libloud_region(self, region: str) -> str:
        if exists('./aws_regions.json'):
            region_file = open('./aws_regions.json')
            aws_regions = json.load(region_file)
            return aws_regions[region]
        else:
            logging.warning('no aws_regions.json file found')
