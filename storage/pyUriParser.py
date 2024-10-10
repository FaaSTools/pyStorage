from storage.pyStorageProvider import pyStorageProvider
from storage.pyObjectInfo import pyObjectInfo
from os.path import exists
import re
import boto3
import json
import logging

# class to parse the input strings of the functions in pyStorage


class pyUriParser:

    # determines whether an object is a file or a bucket
    # also determines whether an object is on amazon or on google
    def parse_to_pyObjectInfo(input: str, just_bucket: bool = False) -> pyObjectInfo:
        # check if object is on s3
        amazon = pyUriParser.parse_aws_uri(input, just_bucket)

        # check if object is on cloud storage
        google = pyUriParser.parse_gcp_uri(input, just_bucket)

        if amazon is not None:
            return pyObjectInfo(pyStorageProvider.AWS, amazon['bucket_name'], amazon['file_name'], amazon['region'])
        if google is not None:
            return pyObjectInfo(pyStorageProvider.GCP, google['bucket_name'], google['file_name'], None)

        else:
            return None

    # if url points to an amazon object, then this parses the string otherwise it returns None
    # just_bucket informs the function whether it should check for a file after retrieving the bucket
    def parse_aws_uri(input: str, just_bucket: bool) -> dict:
        prefix = ""
        # the three cases for an amazon s3 uri
        if input.startswith("arn:aws:s3"):
            bucket_name = input[len("arn:aws:s3:::"):].split("/")[0]
            prefix = f"arn:aws:s3:::{bucket_name}/"
        elif input.startswith("s3://"):
            bucket_name = input[len("s3://"):].split("/")[0]
            prefix = f"s3://{bucket_name}/"
        elif re.search(".s3.*amazonaws.com/", input) is not None:
            bucket_name = input[len("https://"):].split(".")[0]
            prefix = f'https://{bucket_name}.s3.*amazonaws.com/'
        else:
            return None

        if not just_bucket:
            file_name = re.sub(prefix, '', input)
        else:
            file_name = None

        region = pyUriParser.get_aws_region(bucket_name)

        return {'bucket_name': bucket_name, 'file_name': file_name, 'region': region}

    # equal to the function parseAmazonUrl, but for google objects
    def parse_gcp_uri(input: str, just_bucket: bool) -> dict:
        prefix = ""
        if input.startswith("gs://"):
            prefix = "gs://"
        elif re.match("http://", input) and input[len("http://"):].startswith("storage.cloud.google.com"):
            prefix = "http://storage.cloud.google.com/"

        elif re.match("https://", input) and input[len("https://"):].startswith(("storage.cloud.google.com")):
            prefix = "https://storage.cloud.google.com/"
        else:
            return None
        bucket_name = input[len(prefix):].split("/")[0]

        if not just_bucket:
            file_name = input[len(f"{prefix}{bucket_name}/"):]
        else:
            file_name = None

        return {'bucket_name': bucket_name, 'file_name': file_name}

    # function to check if the input string is an amamzon url
    # def isAmazonParsable(input: str) -> bool:
    #     return input.startswith("arn:aws:s3") or input.startswith("s3://") or (re.search(".s3.*amazonaws.com/", input) is not None)

    # # function to check if the input string is a google url
    # def isGoogleParsable(input: str) -> bool:
    #     return input.startswith("gs://") or input.startswith("http://storage.cloud.google.com/") or input.startswith("https://storage.cloud.google.com/")

    # function to check if the input string points to a local file, needs to start with '/' to distinguish it from amazon and google
    def is_local(input: str) -> bool:
        return input.startswith("/")

    # returns aws region of a bucket
    # this is only necessary for amazon, as libcloud has different drivers for the different regions on amazon
    # libcloud does not distinguish between drivers for different regions on google
    def get_aws_region(bucket_name: str) -> str:
        if exists('/tmp/credentials.json'):
            credentials = json.load(open('/tmp/credentials.json'))
        elif exists('/opt/credentials.json'):
            credentials = json.load(open('/opt/credentials.json'))
        else:
            logging.error('no credentials file found')
            return None
        session = boto3.Session(
            aws_access_key_id=credentials['amazon']['aws_access_key_id'],
            aws_secret_access_key=credentials['amazon']['aws_secret_access_key'],
            aws_session_token=credentials['amazon']['aws_session_token']
        )
        s3_client = session.client('s3')
        bucket_location_response = s3_client.get_bucket_location(
            Bucket=bucket_name)

        region = bucket_location_response['LocationConstraint']
        if region == None:
            region = "us-east-1"

        return region

    # retrieves the file name from the path by removing bucket name

    def retrieve_file_name(input_str: str) -> str:
        prefix = ""
        if input_str.startswith("gs://"):
            bucket_name = input_str[len("gs://"):].split("/")[0]
            prefix = f"gs://{bucket_name}/"
        elif re.match("http://", input_str) and input_str[len("http://"):].startswith("storage.cloud.google.com"):
            prefix = "http://storage.cloud.google.com/"
        elif re.match("https://", input_str) and input_str[len("https://"):].startswith(("storage.cloud.google.com")):
            prefix = "https://storage.cloud.google.com/"
        if input_str.startswith("arn:aws:s3"):
            bucket_name = input_str[len("arn:aws:s3:::"):].split("/")[0]
            prefix = f"arn:aws:s3:::{bucket_name}/"
        elif input_str.startswith("s3://"):
            bucket_name = input_str[len("s3://"):].split("/")[0]
            prefix = f"s3://{bucket_name}/"
        elif re.search(".s3.*amazonaws.com/", input_str) is not None:
            bucket_name = input_str[len("https://"):].split(".")[0]
            prefix = f'https://{bucket_name}.s3.*amazonaws.com/'
        bucket_name = input_str[len(prefix):].split("/")[0]
        file_name = re.sub(prefix, '', input_str)
        return file_name
