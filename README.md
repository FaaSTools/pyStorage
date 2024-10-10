# pyStorage library

## Info

This project includes modules with abstractions for different cloud services, which can be used for cloud functions deployed with a `Python 3.7` 🐍 runtime.

## How to use pyStorage

```python
from storage.pyStorage import pyStorage

def fun():
    # provide pyStorage your AWS and/or GCP account credentials
    pyStorage.create_cred_file(
        aws_access_key_id=event["aws_access_key_id"],
        aws_secret_key=event["aws_secret_key"],
        aws_session_token=event["aws_session_token"],
        gcp_client_email=event["gcp_client_email"],
        gcp_private_key=event["gcp_private_key"],
        gcp_project_id=event["gcp_project_id"],
    )

    # copies the file from source_url to the target_url
    pyStorage.copy(source_url, target_url)
```

Example for Download:
```python
    pyStorage.copy("arn:aws:s3:::bucket_name/file_name", "/tmp/file_name")
    # or
    pyStorage.copy("gs://bucket_name/file_name", "/tmp/file_name")
```
Example for Upload:
```py
    pyStorage.copy("/tmp/file_name", "s3://bucket_name/file_name")
```

Example for copy files between two buckets:
```py
    pyStorage.copy("gs://bucket_name/file_name", "arn:aws:s3:::bucket_name/file_name")
    # or 
    pyStorage.copy("s3://bucket_name/file_name", "s3://other_bucket_name/file_name")
```

Special case: 
Example for Copy entire bucket:
```py
    pyStorage.copy_bucket("s3://bucket_name/", "s3://other_bucket_name/")
```


