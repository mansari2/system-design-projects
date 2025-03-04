import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import json

class S3Storage:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None, region_name='us-east-1'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

    def upload_file(self, file_name, object_name=None):
        if object_name is None:
            object_name = file_name
        try:
            self.s3_client.upload_file(file_name, self.bucket_name, object_name)
            print(f"File {file_name} uploaded to {self.bucket_name}/{object_name}.")
        except FileNotFoundError:
            print(f"The file {file_name} was not found.")
        except NoCredentialsError:
            print("Credentials not available.")
        except ClientError as e:
            print(f"Failed to upload {file_name}: {e}")

    def download_file(self, object_name, file_name):
        try:
            self.s3_client.download_file(self.bucket_name, object_name, file_name)
            print(f"File {object_name} downloaded from {self.bucket_name} to {file_name}.")
        except ClientError as e:
            print(f"Failed to download {object_name}: {e}")

    def list_files(self):
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(obj['Key'])
            else:
                print("No files found in the bucket.")
        except ClientError as e:
            print(f"Failed to list files: {e}")

    def delete_file(self, object_name):
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=object_name)
            print(f"File {object_name} deleted from {self.bucket_name}.")
        except ClientError as e:
            print(f"Failed to delete {object_name}: {e}")