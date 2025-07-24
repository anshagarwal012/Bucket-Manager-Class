import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()  # Load variables from .env file

class BucketManager:
    def __init__(self):
        self.space_name = os.getenv("DO_SPACE_NAME")
        self.region = os.getenv("DO_REGION")
        self.access_key = os.getenv("DO_ACCESS_KEY")
        self.secret_key = os.getenv("DO_SECRET_KEY")

        self.endpoint_url = f"https://{self.region}.digitaloceanspaces.com"
        self.base_url = f"https://{self.space_name}.{self.region}.digitaloceanspaces.com"

        self.s3 = boto3.client(
            's3',
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )

    def list_files(self, prefix=''):
        try:
            response = self.s3.list_objects_v2(Bucket=self.space_name, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        except ClientError as e:
            print(f"Error listing files: {e}")
            return []

    def upload_file(self, file_path, dest_name, public=True):
        try:
            extra_args = {'ACL': 'public-read'} if public else {}
            self.s3.upload_file(file_path, self.space_name, dest_name, ExtraArgs=extra_args)
            return True
        except ClientError as e:
            print(f"Error uploading file: {e}")
            return False

    def replace_file(self, file_path, dest_name, public=True):
        return self.upload_file(file_path, dest_name, public)

    def delete_file(self, file_name):
        try:
            self.s3.delete_object(Bucket=self.space_name, Key=file_name)
            return True
        except ClientError as e:
            print(f"Error deleting file: {e}")
            return False

    def get_url(self, file_name):
        return f"{self.base_url}/{file_name}"



manager = BucketManager()
filename = "character.jpg"

# Upload
manager.upload_file(filename, filename)

# List
print(manager.list_files())

# Get public URL
print(manager.get_url(filename))

# Delete
# manager.delete_file(filename)
