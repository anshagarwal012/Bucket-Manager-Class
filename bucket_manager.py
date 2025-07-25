import os
import shutil
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

    def upload_file(self, file_path, dest_name, public=False):
        try:
            extra_args = {'ACL': 'private'}  # default is private
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

    def get_presigned_url(self, file_name, expiration=3600):
        try:
            url = self.s3.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.space_name,
                    'Key': file_name
                },
                ExpiresIn=expiration  # Time in seconds
            )
            return url
        except ClientError as e:
            print(f"Error generating presigned URL: {e}")
            return None

    def upload_all_files_from_folder(self, folder_path, dest_folder):
        # Get all files from the folder
        files = os.listdir(folder_path)
        
        for file in files:
            file_path = os.path.join(folder_path, file)
            
            if os.path.isfile(file_path):
                # Generate the S3 destination path
                dest_name = f"{dest_folder}/{file}"
                
                # Upload the file to S3
                if self.upload_file(file_path, dest_name):
                    print(f"Uploaded {file} to {dest_name}")
                    
                    # After uploading, move the file to a new folder
                    moved_path = os.path.join(folder_path, f"uploaded_books/{file}")
                    try:
                        os.makedirs(os.path.join(folder_path, "uploaded_books"), exist_ok=True)
                        shutil.move(file_path, moved_path)
                        print(f"Moved {file} to {moved_path}")
                    except Exception as e:
                        print(f"Error moving file {file}: {e}")

# Initialize BucketManager
manager = BucketManager()

# Define folder paths
print_book_folder = "print_book"  # Folder where your files are
uploaded_books_folder = "uploaded_books"  # Folder where you want to move uploaded files

# Upload and move files
manager.upload_all_files_from_folder(print_book_folder, uploaded_books_folder)
