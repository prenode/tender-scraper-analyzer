
import boto3
from botocore.exceptions import ClientError
import logging
from typing import Optional, List, BinaryIO
from pathlib import Path

class S3DocumentStorage:
    """
    Wrapper class for AWS S3 storage operations.
    Handles uploading, downloading, and managing documents in S3 buckets.
    """

    def __init__(self, bucket_name: str, aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None, endpoint_url: str = 'https://s3.telemaxx.cloud'):
        """
        Initialize S3 client and bucket.
        
        Args:
            bucket_name: Name of the S3 bucket
            aws_access_key_id: Optional AWS access key ID. If not provided, falls back to environment variables
            aws_secret_access_key: Optional AWS secret access key. If not provided, falls back to environment variables
            region: AWS region name, defaults to eu-central-1
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id = aws_access_key_id,
            aws_secret_access_key = aws_secret_access_key,
            endpoint_url = endpoint_url,
        )

        
    def upload_file(self, file_path: str, s3_key: str) -> bool:
        """
        Upload a file to Ceph S3 bucket.
        
        Args:
            file_path: Local path to the file
            s3_key: Destination path in S3 bucket
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            return True
        except ClientError as e:
            logging.error(f"Error uploading file to Ceph S3: {e}")
            return False        


    def upload_fileobj(self, file_obj: BinaryIO, s3_key: str) -> bool:
        """
        Upload a file-like object to S3 bucket.
        
        Args:
            file_obj: File-like object to upload
            s3_key: Destination path in S3 bucket
            
        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            self.s3_client.upload_fileobj(file_obj, self.bucket_name, s3_key)
            return True
        except ClientError as e:
            logging.error(f"Error uploading file object to S3: {e}")
            return False

    def download_file(self, s3_key: str, local_path: str) -> bool:
        """
        Download a file from S3 bucket.
        
        Args:
            s3_key: Path of file in S3 bucket
            local_path: Local destination path
            
        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            self.s3_client.download_file(self.bucket_name, s3_key, local_path)
            return True
        except ClientError as e:
            logging.error(f"Error downloading file from S3: {e}")
            return False

    def list_files(self, prefix: str = '') -> List[str]:
        """
        List all files in the S3 bucket with given prefix.
        
        Args:
            prefix: Optional prefix to filter results
            
        Returns:
            List of file keys in the bucket
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        except ClientError as e:
            logging.error(f"Error listing files in S3: {e}")
            return []

    def delete_file(self, s3_key: str) -> bool:
        """
        Delete a file from S3 bucket.
        
        Args:
            s3_key: Path of file in S3 bucket
            
        Returns:
            bool: True if deletion successful, False otherwise
        """
        try:
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError as e:
            logging.error(f"Error deleting file from S3: {e}")
            return False

    def file_exists(self, s3_key: str) -> bool:
        """
        Check if a file exists in S3 bucket.
        
        Args:
            s3_key: Path of file in S3 bucket
            
        Returns:
            bool: True if file exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True
        except ClientError:
            return False
