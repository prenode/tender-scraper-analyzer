import boto3
import botocore
from botocore.exceptions import ClientError
import logging
from typing import Optional, List, Dict, Union, BinaryIO, Any, Tuple, Iterator, Callable
from pathlib import Path
import os
import io
import time
import hashlib
import threading
import functools
import mimetypes
import math
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from dotenv import load_dotenv
import json


# Configure logging
logger = logging.getLogger(__name__)

# Initialize mimetypes detection
mimetypes.init()

# Constants for multipart operations
MULTIPART_THRESHOLD = 100 * 1024 * 1024  # 100 MB
MULTIPART_CHUNKSIZE = 10 * 1024 * 1024  # 10 MB
MAX_CONCURRENCY = 10

# Load environment variables from .env file
load_dotenv()


# Retry decorator for S3 operations
def retry_s3_operation(max_retries=3, backoff_factor=0.5, error_codes=None):
    """Retry S3 operations on failure with exponential backoff."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code")
                    if error_codes and error_code not in error_codes:
                        # Don't retry if error code is not in our retry list
                        raise

                    retries += 1
                    if retries > max_retries:
                        logger.error(f"Failed after {max_retries} retries: {e}")
                        raise

                    # Calculate delay with exponential backoff
                    delay = backoff_factor * (2 ** (retries - 1))
                    logger.warning(
                        f"Retrying {func.__name__} after error: {e}. Retry {retries}/{max_retries} in {delay:.2f}s"
                    )
                    time.sleep(delay)
                except Exception as e:
                    # Don't retry on non-ClientError exceptions
                    logger.exception(f"Non-retryable error in {func.__name__}: {e}")
                    raise

        return wrapper

    return decorator


class S3DocumentStorage:
    """
    Wrapper class for S3 storage operations.
    Handles uploading, downloading, and managing documents in S3 buckets with:
    - Retry mechanism for transient failures
    - Thread safety for concurrent operations
    - Detailed logging and error handling
    - Performance optimizations like multipart uploads for large files
    """

    def __init__(
        self,
        bucket_name: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        region_name: Optional[str] = None,
        max_pool_connections: int = 10,
        connect_timeout: int = 5,
        read_timeout: int = 60,
        retries: int = 3,
    ):
        """
        Initialize S3 client and bucket with production-ready settings.

        Args:
            bucket_name: Name of the S3 bucket. If not provided, falls back to S3_BUCKET_NAME environment variable
            aws_access_key_id: AWS access key ID. If not provided, falls back to AWS_ACCESS_KEY_ID environment variable
            aws_secret_access_key: AWS secret access key. If not provided, falls back to AWS_SECRET_ACCESS_KEY environment variable
            endpoint_url: S3 endpoint URL. If not provided, falls back to S3_ENDPOINT_URL environment variable
            region_name: AWS region name. If not provided, falls back to AWS_REGION environment variable
            max_pool_connections: Maximum number of connections in connection pool
            connect_timeout: Connection timeout in seconds
            read_timeout: Read timeout in seconds
            retries: Number of retry attempts for S3 operations
        """
        # Thread lock for thread safety in concurrent operations
        self._lock = threading.RLock()

        # Use provided values or fall back to environment variables
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME")
        if not self.bucket_name:
            raise ValueError(
                "S3 bucket name must be provided either as a parameter or as S3_BUCKET_NAME environment variable"
            )

        # Get credentials
        self.access_key = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        self.secret_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL")
        self.region_name = region_name or os.getenv("AWS_REGION")

        # Store configuration for possible reconnection
        self.config = {
            "max_pool_connections": max_pool_connections,
            "connect_timeout": connect_timeout,
            "read_timeout": read_timeout,
            "retries": retries,
        }

        # Initialize clients
        self._initialize_clients()

        # Cache for bucket existence check to avoid repeated API calls
        self._bucket_exists_cache = None

        logger.info(
            f"Initialized S3DocumentStorage for bucket '{self.bucket_name}' at {self.endpoint_url}"
        )

    def _initialize_clients(self):
        """
        Initialize boto3 clients and resources with appropriate configuration.
        This is separated to allow reconnection if needed.
        """
        # Create a custom session
        self.session = boto3.session.Session()

        # Define the client configuration with timeouts and retries
        client_config = botocore.config.Config(
            signature_version="s3",  # Use signature version compatible with Telemaxx S3
            retries={"max_attempts": self.config["retries"], "mode": "standard"},
            max_pool_connections=self.config["max_pool_connections"],
            connect_timeout=self.config["connect_timeout"],
            read_timeout=self.config["read_timeout"],
            s3={"addressing_style": "path"},  # Use path-style addressing
        )

        # Initialize the S3 client
        self.s3_client = self.session.client(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=client_config,
        )

        # Initialize S3 resource for more object-oriented operations
        self.s3_resource = self.session.resource(
            "s3",
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=client_config,
        )

        # Get the bucket object
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

    def reconnect(self):
        """
        Reconnect to S3 service in case of persistent connection issues.
        """
        with self._lock:
            logger.info(f"Reconnecting to S3 service for bucket '{self.bucket_name}'")
            self._initialize_clients()
            return True

    def _detect_content_type(self, file_path: str) -> str:
        """
        Detect MIME type based on file extension or content.

        Args:
            file_path: Path to the file

        Returns:
            MIME type string
        """
        content_type, _ = mimetypes.guess_type(file_path)
        if not content_type:
            # Default to binary/octet-stream if detection fails
            content_type = "application/octet-stream"
        return content_type

    def _get_multipart_config(self, file_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Configure transfer parameters for multipart operations.

        Args:
            file_size: Size of the file to upload if known

        Returns:
            Dictionary with transfer configuration
        """
        config = {
            "multipart_threshold": MULTIPART_THRESHOLD,
            "multipart_chunksize": MULTIPART_CHUNKSIZE,
            "max_concurrency": MAX_CONCURRENCY,
            "use_threads": True,
        }

        # Optimize chunk size if file size is known
        if file_size and file_size > MULTIPART_THRESHOLD:
            # Calculate optimal chunk size (between 10MB and 100MB)
            # Aim for about 10 chunks for better parallelism but don't go below min chunk size
            optimal_parts = 10
            calculated_chunk_size = math.ceil(file_size / optimal_parts)
            chunk_size = max(
                min(calculated_chunk_size, 100 * 1024 * 1024), MULTIPART_CHUNKSIZE
            )
            # Ensure it's a multiple of 1MB for better compatibility
            chunk_size = math.ceil(chunk_size / (1024 * 1024)) * 1024 * 1024
            config["multipart_chunksize"] = chunk_size

        return config

    @retry_s3_operation(
        max_retries=3,
        error_codes=["RequestTimeout", "ConnectionError", "InternalError"],
    )
    def upload_file(
        self,
        file_path: str,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Upload a file to S3 bucket with content type detection and multipart upload support.

        Args:
            file_path: Local path to the file
            s3_key: Destination path in S3 bucket
            metadata: Optional metadata dictionary to attach to the object
            content_type: Optional MIME type (auto-detected if not provided)
            extra_args: Optional additional S3 arguments

        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            file_size = os.path.getsize(file_path)
            detected_content_type = content_type or self._detect_content_type(file_path)

            upload_args = extra_args or {}
            upload_args["ContentType"] = detected_content_type

            if metadata:
                upload_args["Metadata"] = metadata

            transfer_config = self._get_multipart_config(file_size)
            transfer_manager = boto3.s3.transfer.TransferConfig(**transfer_config)

            logger.info(
                f"Uploading {file_path} ({file_size} bytes) to {self.bucket_name}/{s3_key}"
            )

            self.s3_client.upload_file(
                file_path,
                self.bucket_name,
                s3_key,
                ExtraArgs=upload_args,
                Config=transfer_manager,
            )

            logger.info(
                f"Successfully uploaded {file_path} to {self.bucket_name}/{s3_key}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error uploading {file_path} to {self.bucket_name}/{s3_key}: {e}"
            )
            return False

    def upload_files(
        self,
        file_paths: List[str],
        s3_prefix: Optional[str] = None,
        metadata: Optional[Dict[str, str]] = None,
        content_types: Optional[Dict[str, str]] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, bool]:
        """
        Upload multiple files to S3 bucket with flexible configuration.

        Args:
            file_paths: List of local file paths to upload
            s3_prefix: Optional prefix for S3 destination paths
            metadata: Optional metadata dictionary to attach to all objects
            content_types: Optional dictionary mapping file paths to MIME types
            extra_args: Optional additional S3 arguments to apply to all uploads

        Returns:
            Dictionary with file paths as keys and boolean upload status as values
        """
        upload_results = {}

        for file_path in file_paths:
            try:
                # Determine S3 key
                filename = os.path.basename(file_path)
                s3_key = (
                    os.path.join(s3_prefix or "", filename) if s3_prefix else filename
                )

                # Determine content type
                file_content_type = (
                    content_types.get(file_path) if content_types else None
                )

                # Upload individual file
                upload_status = self.upload_file(
                    file_path=file_path,
                    s3_key=s3_key,
                    metadata=metadata,
                    content_type=file_content_type,
                    extra_args=extra_args,
                )

                upload_results[file_path] = upload_status

            except Exception as e:
                logger.error(f"Failed to process file {file_path}: {e}")
                upload_results[file_path] = False

        # Log summary
        total_files = len(file_paths)
        successful_uploads = sum(upload_results.values())
        failed_uploads = total_files - successful_uploads

        logger.info(
            f"Upload Summary: "
            f"Total Files: {total_files}, "
            f"Successful: {successful_uploads}, "
            f"Failed: {failed_uploads}"
        )

        return upload_results

    @retry_s3_operation(max_retries=3)
    def upload_fileobj(
        self,
        file_obj: BinaryIO,
        s3_key: str,
        metadata: Optional[Dict[str, str]] = None,
        content_type: Optional[str] = None,
        extra_args: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Upload a file-like object to S3 bucket with metadata and content type support.

        Args:
            file_obj: File-like object to upload
            s3_key: Destination path in S3 bucket
            metadata: Optional metadata dictionary to attach to the object
            content_type: Optional MIME type (defaults to application/octet-stream if not provided)
            extra_args: Optional additional S3 arguments

        Returns:
            bool: True if upload successful, False otherwise
        """
        try:
            file_size = None
            if hasattr(file_obj, "seek") and hasattr(file_obj, "tell"):
                current_pos = file_obj.tell()
                file_obj.seek(0, os.SEEK_END)
                file_size = file_obj.tell()
                file_obj.seek(current_pos)  # Reset position

            upload_args = extra_args or {}

            if content_type:
                upload_args["ContentType"] = content_type
            else:
                guessed_type, _ = mimetypes.guess_type(s3_key)
                upload_args["ContentType"] = guessed_type or "application/octet-stream"

            if metadata:
                upload_args["Metadata"] = metadata

            if file_size:
                transfer_config = self._get_multipart_config(file_size)
                transfer_manager = boto3.s3.transfer.TransferConfig(**transfer_config)
            else:
                transfer_manager = boto3.s3.transfer.TransferConfig(
                    multipart_threshold=MULTIPART_THRESHOLD,
                    multipart_chunksize=MULTIPART_CHUNKSIZE,
                    max_concurrency=MAX_CONCURRENCY,
                )

            logger.info(f"Uploading file object to {self.bucket_name}/{s3_key}")

            self.s3_client.upload_fileobj(
                file_obj,
                self.bucket_name,
                s3_key,
                ExtraArgs=upload_args,
                Config=transfer_manager,
            )

            logger.info(
                f"Successfully uploaded file object to {self.bucket_name}/{s3_key}"
            )
            return True
        except Exception as e:
            logger.error(
                f"Error uploading file object to {self.bucket_name}/{s3_key}: {e}"
            )
            return False

    @retry_s3_operation(max_retries=3)
    def download_file(
        self,
        s3_key: str,
        local_path: str,
        callback: Optional[Callable[[int], None]] = None,
    ) -> bool:
        """
        Download a file from S3 bucket with chunked download support.

        Args:
            s3_key: Path of file in S3 bucket
            local_path: Local destination path
            callback: Optional callback function to track progress, receives bytes transferred

        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)

            if not self.file_exists(s3_key):
                logger.error(
                    f"File {s3_key} does not exist in bucket {self.bucket_name}"
                )
                return False

            try:
                response = self.s3_client.head_object(
                    Bucket=self.bucket_name, Key=s3_key
                )
                file_size = response.get("ContentLength", 0)
                transfer_config = self._get_multipart_config(file_size)
            except Exception as e:
                logger.warning(f"Could not get size for {s3_key}: {e}")
                transfer_config = self._get_multipart_config()

            transfer_manager = boto3.s3.transfer.TransferConfig(**transfer_config)

            logger.info(f"Downloading {self.bucket_name}/{s3_key} to {local_path}")

            self.s3_client.download_file(
                self.bucket_name,
                s3_key,
                local_path,
                Callback=callback,
                Config=transfer_manager,
            )

            logger.info(f"Successfully downloaded {s3_key} to {local_path}")
            return True
        except Exception as e:
            logger.error(f"Error downloading {s3_key} from S3: {e}")
            return False

    def download_fileobj(
        self,
        s3_key: str,
        file_obj: BinaryIO,
        callback: Optional[Callable[[int], None]] = None,
    ) -> bool:
        """
        Download a file from S3 bucket to a file-like object.

        Args:
            s3_key: Path of file in S3 bucket
            file_obj: File-like object to write the file contents to
            callback: Optional callback function to track progress, receives bytes transferred

        Returns:
            bool: True if download successful, False otherwise
        """
        try:
            transfer_config = self._get_multipart_config()
            transfer_manager = boto3.s3.transfer.TransferConfig(**transfer_config)

            logger.info(f"Downloading {self.bucket_name}/{s3_key} to file object")

            self.s3_client.download_fileobj(
                self.bucket_name,
                s3_key,
                file_obj,
                Callback=callback,
                Config=transfer_manager,
            )

            logger.info(f"Successfully downloaded {s3_key} to file object")
            return True
        except Exception as e:
            logger.error(f"Error downloading {s3_key} from S3 to file object: {e}")
            return False

    def list_files(self, prefix: str = "") -> List[str]:
        """
        List files in the S3 bucket with given prefix.

        Args:
            prefix: Optional prefix to filter results

        Returns:
            List of file keys in the bucket
        """
        results = []

        try:
            logger.info(
                f"Listing objects in bucket {self.bucket_name} with prefix '{prefix}'"
            )

            config = boto3.session.Config(
                signature_version="s3v4", s3={"addressing_style": "path"}
            )

            temp_client = boto3.client(
                "s3",
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                endpoint_url=self.endpoint_url,
                config=config,
            )

            response = temp_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=prefix
            )

            if "Contents" in response:
                for obj in response["Contents"]:
                    results.append(obj["Key"])

            logger.info(f"Found {len(results)} files in bucket {self.bucket_name}")
            return results

        except Exception as e:
            logger.error(f"Error listing files: {e}")

            # Fall back to single file lookup if looking for a specific file
            if prefix and not prefix.endswith("/") and self.file_exists(prefix):
                logger.info(f"Found specific file: {prefix}")
                return [prefix]

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

    def get_bucket_info(self):
        """
        Get metadata JSON on top level of bucket.
        """
        # check if metadata file exists,
        # if not, create it
        metadata_file = "bucket_metadata.json"
        if not self.file_exists(metadata_file):
            # generate empty json 
            metadata = {"bucket":"" + self.bucket_name, "updated_at": time.time(),
                        "tender_id" :[]}
            # upload empty metadata file
            with open(metadata_file, "w") as f:
                json.dump(metadata, f)
            # upload file to s3
            # create metadata file
            self.upload_file(
                file_path=metadata_file,
                s3_key=metadata_file,
                content_type="application/json",
            )
            return metadata
        else:
            # download metadata file
            self.download_file(s3_key=metadata_file, local_path=metadata_file)

            with open(metadata_file, "r") as f:
                metadata = json.load(f)
            return metadata
        
        
    def update_metadata(self, metadata: Dict[str, Any]) -> bool:
        """
        Update the metadata JSON file in the S3 bucket.

        Args:
            metadata: Dictionary containing metadata to be updated"
        """
        metadata_file = "bucket_metadata.json"
        try:
            with open(metadata_file, "w") as f:
                json.dump(metadata, f)
            self.upload_file(
                file_path=metadata_file,
                s3_key=metadata_file,
                content_type="application/json",
            )
            return True
        except Exception as e:
            logger.error(f"Error updating metadata: {e}")
            return False