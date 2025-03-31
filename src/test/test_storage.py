import pytest 

from src.document_storage.document_storage import S3DocumentStorage


def test_bucket_info():
    """
    Test the bucket information retrieval from S3.
    """
    s3_document_storage = S3DocumentStorage()
    bucket_info = s3_document_storage.get_bucket_info()

    assert bucket_info is not None, "Failed to retrieve bucket information."
