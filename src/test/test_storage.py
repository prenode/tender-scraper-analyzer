import pytest 

from src.document_storage.document_storage import S3DocumentStorage
from src.document_storage.tender_storage import TenderStorage


def test_bucket_info():
    """
    Test the bucket information retrieval from S3.
    """
    s3_document_storage = S3DocumentStorage()
    bucket_info = s3_document_storage.get_bucket_info()

    assert bucket_info is not None, "Failed to retrieve bucket information."

def test_update_metadata():
    """
    Test adding a new tender ID to the metadata JSON file in S3.
    """
    s3_document_storage = S3DocumentStorage()

    tender_storage = TenderStorage(s3_document_storage)
    tender_storage.update_metadata({"tender_ids": ["test_tender_id"]})
    metadata = s3_document_storage.get_bucket_info()
    
    assert metadata["tender_ids"].contains("test_tender_id"), "Failed to update metadata with new tender ID."