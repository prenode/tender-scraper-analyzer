from typing import List, Dict, Any
from src.document_storage import S3DocumentStorage


class TenderStorage:

    def __init__(self):
        """
        Initializes the TenderStorage with an S3DocumentStorage instance.

        Args:
            s3_document_storage (S3DocumentStorage): Storage client for saving documents to S3.
        """
        self.s3_document_storage = S3DocumentStorage()
        bucket_info = self.s3_document_storage.get_bucket_info()
        