from typing import List, Dict, Any
from src.document_storage.document_storage import S3DocumentStorage
from pathlib import Path

class TenderStorage:

    def __init__(self, s3_document_storage: S3DocumentStorage):
        """
        Initializes the TenderStorage with an S3DocumentStorage instance.

        Args:
            s3_document_storage (S3DocumentStorage): Storage client for saving documents to S3.
        """
        self.s3_document_storage = s3_document_storage
        self.bucket_info = self.s3_document_storage.get_bucket_info()


    def upload_new_tender(self, tender_id:str, document_paths:List[Path]) -> bool:
        """
        Generates a new tender in the top level metadata json file. Additionally all documents in document_paths are uploaded to S3. THe path is tender_id/document_name.
        Args:
            tender_id (str): The ID of the tender.
            document_paths (str): The local paths to the documents.
        Returns:
            bool: True if the upload was successful, False otherwise.
        """
        # Upload the documents to S3
        for document_path in document_paths:
            self.s3_document_storage.upload_file(str(document_path), f"{tender_id}/{str(document_path).split('/')[-1]}")
        # Update the metadata JSON file
        metadata = self.s3_document_storage.get_bucket_info()
        tender_ids = metadata.get("tender_ids", [])
        if tender_id not in tender_ids:
            tender_ids.append(tender_id)
            metadata["tender_ids"] = tender_ids
            self.s3_document_storage.update_metadata(metadata)
        return True
    

    def get_tender_documents(self, tender_id:str) -> List[str]:
        """
               Retrieves the documents associated with a specific tender ID from S3.

        """
        pass


    def add_to_tender(self, tender_id: str, document_paths: List[Path]):
        """
        Adds documents to an existing tender in S3. The documents are uploaded to the path tender_id/document_name.

        Args:
            tender_id (str): The ID of the tender.
            document_paths (List[str]): The local paths to the documents.
        """
        # Upload the documents to S3

        for document_path in document_paths:
            self.s3_document_storage.upload_file(str(document_path), f"{tender_id}/documents/{str(document_path).split('/')[-1]}")