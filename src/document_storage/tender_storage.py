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


    def upload_new_tender(self, data:dict, document_paths:List[Path], publication: bool):
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
            self.s3_document_storage.upload_file(str(document_path), f"{data.get('id')}/{str(document_path).split('/')[-1]}")
        # Update the metadata JSON file
        metadata = self.s3_document_storage.get_bucket_info()
        tender_ids =  self.bucket_info.get("tender_id")

        if data.get("id") not in tender_ids:
            if publication:
                tender_ids[data.get('id')] = {"publication": True, "documents": False, "data":data}

            else:
                tender_ids[data.get('id')] = {"publication": False, "documents": False, "data":data}
            self.bucket_info["tender_id"] = tender_ids
            self.s3_document_storage.update_metadata(self.bucket_info)
            self.bucket_info = self.s3_document_storage.get_bucket_info()

    
    def get_tender_documents(self, tender_id:str) -> List[str]:
        """
               Retrieves the documents associated with a specific tender ID from S3.

        """
        # Get the list of documents for the given tender ID
        documents = self.s3_document_storage.list_files(prefix=tender_id)
        return documents


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
        data = self.bucket_info
        data["tender_id"][tender_id]["documents"] = True
        self.s3_document_storage.update_metadata(data)
