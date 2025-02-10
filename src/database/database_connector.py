import chromadb



class DatabaseConnector:
    def __init__(self):
        self.chroma_client = chromadb.PersistentClient()
    
    def add_collection(self, collection_str: str) -> chromadb.Collection:
        # check if collection exists
        collection = self.chroma_client.get_or_create_collection(name = str(collection_str))
        return collection

    def add_document(self, collection_str: str, documents: dict):
        collection = self.chroma_client.get_collection(str(collection_str))
        collection.add(documents)

    def get_document(self, collection_str: str, document_id: str):
        collection = self.chroma_client.client.get_collection(collection_str)
        return collection.get_document(document_id)