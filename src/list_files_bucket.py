import os
import sys
import io
import time
from pathlib import Path
from document_storage.document_storage import S3DocumentStorage

document_store = S3DocumentStorage()

print(document_store.list_files())
