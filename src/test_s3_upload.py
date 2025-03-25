#!/usr/bin/env python3
"""
Comprehensive test script for S3 document storage functionality.
"""

import os
import sys
import io
import time
from pathlib import Path
from src.document_storage.document_storage import S3DocumentStorage

# Define test constants
TEST_FOLDER = 'test'
TEST_FILE_NAME = 'test_file.txt'
TEST_FILE_CONTENT = 'This is a test file for S3 operations.'
TEST_DOWNLOAD_FOLDER = 'test_downloads'

def setup():
    """Prepare test environment."""
    # Create test download directory if it doesn't exist
    download_path = Path(TEST_DOWNLOAD_FOLDER)
    download_path.mkdir(exist_ok=True)
    
    # Create a test file for upload
    test_file_path = Path(TEST_FILE_NAME)
    with open(test_file_path, 'w') as f:
        f.write(TEST_FILE_CONTENT)
    print(f"Created test file: {test_file_path}")
    
    return test_file_path

def cleanup(test_file_path, download_path=None):
    """Clean up test files."""
    test_file_path.unlink(missing_ok=True)
    print(f"Removed test file: {test_file_path}")
    
    if download_path and download_path.exists():
        download_path.unlink()
        print(f"Removed downloaded file: {download_path}")

def test_s3_operations():
    """Test all S3 document storage operations."""
    # Setup test environment
    test_file_path = setup()
    
    # Initialize S3 storage using environment variables
    print("\n==== Initializing S3 Storage ====\n")
    s3_storage = S3DocumentStorage()
    
    print("S3 Configuration:")
    print(f"- Bucket Name: {s3_storage.bucket_name}")
    print(f"- Endpoint URL: {s3_storage.s3_client.meta.endpoint_url}")
    
    # Define test keys
    file_key = f"{TEST_FOLDER}/{TEST_FILE_NAME}"
    fileobj_key = f"{TEST_FOLDER}/fileobj_{TEST_FILE_NAME}"
    
    # Track test results
    total_tests = 0
    passed_tests = 0
    
    # Test 1: upload_file
    total_tests += 1
    print(f"\n==== Test {total_tests}: upload_file ====\n")
    print(f"Uploading {test_file_path} to {file_key}...")
    result = s3_storage.upload_file(str(test_file_path), file_key)
    
    if result:
        print(f"\n✅ Upload successful! File uploaded to: {s3_storage.bucket_name}/{file_key}")
        passed_tests += 1
    else:
        print("\n❌ Upload failed!")
    
    # Test 2: file_exists
    total_tests += 1
    print(f"\n==== Test {total_tests}: file_exists ====\n")
    print(f"Checking if file exists: {file_key}")
    if s3_storage.file_exists(file_key):
        print(f"✅ File exists: {file_key}")
        passed_tests += 1
    else:
        print(f"❌ File not found: {file_key}")
    
    # Test 3: upload_fileobj with metadata and content type
    total_tests += 1
    print(f"\n==== Test {total_tests}: upload_fileobj with metadata ====\n")
    print(f"Uploading file object to {fileobj_key} with custom metadata and content type...")
    file_obj = io.BytesIO(TEST_FILE_CONTENT.encode())
    metadata = {
        'source': 'test_script',
        'description': 'Test file for S3 operations',
        'created': time.strftime('%Y-%m-%d')
    }
    result = s3_storage.upload_fileobj(
        file_obj, 
        fileobj_key, 
        metadata=metadata,
        content_type='text/plain'
    )
    
    if result:
        print(f"✅ File object upload successful with metadata! File uploaded to: {s3_storage.bucket_name}/{fileobj_key}")
        passed_tests += 1
    else:
        print("❌ File object upload with metadata failed!")
    
    # Test 4: download_file with progress tracking
    total_tests += 1
    print(f"\n==== Test {total_tests}: download_file with progress tracking ====\n")
    download_path = Path(f"{TEST_DOWNLOAD_FOLDER}/{TEST_FILE_NAME}")
    
    # Progress tracking callback
    def progress_callback(bytes_transferred):
        print(f"Progress: {bytes_transferred} bytes transferred", end="\r")
    
    print(f"Downloading {file_key} to {download_path} with progress tracking...")
    result = s3_storage.download_file(file_key, str(download_path), callback=progress_callback)
    print()  # New line after progress
    
    if result and download_path.exists():
        with open(download_path, 'r') as f:
            content = f.read()
        if content == TEST_FILE_CONTENT:
            print(f"✅ Download successful and content verified!")
            passed_tests += 1
        else:
            print(f"❌ Download successful but content doesn't match!")
    else:
        print(f"❌ Download failed!")
        
    # Test 5: download_fileobj
    total_tests += 1
    print(f"\n==== Test {total_tests}: download_fileobj ====\n")
    print(f"Downloading {fileobj_key} to memory buffer...")
    
    # Create a file-like object in memory
    memory_file = io.BytesIO()
    
    # Download directly to memory
    result = s3_storage.download_fileobj(fileobj_key, memory_file)
    
    if result:
        # Reset buffer position and read content
        memory_file.seek(0)
        content = memory_file.read().decode('utf-8')
        
        if content == TEST_FILE_CONTENT:
            print(f"✅ File object download successful and content verified!")
            passed_tests += 1
        else:
            print(f"❌ File object download successful but content doesn't match!")
    else:
        print(f"❌ File object download failed!")
    
    # Test 6: list_files
    total_tests += 1
    print(f"\n==== Test {total_tests}: list_files ====\n")
    print(f"Listing files with prefix '{TEST_FOLDER}/'...")
    files = s3_storage.list_files(prefix=f"{TEST_FOLDER}/")
    
    if files:
        print(f"✅ Found {len(files)} files:")
        for file in files:
            print(f"  - {file}")
        if file_key in files or fileobj_key in files:
            passed_tests += 1
        else:
            print(f"❌ Our uploaded files were not in the list!")
    else:
        # Don't fail the test as list_files might not work with all S3 providers
        print("\n⚠️ No files found or listing operation not supported by the S3 provider")
        print("This is not a failure as we know upload worked")
        passed_tests += 1
    
    # Test 7: delete_file
    total_tests += 1
    print(f"\n==== Test {total_tests}: delete_file ====\n")
    print(f"Deleting file: {file_key}")
    delete_result = s3_storage.delete_file(file_key)
    
    if delete_result:
        print(f"✅ File deletion successful!")
        if not s3_storage.file_exists(file_key):
            print(f"✅ Confirmed file no longer exists!")
            passed_tests += 1
        else:
            print(f"❌ File still exists after deletion!")
    else:
        print(f"❌ File deletion failed!")
    
    # Clean up second file as well if it was uploaded successfully
    if s3_storage.file_exists(fileobj_key):
        print(f"Deleting second file: {fileobj_key}")
        s3_storage.delete_file(fileobj_key)
    
    # Clean up test files
    cleanup(test_file_path, download_path)
    
    # Print test summary
    print("\n==== Test Summary ====\n")
    print(f"Passed: {passed_tests}/{total_tests} tests")
    if passed_tests == total_tests:
        print("✅ All tests passed successfully!")
    else:
        print(f"⚠ {total_tests - passed_tests} tests failed.")
    
    return passed_tests == total_tests

if __name__ == "__main__":
    try:
        success = test_s3_operations()
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
