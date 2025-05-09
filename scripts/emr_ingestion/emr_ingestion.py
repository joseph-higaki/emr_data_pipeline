"""
EMR Data Ingestion Script

This script ingests EMR data (FHIR or CSV) and converts it to Parquet in GCS.
Downloads and processes synthetic patient data from a ZIP file.

Usage:
    As standalone script:
        python ingestion_script.py

    In Docker:
        Will be executed via DockerOperator
"""

import os
import sys
import logging
import datetime
from google.cloud import storage
import tempfile
import requests
import zipfile
import io
import shutil
from typing import Iterator, Tuple, Set, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_ingestion')

def get_expected_entities(env_var_name: str = 'EMR_EXPECTED_ENTITIES') -> Set[str]:
    """
    Get the list of expected entities from environment variable.
    Returns a set of entity names.
    
    Args:
        env_var_name: Name of the environment variable to check
        
    Returns:
        Set of entity names
    """
    entities_str = os.environ.get(env_var_name, '')
    if not entities_str:
        logger.warning(f"{env_var_name} not set, using default empty set")
        return set()
    
    # Convert to lowercase set for case-insensitive matching
    entities = {entity.strip().lower() for entity in entities_str.split(',')}
    logger.info(f"Loaded {len(entities)} expected entities: {', '.join(sorted(entities))}")
    return entities

def extract_entity_name(file_name: str) -> str:
    """
    Extracts entity name from a file name.
    
    Args:
        file_name: Name of the file
        
    Returns:
        Entity name
    """
    # Use basename to get the rightmost part of the path
    base_name = os.path.basename(file_name)
    
    # Remove the extension and convert to lowercase
    entity_name = os.path.splitext(base_name)[0].lower()
    return entity_name

def download_zip_file(source_url: str) -> bytes:
    """
    Downloads a ZIP file from the specified URL.
    
    Args:
        source_url: URL to the ZIP file
        
    Returns:
        ZIP file content as bytes
    """
    logger.info(f"Downloading ZIP from: {source_url}")
    response = requests.get(source_url)
    response.raise_for_status()  # Raise exception for HTTP errors
    return response.content

def extract_zip_file(zip_content: bytes, temp_dir: str) -> List[str]:
    """
    Extracts ZIP file content to a temporary directory.
    
    Args:
        zip_content: ZIP file content as bytes
        temp_dir: Directory to extract files to
        
    Returns:
        List of file names in the ZIP
    """
    with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_ref:
        # Get list of files in the ZIP
        file_list = zip_ref.namelist()
        logger.info(f"ZIP contains {len(file_list)} files")
        
        # Extract all files to the temporary directory
        zip_ref.extractall(temp_dir)
        logger.info(f"Extracted all files to {temp_dir}")
        
        return file_list

def read_emr_patient_data(source_url: str) -> Iterator[Tuple[str, str]]:
    """Read EMR patient data
    Downloads synthetic health data ZIP file from the specified URL,
    extracts the files, and returns an iterator of file paths.
    
    Args:
        source_url: URL to the synthetic health data ZIP file
        
    Returns:
        Iterator of paths to extracted files
    """
    # Create a temporary directory to extract files
    temp_dir = tempfile.mkdtemp()
    logger.info(f"Created temporary directory: {temp_dir}")
    
    try:
        # Download and extract the ZIP file
        zip_content = download_zip_file(source_url)
        file_list = extract_zip_file(zip_content, temp_dir)
        
        # Return iterator of full file paths
        for file_name in file_list:
            file_path = os.path.join(temp_dir, file_name)
            if os.path.isfile(file_path):
                logger.debug(f"Yielding file: {file_path}")
                yield file_path, file_name
                    
    except Exception as e:
        logger.error(f"Error processing source data: {str(e)}")
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

def upload_to_gcs(ingested_at: str, file_path: str, file_name: str, 
                  bucket_name: str, destination_path: str, 
                  storage_client: Optional[storage.Client] = None) -> str:
    """Upload a file to GCS
    
    Args:
        ingested_at: Timestamp for the ingestion
        file_path: Local path to the file to upload
        file_name: Original file name for preservation
        bucket_name: GCS bucket name
        destination_path: Path prefix in the bucket
        storage_client: Optional GCS client for dependency injection
        
    Returns:
        GCS URI of the uploaded file
    """
    # Initialize GCS client if not provided
    client = storage_client or storage.Client()
    bucket = client.bucket(bucket_name)
    
    # Use basename to flatten the hierarchy - ignore any subfolder structure in the ZIP
    base_name = os.path.basename(file_name)
    full_destination = f"{destination_path}/{ingested_at}/{base_name}"
    
    logger.info(f"Uploading to GCS: gs://{bucket_name}/{full_destination}")
    
    # Create a blob and upload the file
    blob = bucket.blob(full_destination)
    blob.upload_from_filename(file_path)
    
    logger.info(f"Successfully uploaded to gs://{bucket_name}/{full_destination}")
    return f"gs://{bucket_name}/{full_destination}"

def process_files(source_url: str, bucket_name: str, destination_prefix: str, 
                 expected_entities: Set[str], storage_client: Optional[storage.Client] = None) -> Tuple[List[str], List[str]]:
    """
    Process files from the source URL and upload them to GCS.
    
    Args:
        source_url: URL to the synthetic health data ZIP file
        bucket_name: GCS bucket name
        destination_prefix: Path prefix in the bucket
        expected_entities: Set of expected entity names
        storage_client: Optional GCS client for dependency injection
        
    Returns:
        Tuple of (uploaded_files, skipped_files)
    """
    # Track uploaded files
    uploaded_files = []
    skipped_files = []
    
    # Get ingestion time. Later this should be extracted from the source files manifest
    # For now, we will use the current time
    ingested_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC timestamp        
    
    # Process each file from the ZIP
    for file_path, file_name in read_emr_patient_data(source_url):
        # Extract entity name from file name
        entity_name = extract_entity_name(file_name)
        
        # Check if entity is in expected list
        if entity_name not in expected_entities:
            logger.warning(f"Skipping file {file_name}: entity '{entity_name}' not in expected entities list")
            skipped_files.append(file_name)
            continue

        # Upload to GCS
        output_path = upload_to_gcs(
            ingested_at, file_path, file_name, 
            bucket_name, destination_prefix, 
            storage_client
        )
        uploaded_files.append(output_path)    
    
    # Clean up temporary directory after processing
    temp_dir = os.path.dirname(file_path) if 'file_path' in locals() else None
    if temp_dir:
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info(f"Removed temporary directory: {temp_dir}")
    
    return uploaded_files, skipped_files

def main():
    """Main entry point for the EMR data ingestion process"""
    # Get configuration from environment variables
    source_url = os.environ.get('INGESTION_SYNTHEA_URL_SOURCE')
    bucket_name = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION')    
    destination_prefix = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION_PREFIX')    
    gcs_creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not all([source_url, bucket_name, destination_prefix, gcs_creds_file]):
        missing = []
        if not source_url: missing.append('INGESTION_SYNTHEA_URL_SOURCE')
        if not bucket_name: missing.append('INGESTION_GCS_BUCKET_DESTINATION')
        if not destination_prefix: missing.append('INGESTION_GCS_BUCKET_DESTINATION_PREFIX')
        if not gcs_creds_file: missing.append('GOOGLE_APPLICATION_CREDENTIALS')
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return 1
                
    # For debugging
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Running in DEBUG mode")
    
    logger.debug(f"INGESTION_SYNTHEA_URL_SOURCE = {source_url}")
    logger.debug(f"INGESTION_GCS_BUCKET_DESTINATION = {bucket_name}")
    logger.debug(f"INGESTION_GCS_BUCKET_DESTINATION_PREFIX = {destination_prefix}")    
    logger.debug(f"GOOGLE_APPLICATION_CREDENTIALS = {gcs_creds_file}")  
    
    # Check if the credentials file exists  
    if not os.path.exists(gcs_creds_file):
        logger.debug(f"Google credentials file not found: {gcs_creds_file}")
        return 1

    # Get expected entities
    expected_entities = get_expected_entities()
    if not expected_entities:
        logger.error("No expected entities defined. Set EMR_EXPECTED_ENTITIES in environment.")
        return 1
    
    try:
        # Process the files
        uploaded_files, skipped_files = process_files(
            source_url, bucket_name, destination_prefix, expected_entities
        )
        
        # Print output paths for Airflow to capture
        logger.info(f"Uploaded {len(uploaded_files)} files, skipped {len(skipped_files)} files")        
        return 0
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())