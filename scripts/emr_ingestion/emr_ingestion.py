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
from datetime import datetime
from google.cloud import storage
import tempfile
import requests
import zipfile
import io
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_ingestion')

def read_emr_patient_data(source_url: str): 
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
        # Download the ZIP file
        logger.info(f"Downloading ZIP from: {source_url}")
        response = requests.get(source_url)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Extract the ZIP file content
        with zipfile.ZipFile(io.BytesIO(response.content)) as zip_ref:
            # Get list of files in the ZIP
            file_list = zip_ref.namelist()
            logger.info(f"ZIP contains {len(file_list)} files")
            
            # Extract all files to the temporary directory
            zip_ref.extractall(temp_dir)
            logger.info(f"Extracted all files to {temp_dir}")
            
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

def upload_to_gcs(ingested_at, file_path, file_name, bucket_name, destination_path):
    """Upload a file to GCS
    
    Args:
        file_path: Local path to the file to upload
        file_name: Original file name for preservation
        bucket_name: GCS bucket name
        destination_path: Path prefix in the bucket
        
    Returns:
        GCS URI of the uploaded file
    """
    # Initialize GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)          
    full_destination = f"{destination_path}/{ingested_at}/{file_name}"    
    logger.info(f"Uploading to GCS: gs://{bucket_name}/{full_destination}")
    
    # Create a blob and upload the file
    blob = bucket.blob(full_destination)
    blob.upload_from_filename(file_path)
    
    logger.info(f"Successfully uploaded to gs://{bucket_name}/{full_destination}")
    return f"gs://{bucket_name}/{full_destination}"

def main():
    """Main entry point for the EMR data ingestion process"""
    # Get configuration from environment variables
    source_url = os.environ.get('INGESTION_SYNTHEA_URL_SOURCE')
    bucket_name = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION')    
    destination_prefix = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION_PREFIX')    
    
    # For debugging
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Running in DEBUG mode")
    
    try:
        # Track uploaded files
        uploaded_files = []
        
        #get ingestion time. Later this should be extracted from the source files manifest
        # For now, we will use the current time
        ingested_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC timestamp
        # Process each file from the ZIP
        for file_path, file_name in read_emr_patient_data(source_url):
            # Upload to GCS
            output_path = upload_to_gcs(ingested_at, file_path, file_name, bucket_name, destination_prefix)
            uploaded_files.append(output_path)            
        
        # Clean up temporary directory after processing
        temp_dir = os.path.dirname(file_path)
        shutil.rmtree(temp_dir, ignore_errors=True)
        logger.info(f"Removed temporary directory: {temp_dir}")
        
        # Print output paths for Airflow to capture
        logger.info(f"Uploaded {len(uploaded_files)} files")        
        return 0
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())