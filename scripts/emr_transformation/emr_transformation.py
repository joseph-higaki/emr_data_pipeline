#!/usr/bin/env python3
"""
EMR Data Transformation Script

This script transforms EMR data from CSV to Parquet format in GCS.
It reads CSV files from a GCS bucket, transforms them, and writes them as 
Parquet files to another GCS bucket with appropriate partitioning.

Usage:
    As standalone script:
        python emr_transformation.py

    In Docker:
        Will be executed via DockerOperator
"""

import os
import sys
import logging
import uuid
import pandas as pd
from datetime import datetime
from google.cloud import storage
import tempfile
import re
from typing import List, Dict, Tuple
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_transformation')

# Define expected entity types
EXPECTED_ENTITIES = {
    'allergies',
    'careplans',
    'claims',
    'claims_transactions',
    'conditions',
    'devices',
    'encounters',
    'imaging_studies',
    'immunizations',
    'medications',
    'observations',
    'organizations',
    'patients',
    'payers',
    'payer_transitions',
    'procedures',
    'providers',
    'supplies'
}

# Timestamp format validation regex
# Matches ISO format: YYYY-MM-DDThh:mm:ssZ
TIMESTAMP_REGEX = r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$'

def validate_timestamp(timestamp: str) -> bool:
    """
    Validates that a timestamp string is in the correct format
    and represents a valid datetime.
    
    Args:
        timestamp: Timestamp string to validate
        
    Returns:
        True if valid, False otherwise
    """
    # Check format using regex
    if not re.match(TIMESTAMP_REGEX, timestamp):
        return False
        
    # Check that it's a valid datetime
    try:
        datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except ValueError:
        return False

def list_input_files(bucket_name: str, prefix: str) -> Dict[str, List[str]]:
    """
    Lists all CSV files in the specified GCS bucket and prefix,
    organized by ingestion timestamp folders.
    
    Args:
        bucket_name: GCS bucket name
        prefix: Path prefix in the bucket
        
    Returns:
        Dictionary mapping ingestion timestamps to lists of blob paths
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # List all blobs with the given prefix
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    
    # Organize files by ingestion timestamp
    timestamp_files = {}
    
    for blob in blobs:
        # Extract timestamp from path: prefix/<timestamp>/file.csv
        logger.debug(f"Found file: {blob.name}")
        match = re.search(f"{prefix}/([^/]+)/([^/]+)$", blob.name)        
        if match and blob.name.lower().endswith('.csv'):
            timestamp = match.group(1)
            #filename = match.group(2)
            
            # Validate timestamp before processing
            if not validate_timestamp(timestamp):
                logger.warning(f"Skipping invalid timestamp folder: {timestamp}")
                continue
                
            if timestamp not in timestamp_files:
                timestamp_files[timestamp] = []
                
            timestamp_files[timestamp].append(blob.name)
            
    
    # Log summary of found files
    for timestamp, files in timestamp_files.items():
        logger.info(f"Found {len(files)} files for timestamp {timestamp}")
    
    return timestamp_files

def download_and_transform_file(
    source_bucket_name: str, 
    source_blob_path: str,
    entity_name: str
) -> Tuple[pd.DataFrame, str]:
    """
    Downloads a CSV file from GCS, transforms it to a DataFrame.
    
    Args:
        source_bucket_name: Source GCS bucket name
        source_blob_path: Path to the blob in the source bucket
        entity_name: Extracted entity name for validation
        
    Returns:
        Tuple of (transformed DataFrame, entity name)
    """
    client = storage.Client()
    bucket = client.bucket(source_bucket_name)
    blob = bucket.blob(source_blob_path)
    
    # Create a temporary file to download to
    with tempfile.NamedTemporaryFile(suffix='.csv') as temp_file:
        logger.info(f"Downloading {source_blob_path}")
        blob.download_to_filename(temp_file.name)
        
        # Read CSV into DataFrame
        df = pd.read_csv(
            temp_file.name,
            dtype=str,  # Read all columns as strings for safety
            low_memory=False,  # Avoid dtype guessing issues
            na_filter=False,  # Keep empty strings as is
            keep_default_na=False,  # Do not convert empty strings to NaN
        )
        logger.info(f"Loaded {len(df)} rows from {source_blob_path}")
        
        # Add basic transformations here if needed
        # For now, we're just converting format
        
        return df, entity_name

def upload_parquet_to_gcs(
    df: pd.DataFrame, 
    entity_name: str,
    ingested_at: str,
    dest_bucket_name: str,
    dest_prefix: str
) -> str:
    """
    Uploads a DataFrame to GCS as a Parquet file with appropriate partitioning.
    
    Args:
        df: DataFrame to upload
        entity_name: Entity name (determines the subfolder)
        ingested_at: Ingestion timestamp for partitioning
        dest_bucket_name: Destination GCS bucket name
        dest_prefix: Path prefix in the destination bucket
        
    Returns:
        GCS URI of the uploaded file
    """
    # Validate timestamp before creating partition
    if not validate_timestamp(ingested_at):
        raise ValueError(f"Invalid timestamp format for partitioning: {ingested_at}")
    
    client = storage.Client()
    bucket = client.bucket(dest_bucket_name)
    
    # Define destination path with partitioning
    dest_path = f"{dest_prefix}/{entity_name}/ingested_at={ingested_at}/{entity_name}_{uuid.uuid4().hex}.parquet"
    
    # Create a temporary file and upload it
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
        logger.info(f"Converting {entity_name} data to Parquet")
        
        # Using PyArrow directly for better control and performance
        table = pa.Table.from_pandas(df)
        pq.write_table(
            table, 
            temp_file.name,
            compression='snappy',  # Good balance of speed/size
            use_dictionary=True,
            version='2.6'  # Latest Parquet version for compatibility
        )
        temp_file.flush()
        
        logger.info(f"Uploading to GCS: gs://{dest_bucket_name}/{dest_path}")
        blob = bucket.blob(dest_path)
        blob.upload_from_filename(temp_file.name)
    
    logger.info(f"Successfully uploaded to gs://{dest_bucket_name}/{dest_path}")
    return f"gs://{dest_bucket_name}/{dest_path}"

def extract_entity_name(file_path: str) -> str:
    """
    Extracts entity name from a file path/name.
    
    Args:
        file_path: Path to the file
        
    Returns:
        Entity name
    """
    # Extract just the filename from the path
    filename = os.path.basename(file_path)
    
    # Remove extension
    entity_name = os.path.splitext(filename)[0].lower()
    
    return entity_name

def process_ingestion_batch(
    source_bucket: str,
    timestamp: str,
    file_paths: List[str],
    dest_bucket: str,
    dest_prefix: str
) -> List[str]:
    """
    Process a batch of files from a single ingestion timestamp.
    
    Args:
        source_bucket: Source GCS bucket name
        timestamp: Ingestion timestamp
        file_paths: List of file paths to process
        dest_bucket: Destination GCS bucket name
        dest_prefix: Destination prefix in the bucket
        
    Returns:
        List of GCS URIs of uploaded files
    """
    # Validate timestamp before processing
    if not validate_timestamp(timestamp):
        raise ValueError(f"Invalid timestamp format: {timestamp}")
    
    uploaded_files = []
    processed_entities = set()
    
    # Track which entities we've processed
    for file_path in file_paths:
        entity_name = extract_entity_name(file_path)
        
        # Check if entity is in our expected list
        if entity_name not in EXPECTED_ENTITIES:
            logger.warning(f"Unexpected entity type found: {entity_name} in file {file_path}")
            continue
            
        processed_entities.add(entity_name)
        
        # Process the file
        df, entity = download_and_transform_file(source_bucket, file_path, entity_name)
        
        # Upload as parquet
        output_path = upload_parquet_to_gcs(
            df, entity, timestamp, dest_bucket, dest_prefix
        )
        
        uploaded_files.append(output_path)
    
    # Check for missing entities
    missing_entities = EXPECTED_ENTITIES - processed_entities
    if missing_entities:
        logger.warning(f"Missing expected entities in timestamp {timestamp}: {', '.join(missing_entities)}")
    
    return uploaded_files

def main():
    """Main entry point for the EMR data transformation process"""
    # Get configuration from environment variables
    source_bucket = os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE')
    source_prefix = os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX')
    dest_bucket = os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION')
    dest_prefix = os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX')
    
    # For debugging
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Running in DEBUG mode")
    
    try:
        # List all input files organized by ingestion timestamp
        timestamp_files = list_input_files(source_bucket, source_prefix)
        
        if not timestamp_files:
            logger.warning(f"No input files found in gs://{source_bucket}/{source_prefix}")
            return 0
            
        # Process each ingestion batch
        all_uploaded_files = []
        
        for timestamp, file_paths in timestamp_files.items():
            logger.info(f"Processing {len(file_paths)} files from timestamp {timestamp}")
            
            try:
                uploaded_files = process_ingestion_batch(
                    source_bucket, timestamp, file_paths, 
                    dest_bucket, dest_prefix
                )
                
                all_uploaded_files.extend(uploaded_files)
            except ValueError as e:
                logger.error(f"Error processing batch with timestamp {timestamp}: {str(e)}")
        
        # Print output paths for Airflow to capture
        logger.info(f"Uploaded {len(all_uploaded_files)} files")        
        
        return 0
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())