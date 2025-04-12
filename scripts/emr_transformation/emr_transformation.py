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
import datetime
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
        datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
        return True
    except ValueError:
        return False
    
def get_processed_timestamps(dest_bucket: str, dest_prefix: str) -> Dict[str, str]:
    """
    Retrieves the processed timestamps from a metadata file in GCS.
    The metadata file contains a list of ingested_at and transformed_at timestamp pairs.
    
    Args:
        dest_bucket: Destination GCS bucket name
        dest_prefix: Destination prefix in the bucket
        
    Returns:
        Dictionary mapping ingested_at timestamps to transformed_at timestamps
    """
    metadata_uri = f"gs://{dest_bucket}/{dest_prefix}/_metadata/processed_timestamps.csv"
    processed_timestamps = {}
    
    try:
        # Read the CSV directly from GCS
        logger.info(f"Reading processed timestamps from metadata file {metadata_uri}")
        df = pd.read_csv(
            metadata_uri,
            storage_options={"token": None}
        )
        
        # Validate each timestamp and add it to the dictionary
        for _, row in df.iterrows():
            ingested_at = row['ingested_at']
            transformed_at = row['transformed_at']
            
            if validate_timestamp(ingested_at) and validate_timestamp(transformed_at):
                processed_timestamps[ingested_at] = transformed_at
            else:
                logger.warning(f"Invalid timestamp format in metadata file: {ingested_at} -> {transformed_at}")
        
        if processed_timestamps:
            last_ingested_at = max(processed_timestamps.keys())
            logger.info(f"Found {len(processed_timestamps)} processed timestamps. Last ingested_at: {last_ingested_at}")
        
    except Exception as e:
        logger.info(f"No processed timestamps found (reason: {str(e)}), will process all batches")
    
    return processed_timestamps


def save_processed_timestamp(dest_bucket: str, dest_prefix: str, ingested_at: str) -> None:
    """
    Saves a newly processed ingested_at timestamp to the metadata file in GCS.
    Appends the new timestamp to the existing list.
    
    Args:
        dest_bucket: Destination GCS bucket name
        dest_prefix: Destination prefix in the bucket
        ingested_at: Ingestion timestamp that was processed
    """
    if not validate_timestamp(ingested_at):
        logger.error(f"Cannot save invalid ingested_at timestamp: {ingested_at}")
        return
    
    # Get the current timestamp for transformed_at
    transformed_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata_uri = f"gs://{dest_bucket}/{dest_prefix}/_metadata/processed_timestamps.csv"
    
    # Create a dataframe for the new record
    new_record = pd.DataFrame({
        'ingested_at': [ingested_at],
        'transformed_at': [transformed_at]
    })
    
    try:
        # Try to read existing records
        existing_records = None
        try:
            existing_records = pd.read_csv(metadata_uri, storage_options={"token": None})
        except Exception:
            logger.info("No existing metadata file found, creating new one")
            pass
        
        # Combine existing and new records
        if existing_records is not None:
            # Check if this ingested_at already exists
            if ingested_at in existing_records['ingested_at'].values:
                # Update the transformed_at timestamp
                existing_records.loc[existing_records['ingested_at'] == ingested_at, 'transformed_at'] = transformed_at
                combined_records = existing_records
            else:
                # Append the new record
                combined_records = pd.concat([existing_records, new_record], ignore_index=True)
        else:
            combined_records = new_record
        
        # Write the combined records directly back to GCS
        combined_records.to_csv(
            metadata_uri,
            index=False,
            storage_options={"token": None}
        )
            
        logger.info(f"Saved processed timestamp: ingested_at={ingested_at}, transformed_at={transformed_at}")
        
    except Exception as e:
        logger.error(f"Error saving processed timestamp: {str(e)}")

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
    Reads a CSV file directly from GCS and transforms it to a DataFrame.
    
    Args:
        source_bucket_name: Source GCS bucket name
        source_blob_path: Path to the blob in the source bucket
        entity_name: Extracted entity name for validation
        
    Returns:
        Tuple of (transformed DataFrame, entity name)
    """
    logger.info(f"Reading {source_blob_path} directly from GCS")
    
    # Use pandas to read CSV directly from GCS
    gcs_uri = f"gs://{source_bucket_name}/{source_blob_path}"
    logger.info(f"Loading CSV from {gcs_uri}")
    df = pd.read_csv(
        gcs_uri,
        dtype=str,  # Read all columns as strings for safety
        low_memory=False,  # Avoid dtype guessing issues
        na_filter=False,  # Keep empty strings as is
        keep_default_na=False,  # Do not convert empty strings to NaN
        storage_options={"token": None}  # Use default credentials
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
        logger.info(f"Processing file {file_path} for entity {entity_name}")
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
      # Get previously processed timestamps
        processed_timestamps = get_processed_timestamps(dest_bucket, dest_prefix)
        
        # Find the latest ingested_at timestamp that was already processed
        last_processed_ingested_at = ""
        if processed_timestamps:
            last_processed_ingested_at = max(processed_timestamps.keys())
            logger.info(f"Last processed ingested_at timestamp: {last_processed_ingested_at}")
        
        # List all input files organized by ingestion timestamp
        timestamp_files = list_input_files(source_bucket, source_prefix)
        
        if not timestamp_files:
            logger.warning(f"No input files found in gs://{source_bucket}/{source_prefix}")
            return 0
        
        # Filter and sort timestamps to only process new ones
        sorted_timestamps = sorted(timestamp_files.keys())
        if last_processed_ingested_at:
            new_timestamps = [ts for ts in sorted_timestamps if ts > last_processed_ingested_at]
            if not new_timestamps:
                logger.info(f"No new data to process. Last processed ingested_at: {last_processed_ingested_at}")
                return 0
            logger.info(f"Processing {len(new_timestamps)} new ingested_at batches after {last_processed_ingested_at}")
            sorted_timestamps = new_timestamps
            
        # Process each ingestion batch
        all_uploaded_files = []
        
        for ingested_at in sorted_timestamps:
            file_paths = timestamp_files[ingested_at]
            logger.info(f"Processing {len(file_paths)} files from ingested_at={ingested_at}")
            
            try:
                uploaded_files = process_ingestion_batch(
                    source_bucket, ingested_at, file_paths, 
                    dest_bucket, dest_prefix
                )
                
                all_uploaded_files.extend(uploaded_files)
                
                # Save this ingested_at timestamp as processed
                save_processed_timestamp(dest_bucket, dest_prefix, ingested_at)
                
            except ValueError as e:
                logger.error(f"Error processing batch with ingested_at={ingested_at}: {str(e)}")
        
        # Print output paths for Airflow to capture
        logger.info(f"Uploaded {len(all_uploaded_files)} files")        
        
        return 0
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())