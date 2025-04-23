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
from datetime import datetime, timezone
from google.cloud import storage
import tempfile
import re
from typing import List, Dict, Tuple, Set, Optional
import pyarrow as pa
import pyarrow.parquet as pq

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_transformation')

def get_expected_entities() -> Set[str]:
    """
    Get the list of expected entities from environment variable.
    Returns a set of entity names.
    """
    entities_str = os.environ.get('EMR_EXPECTED_ENTITIES', '')
    if not entities_str:
        logger.warning("EMR_EXPECTED_ENTITIES not set, using default empty set")
        return set()
    
    # Convert to lowercase set for case-insensitive matching
    entities = {entity.strip().lower() for entity in entities_str.split(',')}
    logger.info(f"Loaded {len(entities)} expected entities: {', '.join(sorted(entities))}")
    return entities

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
    
def read_processed_timestamps(dest_bucket: str, dest_prefix: str) -> Dict[str, str]:
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


def write_processed_timestamp(dest_bucket: str, dest_prefix: str, ingested_at: str, 
                              existing_timestamps: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """
    Saves a newly processed ingested_at timestamp to the metadata file in GCS.
    Appends the new timestamp to the existing list.
    
    Args:
        dest_bucket: Destination GCS bucket name
        dest_prefix: Destination prefix in the bucket
        ingested_at: Ingestion timestamp that was processed
        existing_timestamps: Optional dictionary of existing timestamps
        
    Returns:
        Updated dictionary of processed timestamps
    """
    if not validate_timestamp(ingested_at):
        logger.error(f"Cannot save invalid ingested_at timestamp: {ingested_at}")
        return existing_timestamps or {}
    
    # Get the current timestamp for transformed_at
    transformed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata_uri = f"gs://{dest_bucket}/{dest_prefix}/_metadata/processed_timestamps.csv"
    
    # Create a dataframe for the new record
    new_record = pd.DataFrame({
        'ingested_at': [ingested_at],
        'transformed_at': [transformed_at]
    })
    
    # Initialize result with existing timestamps if provided
    result = dict(existing_timestamps or {})
    result[ingested_at] = transformed_at
    
    try:
        # Try to read existing records if not provided
        existing_records = None
        if existing_timestamps is None:
            try:
                existing_records = pd.read_csv(metadata_uri, storage_options={"token": None})
                logger.debug(f"Found existing metadata with {len(existing_records)} records")
                
                # Build result dictionary from existing records
                for _, row in existing_records.iterrows():
                    if validate_timestamp(row['ingested_at']) and validate_timestamp(row['transformed_at']):
                        result[row['ingested_at']] = row['transformed_at']
                
            except Exception as e:
                logger.info(f"No existing metadata file found ({str(e)}), creating new one")
                existing_records = None
        else:
            # Convert existing timestamps to DataFrame
            if existing_timestamps:
                existing_data = {
                    'ingested_at': list(existing_timestamps.keys()),
                    'transformed_at': list(existing_timestamps.values())
                }
                existing_records = pd.DataFrame(existing_data)
            else:
                existing_records = None
        
        # Combine existing and new records
        if existing_records is not None:
            # Check if this ingested_at already exists
            if ingested_at in existing_records['ingested_at'].values:
                # Update the transformed_at timestamp
                existing_records.loc[existing_records['ingested_at'] == ingested_at, 'transformed_at'] = transformed_at
                combined_records = existing_records
                logger.debug(f"Updated existing record for ingested_at={ingested_at}")
            else:
                # Append the new record
                combined_records = pd.concat([existing_records, new_record], ignore_index=True)
                logger.debug(f"Added new record for ingested_at={ingested_at}")
        else:
            combined_records = new_record
            logger.debug(f"Created new metadata file with single record for ingested_at={ingested_at}")
        
        # Write the combined records directly back to GCS
        combined_records.to_csv(
            metadata_uri,
            index=False,
            storage_options={"token": None}
        )
            
        logger.info(f"Saved processed timestamp: ingested_at={ingested_at}, transformed_at={transformed_at}")
        
    except Exception as e:
        logger.error(f"Error saving processed timestamp: {str(e)}")
    
    return result

def list_blobs(bucket_name: str, prefix: str, client=None):
    """
    Lists all blobs in the specified GCS bucket and prefix.
    
    Args:
        bucket_name: GCS bucket name
        prefix: Path prefix in the bucket
        client: Optional storage client
        
    Returns:
        Iterator of blob objects
    """
    client = client or storage.Client()
    return client.list_blobs(bucket_name, prefix=prefix)

def organize_files_by_timestamp(blobs, prefix: str) -> Dict[str, List[str]]:
    """
    Organizes a list of blob objects by ingestion timestamp.
    
    Args:
        blobs: List of blob objects
        prefix: Prefix to extract timestamp from
        
    Returns:
        Dictionary mapping timestamps to lists of blob names
    """
    timestamp_files = {}
    
    for blob in blobs:
        # Extract timestamp from path: prefix/<timestamp>/file.csv
        match = re.search(f"{prefix}/([^/]+)/([^/]+)$", blob.name)        
        if match and blob.name.lower().endswith('.csv'):
            timestamp = match.group(1)
            
            # Validate timestamp before processing
            if not validate_timestamp(timestamp):
                logger.warning(f"Skipping invalid timestamp folder: {timestamp}")
                continue
                
            if timestamp not in timestamp_files:
                timestamp_files[timestamp] = []
                
            timestamp_files[timestamp].append(blob.name)
    
    return timestamp_files

def list_input_files(bucket_name: str, prefix: str, client=None) -> Dict[str, List[str]]:
    """
    Lists all CSV files in the specified GCS bucket and prefix,
    organized by ingestion timestamp folders.
    
    Args:
        bucket_name: GCS bucket name
        prefix: Path prefix in the bucket
        client: Optional storage client
        
    Returns:
        Dictionary mapping ingestion timestamps to lists of blob paths
    """
    client = client or storage.Client()
    
    # List all blobs with the given prefix
    logger.info(f"Listing files in gs://{bucket_name}/{prefix}")
    blobs = list_blobs(bucket_name, prefix, client)
    
    # Organize files by ingestion timestamp
    timestamp_files = organize_files_by_timestamp(blobs, prefix)
    
    # Log summary of found files
    total_files = sum(len(files) for files in timestamp_files.values())
    if timestamp_files:
        logger.info(f"Found {total_files} files across {len(timestamp_files)} timestamp folders")
        for timestamp, files in timestamp_files.items():
            logger.info(f"  {timestamp}: {len(files)} files")
    else:
        logger.warning(f"No CSV files found in gs://{bucket_name}/{prefix}")
    
    return timestamp_files

def read_csv_from_gcs(gcs_uri: str) -> pd.DataFrame:
    """
    Reads a CSV file directly from GCS and returns it as a DataFrame.
    
    Args:
        gcs_uri: GCS URI of the CSV file
        
    Returns:
        DataFrame containing the CSV data
    """
    logger.debug(f"Loading CSV from {gcs_uri}")
    return pd.read_csv(
        gcs_uri,
        dtype=str,  # Read all columns as strings for safety
        low_memory=False,  # Avoid dtype guessing issues
        na_filter=False,  # Keep empty strings as is
        keep_default_na=False,  # Do not convert empty strings to NaN
        storage_options={"token": None}  # Use default credentials
    )

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
    logger.info(f"Processing {entity_name} data from {source_blob_path}")
    
    # Use pandas to read CSV directly from GCS
    gcs_uri = f"gs://{source_bucket_name}/{source_blob_path}"
    
    try:
        df = read_csv_from_gcs(gcs_uri)
        
        row_count = len(df)
        col_count = len(df.columns)
        logger.info(f"Loaded {row_count} rows and {col_count} columns from {entity_name} data")
        
        # Add basic transformations here if needed
        # For now, we're just converting format
        
        return df, entity_name
    except Exception as e:
        logger.error(f"Error reading CSV file {gcs_uri}: {str(e)}")
        raise

def filter_timestamps_to_process(timestamps, processed_timestamps: Dict[str, str]) -> List[datetime]:
    """
    Filter timestamps to only process new ones (not in processed_timestamps).
    
    Args:
        timestamps: List of timestamps as datetime objects
        processed_timestamps: Dictionary of already processed timestamps
        
    Returns:
        List of timestamps to process
    """
    if not processed_timestamps:
        return timestamps
    
    # Convert processed timestamps to datetime for comparison
    processed_datetimes = [
        datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") 
        for ts in processed_timestamps.keys()
    ]
    
    # Find the latest processed timestamp
    if processed_datetimes:
        latest_processed = max(processed_datetimes)
        # Only process timestamps newer than the latest processed
        return [ts for ts in timestamps if ts > latest_processed]
    
    return timestamps

def upload_parquet_to_gcs(
    df: pd.DataFrame, 
    entity_name: str,
    ingested_at: str,
    dest_bucket_name: str,
    dest_prefix: str,
    client=None
) -> str:
    """
    Uploads a DataFrame to GCS as a Parquet file with appropriate partitioning.
    
    Args:
        df: DataFrame to upload
        entity_name: Entity name (determines the subfolder)
        ingested_at: Ingestion timestamp for partitioning
        dest_bucket_name: Destination GCS bucket name
        dest_prefix: Path prefix in the destination bucket
        client: Optional storage client
        
    Returns:
        GCS URI of the uploaded file
    """
    # Validate timestamp before creating partition
    if not validate_timestamp(ingested_at):
        raise ValueError(f"Invalid timestamp format for partitioning: {ingested_at}")
    
    client = client or storage.Client()
    bucket = client.bucket(dest_bucket_name)
    
    # Define destination path with partitioning
    file_id = uuid.uuid4().hex
    dest_path = f"{dest_prefix}/{entity_name}/ingested_at={ingested_at}/{entity_name}_{file_id}.parquet"
    
    # Create a temporary file and upload it
    with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_file:
        logger.debug(f"Converting {entity_name} data to Parquet format")
        
        # Using PyArrow directly for better control and performance
        try:
            table = pa.Table.from_pandas(df)
            pq.write_table(
                table, 
                temp_file.name,
                compression='snappy',  # Good balance of speed/size
                use_dictionary=True,
                version='2.6'  # Latest Parquet version for compatibility
            )
            temp_file.flush()
            
            logger.info(f"Uploading {entity_name} data to gs://{dest_bucket_name}/{dest_path}")
            blob = bucket.blob(dest_path)
            blob.upload_from_filename(temp_file.name)
            
            logger.info(f"Successfully uploaded {entity_name} data ({len(df)} rows) to gs://{dest_bucket_name}/{dest_path}")
            return f"gs://{dest_bucket_name}/{dest_path}"
        except Exception as e:
            logger.error(f"Error converting or uploading {entity_name} data: {str(e)}")
            raise

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
    expected_entities = get_expected_entities()
    
    logger.info(f"Processing batch with ingested_at={timestamp} containing {len(file_paths)} files")
    
    # Track which entities we've processed
    for file_path in file_paths:
        entity_name = extract_entity_name(file_path)
        
        # Check if entity is in our expected list
        if expected_entities and entity_name not in expected_entities:
            logger.warning(f"Unexpected entity type found: {entity_name} in file {file_path}")
            continue
            
        processed_entities.add(entity_name)
        
        try:
            # Process the file
            df, entity = download_and_transform_file(source_bucket, file_path, entity_name)
            
            # Upload as parquet
            output_path = upload_parquet_to_gcs(
                df, entity, timestamp, dest_bucket, dest_prefix
            )
            
            uploaded_files.append(output_path)
        except Exception as e:
            logger.error(f"Failed to process {entity_name} file {file_path}: {str(e)}")
    
    # Check for missing entities
    if expected_entities:
        missing_entities = expected_entities - processed_entities
        if missing_entities:
            logger.warning(f"Missing expected entities in timestamp {timestamp}: {', '.join(missing_entities)}")
    
    logger.info(f"Completed batch processing for ingested_at={timestamp}: {len(uploaded_files)} files processed")
    return uploaded_files

def main():
    """Main entry point for the EMR data transformation process"""
    start_time = datetime.now()
    # Get configuration from environment variables
    source_bucket = os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE')
    source_prefix = os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX')
    dest_bucket = os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION')
    dest_prefix = os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX')
    gcs_creds_file = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
    if not all([source_bucket, source_prefix, dest_bucket, dest_prefix, gcs_creds_file]):
        missing = []
        if not source_bucket: missing.append('TRANSFORMATION_GCS_BUCKET_SOURCE')
        if not source_prefix: missing.append('TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX')
        if not dest_bucket: missing.append('TRANSFORMATION_GCS_BUCKET_DESTINATION')
        if not dest_prefix: missing.append('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX')
        if not gcs_creds_file: missing.append('GOOGLE_APPLICATION_CREDENTIALS')
        logger.error(f"Missing required environment variables: {', '.join(missing)}")
        return 1
    
    # For debugging
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Running in DEBUG mode")

    logger.debug(f"TRANSFORMATION_GCS_BUCKET_SOURCE = {source_bucket}")
    logger.debug(f"TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX = {source_prefix}")      
    logger.debug(f"TRANSFORMATION_GCS_BUCKET_DESTINATION = {dest_bucket}")
    logger.debug(f"TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX = {dest_prefix}")
    logger.debug(f"GOOGLE_APPLICATION_CREDENTIALS = {gcs_creds_file}")  
    # Check if the credentials file exists  
    if not os.path.exists(gcs_creds_file):
        logger.debug(f"Google credentials file not found: {gcs_creds_file}")
        return 1
    
    logger.info(f"Starting EMR transformation: {source_bucket}/{source_prefix} -> {dest_bucket}/{dest_prefix}")
    
    try:
        # Get previously processed timestamps
        processed_timestamps = read_processed_timestamps(dest_bucket, dest_prefix)
        
        # Find the latest ingested_at timestamp that was already processed
        last_processed_ingested_at = None
        if processed_timestamps:
            last_processed_ingested_at = max(
                datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") for ts in processed_timestamps.keys()
            )
            logger.info(f"Last processed ingested_at timestamp: {last_processed_ingested_at.isoformat()}Z")
        
        # List all input files organized by ingestion timestamp
        timestamp_files = list_input_files(source_bucket, source_prefix)
        
        if not timestamp_files:
            logger.warning(f"No input files found in gs://{source_bucket}/{source_prefix}")
            return 0
        
        # Filter and sort timestamps to only process new ones
        sorted_timestamps = sorted(
            datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ") for ts in timestamp_files.keys()
        )
        logger.debug(f"Found {len(sorted_timestamps)} timestamps to process: {', '.join(ts.isoformat() + 'Z' for ts in sorted_timestamps)}")
        
        if last_processed_ingested_at:
            new_timestamps = filter_timestamps_to_process(sorted_timestamps, processed_timestamps)
            if not new_timestamps:
                logger.info(f"No new data to process. Last processed ingested_at: {last_processed_ingested_at.isoformat()}Z")
                return 0
            logger.info(f"Processing {len(new_timestamps)} new ingested_at batches after {last_processed_ingested_at.isoformat()}Z")
            sorted_timestamps = new_timestamps
        
        # Process each ingestion batch
        all_uploaded_files = []
        processed_batch_count = 0
        failed_batch_count = 0
        
        for ingested_at in sorted_timestamps:
            ingested_at_str = ingested_at.strftime("%Y-%m-%dT%H:%M:%SZ")
            file_paths = timestamp_files[ingested_at_str]
            logger.info(f"Processing batch: ingested_at={ingested_at_str} with {len(file_paths)} files")
            
            try:
                uploaded_files = process_ingestion_batch(
                    source_bucket, ingested_at_str, file_paths, 
                    dest_bucket, dest_prefix
                )
                
                if uploaded_files:
                    all_uploaded_files.extend(uploaded_files)
                    processed_batch_count += 1
                
                    # Save this ingested_at timestamp as processed
                    write_processed_timestamp(dest_bucket, dest_prefix, ingested_at_str, processed_timestamps)
                
            except Exception as e:
                logger.error(f"Error processing batch with ingested_at={ingested_at_str}: {str(e)}", exc_info=True)
                failed_batch_count += 1
        
        # Calculate execution time
        execution_time = datetime.now() - start_time
        
        # Log summary
        logger.info(f"EMR transformation completed in {execution_time.total_seconds():.2f} seconds")
        logger.info(f"Processed {processed_batch_count} batches, uploaded {len(all_uploaded_files)} files")
        if failed_batch_count > 0:
            logger.warning(f"Failed to process {failed_batch_count} batches")
        
        return 0 if failed_batch_count == 0 else 1
        
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())