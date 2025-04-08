#!/usr/bin/env python3
"""
EMR Data Ingestion Script

This script ingests EMR data (FHIR or CSV) and converts it to Parquet in GCS.
For now, it's a dummy implementation that creates a simple "hello world" file.

Usage:
    As standalone script:
        python ingestion_script.py

    In Docker:
        Will be executed via DockerOperator
"""

import os
import sys
import logging
import pandas as pd
from datetime import datetime
from google.cloud import storage
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_ingestion')

def read_emr_patient_data(source_url: str): 
    """Read EMR patient data
    EMR systems would typically provide data in FHIR format or CSV.
    Ideally set up in an object storage like GCS or S3.
    For now, we are downloading CSVs from synthetichealthdata.com.
    https://mitre.box.com/shared/static/aw9po06ypfb9hrau4jamtvtz0e5ziucz.zip
    """
    # This is a placeholder for actual data ingestion logic
    logger.info("Creating dummy patient data")
    return pd.DataFrame({
        'message': ['hello world']
    })

def upload_to_gcs(df, bucket_name, destination_path):
    """Upload dataframe to GCS as CSV"""
    # credentials = setup_gcp_credentials()
    # client = storage.Client(credentials=credentials)
    client = storage.Client()
    bucket = client.bucket(bucket_name)
        
    ingested_at = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")  # UTC timestamp
    full_destination = f"{destination_path}/{ingested_at}/test.csv"
    
    logger.info(f"Uploading to GCS: gs://{bucket_name}/{full_destination}")
    
    # Create a temporary file and upload it
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv') as temp_file:
        df.to_csv(temp_file.name, index=False)
        temp_file.flush()
        
        blob = bucket.blob(full_destination)
        blob.upload_from_filename(temp_file.name)
    
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
        # Create dummy patient data
        df = read_emr_patient_data(source_url)
        
        # Upload to GCS
        output_path = upload_to_gcs(df, bucket_name, destination_prefix)
        
        # Print output path for Airflow to capture
        print(f"OUTPUT_PATH={output_path}")
        return 0
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    main()