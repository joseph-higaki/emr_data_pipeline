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
from google.oauth2 import service_account
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('emr_ingestion')

# def setup_gcp_credentials():
#     """Set up GCP credentials from environment variables or default paths"""
#     creds_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    
#     if not creds_path:
#         # Check common locations
#         potential_paths = [
#             '/app/credentials/google_credentials.json',
#             '/opt/airflow/credentials/google_credentials.json',
#             './.gcp.auth/emr-data-pipeline-airflow-ingestion-573bacba9da7.json'
#         ]
        
#         for path in potential_paths:
#             if os.path.exists(path):
#                 creds_path = path
#                 break
    
#     if not creds_path or not os.path.exists(creds_path):
#         logger.error("GCP credentials not found!")
#         sys.exit(1)
    
#     logger.info(f"Using GCP credentials from: {creds_path}")
#     return service_account.Credentials.from_service_account_file(creds_path)

def create_dummy_patient_data():
    """Create a dummy patient dataframe with 'hello world' message"""
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
    bucket_name = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION')
    #bucket_name = "emr-data-pipeline-emr_analytics"
    destination_prefix = os.environ.get('INGESTION_GCS_BUCKET_DESTINATION_PREFIX')
    #destination_prefix = 'emr/raw' 
    
    # For debugging
    debug_mode = os.environ.get('DEBUG', 'false').lower() == 'true'
    if debug_mode:
        logger.setLevel(logging.DEBUG)
        logger.debug("Running in DEBUG mode")
    
    try:
        # Create dummy patient data
        df = create_dummy_patient_data()
        
        # Upload to GCS
        output_path = upload_to_gcs(df, bucket_name, destination_prefix)
        
        # Print output path for Airflow to capture
        print(f"OUTPUT_PATH={output_path}")
        return 0
    except Exception as e:
        logger.error(f"Error during ingestion: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
