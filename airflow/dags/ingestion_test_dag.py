from airflow import DAG
from datetime import datetime
import os
from google.cloud import storage
from airflow.operators.python import PythonOperator
import logging

PROJECT_ID = "emr-data-pipeline"
BUCKET_ID = "emr-data-pipeline-emr_analytics"
dataset_file = "/opt/airflow/dags/patients.csv"

def hello():
    logger = logging.getLogger("airflow.data_ingestion_gcs_dag")
    logger.info("Starting  test")
    print(f"hello world{1/1}")
    logger.info("Finished test")

def local_to_gcs(bucket_name: str, destination_blob_name: str, local_filename: str):
    logger = logging.getLogger("airflow.data_ingestion_gcs_dag.local_to_gcs")
    logger.info("local_to_gcs")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    generation_match_precondition = 0
    blob.upload_from_filename(local_filename, if_generation_match=generation_match_precondition)
    logger.info("uploaded succesfully")    
    return 0

with DAG(
    'data_ingestion_gcs',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'data_ingestion_gcs', 'nyc']
) as dag:
    
     # Test GCS using Python client
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=local_to_gcs,
        op_kwargs={
            "bucket_name":BUCKET_ID,
            "destination_blob_name": dataset_file,
            "local_filename": dataset_file
        }
    )

    hello_task >> local_to_gcs_task 