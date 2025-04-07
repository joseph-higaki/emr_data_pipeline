from airflow import DAG
from datetime import datetime
import os
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import logging

PROJECT_ID = "emr-data-pipeline"
BUCKET_ID = "emr-data-pipeline-emr_analytics"
dataset_file = "/opt/airflow/dags/patients.csv"

def hello():
    logger = logging.getLogger("airflow.data_ingestion_gcs_dag")
    logger.info("Starting  test")
    print(f"hello world{1/1}")
    logger.info("Finished test")


with DAG(
    'ingestion_containerized_test_dag',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'data_ingestion_gcs', 'emr']
) as dag:
    
     # Test GCS using Python client
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    # Run containerized ingestion process
    ingest_data = DockerOperator(
        task_id='ingest_emr_data',
        image='emr-ingestion:latest',  # Your ingestion container image
        api_version='auto',
        auto_remove=True,
        # environment={
        #     'GCS_BUCKET': GCS_BUCKET,
        #     'GCS_DESTINATION_PREFIX': 'emr/raw',
        #     'DEBUG': 'false',
        # },
        # volumes=[f"{GOOGLE_CREDENTIALS_PATH}:/app/credentials/google_credentials.json:ro"],
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        xcom_all=True,  # Capture all container output
        command='python /app/emr_ingestor.py',  # Explicitly call your script
    )

    hello_task >> ingest_data 