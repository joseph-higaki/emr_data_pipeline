from airflow import DAG
from datetime import datetime
import os
from google.cloud import storage
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
import logging

def hello():
    logger = logging.getLogger("airflow.dummy_containerized_test_dag")
    logger.info("Starting  test")
    print(f"hello world{1/1}")
    logger.info("Finished test")


with DAG(
    'dummy_containerized_test_dag',
    schedule=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test',  'emr']
) as dag:
    
     # Test GCS using Python client
    hello_task = PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )

    dummy_hello_world_task = DockerOperator(
        task_id='hello_world',
        image='hello-world',  # Your ingestion container image
        api_version='auto',
        auto_remove=True,
        # environment={
        #     'GCS_BUCKET': GCS_BUCKET,
        #     'GCS_DESTINATION_PREFIX': 'emr/raw',
        #     'DEBUG': 'false',
        # },
        # volumes=[f"{GOOGLE_CREDENTIALS_PATH}:/app/credentials/google_credentials.json:ro"],
        docker_url='tcp://docker-proxy:2375',
        # network_mode='bridge',
        # xcom_all=True,  # Capture all container output
        # command='python /app/emr_ingestion.py',  # Explicitly call your script
    )

    hello_task >> dummy_hello_world_task