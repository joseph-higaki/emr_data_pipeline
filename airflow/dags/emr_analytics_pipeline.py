from datetime import datetime
import os
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from cosmos import DbtTaskGroup
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator


@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 1),
    catchup=False,
    dag_id="emr_analytics_pipeline_dag",
    default_args={"retries": 2},    
)
def emr_analytics_pipeline_dag():
    hello = EmptyOperator(task_id="hello_task")

    # generation = EmptyOperator(task_id="generation_task")

    # ingestion = DockerOperator(
    #     task_id='ingestion_task',
    #     image='emr_ingestion:latest',  # Your ingestion container image
    #     api_version='auto',
    #     auto_remove=True,
    #     environment={
    #             'INGESTION_SYNTHEA_URL_SOURCE': os.environ.get('INGESTION_SYNTHEA_URL_SOURCE'),
    #             'INGESTION_GCS_BUCKET_DESTINATION': os.environ.get('INGESTION_GCS_BUCKET_DESTINATION'),
    #             'INGESTION_GCS_BUCKET_DESTINATION_PREFIX': os.environ.get('INGESTION_GCS_BUCKET_DESTINATION_PREFIX'),
    #             'DEBUG': 'true',
    #     },
    #     docker_url='tcp://docker-proxy:2375',
    #     network_mode='bridge',
    #     xcom_all=True,  # Capture all container output
    #     command='python /app/emr_ingestion.py'  # Explicitly call your script
    # )

    # transformation = DockerOperator(
    #     task_id='transformation_task',
    #     image='emr_transformation:latest',  # Your transformation container image
    #     api_version='auto',
    #     auto_remove=True,
    #     environment={
    #         'TRANSFORMATION_GCS_BUCKET_SOURCE': os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE'),
    #         'TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX': os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX'),
    #         'TRANSFORMATION_GCS_BUCKET_DESTINATION': os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION'),
    #         'TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX': os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX'),
    #         'DEBUG': 'true'
    #     },
    #     docker_url='tcp://docker-proxy:2375',
    #     network_mode='bridge',
    #     xcom_all=True,  # Capture all container output
    #     command='python /app/emr_transformation.py',  # Explicitly call your script
    # )

    # bucket = os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION')
    # bucket_path=os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX')

    # create_external_table = BigQueryCreateExternalTableOperator(    
    #     task_id="create_external_table_task",
    #     table_id="raw_patients",
    #     project_id=os.environ.get('ANALYTICS_BQ_PROJECT_ID'),
    #     dataset_id=os.environ.get('ANALYTICS_BQ_DATASET'),        
    #     bucket=bucket,        
    #     schema_fields=[],  # Schema will be inferred from Parquet
    #     source_format='PARQUET',
    #     source_objects=[f"{bucket_path}/patients/*"],
    #     source_uris=[f"gs://{bucket}/{bucket_path}/patients/*"],
    #     time_partitioning={
    #         "field": "ingested_at",
    #         "type": "TIMESTAMP"
    #     },
    #     external_table_options={
    #         "hive_partition_uri_prefix": f"gs://{bucket}/{bucket_path}/patients/"
    #     }
    # )

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=ProjectConfig(
            "/opt/airflow/dags/dbt/emr_analytics",
        ),
        profile_config=ProfileConfig(
            profile_name='emr_analytics',
            target_name='dev',
            profiles_yml_filepath=Path(f"{os.environ['AIRFLOW_HOME']}/dags/dbt/profiles.yml")
        ),
        execution_config=ExecutionConfig(
            dbt_executable_path=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt",
        )
    )

    post_dbt_task = EmptyOperator(task_id="post_dbt_task")

    #hello >> generation >> ingestion >> transformation >> 
    #create_external_table >> 
    dbt_run >> post_dbt_task 

emr_analytics_pipeline_dag()