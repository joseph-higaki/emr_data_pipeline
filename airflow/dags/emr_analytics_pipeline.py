from datetime import datetime
import os
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from cosmos import DbtTaskGroup, DbtRunOperationLocalOperator
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

PROJECT_DIR= f"{os.environ['AIRFLOW_HOME']}/dags/dbt/emr_analytics"
PROJECT_CONFIG=ProjectConfig(
            PROJECT_DIR
        )

DBT_EXECUTABLE_PATH=f"{os.environ['AIRFLOW_HOME']}/dbt_venv/bin/dbt"

PROFILE_CONFIG=ProfileConfig(
    profile_name='emr_analytics',
    target_name='dev',
    profiles_yml_filepath=Path(f"{os.environ['AIRFLOW_HOME']}/dags/dbt/profiles.yml")
)
EXECUTION_CONFIG=ExecutionConfig(
            dbt_executable_path=DBT_EXECUTABLE_PATH
        )

@dag(
    schedule_interval="@daily",
    start_date=datetime(2024, 4, 1),
    catchup=False,
    dag_id="emr_analytics_pipeline_dag"       
)
def emr_analytics_pipeline_dag():    

    ingestion = DockerOperator(
        task_id='ingestion_task',
        image='emr_ingestion:latest',  # Your ingestion container image
        api_version='auto',
        auto_remove=True,
        environment={
                'INGESTION_SYNTHEA_URL_SOURCE': os.environ.get('INGESTION_SYNTHEA_URL_SOURCE'),
                'INGESTION_GCS_BUCKET_DESTINATION': os.environ.get('INGESTION_GCS_BUCKET_DESTINATION'),
                'INGESTION_GCS_BUCKET_DESTINATION_PREFIX': os.environ.get('INGESTION_GCS_BUCKET_DESTINATION_PREFIX'),
                'EMR_EXPECTED_ENTITIES': os.environ.get('EMR_EXPECTED_ENTITIES'),                
                'DEBUG': 'true',
        },
        docker_url='tcp://docker-proxy:2375',
        network_mode='bridge',
        xcom_all=True,  # Capture all container output
        command='python /app/emr_ingestion.py'  # Explicitly call your script
    )

    transformation = DockerOperator(
        task_id='transformation_task',
        image='emr_transformation:latest',  # Your transformation container image
        api_version='auto',
        auto_remove=True,
        environment={
            'TRANSFORMATION_GCS_BUCKET_SOURCE': os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE'),
            'TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX': os.environ.get('TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX'),
            'TRANSFORMATION_GCS_BUCKET_DESTINATION': os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION'),
            'TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX': os.environ.get('TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX'),
            'EMR_EXPECTED_ENTITIES': os.environ.get('EMR_EXPECTED_ENTITIES'),
            'DEBUG': 'true'
        },
        docker_url='tcp://docker-proxy:2375',
        network_mode='bridge',
        xcom_all=True,  # Capture all container output
        command='python /app/emr_transformation.py',  # Explicitly call your script
    )

    dbt_stage_external_sources = DbtRunOperationLocalOperator(
                task_id=f"dbt_stage_external_sources_task",
                macro_name="stage_external_sources",
                #project_config=PROJECT_CONFIG,
                profile_config=PROFILE_CONFIG,
                dbt_executable_path=DBT_EXECUTABLE_PATH,
                project_dir=PROJECT_DIR,
                #profile_config=profile_config,
                install_deps=True
            )

    dbt_run = DbtTaskGroup(
        group_id="dbt_run",
        project_config=PROJECT_CONFIG,
        profile_config=PROFILE_CONFIG,
        execution_config=EXECUTION_CONFIG
    )

    ingestion >> transformation >>  dbt_stage_external_sources >> dbt_run
   

emr_analytics_pipeline_dag()