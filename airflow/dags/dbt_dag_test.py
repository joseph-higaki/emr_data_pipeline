from datetime import datetime
import os
from pathlib import Path
from cosmos import DbtDag, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping


my_cosmos_dag = DbtDag(
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
    ),
    # normal dag parameters
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_dag",
    default_args={"retries": 2},
)