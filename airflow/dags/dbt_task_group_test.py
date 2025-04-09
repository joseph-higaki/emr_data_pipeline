from datetime import datetime
import os
from pathlib import Path
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from cosmos import DbtTaskGroup
from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig

@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="my_cosmos_task_group_dag",
    default_args={"retries": 2},    
)
def my_cosmos_taskgroup_dag():
    pre_dbt = EmptyOperator(task_id="pre_dbt_task")

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

    pre_dbt >> dbt_run >> post_dbt_task 

my_cosmos_taskgroup_dag()