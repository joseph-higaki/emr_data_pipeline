create a .env within the dbt project so that dbt power user extension can find the `profiles.yml`

 `ln -sf /workspaces/emr_data_pipeline/.env /workspaces/emr_data_pipeline/dbt/emr_analytics/.env`


DAG is recognized


  raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
airflow.exceptions.AirflowNotFoundException: The conn_id `my_google_cloud_platform_connection` isn't defined