create a .env within the dbt project so that dbt power user extension can find the `profiles.yml`

 `ln -sf /workspaces/emr_data_pipeline/.env /workspaces/emr_data_pipeline/dbt/emr_analytics/.env`


DAG is recognized

make dbt profiles to read env variables, so that it runs from airflow and local (power user extension)

