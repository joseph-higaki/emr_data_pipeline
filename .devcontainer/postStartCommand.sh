#!/bin/bash
# filepath: /workspaces/emr_data_pipeline/.devcontainer/postStartCommand.sh

# Store the export commands in variables
ENV_EXPORT="export \$(grep -v '^#' /workspaces/emr_data_pipeline/.env | xargs)"
AIRFLOW_ENV_EXPORT="export \$(grep -v '^#' /workspaces/emr_data_pipeline/airflow/.env | xargs)"
DBT_CREDS_EXPORT="export DBT_DATABASE_CREDENTIALS=\$AIRFLOW_DBT_CREDENTIALS_FILEPATH"

# Append the export commands to .bashrc only if they don't already exist
grep -qF "$ENV_EXPORT" ~/.bashrc || echo "$ENV_EXPORT" >> ~/.bashrc
grep -qF "$AIRFLOW_ENV_EXPORT" ~/.bashrc || echo "$AIRFLOW_ENV_EXPORT" >> ~/.bashrc
grep -qF "$DBT_CREDS_EXPORT" ~/.bashrc || echo "$DBT_CREDS_EXPORT" >> ~/.bashrc

# Set the timezone
sudo ln -sf /usr/share/zoneinfo/Europe/Madrid /etc/localtime

# Enable terraform tfvars generation script
chmod +x /workspaces/emr_data_pipeline/terraform/generate_tfvars.sh

# Enable local run: test/debug ingestion script
chmod +x  /workspaces/emr_data_pipeline/scripts/emr_ingestion/run_local.sh

# Enable local run: test/debug transformation script
chmod +x /workspaces/emr_data_pipeline/scripts/emr_transformation/run_local.sh
