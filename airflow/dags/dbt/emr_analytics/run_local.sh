#!/bin/bash
# Script to enable dbtrunning locally for development and testing
set +e

# Load environment variables from .env file if it exists
PROJECT_DIR="/workspaces/emr_data_pipeline"
echo "Project Directory: $PROJECT_DIR"
ENV_FILE="$PROJECT_DIR/airflow/.env"
if [ -f "$ENV_FILE" ]; then
  echo "Loading environment variables from $ENV_FILE"
  export $(grep -v '^#' "$ENV_FILE" | xargs)
else
  echo "Error: .env file not found at $ENV_FILE"
  exit
fi

# Set up credentials and Python path
export DBT_DATABASE_CREDENTIALS="$AIRFLOW_DBT_CREDENTIALS_FILEPATH"

# Display configuration
echo "=== Configuration ==="
echo "TRANSFORMATION_GCS_BUCKET_DESTINATION: $TRANSFORMATION_GCS_BUCKET_DESTINATION"
echo "TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX: $TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX"
echo "ANALYTICS_BQ_PROJECT_ID: $ANALYTICS_BQ_PROJECT_ID"
echo "ANALYTICS_BQ_DATASET: $ANALYTICS_BQ_DATASET"
echo "DBT_DATABASE_CREDENTIALS: $DBT_DATABASE_CREDENTIALS"
echo "DBT_PROFILES_DIR: $DBT_PROFILES_DIR" 

cd /workspaces/emr_data_pipeline/airflow/dags/dbt/emr_analytics
dbt debug
#  export DEBUG="true"

