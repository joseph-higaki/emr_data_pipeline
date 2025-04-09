#!/bin/bash
# Script to run the EMR ingestion locally for development and testing
set +e

# Load environment variables from .env file if it exists
#PROJECT_DIR="$(pwd)"
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
export GOOGLE_APPLICATION_CREDENTIALS="$AIRFLOW_INGESTION_CREDENTIALS_FILEPATH"
#  export DEBUG="true"
export PYTHONPATH=${PYTHONPATH}:$PROJECT_DIR


# Display configuration
echo "=== Configuration ==="
echo "INGESTION_SYNTHEA_URL_SOURCE: $INGESTION_SYNTHEA_URL_SOURCE"
echo "INGESTION_GCS_BUCKET_DESTINATION: $INGESTION_GCS_BUCKET_DESTINATION"
echo "INGESTION_GCS_BUCKET_DESTINATION_PREFIX: $INGESTION_GCS_BUCKET_DESTINATION_PREFIX"
#echo "ENVIRONMENT: $ENVIRONMENT"
echo "===================="

# Run the ingestion script
echo "Running EMR ingestion script locally..."
python $PROJECT_DIR/scripts/emr_ingestion/emr_ingestion.py

# Display exit code
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo "✅ Ingestion completed successfully!"
else
  echo "❌ Ingestion failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE