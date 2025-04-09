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
echo "TRANSFORMATION_GCS_BUCKET_SOURCE: $TRANSFORMATION_GCS_BUCKET_SOURCE"
echo "TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX: $TRANSFORMATION_GCS_BUCKET_SOURCE_PREFIX"    
echo "TRANSFORMATION_GCS_BUCKET_DESTINATION: $TRANSFORMATION_GCS_BUCKET_DESTINATION"  
echo "TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX: $TRANSFORMATION_GCS_BUCKET_DESTINATION_PREFIX"
echo "GOOGLE_APPLICATION_CREDENTIALS: $GOOGLE_APPLICATION_CREDENTIALS"
echo "DEBUG: $DEBUG"


# Run the ingestion script
echo "Running EMR ingestion script locally..."
python $PROJECT_DIR/scripts/emr_transformation/emr_transformation.py 

# Display exit code
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
  echo "✅ Transformation completed successfully!"
else
  echo "❌ Transformation failed with exit code $EXIT_CODE"
fi

exit $EXIT_CODE