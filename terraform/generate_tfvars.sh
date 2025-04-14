#!/bin/bash

ENV_FILE="/workspaces/emr_data_pipeline/airflow/.env"
TFVARS_FILE="/workspaces/emr_data_pipeline/terraform/terraform.tfvars"

# Check if .env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: Airflow .env file not found at $ENV_FILE"
    exit 1
fi

# Start with an empty tfvars file
echo "# Auto-generated tfvars file from Airflow .env" > $TFVARS_FILE
echo "# Generated on $(date)" >> $TFVARS_FILE
echo "" >> $TFVARS_FILE

# Variables that come from .env file
# Map Airflow env variables to Terraform variables
declare -A VAR_MAPPING=(
    ["ANALYTICS_BQ_PROJECT_ID"]="project_id"
    ["ANALYTICS_BQ_DATASET"]="dataset_id"
    ["INGESTION_GCS_BUCKET_DESTINATION"]="analytics_storage_bucket_name"
)

# Read the .env file and extract values for the variables we're interested in
while IFS='=' read -r key value || [ -n "$key" ]; do
    # Skip comments and empty lines
    [[ $key =~ ^#.*$ || -z $key ]] && continue
    
    # Remove any quotes around the value
    value=$(echo $value | sed -e 's/^"//' -e 's/"$//' -e "s/^'//" -e "s/'$//")
    
    # Check if this is a key we're interested in
    if [[ -n "${VAR_MAPPING[$key]}" ]]; then
        tf_var="${VAR_MAPPING[$key]}"
        echo "$tf_var = \"$value\"" >> $TFVARS_FILE
    fi
done < "$ENV_FILE"


# Add hardcoded values for variables that don't come from .env
echo -e "\n# Default values for variables not in .env" >> $TFVARS_FILE
echo "region = \"europe-southwest1\"" >> $TFVARS_FILE
echo "location = \"EU\"" >> $TFVARS_FILE
echo "credentials_file = \"/workspaces/emr_data_pipeline/.gcp.auth/terraform-infra/google_credentials.json\"" >> $TFVARS_FILE

echo "Created terraform.tfvars with required variables"
