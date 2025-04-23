#!/bin/bash
# Build script for EMR Data Pipeline Docker images

set -e  # Exit on error

PROJECT_ROOT="/workspaces/emr_data_pipeline"

echo "Building Docker images from project root: $PROJECT_ROOT"

# Build the ingestion image
echo "Building emr_ingestion image..."
docker build -t emr_ingestion:latest -f scripts/emr_ingestion/Dockerfile .

# Build the transformation image
echo "Building emr_transformation image..."
docker build -t emr_transformation:latest -f scripts/emr_transformation/Dockerfile .

echo "Docker images built successfully."
echo "Use 'docker images | grep emr_' to see the new images."
