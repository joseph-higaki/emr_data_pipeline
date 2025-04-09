FROM python:3.11-slim

# Install necessary packages
RUN pip install --no-cache-dir \
    google-cloud-storage 
    
# Setup working directory
# mkdir -p /app
WORKDIR /app

# Copy ingestion script and utilities
# Using paths relative to the project root (build context in docker-compose)
COPY scripts/emr_ingestion/*.py /app/
COPY .gcp.auth/airflow-ingestion/google_credentials.json /app/credentials/google_credentials.json
RUN mkdir -p /app/credentials

# Credentials will be mounted at runtime via volume mount
# No need to copy credentials in the Dockerfile

# Set environment variables
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/google_credentials.json
ENV PYTHONPATH=/app

# Set entrypoint to run the ingestion script
#ENTRYPOINT ["python", "emr_ingestion.py"]