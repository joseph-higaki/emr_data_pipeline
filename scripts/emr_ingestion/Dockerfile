FROM python:3.11-slim
WORKDIR /app

# Copy project configuration files first to leverage Docker caching
COPY pyproject.toml setup.py /app/

# Install the package in development mode
RUN pip install -e .

# Copy the script to the container
COPY scripts/emr_ingestion/emr_ingestion.py /app/
COPY .gcp.auth/airflow-ingestion/google_credentials.json /app/credentials/google_credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/google_credentials.json 

# No explicit ENTRYPOINT or CMD - these are provided by Airflow's DockerOperator