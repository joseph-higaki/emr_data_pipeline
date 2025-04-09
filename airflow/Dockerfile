FROM apache/airflow:2.10.2


ADD airflow/requirements/. /opt/airflow/requirements
RUN pip install apache-airflow==2.10.2 -r /opt/airflow/requirements/base.txt && \
    python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-bigquery && deactivate


COPY .gcp.auth/airflow-ingestion/google_credentials.json /opt/airflow/credentials/airflow-ingestion/google_credentials.json
COPY .gcp.auth/dbt-analytics/google_credentials.json /opt/airflow/credentials/dbt-analytics/google_credentials.json