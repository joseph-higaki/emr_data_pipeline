FROM apache/airflow:2.10.2
ADD requirements/. /opt/airflow/requirements
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r /opt/airflow/requirements/base.txt