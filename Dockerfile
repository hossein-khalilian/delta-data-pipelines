ARG AIRFLOW_VERSION=2.10.4
FROM apache/airflow:${AIRFLOW_VERSION}

# Install git
USER root
RUN apt update && apt-get install -y git && apt clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user (optional, recommended)
USER airflow

ADD requirements.txt .
RUN pip install apache-airflow==${AIRFLOW_VERSION} -r requirements.txt

