ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

USER root
RUN apt-get update \
  && apt-get install git -y \
  && apt-get clean 

USER airflow
ADD requirements.txt .
RUN pip install -r requirements.txt
