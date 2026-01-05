ARG AIRFLOW_VERSION=2.10.5
FROM apache/airflow:${AIRFLOW_VERSION}

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    wget \
    ca-certificates \
    tar \
    && rm -rf /var/lib/apt/lists/*

# Install MongoDB Database Tools (mongodump + mongorestore)
RUN wget https://fastdl.mongodb.org/tools/db/mongodb-database-tools-ubuntu2204-x86_64-100.9.4.tgz \
    && tar -xzf mongodb-database-tools-ubuntu2204-x86_64-100.9.4.tgz \
    && cp mongodb-database-tools-*/bin/* /usr/local/bin/ \
    && rm -rf mongodb-database-tools-*

USER airflow

# Python dependencies
ADD requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
