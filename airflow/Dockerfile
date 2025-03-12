
FROM apache/airflow:2.10.5

# Switch to root for initial setup
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY ../requirements.txt /requirements.txt

# Ensure correct file ownership
RUN chown airflow:root /requirements.txt

# Switch to airflow user
USER airflow

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r /requirements.txt

# # We'll install faker directly since we can't access generator folder at build time
RUN pip install faker
RUN pip install tqdm
RUN pip install minio

# Set PYTHONPATH
ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags:/opt/airflow/generator"

WORKDIR /opt/airflow