FROM apache/airflow:2.10.5

# Switch to root for initial setup
USER root

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    bash \
    default-jdk \
    git \
    curl \
    unzip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies for Airflow(if required in the future)

# # Install Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 /opt/spark && \
    rm spark-3.5.0-bin-hadoop3.tgz


# Download Hadoop AWS and AWS SDK JAR files
RUN mkdir -p /opt/spark/jars && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget -P /opt/spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH


# # # Install OpenJDK 17 
RUN apt-get install -y openjdk-17-jdk && \
    update-alternatives --install /usr/bin/java java /usr/lib/jvm/java-17-openjdk-amd64/bin/java 1 && \
    update-alternatives --set java /usr/lib/jvm/java-17-openjdk-amd64/bin/java

    
# Verify Java version 
RUN java -version

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Copy requirements
COPY ../requirements.txt /requirements.txt

# Ensure correct file ownership
RUN chown airflow:root /requirements.txt

# Switch to airflow user
USER airflow

# Install dependencies
RUN pip install --upgrade pip && \
    pip install -r /requirements.txt

# Install additional Python dependencies
RUN pip install faker tqdm minio apache-airflow-providers-apache-spark

# Set PYTHONPATH
# ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags:/opt/airflow/generator"
#ENV PYTHONPATH="/opt/airflow:/opt/airflow/dags:/opt/airflow/generator:/opt/airflow/spark"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow:/opt/airflow/dags:/opt/airflow/utils:/opt/airflow/spark"

# Set working directory
WORKDIR /opt/airflow
