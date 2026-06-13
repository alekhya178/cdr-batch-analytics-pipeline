FROM apache/airflow:2.8.0

# Install OpenJDK-17 and utilities as root
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        wget \
        curl \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up architecture-independent JAVA_HOME
RUN ln -s /usr/lib/jvm/java-17-openjdk-* /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Download and install Apache Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Add Spark bin to PATH
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Switch back to airflow user
USER airflow

# Install PySpark python library (same version as spark installation)
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}
