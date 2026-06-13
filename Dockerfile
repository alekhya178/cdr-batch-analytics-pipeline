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

# Download Spark binaries
ENV SPARK_VERSION=3.5.6
ENV SPARK_HOME=/opt/spark
RUN mkdir -p ${SPARK_HOME} && \
    wget -qO- https://archive.apache.org/dist/spark/spark-3.5.6/spark-3.5.6-bin-hadoop3.tgz | tar -xz -C ${SPARK_HOME} --strip-components=1 && \
    chmod -R a+rx ${SPARK_HOME}

# Add Spark bin to PATH
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Install PySpark python library (same version as spark installation)
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

# Switch back to airflow user
USER airflow
