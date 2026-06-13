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

# Copy local Spark binaries
ENV SPARK_VERSION=3.5.6
ENV SPARK_HOME=/opt/spark
COPY spark_temp ${SPARK_HOME}
RUN chmod -R a+rx ${SPARK_HOME}

# Add Spark bin to PATH
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin

# Install PySpark python library (same version as spark installation)
RUN pip install --no-cache-dir pyspark==${SPARK_VERSION}

# Switch back to airflow user
USER airflow
