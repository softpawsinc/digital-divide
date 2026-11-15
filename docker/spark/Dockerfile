FROM apache/spark:3.5.0-python3

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates build-essential libev-dev \
    && rm -rf /var/lib/apt/lists/*
ARG HADOOP_VERSION=3.3.6
RUN curl -fsSL "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv "/opt/hadoop-${HADOOP_VERSION}" /opt/hadoop \
    && rm /tmp/hadoop.tar.gz
ENV SPARK_HOME=/opt/spark
ENV HADOOP_HOME=/opt/hadoop
ENV PATH="/opt/spark/bin:/opt/hadoop/bin:/opt/hadoop/sbin:${PATH}"
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
COPY docker/hadoop/config/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY docker/hadoop/config/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY requirements-spark.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
USER spark
