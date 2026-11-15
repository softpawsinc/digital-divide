FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

# The base image supplies its normal namenode entrypoint. These files provide
# the same service DNS defaults to Hadoop CLI clients and Spark workers.
COPY docker/hadoop/config/core-site.xml /etc/hadoop/core-site.xml
COPY docker/hadoop/config/hdfs-site.xml /etc/hadoop/hdfs-site.xml
