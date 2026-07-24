FROM apache/spark:3.5.8-java17-python3

USER root

# Iceberg (Hive catalog support đã bundle sẵn trong runtime jar này) + S3A (đọc/ghi MinIO).
# hadoop-aws/aws-java-sdk-bundle phải khớp version Hadoop client image này bundle (3.3.4).
RUN curl -fL -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.11.0/iceberg-spark-runtime-3.5_2.12-1.11.0.jar \
    && curl -fL -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fL -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

COPY spark/conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
