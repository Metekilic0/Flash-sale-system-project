FROM python:3.11-bullseye

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk curl procps && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_VERSION=3.5.1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3

RUN curl -fsSL \
    https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    | tar xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME}

RUN pip install --no-cache-dir psycopg2-binary==2.9.9

COPY start-spark.sh /start-spark.sh
RUN chmod +x /start-spark.sh

ENTRYPOINT ["/start-spark.sh"]
