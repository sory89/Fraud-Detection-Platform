FROM flink:1.18.0-scala_2.12-java11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip curl \
    && ln -sf /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    apache-flink==1.18.0 \
    kafka-python==2.0.2 \
    redis==5.0.1

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar \
    "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar"

RUN curl -fsSL -o /opt/flink/lib/postgresql.jar \
    "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.1/postgresql-42.7.1.jar"

ENV PYFLINK_PYTHON=/usr/bin/python3

WORKDIR /opt/flink-app

COPY fraud_pipeline.py .
COPY submit.sh .
RUN chmod +x submit.sh

ENV PYTHONUNBUFFERED=1
CMD ["/bin/bash", "submit.sh"]
