FROM prefecthq/prefect:2.18.3-python3.12

ARG LOGGING_LEVEL
ARG TOPIC_NAME
ARG KAFKA_URL
ARG SPARK_URL
ARG MINIO_URL
ARG MINIO_ACCESS_KEY
ARG MINIO_SECRET_KEY
ARG MINIO_PATH
ARG VERSION

ENV VERSION=$VERSION
ENV LOGGING_LEVEL=${LOGGING_LEVEL}
ENV PREFECT_LOGGING_LEVEL=${LOGGING_LEVEL}
ENV TOPIC_NAME=${TOPIC_NAME}
ENV KAFKA_URL=${KAFKA_URL}
ENV SPARK_URL=${SPARK_URL}
ENV MINIO_URL=${MINIO_URL}
ENV MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY}
ENV MINIO_SECRET_KEY=${MINIO_SECRET_KEY}
ENV MINIO_PATH=${MINIO_PATH}

COPY pyproject.toml poetry.lock* ./

RUN apt-get update && apt-get install -y openjdk-17-jdk procps \
    && python -m pip install --upgrade pip\
    && pip install --no-cache-dir poetry \
    && poetry config virtualenvs.create false \
    && poetry install --no-root \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "flow.py"]