FROM prefecthq/prefect:2.18.3-python3.10

ARG TOPIC_NAME
ARG KAFKA_URL
ARG SPARK_URL
ARG MINIO_URL

ENV TOPIC_NAME=${TOPIC_NAME}
ENV KAFKA_URL=${KAFKA_URL}
ENV SPARK_URL=${SPARK_URL}
ENV MINIO_URL=${MINIO_URL}

COPY requirements.txt .

RUN python -m pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY . /opt/prefect/flows

WORKDIR /opt/prefect/flows

CMD ["python", "flow.py"]