import os
import time
import json
import pandas as pd
import logging
from confluent_kafka import Consumer, TopicPartition

from prefect import task, flow, get_run_logger

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

# 로깅 설정
def get_logger():
    try:
        logger = get_run_logger()
        logger.setLevel(logging.INFO)  # 로깅 수준 설정
        return logger
    except Exception:
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger(__name__)
logger = get_logger()

@task
def read_kafka(topic_name, kafka_url):
    global logger
    logger.info(f"Attempting to read from Kafka topic: {topic_name}")

    # Consumer 설정
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2minio_flow',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**conf)

    # 토픽의 모든 파티션 조회
    partitions = consumer.list_topics(topic_name).topics[topic_name].partitions.keys()
    topic_partitions = [TopicPartition(topic_name, p) for p in partitions]

    # 모든 파티션을 구독
    consumer.assign(topic_partitions)

    data = []
    try:
        last_message_time = time.time()  # 마지막 메시지를 받은 시간
        timeout_seconds = 10  # 새로운 메시지가 없을 경우 종료할 때까지의 대기 시간(초)

        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if time.time() - last_message_time > timeout_seconds:
                    # 마지막 메시지 받은 후 지정된 시간이 지났으면 루프 종료
                    logger.info("새로운 데이터가 없어 프로그램을 종료합니다.")
                    break
                continue
            if msg.error():
                logger.error(msg.error())
                break

            # 메시지를 받으면 마지막 메시지 시간을 갱신
            last_message_time = time.time()
            # 메시지 처리
            value = json.loads(msg.value().decode('utf-8'))
            data.append({
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'timestamp': msg.timestamp()[1],
                'window_start': value['window']['start'],
                'window_end': value['window']['end'],
                'stock_code': value['종목코드'],  # '종목코드'에 맞게 수정
                'open': value['open'],
                'high': value['high'],
                'low': value['low'],
                'close': value['close'],
                'candle': value['candle'],
            })

    finally:
        consumer.close()

    # 데이터프레임으로 변환
    df = pd.DataFrame(data)

    logger.info(f"Finished reading {len(df)} records from Kafka")
    return df


@task
def write_minio(data_source, spark_url, minio_url, minio_access_key, minio_secret_key, minio_path):
    global logger
    logger.info("Starting to write data to MinIO")

    spark = SparkSession \
        .builder \
        .appName("tick_to_min") \
        .master(spark_url) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.hadoop:hadoop-aws:3.3.1') \
        .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/tick_to_min') \
        .config("spark.hadoop.fs.s3a.endpoint", minio_url) \
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

    try:
        # Pandas DataFrame을 Spark DataFrame으로 변환
        df = spark.createDataFrame(data_source)
        df = df.withColumn('window_start', to_timestamp(df['window_start']))
        # '년', '월', '일' 컬럼 추가
        df = df.withColumn('year', year(df['window_start']))
        df = df.withColumn('month', month(df['window_start']))
        df = df.withColumn('day', dayofmonth(df['window_start']))

        # MinIO에 쓰기 (s3a 프로토콜 사용)
        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('stock_code', 'year', 'month', 'day') \
        .mode("append") \
        .parquet(minio_path)


        logger.info(f"Successfully write {df.count()} records to MinIO")
    finally:
        # SparkSession 종료
        spark.stop()

@flow
def hun_min_kafka2minio_flow():

    topic_name = os.getenv("TOPIC_NAME")
    kafka_url = os.getenv("KAFKA_URL")
    spark_url = os.getenv("SPARK_URL")
    minio_url = os.getenv("MINIO_URL")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    minio_path = os.getenv("MINIO_PATH")

    kafka_data = read_kafka(topic_name, kafka_url)
    write_minio(kafka_data, spark_url, minio_url, minio_access_key, minio_secret_key, minio_path)

if __name__ == "__main__":


    hun_min_kafka2minio_flow()
