import os
import time
import json
import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from prefect import task, flow

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

@task
def read_kafka(topic_name, kafka_url):

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
                    print("새로운 데이터가 없어 프로그램을 종료합니다.")
                    break
                continue
            if msg.error():
                print(msg.error())
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

    return df


@task
def write_minio(data_source, spark_url, minio_url):

    spark = SparkSession \
        .builder \
        .appName("tick_to_min") \
        .master(spark_url) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3') \
        .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/tick_to_min') \
        .getOrCreate()

    try:
        # Pandas DataFrame을 Spark DataFrame으로 변환
        df = spark.createDataFrame(data_source)

        df = df.withColumn('window_start', to_timestamp(df['window_start']))

        # '년', '월' 컬럼 추가
        df = df.withColumn('year', year(df['window_start']))
        df = df.withColumn('month', month(df['window_start']))
        df = df.withColumn('day', dayofmonth(df['window_start']))

        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('stock_code', 'year', 'month', 'day') \
        .mode("append") \
        .parquet(minio_url)
    finally:
        # SparkSession 종료
        spark.stop()

@flow
def min_kafka2minio_flow(topic_name, kafka_url, spark_url, minio_url):
    kafka_data = read_kafka(topic_name, kafka_url)
    write_minio(kafka_data, spark_url, minio_url)

if __name__ == "__main__":
    topic_name = os.getenv("TOPIC_NAME")
    kafka_url = os.getenv("KAFKA_URL")
    spark_url = os.getenv("SPARK_URL")
    minio_url = os.getenv("MINIO_URL")

    min_kafka2minio_flow(topic_name, kafka_url, spark_url, minio_url)
