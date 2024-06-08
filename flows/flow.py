import os
import time
import pandas as pd
from confluent_kafka import Consumer, TopicPartition

from prefect import task, flow

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

@task
def read_kafka(topic_name, kafka_url):

    # Consumer 설정
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'my_group',
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
            data.append({
                'topic': msg.topic(),
                'partition': msg.partition(),
                'offset': msg.offset(),
                'message': msg.value().decode('utf-8')
            })

    finally:
        consumer.close()

    # 데이터프레임으로 변환
    df = pd.DataFrame(data)

    return df


@task
def write_minio(data_source, minio_url):
    
    with SparkSession.builder.appName("Test_transactions").getOrCreate() as spark:
        
        # CSV 파일 읽어오기
        df = spark.read.csv(data_source, header=True, inferSchema=True)

        # DataFrame 출력하기
        #df.show()

        # '년', '월', '일' 컬럼 추가
        df = df.withColumn('year', year(df['t_dat']))
        df = df.withColumn('month', month(df['t_dat']))
        #df = df.withColumn('day', dayofmonth(df['t_dat']))


        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('year', 'month') \
        .mode("append") \
        .parquet(minio_url)

@flow
def min_kafka2minio_flow(topic_name, kafka_url, minio_uri):
    kafka_data = read_kafka(topic_name, kafka_url)
    write_minio(kafka_data, minio_uri)

if __name__ == "__main__":
    topic_name = 'ttmin'
    kafka_url = os.getenv("KAFKA_URL")
    minio_url = os.getenv("MINIO_URL")

    min_kafka2minio_flow(topic_name, kafka_url, minio_url)
