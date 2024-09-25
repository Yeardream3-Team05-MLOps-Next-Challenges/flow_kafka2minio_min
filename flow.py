import os
import time
import json
import pandas as pd
import logging
from confluent_kafka import Consumer, TopicPartition
from prefect import task, flow, get_run_logger
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

# Prefect 환경 여부를 확인하는 함수
def is_prefect_env():
    try:
        get_run_logger()
        return True
    except Exception:
        return False

# 로깅 설정
if not is_prefect_env():
    l_level = getattr(logging, os.getenv('PREFECT_LOGGING_LEVEL'), logging.INFO)
    logging.basicConfig(level=l_level)


def get_logger():
    if is_prefect_env():
        return get_run_logger()
    else:
        return logging.getLogger(__name__)

@task
def read_kafka(topic_name, kafka_url):
    logger = get_logger()
    logger.info(f"Attempting to read from Kafka topic: {topic_name}")

    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2minio_flow',
        'enable.auto.commit': False,
        'auto.offset.reset': 'earliest'
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([topic_name])
    
    try:
        # 파티션 할당 대기
        while True:
            partitions = consumer.assignment()
            if partitions:
                break
            consumer.poll(1.0)
        
        data = []
        final_offsets = {}
        for partition in partitions:
            start_offset, end_offset = consumer.get_watermark_offsets(partition)
            committed = consumer.committed([partition])
            current_offset = committed[0].offset if committed[0].offset > -1 else start_offset
            
            logger.info(f"Partition {partition.partition}: start_offset={start_offset}, "
                        f"end_offset={end_offset}, current_offset={current_offset}")
            
            if current_offset < end_offset:
                tp = TopicPartition(topic_name, partition.partition, current_offset)
                consumer.seek(tp)
                
               while current_offset < end_offset:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        continue
                    if msg.error():
                        logger.error(f"Consumer error: {msg.error()}")
                        continue
                    
                    value = json.loads(msg.value().decode('utf-8'))
                    data.append({
                        'topic': msg.topic(),
                        'partition': msg.partition(),
                        'offset': msg.offset(),
                        'timestamp': msg.timestamp()[1],
                        'window_start': value['window']['start'],
                        'window_end': value['window']['end'],
                        'stock_code': value['종목코드'],
                        'open': value['open'],
                        'high': value['high'],
                        'low': value['low'],
                        'close': value['close'],
                        'candle': value['candle'],
                    })
                    
                    current_offset = msg.offset() + 1
                
                final_offsets[partition.partition] = current_offset - 1
        
        df = pd.DataFrame(data)
        logger.info(f"Successfully read {len(df)} records from Kafka")
        return df, consumer, final_offsets
    
    except Exception as e:
        logger.error(f"Error reading from Kafka: {e}")
        consumer.close()
        raise

@task
def write_minio(data_source, spark_url, minio_url, minio_access_key, minio_secret_key, minio_path):
    logger = get_logger()
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
        df = spark.createDataFrame(data_source)
        df = df.withColumn('window_start', to_timestamp(df['window_start']))
        df = df.withColumn('year', year(df['window_start']))
        df = df.withColumn('month', month(df['window_start']))
        df = df.withColumn('day', dayofmonth(df['window_start']))

        df.write \
        .option("maxRecordsPerFile", 100000) \
        .partitionBy('stock_code', 'year', 'month', 'day') \
        .mode("append") \
        .parquet(minio_path)

        logger.info(f"Successfully write {df.count()} records to MinIO")
        return True
    except Exception as e:
        logger.error(f"Error writing to MinIO: {e}")
        return False
    finally:
        spark.stop()

@flow
def hun_min_kafka2minio_flow():
    logger = get_logger()
    topic_name = os.getenv("TOPIC_NAME")
    kafka_url = os.getenv("KAFKA_URL")
    spark_url = os.getenv("SPARK_URL")
    minio_url = os.getenv("MINIO_URL")
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    minio_path = os.getenv("MINIO_PATH")

    df, consumer, final_offsets = read_kafka(topic_name, kafka_url)
    
    try:
        if df.empty:
            logger.info("No data read from Kafka. Skipping MinIO write.")
            success = True
        else:
            success = write_minio(df, spark_url, minio_url, minio_access_key, minio_secret_key, minio_path)
        
        if success:
            # MinIO 쓰기가 성공했거나 데이터가 없는 경우에만 오프셋 커밋
            for partition, offset in final_offsets.items():
                consumer.commit(offsets=[TopicPartition(topic_name, partition, offset + 1)])
            logger.info("Final offsets committed successfully")
        else:
            logger.warning("MinIO write failed, offsets not committed")
    
    finally:
        consumer.close()
        logger.info("Kafka consumer closed")


if __name__ == "__main__":
    hun_min_kafka2minio_flow()