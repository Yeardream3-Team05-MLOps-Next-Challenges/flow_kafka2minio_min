import time
import json
import pandas as pd
from confluent_kafka import Consumer, TopicPartition, KafkaError, OFFSET_BEGINNING

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth

from src.logger import get_logger

def read_kafka_logic(topic_name, kafka_url):
    """Kafka에서 특정 기간(예: 오늘 하루)의 데이터를 읽어오는 순수 함수 """
    
    logger = get_logger()

    # Consumer 설정
    conf = {
        'bootstrap.servers': kafka_url,
        'group.id': 'min_kafka2minio_flow', 
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False 
    }
    consumer = Consumer(**conf)
    logger.info(f"Kafka Consumer 생성 완료. Group ID: {conf['group.id']}")

    data = []
    processed_offsets = {} # 파티션별 처리된 마지막 오프셋 추적

    try:
        logger.info(f"토픽 '{topic_name}'의 메타데이터를 가져옵니다...")
        # 토픽 메타데이터 가져오기 (타임아웃 설정)
        metadata = consumer.list_topics(topic_name, timeout=10)
        if metadata.topics.get(topic_name) is None:
            logger.error(f"토픽 '{topic_name}'을 찾을 수 없습니다.")
            return pd.DataFrame(data) # 빈 DataFrame 반환
        if metadata.topics[topic_name].error is not None:
             logger.error(f"토픽 '{topic_name}' 메타데이터 오류: {metadata.topics[topic_name].error}")
             return pd.DataFrame(data)

        partitions = list(metadata.topics[topic_name].partitions.keys())
        if not partitions:
            logger.warning(f"토픽 '{topic_name}'에 파티션이 없습니다.")
            return pd.DataFrame(data)

        topic_partitions = [TopicPartition(topic_name, p) for p in partitions]
        logger.info(f"'{topic_name}' 토픽 파티션: {partitions}")

        end_offsets = {}
        logger.info("각 파티션의 현재 high watermark 오프셋을 가져옵니다...")
        for p in topic_partitions:
            try:
                low, high = consumer.get_watermark_offsets(p, timeout=5, cached=False)
                end_offsets[p.partition] = high
                logger.info(f"  - 파티션 {p.partition}: low={low}, high={high}")
                p.offset = OFFSET_BEGINNING 
            except Exception as e:
                 logger.error(f"파티션 {p.partition}의 watermark 오프셋 가져오기 실패: {e}")
                 partitions.remove(p.partition)

        # 유효한 파티션만 다시 필터링
        topic_partitions = [tp for tp in topic_partitions if tp.partition in partitions]
        if not topic_partitions:
             logger.warning("오프셋을 가져올 수 있는 유효한 파티션이 없습니다.")
             return pd.DataFrame(data)

        logger.info(f"파티션 할당 및 읽기 시작: {[(tp.partition, tp.offset) for tp in topic_partitions]}")
        consumer.assign(topic_partitions) # 읽을 파티션과 시작 오프셋 지정

        active_partitions = set(partitions) # 아직 high watermark에 도달하지 않은 파티션 집합
        poll_timeout = 5.0 
        no_message_streak = 0 
        max_no_message_streak = 5 

        logger.info("메시지 읽기 루프 시작...")
        while active_partitions:
            msg = consumer.poll(poll_timeout)

            if msg is None:
                no_message_streak += 1
                logger.debug(f"메시지 없음 (연속 {no_message_streak}회). 남은 활성 파티션: {len(active_partitions)}")
                if no_message_streak >= max_no_message_streak:
                     logger.warning(f"{poll_timeout * max_no_message_streak}초 동안 메시지가 없어 읽기를 중단합니다. 일부 데이터가 누락될 수 있습니다.")
                     break
                continue

            no_message_streak = 0 

            if msg.error():
                # EOF 에러는 파티션 끝에 도달했다는 의미이므로 무시 가능
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info(f"파티션 {msg.partition()}의 끝(EOF)에 도달했습니다.")
                    continue
                else:
                    logger.error(f"Kafka Consume 오류: {msg.error()}")
                    break

            # 메시지 처리
            partition = msg.partition()
            offset = msg.offset()
            logger.debug(f"메시지 수신: 파티션={partition}, 오프셋={offset}")

            try:
                value = json.loads(msg.value().decode('utf-8'))
                data.append({
                    'topic': msg.topic(),
                    'partition': partition,
                    'offset': offset,
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
                processed_offsets[partition] = offset 
            except json.JSONDecodeError as e:
                logger.warning(f"JSON 파싱 오류 (오프셋 {offset}): {e} - 메시지 건너<0xEB><0x9A><0x8D>")
            except Exception as e:
                logger.error(f"메시지 처리 중 오류 (오프셋 {offset}): {e}", exc_info=True)

            # 해당 파티션이 high watermark에 도달했는지 확인
            if partition in end_offsets and offset >= end_offsets[partition] - 1:
                logger.info(f"파티션 {partition}이(가) 목표 오프셋({end_offsets[partition]})에 도달했습니다.")
                active_partitions.discard(partition) 

        logger.info("메시지 읽기 루프 종료.")

    except Exception as e:
        logger.error(f"Kafka 읽기 중 예외 발생: {e}", exc_info=True)
    finally:
        logger.info("Kafka Consumer를 닫습니다.")
        consumer.close()

    logger.info(f"총 {len(data)}개의 메시지를 읽었습니다.")
    if not data:
        return pd.DataFrame() 

    # 데이터프레임으로 변환
    df = pd.DataFrame(data)
    logger.info("Pandas DataFrame으로 변환 완료.")
    return df

def write_minio_logic(data_source, spark_url, minio_url, minio_access_key, minio_secret_key, minio_path):
    """MinIO에 데이터를 쓰는 순수 함수."""
    logger = get_logger()
    spark = None

    try:
        logger.info("SparkSession 생성을 시작합니다 (MinIO 설정 포함)...")

        spark = SparkSession \
            .builder \
            .appName("kafk2minio") \
            .master(spark_url) \
            .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') \
            .config('spark.hadoop.fs.s3a.endpoint', minio_url) \
            .config('spark.hadoop.fs.s3a.access.key', minio_access_key) \
            .config('spark.hadoop.fs.s3a.secret.key', minio_secret_key) \
            .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
            .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false" if minio_url.startswith("http://") else "true") \
            .config('spark.sql.streaming.checkpointLocation', '/tmp/checkpoint/kafk2minio') \
            .getOrCreate()

        logger.info("SparkSession 생성 완료.")

        # Pandas DataFrame을 Spark DataFrame으로 변환
        logger.info("Pandas DataFrame을 Spark DataFrame으로 변환합니다...")
        spark_df = spark.createDataFrame(data_source)
        logger.info("Spark DataFrame 변환 완료.")

        # Spark DataFrame 변환 및 파티션 컬럼 추가
        logger.info("파티션 컬럼 추가 및 데이터 변환 시작...")
        try:
            transformed_df = spark_df \
                .withColumn("window_start_ts", to_timestamp(col("window_start"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
                .withColumn("year", year(col("window_start_ts"))) \
                .withColumn("month", month(col("window_start_ts"))) \
                .withColumn("day", dayofmonth(col("window_start_ts")))

            # 불필요한 컬럼 제거 
            final_df = transformed_df.drop("window_start", "window_start_ts", "topic", "partition", "offset", "timestamp") 
            logger.info("데이터 변환 완료.")
            final_df.printSchema() 

        except Exception as e:
                logger.error(f"Spark DataFrame 변환 중 오류: {e}", exc_info=True)
                logger.error("데이터 변환 실패. 원본 DataFrame 스키마:")
                spark_df.printSchema()
                raise # 오류 재발생

        # MinIO에 Parquet 형식으로 저장
        logger.info(f"MinIO 경로 '{minio_path}'에 Parquet 파일 쓰기를 시작합니다...")
        final_df.write \
            .partitionBy('year', 'month', 'day', 'stock_code') \
            .mode("overwrite") \
            .parquet(minio_path)
        logger.info("MinIO 쓰기 완료.")

    except Exception as e:
        logger.error(f"MinIO 쓰기 작업 중 오류 발생: {e}", exc_info=True)
        raise 
    finally:
        if spark:
            logger.info("SparkSession을 종료합니다.")
            spark.stop()
            logger.info("SparkSession 종료 완료.")