import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pyspark.sql import SparkSession 
from src.logic import read_kafka_logic, write_minio_logic

@patch('src.logic.Consumer')
@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer, MockGetLogger):
    # Mock 설정
    mock_consumer = MockConsumer.return_value

    # list_topics Mock 설정 (error=None 추가)
    mock_topic_metadata = MagicMock()
    mock_topic_metadata.error = None # <-- 중요: 에러 없음을 명시
    mock_topic_metadata.partitions = {0: MagicMock()} # 파티션 정보 모킹
    mock_consumer.list_topics.return_value.topics = {"test_topic": mock_topic_metadata}

    # get_watermark_offsets Mock 설정
    # 파티션 0에 대해 low=0, high=1 반환하도록 설정 (메시지 1개 가정)
    mock_consumer.get_watermark_offsets.return_value = (0, 1)

    # Kafka 메시지 Mock 설정
    mock_message = MagicMock()
    mock_message.error.return_value = None
    mock_message.value.return_value = b'{"window": {"start": "2024-01-01T00:00:00.000Z", "end": "2024-01-01T00:05:00.000Z"}, "종목코드": "005930", "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "candle": "5m"}' # JSON 형식 및 값 타입 맞추기
    mock_message.topic.return_value = "test_topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 0 # high watermark(1)보다 작음
    mock_message.timestamp.return_value = (1, 1672531200000) # 예시 타임스탬프 (ms)

    # poll Mock 설정: 메시지 하나 반환 후 루프 종료 유도 (예: None 반복 또는 EOF)
    # high watermark 기반 종료 로직을 테스트하려면 좀 더 정교한 side_effect 필요
    # 여기서는 간단하게 메시지 하나만 반환하고 None 반환
    mock_consumer.poll.side_effect = [mock_message, None, None, None, None, None, None] # max_no_message_streak 고려

    # 테스트 실행
    df = read_kafka_logic("test_topic", "test_url")

    # 검증
    MockConsumer.assert_called_once() # Consumer가 생성되었는지 확인
    mock_consumer.list_topics.assert_called_once_with("test_topic", timeout=10)
    mock_consumer.get_watermark_offsets.assert_called() # 호출되었는지 확인
    mock_consumer.assign.assert_called_once() # assign 호출 확인
    mock_consumer.poll.assert_called() # poll 호출 확인
    mock_consumer.close.assert_called_once() # close 호출 확인

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1 # 이제 메시지 1개가 읽혀야 함
    assert df['stock_code'].iloc[0] == '005930'
    # 필요시 다른 컬럼 값도 검증
    assert df['open'].iloc[0] == 100.0

@patch('src.logic.SparkSession')
def test_write_minio_logic(MockSparkSession):
    # Mock SparkSession 설정 (변경된 config 체인 반영)
    mock_spark = MagicMock(spec=SparkSession) # spec 추가
    mock_builder = MagicMock()
    MockSparkSession.builder = mock_builder
    # 각 config 호출이 builder 객체 자신을 반환하도록 설정
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder # config 호출 시마다 자신 반환
    mock_builder.getOrCreate.return_value = mock_spark # 최종적으로 mock_spark 반환

    # Mock DataFrame 및 Write Sinker 설정
    mock_spark_df = MagicMock()
    mock_writer = MagicMock()
    mock_partitioned_writer = MagicMock()

    mock_spark.createDataFrame.return_value = mock_spark_df
    # 컬럼 변환 후의 DataFrame 모킹 (간단하게 동일 객체 사용)
    mock_spark_df.withColumn.return_value = mock_spark_df
    mock_spark_df.drop.return_value = mock_spark_df # drop 후에도 동일 객체

    mock_spark_df.write = mock_writer
    mock_writer.partitionBy.return_value = mock_partitioned_writer
    mock_partitioned_writer.mode.return_value = mock_partitioned_writer

    # 테스트 데이터
    test_data = pd.DataFrame({
        'window_start': ['2024-01-01T00:00:00.000Z'], # 형식 맞추기
        'stock_code': ['005930'],
        # read_kafka_logic에서 추가하는 다른 컬럼들도 포함하는 것이 좋음
        'topic': ['test'], 'partition': [0], 'offset': [0], 'timestamp': [0],
        'window_end': ['2024-01-01T00:05:00.000Z'], 'open': [0.0], 'high': [0.0], 'low': [0.0], 'close': [0.0], 'candle': ['5m']
    })

    # 테스트 실행 (모든 인자 전달)
    write_minio_logic(
        test_data,
        "test_spark_url",
        "http://test-minio:9000", # minio_endpoint
        "test_access_key",      # minio_access_key
        "test_secret_key",      # minio_secret_key
        "s3a://test-bucket/path" # minio_path
    )

    # 검증
    # SparkSession 빌더가 올바른 설정으로 호출되었는지 확인
    mock_builder.config.assert_any_call('spark.hadoop.fs.s3a.endpoint', "http://test-minio:9000")
    mock_builder.config.assert_any_call('spark.hadoop.fs.s3a.access.key', "test_access_key")
    mock_builder.config.assert_any_call('spark.hadoop.fs.s3a.secret.key', "test_secret_key")
    mock_builder.config.assert_any_call('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262') # 버전 확인

    mock_spark.createDataFrame.assert_called_once() # createDataFrame 호출 확인
    # createDataFrame에 test_data가 전달되었는지 확인 (더 정확한 검증)
    # args, kwargs = mock_spark.createDataFrame.call_args
    # assert args[0].equals(test_data)

    mock_spark_df.withColumn.assert_called() # 컬럼 변환 호출 확인
    mock_writer.partitionBy.assert_called_once_with('year', 'month', 'day', 'stock_code') # 파티션 확인
    mock_partitioned_writer.mode.assert_called_once_with("overwrite") # 쓰기 모드 확인
    mock_partitioned_writer.parquet.assert_called_once_with("s3a://test-bucket/path") # 쓰기 경로 확인
    mock_spark.stop.assert_called_once() # stop 호출 확인