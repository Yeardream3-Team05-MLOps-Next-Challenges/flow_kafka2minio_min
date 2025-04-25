import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
from pyspark.sql import SparkSession 
from src.logic import read_kafka_logic, write_minio_logic

@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer):
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
    message_str = '{"window": {"start": "2024-01-01T00:00:00.000Z", "end": "2024-01-01T00:05:00.000Z"}, "종목코드": "005930", "open": 100.0, "high": 110.0, "low": 90.0, "close": 105.0, "candle": "5m"}'
    mock_message.value.return_value = message_str.encode('utf-8')
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
    """
    write_minio_logic 함수의 기본적인 호출 및 종료 흐름만 간략하게 테스트합니다.
    """
    # 1. SparkSession 생성 모킹 (기존과 유사)
    mock_spark = MagicMock(spec=SparkSession)
    mock_builder = MagicMock()
    MockSparkSession.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.master.return_value = mock_builder
    mock_builder.config.return_value = mock_builder # 단순화: config 호출 시 항상 자신 반환
    mock_builder.getOrCreate.return_value = mock_spark

    # 2. createDataFrame 호출 시 예외 발생시키도록 설정
    # 테스트에서 잡을 수 있는 특정 예외 사용 (예: ValueError)
    mock_spark.createDataFrame.side_effect = ValueError("Simulated error after createDataFrame")

    # 3. 테스트 데이터 (간단하게)
    test_data = pd.DataFrame({'col1': [1]})

    # 4. 함수 호출 및 예외 발생 검증
    # pytest.raises를 사용하여 특정 예외가 발생하는지 확인
    with pytest.raises(ValueError, match="Simulated error after createDataFrame"):
        write_minio_logic(
            test_data,
            "test_spark_url",
            "http://test-minio:9000", # 엔드포인트 (더미 값)
            "test_access_key",      # Access Key (더미 값)
            "test_secret_key",      # Secret Key (더미 값)
            "s3a://test-bucket/path" # S3 경로 (더미 값)
        )

    # 5. 핵심 함수 호출 검증
    # SparkSession 생성이 시도되었는지 확인
    mock_builder.getOrCreate.assert_called_once()
    # DataFrame 생성이 시도되었는지 확인
    mock_spark.createDataFrame.assert_called_once_with(test_data)
    # finally 블록의 stop()이 호출되었는지 확인 (예외 발생 시에도 finally는 실행됨)
    mock_spark.stop.assert_called_once()