import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
from confluent_kafka import KafkaError
from src.logic import read_kafka_logic, write_minio_logic
from pyspark.sql.functions import year, month, dayofmonth

@patch('src.logic.Consumer')
@patch('src.logic.time.time')  # time.time() mocking 추가
def test_read_kafka_logic(mock_time, MockConsumer):
    # Mock 설정
    mock_consumer = MockConsumer.return_value
    mock_consumer.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: MagicMock(), 1: MagicMock()})
    }

    # poll() 이 처음 한 번은 메시지를 반환하고, 그 다음부터는 None 을 반환하도록 설정
    mock_consumer.poll.side_effect = [
        MagicMock(
            error=lambda: None,
            value=lambda: json.dumps({
                "window": {"start": "20240702", "end": "20240703"},
                "종목코드": "005930",
                "open": 100,
                "high": 110,
                "low": 90,
                "close": 105,
                "candle": 1
            }).encode('utf-8'),
            topic=lambda: "test_topic",
            partition=lambda: 0,
            offset=lambda: 0,
            timestamp=lambda: (1, 1000)
        ),
        None
    ]
    # time.time() 이 처음 한 번은 1000을 반환하고, 그 다음부터는 1000 + timeout_seconds + 1 을 반환하도록 설정
    mock_time.side_effect = [1000, 1000 + 10 + 1]

    # 테스트 실행
    topic_name = "test_topic"
    kafka_url = "test_kafka_url"
    df = read_kafka_logic(topic_name, kafka_url)

    # 검증
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert 'stock_code' in df.columns
    assert df['stock_code'].iloc[0] == '005930'

@patch('src.logic.SparkSession')
def test_write_minio_logic(MockSparkSession):
    #Mock 설정정
    mock_spark = MagicMock()
    mock_df = MagicMock()
    MockSparkSession.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
    mock_spark.createDataFrame.return_value = mock_df
    
    # Mock 객체의 반환값으로 자기 자신(mock_df)을 반환하도록 설정
    mock_df.withColumn.return_value = mock_df
    mock_df.write.partitionBy.return_value = mock_df
    mock_df.write.mode.return_value = mock_df
    
    # 테스트 실행
    data_source = pd.DataFrame([{'test': 'data', 'window_start': '2024-01-01'}])
    write_minio_logic(data_source, "test_spark_url", "test_minio_url")

    # 검증
    assert mock_spark.createDataFrame.called  # DataFrame 생성 확인
    mock_df.withColumn.assert_called() # withColumn  호출확인
    mock_df.write.partitionBy.assert_called_with('stock_code', 'year', 'month', 'day')  # partitionBy 호출 및 인자 확인
    assert mock_spark.stop.called  # 자원 해제 확인