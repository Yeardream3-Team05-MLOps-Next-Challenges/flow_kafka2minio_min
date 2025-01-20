import pytest
from unittest.mock import patch, MagicMock
import pandas as pd

from src.logic import read_kafka_logic, write_minio_logic

@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer):
    # Mock 설정
    mock_consumer = MockConsumer.return_value
    mock_consumer.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: None})
    }
    
    # ASCII 문자만 사용하도록 수정된 메시지
    mock_message = MagicMock()
    mock_message.error.return_value = None
    mock_message.value.return_value = b'{"window": {"start": "2024-01-01", "end": "2024-01-02"}, "stock_code": "005930", "open": 100, "high": 110, "low": 90, "close": 105, "candle": 1}'
    mock_message.topic.return_value = "test_topic"
    mock_message.partition.return_value = 0
    mock_message.offset.return_value = 0
    mock_message.timestamp.return_value = (1, 1000)
    
    # StopIteration 에러 해결을 위한 수정
    mock_consumer.poll.side_effect = [mock_message, StopIteration]
    
    # 테스트 실행
    df = read_kafka_logic("test_topic", "test_url")
    
    # 검증
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df['stock_code'].iloc[0] == '005930'

@patch('src.logic.SparkSession')
def test_write_minio_logic(MockSparkSession):
    # Mock 설정
    mock_spark = MagicMock()
    MockSparkSession.builder.appName.return_value.master.return_value.config.return_value.config.return_value.getOrCreate.return_value = mock_spark
    
    # 테스트 데이터
    test_data = pd.DataFrame({
        'window_start': ['2024-01-01'],
        'stock_code': ['005930']
    })
    
    # 테스트 실행
    write_minio_logic(test_data, "test_spark_url", "test_minio_url")
    
    # 검증
    assert mock_spark.createDataFrame.called
    assert mock_spark.stop.called