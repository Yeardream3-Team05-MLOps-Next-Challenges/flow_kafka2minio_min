import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import json
from src.logic import read_kafka_logic, write_minio_logic

# read_kafka_logic 테스트
@patch('src.logic.Consumer')
def test_read_kafka_logic(MockConsumer):
    # Mock 설정
    mock_consumer = MockConsumer.return_value
    mock_consumer.list_topics.return_value.topics = {
        "test_topic": MagicMock(partitions={0: MagicMock(), 1:MagicMock()})
    }
    mock_consumer.poll.side_effect = [
         MagicMock(error=lambda : None, value=lambda : json.dumps({
            "window": {"start": "20240702", "end": "20240703"},
            "종목코드": "005930",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "candle": 1
            }).encode('utf-8'), topic=lambda: "test_topic", partition=lambda: 0, offset=lambda: 0, timestamp=lambda: (1,1000)),
          None,
         MagicMock(error=lambda : None, value=lambda : json.dumps({
            "window": {"start": "20240702", "end": "20240703"},
            "종목코드": "005930",
            "open": 100,
            "high": 110,
            "low": 90,
            "close": 105,
            "candle": 1
            }).encode('utf-8'), topic=lambda: "test_topic", partition=lambda: 1, offset=lambda: 0, timestamp=lambda: (1,1000)),
          None
    ]


    # 테스트 실행
    topic_name = "test_topic"
    kafka_url = "test_kafka_url"
    df = read_kafka_logic(topic_name, kafka_url)
    
    # 검증
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert 'topic' in df.columns
    assert 'partition' in df.columns
    assert 'offset' in df.columns
    assert 'timestamp' in df.columns
    assert 'window_start' in df.columns
    assert 'window_end' in df.columns
    assert 'stock_code' in df.columns
    assert 'open' in df.columns
    assert 'high' in df.columns
    assert 'low' in df.columns
    assert 'close' in df.columns
    assert 'candle' in df.columns

# write_minio_logic 테스트
@patch('src.logic.SparkSession.builder.appName')
def test_write_minio_logic(MockAppName):
     # Mock 설정
    mock_spark_builder = MockAppName.return_value
    mock_spark_builder.master.return_value = mock_spark_builder
    mock_spark_builder.config.return_value = mock_spark_builder
    mock_spark_builder.getOrCreate.return_value = MagicMock()

    mock_spark = mock_spark_builder.getOrCreate.return_value
    mock_df = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    # 테스트 실행
    data_source = pd.DataFrame([{'test':'data'}])
    spark_url = "test_spark_url"
    minio_url = "test_minio_url"
    write_minio_logic(data_source, spark_url, minio_url)
    
    # 검증
    mock_spark_builder.appName.assert_called_once_with("tick_to_min")
    mock_spark_builder.master.assert_called_once_with("test_spark_url")
    mock_spark_builder.config.assert_called()
    mock_spark.createDataFrame.assert_called_once()
    mock_df.withColumn.assert_called()
    mock_df.write.partitionBy.assert_called_once()
    mock_spark.stop.assert_called_once()