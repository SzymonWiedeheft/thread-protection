"""Unit tests for Bronze stream."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType

from src.streams.bronze_stream import BronzeStream, create_bronze_stream


class TestBronzeStream:
    """Test cases for BronzeStream class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock()
        spark.sparkContext.master = "local[*]"
        spark.version = "3.5.0"
        return spark

    @pytest.fixture
    def bronze_stream(self, mock_spark):
        """Create a BronzeStream instance with mocked Spark."""
        with patch(
            "src.streams.bronze_stream.get_kafka_bootstrap_servers",
            return_value="kafka:9092",
        ), patch(
            "src.streams.bronze_stream.get_kafka_topics",
            return_value=["malware_domains", "ads_trackers_domains"],
        ), patch(
            "src.streams.bronze_stream.get_delta_path",
            return_value="/data/delta/bronze",
        ), patch(
            "src.streams.bronze_stream.get_checkpoint_path",
            return_value="/data/checkpoints/bronze",
        ), patch(
            "src.streams.bronze_stream.get_trigger_interval", return_value="5 seconds"
        ):
            return BronzeStream(mock_spark)

    def test_initialization(self, bronze_stream, mock_spark):
        """Test BronzeStream initialization."""
        assert bronze_stream.spark == mock_spark
        assert bronze_stream.bootstrap_servers == "kafka:9092"
        assert bronze_stream.topics == ["malware_domains", "ads_trackers_domains"]
        assert bronze_stream.delta_path == "/data/delta/bronze"
        assert bronze_stream.checkpoint_path == "/data/checkpoints/bronze"
        assert bronze_stream.trigger_interval == "5 seconds"

    def test_initialization_with_custom_params(self, mock_spark):
        """Test BronzeStream initialization with custom parameters."""
        bronze = BronzeStream(
            mock_spark,
            bootstrap_servers="custom:9092",
            topics=["custom_topic"],
            delta_path="/custom/delta",
            checkpoint_path="/custom/checkpoint",
            trigger_interval="10 seconds",
        )

        assert bronze.bootstrap_servers == "custom:9092"
        assert bronze.topics == ["custom_topic"]
        assert bronze.delta_path == "/custom/delta"
        assert bronze.checkpoint_path == "/custom/checkpoint"
        assert bronze.trigger_interval == "10 seconds"

    def test_read_from_kafka(self, bronze_stream, mock_spark):
        """Test Kafka read stream initialization."""
        # Mock readStream
        mock_read_stream = Mock()
        mock_format = Mock()
        mock_options = Mock()
        mock_df = Mock()

        mock_spark.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_format
        mock_format.options.return_value = mock_options
        mock_options.load.return_value = mock_df

        # Call method
        result = bronze_stream.read_from_kafka()

        # Assertions
        mock_read_stream.format.assert_called_once_with("kafka")
        assert result == mock_df

    def test_transform(self, bronze_stream, mock_spark):
        """Test Kafka message transformation."""
        # Mock the transform method since it requires active Spark context
        # This test verifies the method exists and can be called
        mock_kafka_df = Mock()

        # Patch the Spark functions to avoid needing active context
        with patch("src.streams.bronze_stream.from_json"), patch(
            "src.streams.bronze_stream.col"
        ), patch("src.streams.bronze_stream.current_timestamp"), patch(
            "src.streams.bronze_stream.to_date"
        ), patch("src.streams.bronze_stream.to_timestamp"):
            # Setup mock chain
            mock_select1 = Mock()
            mock_select2 = Mock()
            mock_withColumn1 = Mock()
            mock_transformed_df = Mock()

            mock_kafka_df.select.return_value = mock_select1
            mock_select1.select.return_value = mock_select2
            mock_select2.withColumn.return_value = mock_withColumn1
            mock_withColumn1.withColumn.return_value = mock_transformed_df
            mock_transformed_df.schema.simpleString.return_value = "struct<...>"

            # Call transform
            result = bronze_stream.transform(mock_kafka_df)

            # Assertions
            assert mock_kafka_df.select.called

    def test_initialize_delta_table(self, bronze_stream, mock_spark):
        """Test Delta table initialization."""
        with patch(
            "src.streams.bronze_stream.create_delta_table_if_not_exists"
        ) as mock_create:
            bronze_stream.initialize_delta_table()

            # Verify create_delta_table_if_not_exists was called
            mock_create.assert_called_once()
            call_args = mock_create.call_args

            assert call_args[0][0] == mock_spark
            assert call_args[0][1] == "/data/delta/bronze"
            assert call_args[1]["partition_columns"] == ["ingestion_date"]
            assert "delta.enableChangeDataFeed" in call_args[1]["table_properties"]

    def test_write_to_delta(self, bronze_stream):
        """Test writing to Delta table."""
        mock_df = Mock()
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.name = "bronze_stream"

        with patch(
            "src.streams.bronze_stream.write_stream_to_delta", return_value=mock_query
        ) as mock_write:
            result = bronze_stream.write_to_delta(mock_df)

            # Verify write_stream_to_delta was called
            mock_write.assert_called_once_with(
                mock_df,
                "/data/delta/bronze",
                "/data/checkpoints/bronze",
                output_mode="append",
                partition_by=["ingestion_date"],
                trigger_interval="5 seconds",
                query_name="bronze_stream",
            )

            assert result == mock_query

    def test_start_success(self, bronze_stream, mock_spark):
        """Test successful start of Bronze streaming pipeline."""
        mock_kafka_df = Mock()
        mock_bronze_df = Mock()
        mock_query = Mock()

        with patch.object(bronze_stream, "initialize_delta_table"), patch.object(
            bronze_stream, "read_from_kafka", return_value=mock_kafka_df
        ), patch.object(
            bronze_stream, "transform", return_value=mock_bronze_df
        ), patch.object(
            bronze_stream, "write_to_delta", return_value=mock_query
        ):
            result = bronze_stream.start()

            # Assertions
            bronze_stream.initialize_delta_table.assert_called_once()
            bronze_stream.read_from_kafka.assert_called_once()
            bronze_stream.transform.assert_called_once_with(mock_kafka_df)
            bronze_stream.write_to_delta.assert_called_once_with(mock_bronze_df)
            assert result == mock_query

    def test_start_failure(self, bronze_stream):
        """Test Bronze stream start failure handling."""
        with patch.object(
            bronze_stream, "initialize_delta_table", side_effect=Exception("Test error")
        ):
            with pytest.raises(Exception, match="Test error"):
                bronze_stream.start()

    def test_stop(self, bronze_stream):
        """Test stopping streaming query."""
        mock_query = Mock()
        mock_query.id = "test-query-id"

        bronze_stream.stop(mock_query)

        mock_query.stop.assert_called_once()

    def test_stop_with_error(self, bronze_stream):
        """Test stopping streaming query with error."""
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.stop.side_effect = Exception("Stop error")

        with pytest.raises(Exception, match="Stop error"):
            bronze_stream.stop(mock_query)


def test_create_bronze_stream():
    """Test factory function."""
    mock_spark = Mock()

    with patch(
        "src.streams.bronze_stream.get_kafka_bootstrap_servers",
        return_value="kafka:9092",
    ), patch(
        "src.streams.bronze_stream.get_kafka_topics", return_value=["topic1"]
    ), patch(
        "src.streams.bronze_stream.get_delta_path", return_value="/data/delta"
    ), patch(
        "src.streams.bronze_stream.get_checkpoint_path", return_value="/data/checkpoint"
    ), patch(
        "src.streams.bronze_stream.get_trigger_interval", return_value="5 seconds"
    ):
        bronze = create_bronze_stream(mock_spark)

        assert isinstance(bronze, BronzeStream)
        assert bronze.spark == mock_spark


def test_create_bronze_stream_with_kwargs():
    """Test factory function with custom kwargs."""
    mock_spark = Mock()

    bronze = create_bronze_stream(
        mock_spark,
        bootstrap_servers="custom:9092",
        topics=["custom"],
    )

    assert isinstance(bronze, BronzeStream)
    assert bronze.bootstrap_servers == "custom:9092"
    assert bronze.topics == ["custom"]
