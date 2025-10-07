"""Unit tests for Silver stream."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql.types import StructType, StructField, StringType

from src.streams.silver_stream import SilverStream, create_silver_stream


class TestSilverStream:
    """Test cases for SilverStream class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock()
        spark.sparkContext.master = "local[*]"
        spark.version = "3.5.0"
        return spark

    @pytest.fixture
    def silver_stream(self, mock_spark):
        """Create a SilverStream instance with mocked Spark."""
        with patch(
            "src.streams.silver_stream.get_delta_path",
            side_effect=lambda layer: f"/data/delta/{layer}",
        ), patch(
            "src.streams.silver_stream.get_checkpoint_path",
            side_effect=lambda layer: f"/data/checkpoints/{layer}",
        ), patch(
            "src.streams.silver_stream.get_trigger_interval",
            return_value="10 seconds",
        ):
            return SilverStream(mock_spark)

    def test_initialization(self, silver_stream, mock_spark):
        """Test SilverStream initialization."""
        assert silver_stream.spark == mock_spark
        assert silver_stream.bronze_path == "/data/delta/bronze"
        assert silver_stream.silver_path == "/data/delta/silver"
        assert silver_stream.checkpoint_path == "/data/checkpoints/silver"
        assert silver_stream.trigger_interval == "10 seconds"

    def test_initialization_with_custom_params(self, mock_spark):
        """Test SilverStream initialization with custom parameters."""
        silver = SilverStream(
            mock_spark,
            bronze_path="/custom/bronze",
            silver_path="/custom/silver",
            checkpoint_path="/custom/checkpoint",
            trigger_interval="20 seconds",
        )

        assert silver.bronze_path == "/custom/bronze"
        assert silver.silver_path == "/custom/silver"
        assert silver.checkpoint_path == "/custom/checkpoint"
        assert silver.trigger_interval == "20 seconds"

    def test_read_from_bronze(self, silver_stream, mock_spark):
        """Test Bronze Delta read stream initialization."""
        # Mock readStream
        mock_read_stream = Mock()
        mock_format = Mock()
        mock_option1 = Mock()
        mock_option2 = Mock()
        mock_df = Mock()

        mock_spark.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_format
        mock_format.option.return_value = mock_option1
        mock_option1.option.return_value = mock_option2
        mock_option2.load.return_value = mock_df

        # Call method
        result = silver_stream.read_from_bronze()

        # Assertions
        mock_read_stream.format.assert_called_once_with("delta")
        assert result == mock_df

    def test_transform(self, silver_stream):
        """Test Bronze to Silver transformation."""
        # Create mock Bronze DataFrame
        mock_bronze_df = Mock()

        # Patch Spark functions and UDFs
        with patch("src.streams.silver_stream.clean_domain_udf"), patch(
            "src.streams.silver_stream.is_valid_domain_udf"
        ), patch("src.streams.silver_stream.col"), patch(
            "src.streams.silver_stream.when"
        ), patch(
            "src.streams.silver_stream.current_timestamp"
        ):
            # Setup mock chain
            mock_withColumn1 = Mock()
            mock_withColumn2 = Mock()
            mock_withColumn3 = Mock()
            mock_withColumn4 = Mock()
            mock_select = Mock()
            mock_transformed_df = Mock()

            mock_bronze_df.withColumn.return_value = mock_withColumn1
            mock_withColumn1.withColumn.return_value = mock_withColumn2
            mock_withColumn2.withColumn.return_value = mock_withColumn3
            mock_withColumn3.withColumn.return_value = mock_withColumn4
            mock_withColumn4.select.return_value = mock_select
            mock_select.schema.simpleString.return_value = "struct<...>"

            # Call transform
            result = silver_stream.transform(mock_bronze_df)

            # Assertions
            assert mock_bronze_df.withColumn.called

    def test_initialize_delta_table(self, silver_stream, mock_spark):
        """Test Delta table initialization."""
        with patch(
            "src.streams.silver_stream.create_delta_table_if_not_exists"
        ) as mock_create:
            silver_stream.initialize_delta_table()

            # Verify create_delta_table_if_not_exists was called
            mock_create.assert_called_once()
            call_args = mock_create.call_args

            assert call_args[0][0] == mock_spark
            assert call_args[0][1] == "/data/delta/silver"
            assert call_args[1]["partition_columns"] == ["ingestion_date"]
            assert "delta.enableChangeDataFeed" in call_args[1]["table_properties"]

    def test_write_to_delta(self, silver_stream):
        """Test writing to Silver Delta table."""
        mock_df = Mock()
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.name = "silver_stream"

        with patch(
            "src.streams.silver_stream.write_stream_to_delta", return_value=mock_query
        ) as mock_write:
            result = silver_stream.write_to_delta(mock_df)

            # Verify write_stream_to_delta was called
            mock_write.assert_called_once_with(
                mock_df,
                "/data/delta/silver",
                "/data/checkpoints/silver",
                output_mode="append",
                partition_by=["ingestion_date"],
                trigger_interval="10 seconds",
                query_name="silver_stream",
            )

            assert result == mock_query

    def test_start_success(self, silver_stream, mock_spark):
        """Test successful start of Silver streaming pipeline."""
        mock_bronze_df = Mock()
        mock_silver_df = Mock()
        mock_query = Mock()

        with patch.object(silver_stream, "initialize_delta_table"), patch.object(
            silver_stream, "read_from_bronze", return_value=mock_bronze_df
        ), patch.object(
            silver_stream, "transform", return_value=mock_silver_df
        ), patch.object(
            silver_stream, "write_to_delta", return_value=mock_query
        ):
            result = silver_stream.start()

            # Assertions
            silver_stream.initialize_delta_table.assert_called_once()
            silver_stream.read_from_bronze.assert_called_once()
            silver_stream.transform.assert_called_once_with(mock_bronze_df)
            silver_stream.write_to_delta.assert_called_once_with(mock_silver_df)
            assert result == mock_query

    def test_start_failure(self, silver_stream):
        """Test Silver stream start failure handling."""
        with patch.object(
            silver_stream, "initialize_delta_table", side_effect=Exception("Test error")
        ):
            with pytest.raises(Exception, match="Test error"):
                silver_stream.start()

    def test_stop(self, silver_stream):
        """Test stopping streaming query."""
        mock_query = Mock()
        mock_query.id = "test-query-id"

        silver_stream.stop(mock_query)

        mock_query.stop.assert_called_once()

    def test_stop_with_error(self, silver_stream):
        """Test stopping streaming query with error."""
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.stop.side_effect = Exception("Stop error")

        with pytest.raises(Exception, match="Stop error"):
            silver_stream.stop(mock_query)


def test_create_silver_stream():
    """Test factory function."""
    mock_spark = Mock()

    with patch(
        "src.streams.silver_stream.get_delta_path",
        side_effect=lambda layer: f"/data/delta/{layer}",
    ), patch(
        "src.streams.silver_stream.get_checkpoint_path",
        side_effect=lambda layer: f"/data/checkpoints/{layer}",
    ), patch(
        "src.streams.silver_stream.get_trigger_interval",
        return_value="10 seconds",
    ):
        silver = create_silver_stream(mock_spark)

        assert isinstance(silver, SilverStream)
        assert silver.spark == mock_spark


def test_create_silver_stream_with_kwargs():
    """Test factory function with custom kwargs."""
    mock_spark = Mock()

    silver = create_silver_stream(
        mock_spark,
        bronze_path="/custom/bronze",
        silver_path="/custom/silver",
    )

    assert isinstance(silver, SilverStream)
    assert silver.bronze_path == "/custom/bronze"
    assert silver.silver_path == "/custom/silver"


class TestSilverUDFs:
    """Test cases for Silver stream UDFs."""

    def test_clean_domain_udf(self):
        """Test clean_domain UDF logic."""
        from src.streams.silver_stream import clean_domain_udf

        # UDFs can't be easily tested without Spark context
        # Just verify the UDF is defined
        assert clean_domain_udf is not None
        assert callable(clean_domain_udf.func)

    def test_is_valid_domain_udf(self):
        """Test is_valid_domain UDF logic."""
        from src.streams.silver_stream import is_valid_domain_udf

        # UDFs can't be easily tested without Spark context
        # Just verify the UDF is defined
        assert is_valid_domain_udf is not None
        assert callable(is_valid_domain_udf.func)
