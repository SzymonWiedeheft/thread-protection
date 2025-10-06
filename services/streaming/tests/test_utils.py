"""Unit tests for utility functions."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql.types import StructType, StructField, StringType

from src.utils.kafka_utils import (
    get_kafka_options,
    get_kafka_write_options,
    validate_kafka_connectivity,
    get_kafka_metadata_columns,
    extract_kafka_metadata_expr,
)

from src.utils.delta_utils import (
    create_delta_table_if_not_exists,
    get_delta_table,
    optimize_delta_table,
    vacuum_delta_table,
    get_delta_table_version,
    get_streaming_write_options,
    write_stream_to_delta,
    get_table_statistics,
)


class TestKafkaUtils:
    """Test cases for Kafka utilities."""

    def test_get_kafka_options_basic(self):
        """Test getting basic Kafka options."""
        result = get_kafka_options(
            bootstrap_servers="kafka:9092",
            topics=["topic1", "topic2"],
        )

        assert result["kafka.bootstrap.servers"] == "kafka:9092"
        assert result["subscribe"] == "topic1,topic2"
        assert result["startingOffsets"] == "latest"
        assert result["failOnDataLoss"] == "false"

    def test_get_kafka_options_with_all_params(self):
        """Test getting Kafka options with all parameters."""
        result = get_kafka_options(
            bootstrap_servers="kafka:9092",
            topics=["topic1"],
            starting_offsets="earliest",
            max_offsets_per_trigger=1000,
            fail_on_data_loss=True,
        )

        assert result["startingOffsets"] == "earliest"
        assert result["maxOffsetsPerTrigger"] == "1000"
        assert result["failOnDataLoss"] == "true"

    def test_get_kafka_options_single_topic(self):
        """Test getting Kafka options with single topic."""
        result = get_kafka_options(
            bootstrap_servers="kafka:9092",
            topics=["single_topic"],
        )

        assert result["subscribe"] == "single_topic"

    def test_get_kafka_write_options(self):
        """Test getting Kafka write options."""
        result = get_kafka_write_options(
            bootstrap_servers="kafka:9092",
            topic="output_topic",
            checkpoint_location="/checkpoints/kafka",
        )

        assert result["kafka.bootstrap.servers"] == "kafka:9092"
        assert result["topic"] == "output_topic"
        assert result["checkpointLocation"] == "/checkpoints/kafka"

    @pytest.mark.skip(reason="kafka-python not installed in test environment")
    def test_validate_kafka_connectivity_success(self):
        """Test successful Kafka connectivity validation."""
        # This test requires kafka-python to be installed
        pass

    @pytest.mark.skip(reason="kafka-python not installed in test environment")
    def test_validate_kafka_connectivity_failure(self):
        """Test Kafka connectivity validation failure."""
        # This test requires kafka-python to be installed
        pass

    def test_validate_kafka_connectivity_import_error(self):
        """Test Kafka connectivity validation when kafka-python is not available."""
        # Since kafka-python is not installed, this function should return True
        # indicating it skips the connectivity check gracefully
        result = validate_kafka_connectivity("kafka:9092")
        # Should return True when kafka-python not available
        assert result is True

    def test_get_kafka_metadata_columns(self):
        """Test getting Kafka metadata columns."""
        result = get_kafka_metadata_columns()

        assert isinstance(result, list)
        assert "topic" in result
        assert "partition" in result
        assert "offset" in result
        assert "timestamp" in result
        assert "timestampType" in result

    def test_extract_kafka_metadata_expr(self):
        """Test extracting Kafka metadata expressions."""
        result = extract_kafka_metadata_expr()

        assert isinstance(result, dict)
        assert result["kafka_topic"] == "topic"
        assert result["kafka_partition"] == "partition"
        assert result["kafka_offset"] == "offset"
        assert result["kafka_timestamp"] == "timestamp"


class TestDeltaUtils:
    """Test cases for Delta Lake utilities."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock()
        return spark

    @pytest.fixture
    def test_schema(self):
        """Create a test schema."""
        return StructType(
            [
                StructField("id", StringType(), nullable=False),
                StructField("value", StringType(), nullable=True),
            ]
        )

    @patch("src.utils.delta_utils.DeltaTable")
    def test_create_delta_table_if_not_exists_new_table(
        self, mock_delta_table_class, mock_spark, test_schema
    ):
        """Test creating a new Delta table."""
        mock_delta_table_class.isDeltaTable.return_value = False

        mock_df = Mock()
        mock_spark.createDataFrame.return_value = mock_df

        mock_writer = Mock()
        mock_df.write.format.return_value = mock_writer
        mock_writer.partitionBy.return_value = mock_writer
        mock_writer.option.return_value = mock_writer

        create_delta_table_if_not_exists(
            mock_spark,
            "/data/delta/test",
            test_schema,
            partition_columns=["date"],
            table_properties={"delta.enableChangeDataFeed": "true"},
        )

        # Verify table creation
        mock_delta_table_class.isDeltaTable.assert_called_once_with(
            mock_spark, "/data/delta/test"
        )
        mock_spark.createDataFrame.assert_called_once_with([], test_schema)
        mock_df.write.format.assert_called_once_with("delta")
        mock_writer.partitionBy.assert_called_once_with("date")
        mock_writer.save.assert_called_once_with("/data/delta/test")

    @patch("src.utils.delta_utils.DeltaTable")
    def test_create_delta_table_if_exists(
        self, mock_delta_table_class, mock_spark, test_schema
    ):
        """Test when Delta table already exists."""
        mock_delta_table_class.isDeltaTable.return_value = True

        create_delta_table_if_not_exists(
            mock_spark,
            "/data/delta/test",
            test_schema,
        )

        # Verify no table creation
        mock_spark.createDataFrame.assert_not_called()

    @patch("src.utils.delta_utils.DeltaTable")
    def test_get_delta_table_success(self, mock_delta_table_class, mock_spark):
        """Test getting Delta table."""
        mock_delta_table_class.isDeltaTable.return_value = True
        mock_table = Mock()
        mock_delta_table_class.forPath.return_value = mock_table

        result = get_delta_table(mock_spark, "/data/delta/test")

        assert result == mock_table
        mock_delta_table_class.forPath.assert_called_once_with(
            mock_spark, "/data/delta/test"
        )

    @patch("src.utils.delta_utils.DeltaTable")
    def test_get_delta_table_not_delta(self, mock_delta_table_class, mock_spark):
        """Test getting non-Delta table raises error."""
        mock_delta_table_class.isDeltaTable.return_value = False

        with pytest.raises(ValueError, match="Path is not a Delta table"):
            get_delta_table(mock_spark, "/data/not-delta")

    @patch("src.utils.delta_utils.get_delta_table")
    def test_optimize_delta_table_with_zorder(self, mock_get_table, mock_spark):
        """Test optimizing Delta table with Z-ordering."""
        mock_table = Mock()
        mock_optimize = Mock()
        mock_get_table.return_value = mock_table
        mock_table.optimize.return_value = mock_optimize

        optimize_delta_table(
            mock_spark,
            "/data/delta/test",
            zorder_columns=["domain", "category"],
        )

        mock_table.optimize.assert_called_once()
        mock_optimize.executeZOrderBy.assert_called_once_with("domain", "category")

    @patch("src.utils.delta_utils.get_delta_table")
    def test_optimize_delta_table_compaction(self, mock_get_table, mock_spark):
        """Test optimizing Delta table with compaction only."""
        mock_table = Mock()
        mock_optimize = Mock()
        mock_get_table.return_value = mock_table
        mock_table.optimize.return_value = mock_optimize

        optimize_delta_table(mock_spark, "/data/delta/test")

        mock_table.optimize.assert_called_once()
        mock_optimize.executeCompaction.assert_called_once()

    @patch("src.utils.delta_utils.get_delta_table")
    def test_vacuum_delta_table(self, mock_get_table, mock_spark):
        """Test vacuuming Delta table."""
        mock_table = Mock()
        mock_get_table.return_value = mock_table

        vacuum_delta_table(mock_spark, "/data/delta/test", retention_hours=72)

        mock_table.vacuum.assert_called_once_with(72)

    @patch("src.utils.delta_utils.get_delta_table")
    def test_get_delta_table_version(self, mock_get_table, mock_spark):
        """Test getting Delta table version."""
        mock_table = Mock()
        mock_history = Mock()
        mock_select = Mock()
        mock_row = {"version": 42}

        mock_get_table.return_value = mock_table
        mock_table.history.return_value = mock_history
        mock_history.select.return_value = mock_select
        mock_select.collect.return_value = [mock_row]

        result = get_delta_table_version(mock_spark, "/data/delta/test")

        assert result == 42

    def test_get_streaming_write_options(self):
        """Test getting streaming write options."""
        result = get_streaming_write_options(
            checkpoint_location="/checkpoints/test",
            output_mode="update",
            trigger_interval="10 seconds",
        )

        assert result["checkpointLocation"] == "/checkpoints/test"
        assert result["outputMode"] == "update"
        assert result["trigger"] == "10 seconds"

    def test_write_stream_to_delta(self):
        """Test writing stream to Delta."""
        mock_df = Mock()
        mock_write_stream = Mock()
        mock_format = Mock()
        mock_output_mode = Mock()
        mock_option = Mock()
        mock_partition = Mock()
        mock_query_name = Mock()
        mock_trigger = Mock()
        mock_query = Mock()
        mock_query.id = "test-id"
        mock_query.name = "test-query"

        # Setup mock chain
        mock_df.writeStream = mock_write_stream
        mock_write_stream.format.return_value = mock_format
        mock_format.outputMode.return_value = mock_output_mode
        mock_output_mode.option.return_value = mock_option
        mock_option.partitionBy.return_value = mock_partition
        mock_partition.queryName.return_value = mock_query_name
        mock_query_name.trigger.return_value = mock_trigger
        mock_trigger.start.return_value = mock_query

        result = write_stream_to_delta(
            mock_df,
            "/data/delta/test",
            "/checkpoints/test",
            output_mode="append",
            partition_by=["date"],
            trigger_interval="5 seconds",
            query_name="test_query",
        )

        assert result == mock_query
        mock_write_stream.format.assert_called_once_with("delta")
        mock_format.outputMode.assert_called_once_with("append")
        mock_partition.queryName.assert_called_once_with("test_query")

    @patch("src.utils.delta_utils.get_delta_table")
    def test_get_table_statistics_success(self, mock_get_table, mock_spark):
        """Test getting table statistics."""
        mock_table = Mock()
        mock_df = Mock()
        mock_read = Mock()
        mock_format = Mock()
        mock_history = Mock()

        mock_get_table.return_value = mock_table
        mock_spark.read = mock_read
        mock_read.format.return_value = mock_format
        mock_format.load.return_value = mock_df
        mock_df.count.return_value = 1000

        mock_table.history.return_value = mock_history
        mock_history.collect.return_value = [{"operation": "WRITE"}]

        result = get_table_statistics(mock_spark, "/data/delta/test")

        assert result["table_path"] == "/data/delta/test"
        assert result["row_count"] == 1000
        assert result["last_operation"] == "WRITE"
        assert result["is_delta"] is True

    @patch("src.utils.delta_utils.get_delta_table")
    def test_get_table_statistics_error(self, mock_get_table, mock_spark):
        """Test getting table statistics with error."""
        mock_get_table.side_effect = Exception("Table error")

        result = get_table_statistics(mock_spark, "/data/delta/test")

        assert result["table_path"] == "/data/delta/test"
        assert "error" in result
        assert result["is_delta"] is False
