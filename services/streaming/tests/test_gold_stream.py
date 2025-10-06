"""Unit tests for Gold stream."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from pyspark.sql.types import StructType, StructField, StringType

from src.streams.gold_stream import GoldStream, create_gold_stream


class MockColumn(Mock):
    """Mock Column that supports comparison operators."""

    def __lt__(self, other):
        return Mock()

    def __gt__(self, other):
        return Mock()

    def __ge__(self, other):
        return Mock()

    def __le__(self, other):
        return Mock()

    def __eq__(self, other):
        return Mock()

    def __ne__(self, other):
        return Mock()

    def __sub__(self, other):
        return Mock()

    def __add__(self, other):
        return Mock()


class TestGoldStream:
    """Test cases for GoldStream class."""

    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock()
        spark.sparkContext.master = "local[*]"
        spark.version = "3.5.0"
        return spark

    @pytest.fixture
    def gold_stream(self, mock_spark):
        """Create a GoldStream instance with mocked Spark."""
        with patch(
            "src.streams.gold_stream.get_delta_path",
            side_effect=lambda layer: f"/data/delta/{layer}",
        ), patch(
            "src.streams.gold_stream.get_checkpoint_path",
            side_effect=lambda layer: f"/data/checkpoints/{layer}",
        ), patch(
            "src.streams.gold_stream.get_trigger_interval",
            return_value="30 seconds",
        ):
            return GoldStream(mock_spark)

    def test_initialization(self, gold_stream, mock_spark):
        """Test GoldStream initialization."""
        assert gold_stream.spark == mock_spark
        assert gold_stream.silver_path == "/data/delta/silver"
        assert gold_stream.gold_path == "/data/delta/gold"
        assert gold_stream.checkpoint_path == "/data/checkpoints/gold"
        assert gold_stream.trigger_interval == "30 seconds"
        assert gold_stream.optimize_every_n_batches == 100
        assert gold_stream.batch_counter == 0

    def test_initialization_with_custom_params(self, mock_spark):
        """Test GoldStream initialization with custom parameters."""
        gold = GoldStream(
            mock_spark,
            silver_path="/custom/silver",
            gold_path="/custom/gold",
            checkpoint_path="/custom/checkpoint",
            trigger_interval="60 seconds",
            optimize_every_n_batches=50,
        )

        assert gold.silver_path == "/custom/silver"
        assert gold.gold_path == "/custom/gold"
        assert gold.checkpoint_path == "/custom/checkpoint"
        assert gold.trigger_interval == "60 seconds"
        assert gold.optimize_every_n_batches == 50

    def test_read_from_silver(self, gold_stream, mock_spark):
        """Test Silver Delta read stream initialization."""
        # Mock readStream
        mock_read_stream = Mock()
        mock_format = Mock()
        mock_option1 = Mock()
        mock_option2 = Mock()
        mock_load = Mock()
        mock_filter = Mock()
        mock_df = Mock()

        mock_spark.readStream = mock_read_stream
        mock_read_stream.format.return_value = mock_format
        mock_format.option.return_value = mock_option1
        mock_option1.option.return_value = mock_option2
        mock_option2.load.return_value = mock_load
        mock_load.filter.return_value = mock_filter

        # Patch col function
        with patch("src.streams.gold_stream.col"):
            # Call method
            result = gold_stream.read_from_silver()

            # Assertions
            mock_read_stream.format.assert_called_once_with("delta")
            mock_load.filter.assert_called_once()

    def test_aggregate(self, gold_stream):
        """Test Silver to Gold aggregation."""
        # Create mock Silver DataFrame
        mock_silver_df = Mock()

        # Create a mock that supports chaining for when()
        mock_when_result = Mock()
        mock_when_result.otherwise.return_value = Mock()
        mock_when_result.otherwise.return_value.alias.return_value = Mock()

        # Patch Spark functions
        with patch("src.streams.gold_stream.when", return_value=mock_when_result), patch(
            "src.streams.gold_stream.collect_set", return_value=MockColumn()
        ), patch("src.streams.gold_stream.to_json", return_value=MockColumn()), patch(
            "src.streams.gold_stream.size", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.first", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.spark_min", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.spark_max", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.current_timestamp", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.hash", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.col", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.array_contains", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.expr", return_value=MockColumn()
        ):
            # Setup mock chain
            mock_grouped = Mock()
            mock_agg = Mock()
            mock_withColumn1 = Mock()
            mock_withColumn2 = Mock()
            mock_withColumn3 = Mock()

            mock_silver_df.groupBy.return_value = mock_grouped
            mock_grouped.agg.return_value = mock_agg
            mock_agg.withColumn.return_value = mock_withColumn1
            mock_withColumn1.withColumn.return_value = mock_withColumn2
            mock_withColumn2.withColumn.return_value = mock_withColumn3
            mock_withColumn3.schema.simpleString.return_value = "struct<...>"

            # Call aggregate
            result = gold_stream.aggregate(mock_silver_df)

            # Assertions
            mock_silver_df.groupBy.assert_called_once_with("domain")
            assert mock_grouped.agg.called

    def test_initialize_delta_table(self, gold_stream, mock_spark):
        """Test Delta table initialization."""
        with patch(
            "src.streams.gold_stream.create_delta_table_if_not_exists"
        ) as mock_create:
            gold_stream.initialize_delta_table()

            # Verify create_delta_table_if_not_exists was called
            mock_create.assert_called_once()
            call_args = mock_create.call_args

            assert call_args[0][0] == mock_spark
            assert call_args[0][1] == "/data/delta/gold"
            assert call_args[1]["partition_columns"] is None  # Gold not partitioned
            assert "delta.enableChangeDataFeed" in call_args[1]["table_properties"]

    @patch("src.streams.gold_stream.DeltaTable")
    def test_upsert_to_gold_table_exists(self, mock_delta_table_class, gold_stream):
        """Test UPSERT operation when table exists."""
        # Create mock that supports chaining for when()
        mock_when_result = Mock()
        mock_when_result.when.return_value = mock_when_result
        mock_when_result.otherwise.return_value = Mock()

        # Patch all PySpark functions used in upsert_to_gold
        with patch("src.streams.gold_stream.when", return_value=mock_when_result), patch(
            "src.streams.gold_stream.col", side_effect=lambda x: MockColumn()
        ), patch("src.streams.gold_stream.to_json", return_value=MockColumn()), patch(
            "src.streams.gold_stream.array_distinct", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.array_union", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.from_json", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.size", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.greatest", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.expr", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.current_timestamp", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.ArrayType", return_value=Mock()
        ), patch(
            "src.streams.gold_stream.StringType", return_value=Mock()
        ):
            # Mock table exists
            mock_delta_table_class.isDeltaTable.return_value = True

            # Mock DeltaTable and MERGE operations
            mock_table = Mock()
            mock_delta_table_class.forPath.return_value = mock_table

            mock_alias = Mock()
            mock_merge = Mock()
            mock_when_matched = Mock()
            mock_when_not_matched = Mock()

            mock_table.alias.return_value = mock_alias
            mock_alias.merge.return_value = mock_merge
            mock_merge.whenMatchedUpdate.return_value = mock_when_matched
            mock_when_matched.whenNotMatchedInsert.return_value = mock_when_not_matched

            # Mock micro-batch DataFrame
            mock_batch_df = Mock()
            mock_batch_df.count.return_value = 10

            # Call upsert
            gold_stream.upsert_to_gold(mock_batch_df, batch_id=1)

            # Assertions
            mock_delta_table_class.isDeltaTable.assert_called_once_with(
                gold_stream.spark, "/data/delta/gold"
            )
            mock_table.alias.assert_called_once_with("gold")
            mock_when_not_matched.execute.assert_called_once()
            assert gold_stream.batch_counter == 1

    @patch("src.streams.gold_stream.DeltaTable")
    def test_upsert_to_gold_table_not_exists(
        self, mock_delta_table_class, gold_stream
    ):
        """Test UPSERT operation when table doesn't exist (first batch)."""
        # Mock table doesn't exist
        mock_delta_table_class.isDeltaTable.return_value = False

        # Mock micro-batch DataFrame
        mock_batch_df = Mock()
        mock_batch_df.count.return_value = 10
        mock_write = Mock()
        mock_format = Mock()
        mock_mode = Mock()

        mock_batch_df.write = mock_write
        mock_write.format.return_value = mock_format
        mock_format.mode.return_value = mock_mode

        # Call upsert
        gold_stream.upsert_to_gold(mock_batch_df, batch_id=0)

        # Assertions
        mock_write.format.assert_called_once_with("delta")
        mock_format.mode.assert_called_once_with("append")
        mock_mode.save.assert_called_once_with("/data/delta/gold")

    @patch("src.streams.gold_stream.DeltaTable")
    def test_upsert_triggers_optimization(self, mock_delta_table_class, gold_stream):
        """Test that optimization is triggered every N batches."""
        # Create mock that supports chaining for when()
        mock_when_result = Mock()
        mock_when_result.when.return_value = mock_when_result
        mock_when_result.otherwise.return_value = Mock()

        # Patch all PySpark functions used in upsert_to_gold
        with patch("src.streams.gold_stream.when", return_value=mock_when_result), patch(
            "src.streams.gold_stream.col", side_effect=lambda x: MockColumn()
        ), patch("src.streams.gold_stream.to_json", return_value=MockColumn()), patch(
            "src.streams.gold_stream.array_distinct", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.array_union", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.from_json", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.size", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.greatest", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.expr", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.current_timestamp", return_value=MockColumn()
        ), patch(
            "src.streams.gold_stream.ArrayType", return_value=Mock()
        ), patch(
            "src.streams.gold_stream.StringType", return_value=Mock()
        ):
            # Set batch counter close to optimization threshold
            gold_stream.batch_counter = 99
            gold_stream.optimize_every_n_batches = 100

            # Mock table exists
            mock_delta_table_class.isDeltaTable.return_value = True

            # Mock DeltaTable and operations
            mock_table = Mock()
            mock_delta_table_class.forPath.return_value = mock_table

            mock_alias = Mock()
            mock_merge = Mock()
            mock_when_matched = Mock()
            mock_when_not_matched = Mock()
            mock_optimize = Mock()

            mock_table.alias.return_value = mock_alias
            mock_alias.merge.return_value = mock_merge
            mock_merge.whenMatchedUpdate.return_value = mock_when_matched
            mock_when_matched.whenNotMatchedInsert.return_value = mock_when_not_matched
            mock_table.optimize.return_value = mock_optimize

            # Mock micro-batch DataFrame
            mock_batch_df = Mock()
            mock_batch_df.count.return_value = 10

            # Call upsert (should trigger optimization at batch 100)
            gold_stream.upsert_to_gold(mock_batch_df, batch_id=100)

            # Assertions
            assert gold_stream.batch_counter == 100
            mock_optimize.executeZOrderBy.assert_called_once_with("domain")

    @patch("src.streams.gold_stream.DeltaTable")
    def test_optimize_gold_table(self, mock_delta_table_class, gold_stream):
        """Test Gold table optimization."""
        mock_table = Mock()
        mock_optimize = Mock()

        mock_delta_table_class.forPath.return_value = mock_table
        mock_table.optimize.return_value = mock_optimize

        # Call optimize
        gold_stream.optimize_gold_table()

        # Assertions
        mock_delta_table_class.forPath.assert_called_once_with(
            gold_stream.spark, "/data/delta/gold"
        )
        mock_optimize.executeZOrderBy.assert_called_once_with("domain")

    def test_write_to_delta(self, gold_stream):
        """Test writing to Gold Delta table with foreachBatch."""
        mock_df = Mock()
        mock_write_stream = Mock()
        mock_foreach = Mock()
        mock_output_mode = Mock()
        mock_option = Mock()
        mock_query_name = Mock()
        mock_trigger = Mock()
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.name = "gold_stream"

        # Setup mock chain
        mock_df.writeStream = mock_write_stream
        mock_write_stream.foreachBatch.return_value = mock_foreach
        mock_foreach.outputMode.return_value = mock_output_mode
        mock_output_mode.option.return_value = mock_option
        mock_option.queryName.return_value = mock_query_name
        mock_query_name.trigger.return_value = mock_trigger
        mock_trigger.start.return_value = mock_query

        # Call write_to_delta
        result = gold_stream.write_to_delta(mock_df)

        # Assertions
        mock_write_stream.foreachBatch.assert_called_once()
        mock_foreach.outputMode.assert_called_once_with("update")
        mock_option.queryName.assert_called_once_with("gold_stream")
        assert result == mock_query

    def test_start_success(self, gold_stream, mock_spark):
        """Test successful start of Gold streaming pipeline."""
        mock_silver_df = Mock()
        mock_aggregated_df = Mock()
        mock_query = Mock()

        with patch.object(gold_stream, "initialize_delta_table"), patch.object(
            gold_stream, "read_from_silver", return_value=mock_silver_df
        ), patch.object(
            gold_stream, "aggregate", return_value=mock_aggregated_df
        ), patch.object(
            gold_stream, "write_to_delta", return_value=mock_query
        ):
            result = gold_stream.start()

            # Assertions
            gold_stream.initialize_delta_table.assert_called_once()
            gold_stream.read_from_silver.assert_called_once()
            gold_stream.aggregate.assert_called_once_with(mock_silver_df)
            gold_stream.write_to_delta.assert_called_once_with(mock_aggregated_df)
            assert result == mock_query

    def test_start_failure(self, gold_stream):
        """Test Gold stream start failure handling."""
        with patch.object(
            gold_stream, "initialize_delta_table", side_effect=Exception("Test error")
        ):
            with pytest.raises(Exception, match="Test error"):
                gold_stream.start()

    def test_stop(self, gold_stream):
        """Test stopping streaming query."""
        mock_query = Mock()
        mock_query.id = "test-query-id"

        gold_stream.stop(mock_query)

        mock_query.stop.assert_called_once()

    def test_stop_with_error(self, gold_stream):
        """Test stopping streaming query with error."""
        mock_query = Mock()
        mock_query.id = "test-query-id"
        mock_query.stop.side_effect = Exception("Stop error")

        with pytest.raises(Exception, match="Stop error"):
            gold_stream.stop(mock_query)


def test_create_gold_stream():
    """Test factory function."""
    mock_spark = Mock()

    with patch(
        "src.streams.gold_stream.get_delta_path",
        side_effect=lambda layer: f"/data/delta/{layer}",
    ), patch(
        "src.streams.gold_stream.get_checkpoint_path",
        side_effect=lambda layer: f"/data/checkpoints/{layer}",
    ), patch(
        "src.streams.gold_stream.get_trigger_interval",
        return_value="30 seconds",
    ):
        gold = create_gold_stream(mock_spark)

        assert isinstance(gold, GoldStream)
        assert gold.spark == mock_spark


def test_create_gold_stream_with_kwargs():
    """Test factory function with custom kwargs."""
    mock_spark = Mock()

    gold = create_gold_stream(
        mock_spark,
        silver_path="/custom/silver",
        gold_path="/custom/gold",
        optimize_every_n_batches=50,
    )

    assert isinstance(gold, GoldStream)
    assert gold.silver_path == "/custom/silver"
    assert gold.gold_path == "/custom/gold"
    assert gold.optimize_every_n_batches == 50
