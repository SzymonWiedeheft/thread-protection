"""Unit tests for Spark configuration."""

import pytest
import os
from unittest.mock import Mock, patch

from src.config.spark_config import (
    create_spark_session,
    get_kafka_bootstrap_servers,
    get_delta_path,
    get_checkpoint_path,
    get_kafka_topics,
    get_trigger_interval,
    KAFKA_CONF,
    DELTA_PATHS,
    CHECKPOINT_PATHS,
    KAFKA_TOPICS,
    TRIGGER_INTERVALS,
)


class TestSparkConfig:
    """Test cases for Spark configuration functions."""

    def test_get_kafka_bootstrap_servers_default(self):
        """Test getting default Kafka bootstrap servers."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_kafka_bootstrap_servers()
            assert result == KAFKA_CONF["kafka.bootstrap.servers"]

    def test_get_kafka_bootstrap_servers_from_env(self):
        """Test getting Kafka bootstrap servers from environment."""
        with patch.dict(os.environ, {"KAFKA_BOOTSTRAP_SERVERS": "custom:9092"}):
            result = get_kafka_bootstrap_servers()
            assert result == "custom:9092"

    def test_get_delta_path_bronze(self):
        """Test getting Bronze Delta path."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_delta_path("bronze")
            assert result == DELTA_PATHS["bronze"]

    def test_get_delta_path_silver(self):
        """Test getting Silver Delta path."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_delta_path("silver")
            assert result == DELTA_PATHS["silver"]

    def test_get_delta_path_gold(self):
        """Test getting Gold Delta path."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_delta_path("gold")
            assert result == DELTA_PATHS["gold"]

    def test_get_delta_path_from_env(self):
        """Test getting Delta path from environment."""
        with patch.dict(os.environ, {"DELTA_BRONZE_PATH": "/custom/bronze"}):
            result = get_delta_path("bronze")
            assert result == "/custom/bronze"

    def test_get_delta_path_invalid_layer(self):
        """Test getting Delta path with invalid layer."""
        with pytest.raises(ValueError, match="Invalid layer"):
            get_delta_path("invalid")

    def test_get_delta_path_case_insensitive(self):
        """Test Delta path is case-insensitive."""
        with patch.dict(os.environ, {}, clear=True):
            result1 = get_delta_path("BRONZE")
            result2 = get_delta_path("Bronze")
            result3 = get_delta_path("bronze")
            assert result1 == result2 == result3

    def test_get_checkpoint_path_bronze(self):
        """Test getting Bronze checkpoint path."""
        with patch.dict(os.environ, {}, clear=True):
            result = get_checkpoint_path("bronze")
            assert result == CHECKPOINT_PATHS["bronze"]

    def test_get_checkpoint_path_from_env(self):
        """Test getting checkpoint path from environment."""
        with patch.dict(os.environ, {"CHECKPOINT_SILVER_PATH": "/custom/checkpoint"}):
            result = get_checkpoint_path("silver")
            assert result == "/custom/checkpoint"

    def test_get_checkpoint_path_invalid_layer(self):
        """Test getting checkpoint path with invalid layer."""
        with pytest.raises(ValueError, match="Invalid layer"):
            get_checkpoint_path("invalid")

    def test_get_kafka_topics(self):
        """Test getting Kafka topics."""
        result = get_kafka_topics()
        expected = list(KAFKA_TOPICS.values())
        assert result == expected
        assert "malware_domains" in result
        assert "ads_trackers_domains" in result

    def test_get_trigger_interval_bronze(self):
        """Test getting Bronze trigger interval."""
        result = get_trigger_interval("bronze")
        assert result == TRIGGER_INTERVALS["bronze"]
        assert result == "5 seconds"

    def test_get_trigger_interval_silver(self):
        """Test getting Silver trigger interval."""
        result = get_trigger_interval("silver")
        assert result == TRIGGER_INTERVALS["silver"]
        assert result == "10 seconds"

    def test_get_trigger_interval_gold(self):
        """Test getting Gold trigger interval."""
        result = get_trigger_interval("gold")
        assert result == TRIGGER_INTERVALS["gold"]
        assert result == "30 seconds"

    def test_get_trigger_interval_invalid_layer(self):
        """Test getting trigger interval with invalid layer."""
        with pytest.raises(ValueError, match="Invalid layer"):
            get_trigger_interval("invalid")

    @patch("src.config.spark_config.SparkSession")
    def test_create_spark_session_default(self, mock_spark_session_class):
        """Test creating Spark session with defaults."""
        mock_builder = Mock()
        mock_spark = Mock()
        mock_spark.version = "3.5.0"
        mock_spark.sparkContext.master = "local[*]"

        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        result = create_spark_session()

        # Verify builder chain
        mock_builder.appName.assert_called_once_with("DomainStreamingPipeline")
        mock_builder.getOrCreate.assert_called_once()
        mock_spark.sparkContext.setLogLevel.assert_called_once_with("WARN")
        assert result == mock_spark

    @patch("src.config.spark_config.SparkSession")
    def test_create_spark_session_with_custom_params(self, mock_spark_session_class):
        """Test creating Spark session with custom parameters."""
        mock_builder = Mock()
        mock_spark = Mock()
        mock_spark.version = "3.5.0"
        mock_spark.sparkContext.master = "local[*]"

        mock_spark_session_class.builder = mock_builder
        mock_builder.appName.return_value = mock_builder
        mock_builder.master.return_value = mock_builder
        mock_builder.config.return_value = mock_builder
        mock_builder.getOrCreate.return_value = mock_spark

        custom_conf = {"spark.executor.cores": "4"}
        result = create_spark_session(
            app_name="CustomApp",
            master="local[4]",
            additional_conf=custom_conf,
        )

        # Verify custom parameters
        mock_builder.appName.assert_called_once_with("CustomApp")
        mock_builder.master.assert_called_once_with("local[4]")
        assert result == mock_spark


class TestConstants:
    """Test configuration constants."""

    def test_kafka_conf_structure(self):
        """Test KAFKA_CONF structure."""
        assert "kafka.bootstrap.servers" in KAFKA_CONF
        assert "kafka.session.timeout.ms" in KAFKA_CONF
        assert "kafka.request.timeout.ms" in KAFKA_CONF

    def test_delta_paths_structure(self):
        """Test DELTA_PATHS structure."""
        assert "bronze" in DELTA_PATHS
        assert "silver" in DELTA_PATHS
        assert "gold" in DELTA_PATHS

    def test_checkpoint_paths_structure(self):
        """Test CHECKPOINT_PATHS structure."""
        assert "bronze" in CHECKPOINT_PATHS
        assert "silver" in CHECKPOINT_PATHS
        assert "gold" in CHECKPOINT_PATHS

    def test_kafka_topics_structure(self):
        """Test KAFKA_TOPICS structure."""
        assert "malware" in KAFKA_TOPICS
        assert "ads_trackers" in KAFKA_TOPICS
        assert KAFKA_TOPICS["malware"] == "malware_domains"
        assert KAFKA_TOPICS["ads_trackers"] == "ads_trackers_domains"

    def test_trigger_intervals_structure(self):
        """Test TRIGGER_INTERVALS structure."""
        assert "bronze" in TRIGGER_INTERVALS
        assert "silver" in TRIGGER_INTERVALS
        assert "gold" in TRIGGER_INTERVALS
        assert "second" in TRIGGER_INTERVALS["bronze"].lower()
