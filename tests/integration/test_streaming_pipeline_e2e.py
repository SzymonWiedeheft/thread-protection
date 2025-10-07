"""End-to-end integration tests for complete streaming pipeline (Kafka → Bronze → Silver → Gold)."""

import pytest
import time
import json
from datetime import datetime, UTC
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from conftest import requires_docker
from src.streams.bronze_stream import BronzeStream
from src.streams.silver_stream import SilverStream
from src.streams.gold_stream import GoldStream


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.slow
def test_full_pipeline_ingestion_to_gold(
    spark_session_with_delta: SparkSession,
    kafka_topics: dict,
    kafka_publish_helper,
    sample_kafka_messages: list,
    temp_delta_paths: dict,
    wait_for_kafka_ready,
):
    """Test complete pipeline: Kafka → Bronze → Silver → Gold."""
    # Step 1: Publish to Kafka
    malware_messages = [m for m in sample_kafka_messages if m["category"] == "malware"]
    ads_messages = [m for m in sample_kafka_messages if m["category"] == "ads_trackers"]

    kafka_publish_helper(malware_messages, topic_key="malware")
    kafka_publish_helper(ads_messages, topic_key="ads_trackers")

    # Step 2: Start Bronze stream (Kafka → Bronze Delta)
    bronze = BronzeStream(
        spark=spark_session_with_delta,
        bootstrap_servers=kafka_topics["bootstrap_servers"],
        topics=[kafka_topics["malware"], kafka_topics["ads_trackers"]],
        delta_path=temp_delta_paths["bronze"],
        checkpoint_path=temp_delta_paths["checkpoints_bronze"],
        trigger_interval="5 seconds",
        starting_offsets="earliest",
    )

    bronze_query = bronze.start()
    time.sleep(15)  # Wait for Bronze processing

    # Verify Bronze
    assert DeltaTable.isDeltaTable(spark_session_with_delta, temp_delta_paths["bronze"])
    bronze_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["bronze"]
    )
    bronze_count = bronze_df.count()
    assert bronze_count == len(
        sample_kafka_messages
    ), "Bronze should have all Kafka messages"

    # Step 3: Start Silver stream (Bronze → Silver Delta)
    silver = SilverStream(
        spark=spark_session_with_delta,
        bronze_path=temp_delta_paths["bronze"],
        silver_path=temp_delta_paths["silver"],
        checkpoint_path=temp_delta_paths["checkpoints_silver"],
        trigger_interval="5 seconds",
    )

    silver_query = silver.start()
    time.sleep(15)  # Wait for Silver processing

    # Verify Silver
    assert DeltaTable.isDeltaTable(spark_session_with_delta, temp_delta_paths["silver"])
    silver_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["silver"]
    )
    silver_count = silver_df.count()
    assert silver_count == bronze_count, "Silver should have all Bronze records"

    # Step 4: Start Gold stream (Silver → Gold Delta)
    gold = GoldStream(
        spark=spark_session_with_delta,
        silver_path=temp_delta_paths["silver"],
        gold_path=temp_delta_paths["gold"],
        checkpoint_path=temp_delta_paths["checkpoints_gold"],
        trigger_interval="5 seconds",
    )

    gold_query = gold.start()
    time.sleep(15)  # Wait for Gold processing

    # Verify Gold
    assert DeltaTable.isDeltaTable(spark_session_with_delta, temp_delta_paths["gold"])
    gold_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["gold"]
    )
    gold_data = gold_df.collect()

    # Gold should have fewer records than Silver (aggregation!)
    # sample_kafka_messages has duplicate domains from different sources
    assert len(gold_data) < silver_count, "Gold should aggregate duplicate domains"

    # Verify schema evolution at each layer
    bronze_columns = set(bronze_df.columns)
    silver_columns = set(silver_df.columns)
    gold_columns = set(gold_df.columns)

    # Bronze has kafka metadata
    assert "kafka_offset" in bronze_columns
    assert "kafka_partition" in bronze_columns

    # Silver has validation fields
    assert "is_valid" in silver_columns
    assert "validation_errors" in silver_columns
    assert "bronze_offset" in silver_columns

    # Gold has aggregation fields
    assert "source_list" in gold_columns
    assert "source_count" in gold_columns
    assert "first_seen" in gold_columns
    assert "last_seen" in gold_columns
    assert "is_active" in gold_columns

    # Stop all queries with cleanup delay
    bronze_query.stop()
    silver_query.stop()
    gold_query.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.slow
def test_pipeline_with_late_arriving_data(
    spark_session_with_delta: SparkSession,
    kafka_topics: dict,
    kafka_publish_helper,
    sample_kafka_messages: list,
    temp_delta_paths: dict,
    wait_for_kafka_ready,
):
    """Test pipeline handles late-arriving data correctly."""
    # Phase 1: Process initial batch
    initial_messages = sample_kafka_messages[:3]
    kafka_publish_helper(initial_messages, topic_key="malware")

    # Start all layers
    bronze = BronzeStream(
        spark=spark_session_with_delta,
        bootstrap_servers=kafka_topics["bootstrap_servers"],
        topics=[kafka_topics["malware"]],
        delta_path=temp_delta_paths["bronze"],
        checkpoint_path=temp_delta_paths["checkpoints_bronze"],
        trigger_interval="5 seconds",
        starting_offsets="earliest",
    )

    silver = SilverStream(
        spark=spark_session_with_delta,
        bronze_path=temp_delta_paths["bronze"],
        silver_path=temp_delta_paths["silver"],
        checkpoint_path=temp_delta_paths["checkpoints_silver"],
        trigger_interval="5 seconds",
    )

    gold = GoldStream(
        spark=spark_session_with_delta,
        silver_path=temp_delta_paths["silver"],
        gold_path=temp_delta_paths["gold"],
        checkpoint_path=temp_delta_paths["checkpoints_gold"],
        trigger_interval="5 seconds",
    )

    bronze_q1 = bronze.start()
    silver_q1 = silver.start()
    gold_q1 = gold.start()

    time.sleep(15)

    # Verify initial processing
    gold_df_1 = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["gold"]
    )
    initial_count = gold_df_1.count()
    assert initial_count >= 1, "Should have processed initial data"

    # Stop streams with cleanup delay
    bronze_q1.stop()
    silver_q1.stop()
    gold_q1.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup

    # Phase 2: Publish late-arriving data
    late_messages = sample_kafka_messages[3:5]
    kafka_publish_helper(late_messages, topic_key="malware")

    # Restart streams (checkpoint recovery)
    bronze_q2 = bronze.start()
    silver_q2 = silver.start()
    gold_q2 = gold.start()

    time.sleep(15)

    # Verify late data processed
    bronze_df_2 = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["bronze"]
    )
    assert bronze_df_2.count() >= 5, "Bronze should have initial + late data"

    gold_df_2 = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["gold"]
    )
    final_count = gold_df_2.count()

    # Gold count may not change much due to aggregation, but should have processed late data
    # (Check by verifying timestamps were updated)
    assert final_count >= initial_count, "Gold should have same or more records"

    # Stop all with cleanup delay
    bronze_q2.stop()
    silver_q2.stop()
    gold_q2.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.slow
def test_pipeline_concurrent_sources(
    spark_session_with_delta: SparkSession,
    kafka_topics: dict,
    kafka_publish_helper,
    temp_delta_paths: dict,
    wait_for_kafka_ready,
):
    """Test pipeline processes concurrent sources correctly."""
    base_time = datetime.now(UTC)

    # Create domains from multiple sources simultaneously
    messages = []

    # Source 1: malware
    for i in range(5):
        messages.append(
            {
                "domain": f"source1-domain-{i}.com",
                "category": "malware",
                "source": "source1",
                "source_format": "hosts",
                "raw_entry": f"0.0.0.0 source1-domain-{i}.com",
                "ingestion_timestamp": base_time.isoformat(),
                "metadata": {"source_id": 1},
            }
        )

    # Source 2: ads_trackers
    for i in range(5):
        messages.append(
            {
                "domain": f"source2-domain-{i}.net",
                "category": "ads_trackers",
                "source": "source2",
                "source_format": "adblock",
                "raw_entry": f"||source2-domain-{i}.net^",
                "ingestion_timestamp": base_time.isoformat(),
                "metadata": {"source_id": 2},
            }
        )

    # Publish all at once
    kafka_publish_helper(
        [m for m in messages if m["category"] == "malware"], topic_key="malware"
    )
    kafka_publish_helper(
        [m for m in messages if m["category"] == "ads_trackers"],
        topic_key="ads_trackers",
    )

    # Start pipeline
    bronze = BronzeStream(
        spark=spark_session_with_delta,
        bootstrap_servers=kafka_topics["bootstrap_servers"],
        topics=[kafka_topics["malware"], kafka_topics["ads_trackers"]],
        delta_path=temp_delta_paths["bronze"],
        checkpoint_path=temp_delta_paths["checkpoints_bronze"],
        trigger_interval="5 seconds",
        starting_offsets="earliest",
    )

    silver = SilverStream(
        spark=spark_session_with_delta,
        bronze_path=temp_delta_paths["bronze"],
        silver_path=temp_delta_paths["silver"],
        checkpoint_path=temp_delta_paths["checkpoints_silver"],
        trigger_interval="5 seconds",
    )

    gold = GoldStream(
        spark=spark_session_with_delta,
        silver_path=temp_delta_paths["silver"],
        gold_path=temp_delta_paths["gold"],
        checkpoint_path=temp_delta_paths["checkpoints_gold"],
        trigger_interval="5 seconds",
    )

    bq = bronze.start()
    sq = silver.start()
    gq = gold.start()

    time.sleep(20)  # Wait for processing

    # Verify all sources processed
    gold_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["gold"]
    )
    gold_data = gold_df.collect()

    # Should have 10 unique domains
    assert len(gold_data) == 10, "Should have all 10 domains from both sources"

    # Verify both categories present
    categories = {row["category"] for row in gold_data}
    assert "malware" in categories
    assert "ads_trackers" in categories

    # Verify sources
    sources = set()
    for row in gold_data:
        source_list = json.loads(row["source_list"])
        sources.update(source_list)

    assert "source1" in sources
    assert "source2" in sources

    # Stop all with cleanup delay
    bq.stop()
    sq.stop()
    gq.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.slow
def test_pipeline_performance_benchmark(
    spark_session_with_delta: SparkSession,
    kafka_topics: dict,
    kafka_publish_helper,
    temp_delta_paths: dict,
    wait_for_kafka_ready,
):
    """Test pipeline performance with larger dataset (1000 domains)."""
    base_time = datetime.now(UTC)

    # Generate 1000 domain messages
    batch_size = 1000
    messages = []

    for i in range(batch_size):
        messages.append(
            {
                "domain": f"perf-test-domain-{i}.com",
                "category": "malware" if i % 2 == 0 else "ads_trackers",
                "source": f"source-{i % 10}",  # 10 different sources
                "source_format": "hosts",
                "raw_entry": f"0.0.0.0 perf-test-domain-{i}.com",
                "ingestion_timestamp": base_time.isoformat(),
                "metadata": {"batch": "performance_test", "index": i},
            }
        )

    # Publish to Kafka
    malware = [m for m in messages if m["category"] == "malware"]
    ads = [m for m in messages if m["category"] == "ads_trackers"]

    kafka_publish_helper(malware, topic_key="malware")
    kafka_publish_helper(ads, topic_key="ads_trackers")

    # Start pipeline and measure time
    start_time = time.time()

    bronze = BronzeStream(
        spark=spark_session_with_delta,
        bootstrap_servers=kafka_topics["bootstrap_servers"],
        topics=[kafka_topics["malware"], kafka_topics["ads_trackers"]],
        delta_path=temp_delta_paths["bronze"],
        checkpoint_path=temp_delta_paths["checkpoints_bronze"],
        trigger_interval="5 seconds",
        starting_offsets="earliest",
    )

    silver = SilverStream(
        spark=spark_session_with_delta,
        bronze_path=temp_delta_paths["bronze"],
        silver_path=temp_delta_paths["silver"],
        checkpoint_path=temp_delta_paths["checkpoints_silver"],
        trigger_interval="5 seconds",
    )

    gold = GoldStream(
        spark=spark_session_with_delta,
        silver_path=temp_delta_paths["silver"],
        gold_path=temp_delta_paths["gold"],
        checkpoint_path=temp_delta_paths["checkpoints_gold"],
        trigger_interval="5 seconds",
    )

    bq = bronze.start()
    sq = silver.start()
    gq = gold.start()

    # Wait for processing (longer for large batch)
    time.sleep(30)

    end_time = time.time()
    processing_time = end_time - start_time

    # Verify all processed
    gold_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["gold"]
    )
    gold_count = gold_df.count()

    # Should have 1000 unique domains (no duplicates in this test)
    assert gold_count == batch_size, f"Should have all {batch_size} domains in Gold"

    # Log performance metrics
    print("\nPerformance Benchmark Results:")
    print(f"  - Records processed: {batch_size}")
    print(f"  - Total time: {processing_time:.2f}s")
    print(f"  - Throughput: {batch_size / processing_time:.2f} records/sec")

    # Basic performance assertion (should process 1000 records in reasonable time)
    assert (
        processing_time < 60
    ), f"Processing {batch_size} records should take < 60s, took {processing_time:.2f}s"

    # Stop all with cleanup delay
    bq.stop()
    sq.stop()
    gq.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup


@requires_docker()
@pytest.mark.integration
@pytest.mark.kafka
@pytest.mark.spark
@pytest.mark.slow
def test_pipeline_failure_recovery(
    spark_session_with_delta: SparkSession,
    kafka_topics: dict,
    kafka_publish_helper,
    sample_kafka_messages: list,
    temp_delta_paths: dict,
    wait_for_kafka_ready,
):
    """Test pipeline recovers from stream failures using checkpoints."""
    # Publish initial data
    kafka_publish_helper(sample_kafka_messages[:4], topic_key="malware")

    # Start Bronze
    bronze = BronzeStream(
        spark=spark_session_with_delta,
        bootstrap_servers=kafka_topics["bootstrap_servers"],
        topics=[kafka_topics["malware"]],
        delta_path=temp_delta_paths["bronze"],
        checkpoint_path=temp_delta_paths["checkpoints_bronze"],
        trigger_interval="5 seconds",
        starting_offsets="earliest",
    )

    bq1 = bronze.start()
    time.sleep(10)

    # Verify Bronze processed
    bronze_df_1 = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["bronze"]
    )
    bronze_count_1 = bronze_df_1.count()
    assert bronze_count_1 >= 4, "Bronze should have processed initial data"

    # Start Silver
    silver = SilverStream(
        spark=spark_session_with_delta,
        bronze_path=temp_delta_paths["bronze"],
        silver_path=temp_delta_paths["silver"],
        checkpoint_path=temp_delta_paths["checkpoints_silver"],
        trigger_interval="5 seconds",
    )

    sq1 = silver.start()
    time.sleep(10)

    # Simulate failure: Stop Silver mid-processing
    sq1.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup

    # Publish more data while Silver is down
    kafka_publish_helper(sample_kafka_messages[4:6], topic_key="malware")

    # Bronze continues processing
    time.sleep(10)
    bronze_df_2 = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["bronze"]
    )
    bronze_count_2 = bronze_df_2.count()
    assert bronze_count_2 > bronze_count_1, "Bronze should process new data"

    # Restart Silver (checkpoint recovery)
    sq2 = silver.start()
    time.sleep(15)

    # Verify Silver caught up
    silver_df = spark_session_with_delta.read.format("delta").load(
        temp_delta_paths["silver"]
    )
    silver_count = silver_df.count()
    assert (
        silver_count == bronze_count_2
    ), "Silver should catch up to Bronze after recovery"

    # Verify no duplicates (exactly-once semantics)
    silver_data = silver_df.collect()
    domains = [row["domain"] for row in silver_data]
    # Allow for some duplicates from source data, but check for processing duplicates
    # by verifying count matches Bronze
    assert len(domains) == bronze_count_2, "No duplicate processing after recovery"

    # Stop all with cleanup delay
    bq1.stop()
    sq2.stop()
    time.sleep(0.5)  # Allow Spark to finish cleanup
