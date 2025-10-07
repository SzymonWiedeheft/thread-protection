"""Bronze stream: Kafka � Bronze Delta Lake.

This module implements the Bronze layer of the Medallion architecture.
It reads raw domain data from Kafka topics and writes it to Delta Lake
with minimal transformation, preserving audit trail and lineage.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    from_json,
    col,
    current_timestamp,
    to_date,
    to_timestamp,
    lit,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    MapType,
)
import structlog

from schemas import delta_schemas
from ..utils.kafka_utils import get_kafka_options, extract_kafka_metadata_expr
from ..utils.delta_utils import create_delta_table_if_not_exists, write_stream_to_delta
from ..config.spark_config import (
    get_kafka_bootstrap_servers,
    get_delta_path,
    get_checkpoint_path,
    get_kafka_topics,
    get_trigger_interval,
)

logger = structlog.get_logger()


# Kafka message value schema (matches KafkaMessageSchema)
KAFKA_VALUE_SCHEMA = StructType(
    [
        StructField("domain", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_format", StringType(), nullable=False),
        StructField("raw_entry", StringType(), nullable=False),
        StructField("ingestion_timestamp", StringType(), nullable=False),
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
    ]
)


class BronzeStream:
    """
    Bronze stream processor: Kafka � Bronze Delta Lake.

    Reads domain data from Kafka topics and writes to Bronze Delta table
    with minimal transformation. Adds Kafka metadata for lineage tracking.

    Attributes:
        spark: SparkSession
        bootstrap_servers: Kafka bootstrap servers
        topics: List of Kafka topics to read
        delta_path: Path to Bronze Delta table
        checkpoint_path: Path to checkpoint directory
        trigger_interval: Micro-batch trigger interval
    """

    def __init__(
        self,
        spark: SparkSession,
        bootstrap_servers: Optional[str] = None,
        topics: Optional[list] = None,
        delta_path: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        trigger_interval: Optional[str] = None,
        starting_offsets: Optional[str] = None,
    ):
        """
        Initialize Bronze stream processor.

        Args:
            spark: SparkSession
            bootstrap_servers: Kafka bootstrap servers (default: from config)
            topics: Kafka topics to read (default: from config)
            delta_path: Bronze Delta table path (default: from config)
            checkpoint_path: Checkpoint path (default: from config)
            trigger_interval: Trigger interval (default: from config)
            starting_offsets: Kafka starting offsets - 'earliest' or 'latest' (default: 'latest')
        """
        self.spark = spark
        self.bootstrap_servers = bootstrap_servers or get_kafka_bootstrap_servers()
        self.topics = topics or get_kafka_topics()
        self.delta_path = delta_path or get_delta_path("bronze")
        self.checkpoint_path = checkpoint_path or get_checkpoint_path("bronze")
        self.trigger_interval = trigger_interval or get_trigger_interval("bronze")
        self.starting_offsets = starting_offsets or "latest"

        logger.info(
            "Initializing Bronze stream",
            bootstrap_servers=self.bootstrap_servers,
            topics=self.topics,
            delta_path=self.delta_path,
            checkpoint_path=self.checkpoint_path,
            trigger_interval=self.trigger_interval,
        )

    def read_from_kafka(self) -> DataFrame:
        """
        Read streaming data from Kafka.

        Returns:
            Streaming DataFrame with raw Kafka messages
        """
        logger.info("Starting Kafka read stream", topics=self.topics, starting_offsets=self.starting_offsets)

        kafka_options = get_kafka_options(
            bootstrap_servers=self.bootstrap_servers,
            topics=self.topics,
            starting_offsets=self.starting_offsets,
            fail_on_data_loss=False,  # Handle topic recreation gracefully
        )

        df = self.spark.readStream.format("kafka").options(**kafka_options).load()

        logger.info("Kafka stream initialized", topics=self.topics)

        return df

    def transform(self, kafka_df: DataFrame) -> DataFrame:
        """
        Transform Kafka messages to Bronze schema.

        Transformations:
        1. Parse JSON value to structured columns
        2. Extract Kafka metadata (topic, partition, offset, timestamp)
        3. Add Bronze processing timestamp
        4. Add partition key (ingestion_date)
        5. Cast timestamp strings to timestamp type

        Args:
            kafka_df: Raw Kafka DataFrame

        Returns:
            Transformed DataFrame matching Bronze schema
        """
        logger.info("Transforming Kafka messages to Bronze schema")

        # Parse JSON value column
        parsed_df = kafka_df.select(
            # Parse the value (JSON) column
            from_json(col("value").cast("string"), KAFKA_VALUE_SCHEMA).alias("data"),
            # Extract Kafka metadata
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )

        # Flatten the parsed data structure
        flattened_df = parsed_df.select(
            # Domain data fields
            col("data.domain"),
            col("data.category"),
            col("data.source"),
            col("data.source_format"),
            col("data.raw_entry"),
            to_timestamp(col("data.ingestion_timestamp")).alias("ingestion_timestamp"),
            col("data.metadata"),
            # Kafka metadata (cast to match schema)
            col("kafka_topic"),
            col("kafka_partition").cast("long").alias("kafka_partition"),
            col("kafka_offset").cast("long").alias("kafka_offset"),
            col("kafka_timestamp"),
        )

        # Add Bronze-specific columns
        bronze_df = flattened_df.withColumn(
            "bronze_timestamp", current_timestamp()
        ).withColumn("ingestion_date", to_date(col("ingestion_timestamp")))

        logger.info("Transformation complete", schema=bronze_df.schema.simpleString())

        return bronze_df

    def initialize_delta_table(self) -> None:
        """Initialize Bronze Delta table if it doesn't exist."""
        logger.info("Initializing Bronze Delta table", delta_path=self.delta_path)

        create_delta_table_if_not_exists(
            self.spark,
            self.delta_path,
            delta_schemas.BRONZE_SCHEMA,
            partition_columns=["ingestion_date"],
            table_properties={
                "delta.enableChangeDataFeed": "true",  # Enable CDC for downstream
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
            },
        )

        logger.info("Bronze Delta table initialized")

    def write_to_delta(self, bronze_df: DataFrame):
        """
        Write streaming DataFrame to Bronze Delta table.
        Table will be auto-created on first write with specified properties.

        Args:
            bronze_df: Transformed DataFrame matching Bronze schema

        Returns:
            StreamingQuery object
        """
        logger.info(
            "Writing to Bronze Delta table",
            delta_path=self.delta_path,
            checkpoint=self.checkpoint_path,
        )

        # Configure write with table properties
        writer = (
            bronze_df.writeStream.format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_path)
            .partitionBy("ingestion_date")
            .queryName("bronze_stream")
            # Delta table properties (applied on table creation)
            .option("delta.enableChangeDataFeed", "true")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .option("delta.autoOptimize.autoCompact", "true")
        )

        # Parse trigger interval
        if self.trigger_interval:
            if "second" in self.trigger_interval.lower():
                import re
                seconds = int(re.findall(r"\d+", self.trigger_interval)[0])
                writer = writer.trigger(processingTime=f"{seconds} seconds")
            elif "minute" in self.trigger_interval.lower():
                import re
                minutes = int(re.findall(r"\d+", self.trigger_interval)[0])
                writer = writer.trigger(processingTime=f"{minutes} minutes")

        query = writer.start(self.delta_path)

        logger.info(
            "Bronze stream query started",
            query_id=query.id,
            query_name=query.name,
        )

        return query

    def start(self):
        """
        Start the Bronze streaming pipeline.

        Pipeline flow:
        1. Read from Kafka
        2. Transform to Bronze schema
        3. Write to Delta Lake (table created on first write)
        4. Return streaming query

        Returns:
            StreamingQuery object

        Examples:
            >>> bronze = BronzeStream(spark)
            >>> query = bronze.start()
            >>> query.awaitTermination()
        """
        logger.info("Starting Bronze streaming pipeline")

        try:
            # Step 1: Read from Kafka
            kafka_df = self.read_from_kafka()

            # Step 2: Transform
            bronze_df = self.transform(kafka_df)

            # Step 3: Write to Delta (table will be created on first write)
            query = self.write_to_delta(bronze_df)

            logger.info("Bronze streaming pipeline started successfully")

            return query

        except Exception as e:
            logger.error(
                "Failed to start Bronze streaming pipeline",
                error=str(e),
                error_type=type(e).__name__,
            )
            raise

    def stop(self, query) -> None:
        """
        Stop the streaming query gracefully.

        Args:
            query: StreamingQuery to stop
        """
        logger.info("Stopping Bronze stream", query_id=query.id)

        try:
            query.stop()
            logger.info("Bronze stream stopped successfully")
        except Exception as e:
            logger.error("Error stopping Bronze stream", error=str(e))
            raise


def create_bronze_stream(
    spark: SparkSession,
    **kwargs,
) -> BronzeStream:
    """
    Factory function to create BronzeStream instance.

    Args:
        spark: SparkSession
        **kwargs: Additional arguments for BronzeStream

    Returns:
        BronzeStream instance

    Examples:
        >>> spark = create_spark_session()
        >>> bronze = create_bronze_stream(spark)
        >>> query = bronze.start()
    """
    return BronzeStream(spark, **kwargs)
