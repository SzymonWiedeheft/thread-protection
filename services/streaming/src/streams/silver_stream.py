"""Silver stream: Bronze Delta -> Silver Delta Lake.

This module implements the Silver layer of the Medallion architecture.
It reads from Bronze Delta table, applies data quality checks and transformations,
and writes to Silver Delta Lake.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when,
    udf,
)
from pyspark.sql.types import StringType, BooleanType
import structlog

from schemas import delta_schemas
from schemas.validation_rules import is_valid_domain, clean_domain
from ..utils.delta_utils import (
    create_delta_table_if_not_exists,
    register_delta_table,
    write_stream_to_delta,
)
from ..config.spark_config import (
    get_delta_path,
    get_checkpoint_path,
    get_trigger_interval,
)

logger = structlog.get_logger()


# UDFs for domain validation and cleaning
@udf(returnType=StringType())
def clean_domain_udf(domain: str) -> str:
    """UDF to clean domain names."""
    if domain is None:
        return None
    return clean_domain(domain)


@udf(returnType=BooleanType())
def is_valid_domain_udf(domain: str) -> bool:
    """UDF to validate domain names."""
    if domain is None:
        return False
    return is_valid_domain(domain)


class SilverStream:
    """
    Silver stream processor: Bronze Delta � Silver Delta Lake.

    Reads from Bronze Delta table, validates and cleans domains,
    and writes to Silver Delta table with quality flags.

    Attributes:
        spark: SparkSession
        bronze_path: Path to Bronze Delta table (source)
        silver_path: Path to Silver Delta table (destination)
        checkpoint_path: Path to checkpoint directory
        trigger_interval: Micro-batch trigger interval
    """

    def __init__(
        self,
        spark: SparkSession,
        bronze_path: Optional[str] = None,
        silver_path: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        trigger_interval: Optional[str] = None,
    ):
        """
        Initialize Silver stream processor.

        Args:
            spark: SparkSession
            bronze_path: Bronze Delta table path (default: from config)
            silver_path: Silver Delta table path (default: from config)
            checkpoint_path: Checkpoint path (default: from config)
            trigger_interval: Trigger interval (default: from config)
        """
        self.spark = spark
        self.bronze_path = bronze_path or get_delta_path("bronze")
        self.silver_path = silver_path or get_delta_path("silver")
        self.checkpoint_path = checkpoint_path or get_checkpoint_path("silver")
        self.trigger_interval = trigger_interval or get_trigger_interval("silver")

        logger.info(
            "Initializing Silver stream",
            bronze_path=self.bronze_path,
            silver_path=self.silver_path,
            checkpoint_path=self.checkpoint_path,
            trigger_interval=self.trigger_interval,
        )

    def read_from_bronze(self) -> DataFrame:
        """
        Read streaming data from Bronze Delta table.

        Returns:
            Streaming DataFrame with Bronze data
        """
        logger.info("Starting Bronze Delta read stream", bronze_path=self.bronze_path)

        # Read from Bronze Delta table as stream
        df = (
            self.spark.readStream.format("delta")
            .option("ignoreChanges", "true")  # Handle Delta updates gracefully
            .option("ignoreDeletes", "true")  # Handle Delta deletes gracefully
            .load(self.bronze_path)
        )

        logger.info("Bronze Delta stream initialized", bronze_path=self.bronze_path)

        return df

    def transform(self, bronze_df: DataFrame) -> DataFrame:
        """
        Transform Bronze data to Silver schema with validation.

        Transformations:
        1. Clean domain names (lowercase, trim)
        2. Validate domain format
        3. Add validation flags (is_valid, validation_errors)
        4. Add Silver processing timestamp
        5. Map kafka_offset to bronze_offset for lineage
        6. Select fields matching Silver schema

        Args:
            bronze_df: Bronze DataFrame

        Returns:
            Transformed DataFrame matching Silver schema
        """
        logger.info("Transforming Bronze data to Silver schema")

        # Step 1: Clean domains
        cleaned_df = bronze_df.withColumn(
            "cleaned_domain", clean_domain_udf(col("domain"))
        )

        # Step 2: Validate domains
        validated_df = cleaned_df.withColumn(
            "is_valid_bool", is_valid_domain_udf(col("cleaned_domain"))
        )

        # Step 3: Add validation flags and errors
        flagged_df = validated_df.withColumn(
            "is_valid",
            when(col("is_valid_bool"), "true").otherwise("false"),
        ).withColumn(
            "validation_errors",
            when(~col("is_valid_bool"), "Invalid domain format").otherwise(None),
        )

        # Step 4: Build Silver DataFrame with correct schema
        silver_df = flagged_df.select(
            # Use cleaned domain
            col("cleaned_domain").alias("domain"),
            col("category"),
            col("source"),
            col("source_format"),
            col("raw_entry"),
            # Timestamps
            col("ingestion_timestamp"),
            col("bronze_timestamp"),
            current_timestamp().alias("silver_timestamp"),
            # Metadata
            col("metadata"),
            # Validation flags
            col("is_valid"),
            col("validation_errors"),
            # Partitioning
            col("ingestion_date"),
            # Lineage
            col("kafka_offset").alias("bronze_offset"),
        )

        logger.info(
            "Transformation complete",
            schema=silver_df.schema.simpleString(),
        )

        return silver_df

    def initialize_delta_table(self) -> None:
        """Initialize Silver Delta table if it doesn't exist."""
        logger.info("Initializing Silver Delta table", delta_path=self.silver_path)

        table_properties = {
            "delta.enableChangeDataFeed": "true",  # Enable CDC for downstream
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        }

        create_delta_table_if_not_exists(
            self.spark,
            self.silver_path,
            delta_schemas.SILVER_SCHEMA,
            partition_columns=["ingestion_date"],
            table_properties=table_properties,
        )

        register_delta_table(
            spark=self.spark,
            database="silver",
            table_name="domains",
            table_path=self.silver_path,
            schema=delta_schemas.SILVER_SCHEMA,
            partition_columns=["ingestion_date"],
            table_properties=table_properties,
        )

        logger.info("Silver Delta table initialized")

    def write_to_delta(self, silver_df: DataFrame):
        """
        Write streaming DataFrame to Silver Delta table.

        Args:
            silver_df: Transformed DataFrame matching Silver schema

        Returns:
            StreamingQuery object
        """
        logger.info(
            "Writing to Silver Delta table",
            delta_path=self.silver_path,
            checkpoint=self.checkpoint_path,
        )

        query = write_stream_to_delta(
            silver_df,
            self.silver_path,
            self.checkpoint_path,
            output_mode="append",
            partition_by=["ingestion_date"],
            trigger_interval=self.trigger_interval,
            query_name="silver_stream",
        )

        logger.info(
            "Silver stream query started",
            query_id=query.id,
            query_name=query.name,
        )

        return query

    def start(self):
        """
        Start the Silver streaming pipeline.

        Pipeline flow:
        1. Initialize Delta table (if needed)
        2. Read from Bronze Delta
        3. Transform to Silver schema (validate & clean)
        4. Write to Silver Delta Lake
        5. Return streaming query

        Returns:
            StreamingQuery object

        Examples:
            >>> silver = SilverStream(spark)
            >>> query = silver.start()
            >>> query.awaitTermination()
        """
        logger.info("Starting Silver streaming pipeline")

        try:
            # Ensure destination table is ready and registered
            self.initialize_delta_table()

            # Step 1: Read from Bronze Delta
            bronze_df = self.read_from_bronze()

            # Step 2: Transform
            silver_df = self.transform(bronze_df)

            # Step 3: Write to Silver Delta (table created on first write)
            query = self.write_to_delta(silver_df)

            logger.info("Silver streaming pipeline started successfully")

            return query

        except Exception as e:
            logger.error(
                "Failed to start Silver streaming pipeline",
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
        logger.info("Stopping Silver stream", query_id=query.id)

        try:
            query.stop()
            logger.info("Silver stream stopped successfully")
        except Exception as e:
            logger.error("Error stopping Silver stream", error=str(e))
            raise


def create_silver_stream(
    spark: SparkSession,
    **kwargs,
) -> SilverStream:
    """
    Factory function to create SilverStream instance.

    Args:
        spark: SparkSession
        **kwargs: Additional arguments for SilverStream

    Returns:
        SilverStream instance

    Examples:
        >>> spark = create_spark_session()
        >>> silver = create_silver_stream(spark)
        >>> query = silver.start()
    """
    return SilverStream(spark, **kwargs)
