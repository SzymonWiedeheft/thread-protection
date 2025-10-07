"""Gold stream: Silver Delta -> Gold Delta Lake.

This module implements the Gold layer of the Medallion architecture.
It reads from Silver Delta table, performs aggregation and deduplication,
and writes to Gold Delta Lake using MERGE (UPSERT) operations.
"""

from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    when,
    collect_set,
    min as spark_min,
    max as spark_max,
    first,
    to_json,
    from_json,
    array_contains,
    hash,
    expr,
    greatest,
    size,
    array_distinct,
    array_union,
)
from pyspark.sql.types import ArrayType, StringType
import structlog
from delta.tables import DeltaTable

from schemas import delta_schemas
from ..utils.delta_utils import create_delta_table_if_not_exists, register_delta_table
from ..config.spark_config import (
    get_delta_path,
    get_checkpoint_path,
    get_trigger_interval,
)

logger = structlog.get_logger()


class GoldStream:
    """
    Gold stream processor: Silver Delta -> Gold Delta Lake.

    Reads from Silver Delta table, aggregates by domain, and performs
    UPSERT operations to maintain a deduplicated, query-optimized Gold table.

    Attributes:
        spark: SparkSession
        silver_path: Path to Silver Delta table (source)
        gold_path: Path to Gold Delta table (destination)
        checkpoint_path: Path to checkpoint directory
        trigger_interval: Micro-batch trigger interval
        optimize_every_n_batches: Optimize table every N batches
    """

    def __init__(
        self,
        spark: SparkSession,
        silver_path: Optional[str] = None,
        gold_path: Optional[str] = None,
        checkpoint_path: Optional[str] = None,
        trigger_interval: Optional[str] = None,
        optimize_every_n_batches: int = 100,
    ):
        """
        Initialize Gold stream processor.

        Args:
            spark: SparkSession
            silver_path: Silver Delta table path (default: from config)
            gold_path: Gold Delta table path (default: from config)
            checkpoint_path: Checkpoint path (default: from config)
            trigger_interval: Trigger interval (default: from config)
            optimize_every_n_batches: Z-order optimization frequency
        """
        self.spark = spark
        self.silver_path = silver_path or get_delta_path("silver")
        self.gold_path = gold_path or get_delta_path("gold")
        self.checkpoint_path = checkpoint_path or get_checkpoint_path("gold")
        self.trigger_interval = trigger_interval or get_trigger_interval("gold")
        self.optimize_every_n_batches = optimize_every_n_batches
        self.batch_counter = 0

        logger.info(
            "Initializing Gold stream",
            silver_path=self.silver_path,
            gold_path=self.gold_path,
            checkpoint_path=self.checkpoint_path,
            trigger_interval=self.trigger_interval,
        )

    def read_from_silver(self) -> DataFrame:
        """
        Read streaming data from Silver Delta table.

        Only reads valid domains (is_valid = 'true').

        Returns:
            Streaming DataFrame with Silver data
        """
        logger.info("Starting Silver Delta read stream", silver_path=self.silver_path)

        # Read from Silver Delta table as stream
        df = (
            self.spark.readStream.format("delta")
            .option("ignoreChanges", "true")
            .option("ignoreDeletes", "true")
            .load(self.silver_path)
        )

        # Filter only valid domains
        valid_df = df.filter(col("is_valid") == "true")

        logger.info("Silver Delta stream initialized with valid domains only")

        return valid_df

    def aggregate(self, silver_df: DataFrame) -> DataFrame:
        """
        Aggregate Silver data by domain.

        Aggregations:
        1. Category priority: malware > ads_trackers
        2. Collect unique sources as JSON array
        3. Count unique sources
        4. Track first_seen and last_seen timestamps
        5. Merge metadata maps
        6. Calculate domain hash for Z-ordering
        7. Determine active status

        Args:
            silver_df: Silver DataFrame

        Returns:
            Aggregated DataFrame matching Gold schema
        """
        logger.info("Aggregating Silver data by domain")

        # Group by domain and aggregate
        aggregated = silver_df.groupBy("domain").agg(
            # Category priority: malware > ads_trackers
            # If domain appears in multiple categories, choose malware
            when(array_contains(collect_set("category"), "malware"), "malware")
            .otherwise("ads_trackers")
            .alias("category"),
            # Collect unique sources as JSON array
            to_json(collect_set("source")).alias("source_list"),
            # Count unique sources (using size of collect_set for streaming compatibility)
            size(collect_set("source")).alias("source_count"),
            # Temporal tracking
            spark_min("ingestion_timestamp").alias("first_seen"),
            spark_max("ingestion_timestamp").alias("last_seen"),
            # Metadata (first non-null for now, could be enhanced to merge)
            first("metadata", ignorenulls=True).alias("metadata"),
        )

        # Add derived columns
        with_derived = (
            aggregated.withColumn("last_updated", current_timestamp())
            .withColumn(
                "is_active",
                when(
                    col("last_seen") >= current_timestamp() - expr("INTERVAL 7 DAYS"),
                    "true",
                ).otherwise("false"),
            )
            .withColumn("domain_hash", hash(col("domain")).cast("long"))
        )

        logger.info(
            "Aggregation complete",
            schema=with_derived.schema.simpleString(),
        )

        return with_derived

    def initialize_delta_table(self) -> None:
        """Initialize Gold Delta table if it doesn't exist."""
        logger.info("Initializing Gold Delta table", delta_path=self.gold_path)

        table_properties = {
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        }

        create_delta_table_if_not_exists(
            self.spark,
            self.gold_path,
            delta_schemas.GOLD_SCHEMA,
            partition_columns=None,  # Gold is not partitioned (optimized for lookups)
            table_properties=table_properties,
        )

        register_delta_table(
            spark=self.spark,
            database="gold",
            table_name="domains",
            table_path=self.gold_path,
            schema=delta_schemas.GOLD_SCHEMA,
            partition_columns=None,
            table_properties=table_properties,
        )

        logger.info("Gold Delta table initialized")

    def upsert_to_gold(self, micro_batch_df: DataFrame, batch_id: int) -> None:
        """
        Upsert micro-batch to Gold Delta table using MERGE.

        This function is called for each micro-batch via foreachBatch.
        It performs UPSERT (UPDATE if exists, INSERT if new) to maintain
        a single record per domain with aggregated information.

        Args:
            micro_batch_df: Micro-batch DataFrame
            batch_id: Batch ID from foreachBatch
        """
        logger.info(
            "Processing micro-batch for Gold MERGE",
            batch_id=batch_id,
            record_count=micro_batch_df.count(),
        )

        # Check if table exists
        if DeltaTable.isDeltaTable(self.spark, self.gold_path):
            gold_table = DeltaTable.forPath(self.spark, self.gold_path)

            # MERGE operation: UPSERT logic
            gold_table.alias("gold").merge(
                micro_batch_df.alias("updates"), "gold.domain = updates.domain"
            ).whenMatchedUpdate(
                set={
                    # Category: Prioritize malware over ads_trackers
                    "category": when(col("updates.category") == "malware", "malware")
                    .when(col("gold.category") == "malware", "malware")
                    .otherwise("ads_trackers"),
                    # Source list: Union and deduplicate
                    "source_list": to_json(
                        array_distinct(
                            array_union(
                                from_json(
                                    col("gold.source_list"), ArrayType(StringType())
                                ),
                                from_json(
                                    col("updates.source_list"), ArrayType(StringType())
                                ),
                            )
                        )
                    ),
                    # Source count: Count of unique sources after union
                    "source_count": size(
                        array_distinct(
                            array_union(
                                from_json(
                                    col("gold.source_list"), ArrayType(StringType())
                                ),
                                from_json(
                                    col("updates.source_list"), ArrayType(StringType())
                                ),
                            )
                        )
                    ),
                    # First seen: Keep earliest timestamp
                    "first_seen": when(
                        col("gold.first_seen") < col("updates.first_seen"),
                        col("gold.first_seen"),
                    ).otherwise(col("updates.first_seen")),
                    # Last seen: Keep latest timestamp
                    "last_seen": greatest(
                        col("gold.last_seen"), col("updates.last_seen")
                    ),
                    # Last updated: Current timestamp
                    "last_updated": current_timestamp(),
                    # Active status: Update based on last_seen
                    "is_active": when(
                        greatest(col("gold.last_seen"), col("updates.last_seen"))
                        >= current_timestamp() - expr("INTERVAL 7 DAYS"),
                        "true",
                    ).otherwise("false"),
                    # Metadata: Use updates (could be enhanced to merge)
                    "metadata": col("updates.metadata"),
                }
            ).whenNotMatchedInsert(
                values={
                    "domain": col("updates.domain"),
                    "category": col("updates.category"),
                    "source_list": col("updates.source_list"),
                    "source_count": col("updates.source_count"),
                    "first_seen": col("updates.first_seen"),
                    "last_seen": col("updates.last_seen"),
                    "last_updated": current_timestamp(),
                    "is_active": "true",
                    "metadata": col("updates.metadata"),
                    "domain_hash": col("updates.domain_hash"),
                }
            ).execute()

            logger.info("MERGE operation completed", batch_id=batch_id)

            # Increment batch counter for optimization
            self.batch_counter += 1

            # Optimize table periodically
            if self.batch_counter % self.optimize_every_n_batches == 0:
                self.optimize_gold_table()

        else:
            # First batch: Write directly
            logger.info("First batch: Writing directly to Gold table")
            micro_batch_df.write.format("delta").mode("append").save(self.gold_path)

    def optimize_gold_table(self) -> None:
        """Optimize Gold Delta table using Z-ordering by domain."""
        logger.info("Optimizing Gold table with Z-ordering", gold_path=self.gold_path)

        try:
            gold_table = DeltaTable.forPath(self.spark, self.gold_path)
            gold_table.optimize().executeZOrderBy("domain")

            logger.info(
                "Gold table optimized successfully",
                batch_counter=self.batch_counter,
            )
        except Exception as e:
            logger.error(
                "Failed to optimize Gold table",
                error=str(e),
                error_type=type(e).__name__,
            )
            # Don't fail the stream on optimization errors

    def write_to_delta(self, aggregated_df: DataFrame):
        """
        Write streaming DataFrame to Gold Delta table using foreachBatch.

        Args:
            aggregated_df: Aggregated DataFrame matching Gold schema

        Returns:
            StreamingQuery object
        """
        logger.info(
            "Starting Gold Delta write with foreachBatch",
            delta_path=self.gold_path,
            checkpoint=self.checkpoint_path,
        )

        # Parse trigger interval
        trigger_dict = {}
        if self.trigger_interval:
            if "second" in self.trigger_interval.lower():
                import re

                seconds = int(re.findall(r"\d+", self.trigger_interval)[0])
                trigger_dict = {"processingTime": f"{seconds} seconds"}
            elif "minute" in self.trigger_interval.lower():
                import re

                minutes = int(re.findall(r"\d+", self.trigger_interval)[0])
                trigger_dict = {"processingTime": f"{minutes} minutes"}

        # Write stream using foreachBatch for MERGE operations
        query = (
            aggregated_df.writeStream.foreachBatch(self.upsert_to_gold)
            .outputMode("update")  # Update mode for aggregations
            .option("checkpointLocation", self.checkpoint_path)
            .queryName("gold_stream")
            .trigger(**trigger_dict)
            .start()
        )

        logger.info(
            "Gold stream query started",
            query_id=query.id,
            query_name=query.name,
        )

        return query

    def start(self):
        """
        Start the Gold streaming pipeline.

        Pipeline flow:
        1. Initialize Delta table (if needed)
        2. Read from Silver Delta (valid domains only)
        3. Aggregate by domain (groupBy + agg)
        4. Write to Gold Delta (MERGE/UPSERT)
        5. Return streaming query

        Returns:
            StreamingQuery object

        Examples:
            >>> gold = GoldStream(spark)
            >>> query = gold.start()
            >>> query.awaitTermination()
        """
        logger.info("Starting Gold streaming pipeline")

        try:
            # Ensure destination table is ready and registered
            self.initialize_delta_table()

            # Step 1: Read from Silver Delta
            silver_df = self.read_from_silver()

            # Step 2: Aggregate
            aggregated_df = self.aggregate(silver_df)

            # Step 3: Write to Gold Delta with MERGE (table created on first write)
            query = self.write_to_delta(aggregated_df)

            logger.info("Gold streaming pipeline started successfully")

            return query

        except Exception as e:
            logger.error(
                "Failed to start Gold streaming pipeline",
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
        logger.info("Stopping Gold stream", query_id=query.id)

        try:
            query.stop()
            logger.info("Gold stream stopped successfully")
        except Exception as e:
            logger.error("Error stopping Gold stream", error=str(e))
            raise


def create_gold_stream(
    spark: SparkSession,
    **kwargs,
) -> GoldStream:
    """
    Factory function to create GoldStream instance.

    Args:
        spark: SparkSession
        **kwargs: Additional arguments for GoldStream

    Returns:
        GoldStream instance

    Examples:
        >>> spark = create_spark_session()
        >>> gold = create_gold_stream(spark)
        >>> query = gold.start()
    """
    return GoldStream(spark, **kwargs)
