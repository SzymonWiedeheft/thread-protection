"""Delta Lake table schemas for Bronze, Silver, and Gold layers."""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    MapType,
    DateType,
)


# Bronze Layer Schema
# Raw data from Kafka with all metadata
BRONZE_SCHEMA = StructType(
    [
        # Original domain data fields
        StructField("domain", StringType(), nullable=False),
        StructField(
            "category", StringType(), nullable=False
        ),  # malware or ads_trackers
        StructField("source", StringType(), nullable=False),  # urlhaus, stevenblack
        StructField("source_format", StringType(), nullable=False),  # hosts or adblock
        StructField("raw_entry", StringType(), nullable=False),  # Original line
        StructField(
            "ingestion_timestamp", TimestampType(), nullable=False
        ),  # When ingested
        StructField(
            "metadata", MapType(StringType(), StringType()), nullable=True
        ),  # Additional metadata
        # Kafka metadata fields
        StructField("kafka_topic", StringType(), nullable=False),  # Source topic
        StructField("kafka_partition", LongType(), nullable=False),  # Partition number
        StructField("kafka_offset", LongType(), nullable=False),  # Message offset
        StructField(
            "kafka_timestamp", TimestampType(), nullable=False
        ),  # Kafka message timestamp
        # Processing metadata
        StructField(
            "bronze_timestamp", TimestampType(), nullable=False
        ),  # When written to Bronze
        StructField(
            "ingestion_date", DateType(), nullable=False
        ),  # Partition key (date type)
    ]
)


# Silver Layer Schema
# Cleaned and validated data
SILVER_SCHEMA = StructType(
    [
        # Domain fields (validated and cleaned)
        StructField("domain", StringType(), nullable=False),
        StructField("category", StringType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("source_format", StringType(), nullable=False),
        StructField("raw_entry", StringType(), nullable=False),
        # Timestamps
        StructField("ingestion_timestamp", TimestampType(), nullable=False),
        StructField("bronze_timestamp", TimestampType(), nullable=False),
        StructField("silver_timestamp", TimestampType(), nullable=False),
        # Metadata
        StructField("metadata", MapType(StringType(), StringType()), nullable=True),
        # Validation flags
        StructField("is_valid", StringType(), nullable=False),  # true/false
        StructField(
            "validation_errors", StringType(), nullable=True
        ),  # Error messages if invalid
        # Partitioning
        StructField("ingestion_date", DateType(), nullable=False),
        # Lineage
        StructField("bronze_offset", LongType(), nullable=False),
    ]
)


# Gold Layer Schema
# Aggregated, de-duplicated, query-optimized
GOLD_SCHEMA = StructType(
    [
        # Primary key
        StructField("domain", StringType(), nullable=False),
        # Aggregated fields
        StructField(
            "category", StringType(), nullable=False
        ),  # Primary category (if multiple, most severe)
        StructField(
            "source_list", StringType(), nullable=False
        ),  # JSON array of sources
        StructField("source_count", LongType(), nullable=False),  # Number of sources
        StructField("first_seen", TimestampType(), nullable=False),  # First ingestion
        StructField("last_seen", TimestampType(), nullable=False),  # Latest ingestion
        StructField(
            "last_updated", TimestampType(), nullable=False
        ),  # Last update time
        # Status
        StructField(
            "is_active", StringType(), nullable=False
        ),  # true/false (for soft deletes)
        # Metadata
        StructField(
            "metadata", MapType(StringType(), StringType()), nullable=True
        ),  # Aggregated metadata
        # Performance optimization
        StructField("domain_hash", LongType(), nullable=True),  # Hash for Z-ordering
    ]
)


# Table paths (will be configurable via environment)
DEFAULT_BRONZE_PATH = "/data/delta/bronze/domains"
DEFAULT_SILVER_PATH = "/data/delta/silver/domains"
DEFAULT_GOLD_PATH = "/data/delta/gold/domains"

# Checkpoint paths
DEFAULT_BRONZE_CHECKPOINT = "/data/checkpoints/bronze"
DEFAULT_SILVER_CHECKPOINT = "/data/checkpoints/silver"
DEFAULT_GOLD_CHECKPOINT = "/data/checkpoints/gold"
