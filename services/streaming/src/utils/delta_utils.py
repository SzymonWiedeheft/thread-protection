"""Delta Lake utilities for Spark Structured Streaming."""

from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField
from pyspark.sql.utils import AnalysisException
from delta import DeltaTable
import structlog

logger = structlog.get_logger()


def create_delta_table_if_not_exists(
    spark: SparkSession,
    table_path: str,
    schema: StructType,
    partition_columns: Optional[list] = None,
    table_properties: Optional[dict] = None,
) -> None:
    """
    Create Delta table if it doesn't exist.

    Args:
        spark: Spark session
        table_path: Path to Delta table
        schema: Table schema
        partition_columns: List of partition column names
        table_properties: Delta table properties (e.g., optimization settings)

    Examples:
        >>> create_delta_table_if_not_exists(
        ...     spark,
        ...     "/data/delta/bronze",
        ...     BRONZE_SCHEMA,
        ...     partition_columns=["ingestion_date"]
        ... )
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        logger.info("Creating Delta table", table_path=table_path)

        normalized_schema = _with_nullable_fields(schema)

        # Create empty DataFrame with schema
        empty_df = spark.createDataFrame([], normalized_schema)

        # Build writer
        writer = empty_df.write.format("delta")

        if partition_columns:
            writer = writer.partitionBy(*partition_columns)

        if table_properties:
            for key, value in table_properties.items():
                writer = writer.option(key, value)

        writer.save(table_path)

        logger.info(
            "Delta table created",
            table_path=table_path,
            partitions=partition_columns,
        )

        # Verify table is readable by executors (forces file system sync)
        # This prevents race conditions in distributed environments
        try:
            verify_df = spark.read.format("delta").load(table_path)
            verify_df.count()  # Force executor interaction
            logger.info("Delta table verified and readable by executors")
        except Exception as e:
            logger.warning(
                "Delta table verification read returned error (may be transient)",
                error=str(e)
            )
    else:
        logger.info("Delta table already exists", table_path=table_path)


def _escape_sql_string(value: str) -> str:
    """Escape single quotes for safe SQL string literals."""

    return value.replace("'", "\\'")


def _schema_to_sql_columns(schema: StructType) -> str:
    """Convert a StructType into SQL column definitions."""

    column_defs = []
    for field in schema:
        data_type = field.dataType.simpleString()
        nullability = " NOT NULL" if not field.nullable else ""
        column_defs.append(f"{field.name} {data_type}{nullability}")

    return ",\n  ".join(column_defs)


def _with_nullable_fields(schema: StructType) -> StructType:
    """Return a copy of schema with all top-level fields nullable."""

    return StructType(
        [
            StructField(field.name, field.dataType, True, field.metadata)
            for field in schema
        ]
    )


def register_delta_table(
    spark: SparkSession,
    database: str,
    table_name: str,
    table_path: str,
    schema: StructType,
    partition_columns: Optional[list] = None,
    table_properties: Optional[dict] = None,
) -> None:
    """Register a Delta table with the Spark catalog if needed."""

    full_table_name = f"{database}.{table_name}"
    table_location = Path(table_path).as_posix()
    schema_location_path = Path(table_path).parent
    schema_location = schema_location_path.as_posix()
    try:
        schema_location_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # pragma: no cover - filesystem permissions guard
        logger.warning(
            "Unable to ensure schema directory exists",
            schema=database,
            path=schema_location,
            error=str(exc),
        )

    logger.info(
        "Ensuring schema exists",
        schema=database,
        location=schema_location,
    )
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {database}"
    if schema_location:
        schema_sql += f" LOCATION '{_escape_sql_string(schema_location)}'"

    spark.sql(schema_sql)

    table_identifier = f"{database}.{table_name}" if database else table_name
    try:
        table_exists_raw = spark.catalog.tableExists(table_identifier)
    except TypeError:
        table_exists_raw = spark.catalog.tableExists(database, table_name)

    table_exists = bool(table_exists_raw)

    normalized_schema = _with_nullable_fields(schema)

    if not table_exists:
        columns_sql = _schema_to_sql_columns(normalized_schema)
        partition_clause = (
            f"PARTITIONED BY ({', '.join(partition_columns)})"
            if partition_columns
            else ""
        )
        properties_clause = ""
        if table_properties:
            formatted_props = ", ".join(
                [f"'{key}'='{value}'" for key, value in table_properties.items()]
            )
            properties_clause = f"TBLPROPERTIES ({formatted_props})"

        ddl = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
  {columns_sql}
)
USING DELTA
{partition_clause}
LOCATION '{_escape_sql_string(table_location)}'
{properties_clause}
"""

        logger.info(
            "Registering Delta table",
            table=full_table_name,
            location=table_location,
        )
        spark.sql(ddl)
    else:
        try:
            details = spark.sql(f"DESCRIBE DETAIL {full_table_name}").collect()
            if isinstance(details, list) and details:
                first_entry = details[0]
                if isinstance(first_entry, dict) and "location" in first_entry:
                    existing_location = Path(first_entry["location"]).as_posix()
                    if existing_location != table_location:
                        logger.warning(
                            "Existing table registered at different location",
                            table=full_table_name,
                            existing_location=existing_location,
                            requested_location=table_location,
                        )
                    else:
                        logger.info(
                            "Delta table already registered",
                            table=full_table_name,
                            location=existing_location,
                        )
        except AnalysisException as exc:  # pragma: no cover - defensive
            logger.warning(
                "Unable to describe existing Delta table",
                table=full_table_name,
                error=str(exc),
            )

    if table_properties:
        formatted_props = ", ".join(
            [f"'{key}'='{value}'" for key, value in table_properties.items()]
        )
        logger.info(
            "Aligning Delta table properties",
            table=full_table_name,
            properties=table_properties,
        )
        spark.sql(
            f"ALTER TABLE {full_table_name} SET TBLPROPERTIES ({formatted_props})"
        )


def get_delta_table(spark: SparkSession, table_path: str) -> DeltaTable:
    """
    Get DeltaTable object for table operations.

    Args:
        spark: Spark session
        table_path: Path to Delta table

    Returns:
        DeltaTable object

    Raises:
        ValueError: If path is not a Delta table
    """
    if not DeltaTable.isDeltaTable(spark, table_path):
        raise ValueError(f"Path is not a Delta table: {table_path}")

    return DeltaTable.forPath(spark, table_path)


def optimize_delta_table(
    spark: SparkSession,
    table_path: str,
    zorder_columns: Optional[list] = None,
) -> None:
    """
    Optimize Delta table (compaction and Z-ordering).

    Args:
        spark: Spark session
        table_path: Path to Delta table
        zorder_columns: Columns to Z-order by (for query performance)

    Examples:
        >>> optimize_delta_table(
        ...     spark,
        ...     "/data/delta/bronze",
        ...     zorder_columns=["domain"]
        ... )
    """
    logger.info("Optimizing Delta table", table_path=table_path)

    delta_table = get_delta_table(spark, table_path)

    if zorder_columns:
        delta_table.optimize().executeZOrderBy(*zorder_columns)
        logger.info(
            "Delta table optimized with Z-order",
            table_path=table_path,
            zorder_columns=zorder_columns,
        )
    else:
        delta_table.optimize().executeCompaction()
        logger.info("Delta table optimized (compaction)", table_path=table_path)


def vacuum_delta_table(
    spark: SparkSession,
    table_path: str,
    retention_hours: int = 168,  # 7 days default
) -> None:
    """
    Vacuum Delta table to remove old files.

    Args:
        spark: Spark session
        table_path: Path to Delta table
        retention_hours: Retention period in hours (default: 168 = 7 days)

    Warning:
        This permanently deletes files. Ensure retention_hours is >= 168
        for time travel safety.
    """
    logger.info(
        "Vacuuming Delta table",
        table_path=table_path,
        retention_hours=retention_hours,
    )

    delta_table = get_delta_table(spark, table_path)
    delta_table.vacuum(retention_hours)

    logger.info("Delta table vacuumed", table_path=table_path)


def get_delta_table_version(spark: SparkSession, table_path: str) -> int:
    """
    Get current version of Delta table.

    Args:
        spark: Spark session
        table_path: Path to Delta table

    Returns:
        Current table version number
    """
    delta_table = get_delta_table(spark, table_path)
    history = delta_table.history(1).select("version").collect()
    return history[0]["version"] if history else 0


def get_streaming_write_options(
    checkpoint_location: str,
    output_mode: str = "append",
    trigger_interval: str = "5 seconds",
) -> dict:
    """
    Get options for Delta streaming write.

    Args:
        checkpoint_location: Checkpoint directory path
        output_mode: Streaming output mode (append, update, complete)
        trigger_interval: Trigger interval (e.g., "5 seconds", "1 minute")

    Returns:
        Dictionary of write options

    Examples:
        >>> options = get_streaming_write_options(
        ...     "/data/checkpoints/bronze",
        ...     output_mode="append",
        ...     trigger_interval="5 seconds"
        ... )
    """
    return {
        "checkpointLocation": checkpoint_location,
        "outputMode": output_mode,
        "trigger": trigger_interval,
    }


def write_stream_to_delta(
    df: DataFrame,
    table_path: str,
    checkpoint_location: str,
    output_mode: str = "append",
    partition_by: Optional[list] = None,
    trigger_interval: str = "5 seconds",
    query_name: Optional[str] = None,
) -> None:
    """
    Write streaming DataFrame to Delta table.

    Args:
        df: Streaming DataFrame
        table_path: Delta table path
        checkpoint_location: Checkpoint directory
        output_mode: Output mode (append, update, complete)
        partition_by: List of partition column names
        trigger_interval: Micro-batch trigger interval
        query_name: Name for the streaming query

    Returns:
        StreamingQuery object

    Examples:
        >>> query = write_stream_to_delta(
        ...     streaming_df,
        ...     "/data/delta/bronze",
        ...     "/data/checkpoints/bronze",
        ...     partition_by=["ingestion_date"]
        ... )
        >>> query.awaitTermination()
    """
    logger.info(
        "Starting Delta stream write",
        table_path=table_path,
        checkpoint_location=checkpoint_location,
        output_mode=output_mode,
    )

    writer = (
        df.writeStream.format("delta")
        .outputMode(output_mode)
        .option("checkpointLocation", checkpoint_location)
    )

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if query_name:
        writer = writer.queryName(query_name)

    # Parse trigger interval
    if trigger_interval:
        if "second" in trigger_interval.lower():
            import re

            seconds = int(re.findall(r"\d+", trigger_interval)[0])
            writer = writer.trigger(processingTime=f"{seconds} seconds")
        elif "minute" in trigger_interval.lower():
            import re

            minutes = int(re.findall(r"\d+", trigger_interval)[0])
            writer = writer.trigger(processingTime=f"{minutes} minutes")

    query = writer.start(table_path)

    logger.info(
        "Delta stream write started",
        query_id=query.id,
        query_name=query.name,
    )

    return query


def get_table_statistics(spark: SparkSession, table_path: str) -> dict:
    """
    Get statistics about a Delta table.

    Args:
        spark: Spark session
        table_path: Path to Delta table

    Returns:
        Dictionary with table statistics
    """
    try:
        delta_table = get_delta_table(spark, table_path)

        # Get basic stats
        df = spark.read.format("delta").load(table_path)
        count = df.count()

        # Get history
        history = delta_table.history(1).collect()
        last_operation = history[0]["operation"] if history else "NONE"

        return {
            "table_path": table_path,
            "row_count": count,
            "last_operation": last_operation,
            "is_delta": True,
        }
    except Exception as e:
        logger.error("Failed to get table statistics", error=str(e))
        return {"table_path": table_path, "error": str(e), "is_delta": False}
