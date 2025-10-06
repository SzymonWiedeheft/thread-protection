#!/usr/bin/env python3
"""Main entry point for Bronze streaming service.

This module runs the Bronze layer streaming pipeline that reads domain data
from Kafka topics and writes to Delta Lake Bronze table.
"""

import sys
import signal
import argparse
import time
from typing import Optional
import structlog

from .config.spark_config import (
    create_spark_session,
    get_kafka_bootstrap_servers,
    get_delta_path,
    get_checkpoint_path,
    get_kafka_topics,
    get_trigger_interval,
)
from .streams.bronze_stream import create_bronze_stream

# Configure structured logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging_level=20),  # INFO level
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Global streaming query reference for signal handlers
streaming_query = None


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description="Bronze layer streaming service - Kafka to Delta Lake"
    )

    # Spark configuration
    parser.add_argument(
        "--master",
        type=str,
        default=None,
        help="Spark master URL (default: local mode)",
    )
    parser.add_argument(
        "--app-name",
        type=str,
        default="BronzeStreamingService",
        help="Spark application name",
    )

    # Kafka configuration
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help=f"Kafka bootstrap servers (default: {get_kafka_bootstrap_servers()})",
    )
    parser.add_argument(
        "--topics",
        type=str,
        nargs="+",
        default=None,
        help=f"Kafka topics to subscribe (default: {get_kafka_topics()})",
    )
    parser.add_argument(
        "--starting-offsets",
        type=str,
        default="latest",
        choices=["earliest", "latest"],
        help="Kafka starting offsets strategy",
    )

    # Delta Lake configuration
    parser.add_argument(
        "--delta-path",
        type=str,
        default=None,
        help=f"Bronze Delta table path (default: {get_delta_path('bronze')})",
    )
    parser.add_argument(
        "--checkpoint-path",
        type=str,
        default=None,
        help=f"Checkpoint path (default: {get_checkpoint_path('bronze')})",
    )

    # Streaming configuration
    parser.add_argument(
        "--trigger-interval",
        type=str,
        default=None,
        help=f"Trigger interval (default: {get_trigger_interval('bronze')})",
    )
    parser.add_argument(
        "--monitor-interval",
        type=int,
        default=30,
        help="Query monitoring interval in seconds (default: 30)",
    )

    return parser.parse_args()


def setup_signal_handlers() -> None:
    """Setup signal handlers for graceful shutdown."""

    def signal_handler(signum, frame):
        """Handle shutdown signals."""
        sig_name = signal.Signals(signum).name
        logger.info(f"Received signal {sig_name}, initiating graceful shutdown...")

        if streaming_query and streaming_query.isActive:
            logger.info("Stopping streaming query...")
            streaming_query.stop()
            logger.info("Streaming query stopped successfully")

        logger.info("Shutdown complete")
        sys.exit(0)

    # Register handlers for SIGTERM and SIGINT
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    logger.info("Signal handlers registered for graceful shutdown")


def monitor_query_progress(query, interval: int = 30) -> None:
    """Monitor streaming query progress and log metrics.

    Args:
        query: Streaming query to monitor
        interval: Monitoring interval in seconds
    """
    logger.info(
        "Starting query monitoring",
        query_id=query.id,
        interval_seconds=interval,
    )

    try:
        while query.isActive:
            # Get progress
            progress = query.lastProgress

            if progress:
                logger.info(
                    "Query progress",
                    query_id=query.id,
                    batch_id=progress.get("batchId"),
                    num_input_rows=progress.get("numInputRows"),
                    input_rows_per_second=progress.get("inputRowsPerSecond"),
                    process_rows_per_second=progress.get("processedRowsPerSecond"),
                    sources=progress.get("sources"),
                    sink=progress.get("sink"),
                )

            # Sleep for monitoring interval
            time.sleep(interval)

    except Exception as e:
        logger.error(
            "Error during query monitoring",
            error=str(e),
            error_type=type(e).__name__,
        )
        raise


def main() -> int:
    """Main function to run Bronze streaming service.

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    global streaming_query

    # Parse arguments
    args = parse_args()

    logger.info(
        "Starting Bronze streaming service",
        app_name=args.app_name,
        master=args.master or "local",
    )

    # Setup signal handlers
    setup_signal_handlers()

    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session(
            app_name=args.app_name,
            master=args.master,
        )

        logger.info(
            "Spark session created",
            version=spark.version,
            master=spark.sparkContext.master,
        )

        # Prepare Bronze stream configuration
        bronze_config = {}

        if args.bootstrap_servers:
            bronze_config["bootstrap_servers"] = args.bootstrap_servers
        if args.topics:
            bronze_config["topics"] = args.topics
        if args.delta_path:
            bronze_config["delta_path"] = args.delta_path
        if args.checkpoint_path:
            bronze_config["checkpoint_path"] = args.checkpoint_path
        if args.trigger_interval:
            bronze_config["trigger_interval"] = args.trigger_interval

        logger.info("Bronze stream configuration", config=bronze_config)

        # Create and start Bronze stream
        logger.info("Creating Bronze stream...")
        bronze = create_bronze_stream(spark, **bronze_config)

        logger.info("Starting Bronze streaming pipeline...")
        streaming_query = bronze.start()

        logger.info(
            "Bronze streaming pipeline started successfully",
            query_id=streaming_query.id,
            query_name=streaming_query.name,
        )

        # Monitor query progress
        monitor_query_progress(streaming_query, interval=args.monitor_interval)

        # Wait for termination (should not reach here unless query stops)
        streaming_query.awaitTermination()

        logger.info("Bronze streaming pipeline terminated")
        return 0

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        if streaming_query and streaming_query.isActive:
            streaming_query.stop()
        return 0

    except Exception as e:
        logger.error(
            "Fatal error in Bronze streaming service",
            error=str(e),
            error_type=type(e).__name__,
            exc_info=True,
        )

        # Try to stop query gracefully
        if streaming_query and streaming_query.isActive:
            try:
                streaming_query.stop()
            except Exception as stop_error:
                logger.error(
                    "Error stopping streaming query",
                    error=str(stop_error),
                )

        return 1

    finally:
        logger.info("Bronze streaming service shutting down")


if __name__ == "__main__":
    sys.exit(main())
