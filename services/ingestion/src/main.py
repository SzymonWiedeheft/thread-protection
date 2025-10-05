"""Main ingestion service orchestration."""

import asyncio
import argparse
import sys
from pathlib import Path
from typing import Dict, List, Any
import yaml
import structlog
from common import setup_logging, get_env, PipelineException
from schemas import DomainModel
from fetchers import HTTPFetcher
from parsers import HostsParser, AdBlockParser
from validators import DomainValidator
from producers import KafkaProducer

logger = structlog.get_logger()


class IngestionService:
    """Main ingestion service orchestrator."""

    def __init__(self, config_path: str, kafka_bootstrap_servers: str):
        """
        Initialize ingestion service.

        Args:
            config_path: Path to sources.yaml configuration file
            kafka_bootstrap_servers: Kafka bootstrap servers
        """
        self.config_path = config_path
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.sources_config = {}
        self.stats = {
            "sources_processed": 0,
            "sources_failed": 0,
            "domains_fetched": 0,
            "domains_validated": 0,
            "domains_published": 0,
        }

    def load_config(self) -> Dict[str, Any]:
        """
        Load sources configuration from YAML file.

        Returns:
            Dictionary of source configurations

        Raises:
            FileNotFoundError: If config file not found
            yaml.YAMLError: If config file is invalid
        """
        config_file = Path(self.config_path)

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")

        logger.info("Loading configuration", config_path=self.config_path)

        with open(config_file, "r") as f:
            config = yaml.safe_load(f)

        self.sources_config = config.get("sources", {})

        logger.info(
            "Configuration loaded",
            sources_count=len(self.sources_config),
            sources=list(self.sources_config.keys()),
        )

        return self.sources_config

    async def fetch_source(
        self, source_name: str, source_config: Dict[str, Any]
    ) -> List[DomainModel]:
        """
        Fetch and parse data from a single source.

        Args:
            source_name: Name of the source
            source_config: Source configuration dictionary

        Returns:
            List of DomainModel objects
        """
        logger.info(
            "Fetching source",
            source=source_name,
            url=source_config.get("url"),
            format=source_config.get("format"),
            category=source_config.get("category"),
        )

        try:
            # Create fetcher
            fetcher = HTTPFetcher(
                source_name=source_name,
                url=source_config["url"],
                timeout=int(get_env("HTTP_TIMEOUT", "30")),
                retries=int(get_env("HTTP_RETRIES", "3")),
                backoff=float(get_env("HTTP_BACKOFF", "5.0")),
            )

            # Fetch data
            result = await fetcher.fetch()
            content = result["content"]
            metadata = result["metadata"]

            # Select appropriate parser
            format_type = source_config["format"]
            category = source_config["category"]

            if format_type == "hosts":
                parser = HostsParser(source_name, category)
            elif format_type == "adblock":
                parser = AdBlockParser(source_name, category)
            else:
                raise ValueError(f"Unknown format type: {format_type}")

            # Parse content
            domains = parser.parse(content, metadata)

            logger.info(
                "Source fetched successfully",
                source=source_name,
                domains_extracted=len(domains),
            )

            self.stats["domains_fetched"] += len(domains)
            return domains

        except Exception as e:
            logger.error(
                "Failed to fetch source",
                source=source_name,
                error=str(e),
                error_type=type(e).__name__,
            )
            self.stats["sources_failed"] += 1
            # Don't raise - continue with other sources
            return []

    async def fetch_all_sources(self) -> Dict[str, List[DomainModel]]:
        """
        Fetch data from all configured sources in parallel.

        Returns:
            Dictionary mapping source names to domain lists
        """
        logger.info("Starting parallel fetch", sources_count=len(self.sources_config))

        # Create tasks for parallel fetching
        tasks = []
        source_names = []

        for source_name, source_config in self.sources_config.items():
            task = self.fetch_source(source_name, source_config)
            tasks.append(task)
            source_names.append(source_name)

        # Execute all tasks in parallel
        results = await asyncio.gather(*tasks)

        # Map results to source names
        domains_by_source = {
            source_names[i]: results[i] for i in range(len(source_names))
        }

        # Update statistics
        for source_name, domains in domains_by_source.items():
            if domains:
                self.stats["sources_processed"] += 1

        logger.info(
            "Parallel fetch complete",
            sources_processed=self.stats["sources_processed"],
            sources_failed=self.stats["sources_failed"],
            total_domains=sum(len(d) for d in domains_by_source.values()),
        )

        return domains_by_source

    def validate_domains(
        self, domains_by_source: Dict[str, List[DomainModel]]
    ) -> Dict[str, List[DomainModel]]:
        """
        Validate domains from all sources.

        Args:
            domains_by_source: Dictionary mapping source names to domain lists

        Returns:
            Dictionary mapping source names to validated domain lists
        """
        logger.info("Starting domain validation")

        validated_by_source = {}
        validator = DomainValidator(strict=False)

        for source_name, domains in domains_by_source.items():
            if not domains:
                validated_by_source[source_name] = []
                continue

            logger.info(
                "Validating domains for source",
                source=source_name,
                count=len(domains),
            )

            # Validate and clean
            valid_domains = validator.validate_and_clean(domains)
            validated_by_source[source_name] = valid_domains

            self.stats["domains_validated"] += len(valid_domains)

            logger.info(
                "Validation complete for source",
                source=source_name,
                valid=len(valid_domains),
                invalid=len(domains) - len(valid_domains),
            )

        # Log overall validation stats
        validation_stats = validator.get_statistics()
        logger.info(
            "Overall validation complete",
            total=validation_stats["total"],
            valid=validation_stats["valid"],
            invalid=validation_stats["invalid"],
            duplicates=validation_stats["duplicates"],
        )

        return validated_by_source

    def publish_to_kafka(
        self, domains_by_source: Dict[str, List[DomainModel]]
    ) -> Dict[str, Any]:
        """
        Publish validated domains to Kafka.

        Args:
            domains_by_source: Dictionary mapping source names to domain lists

        Returns:
            Publishing statistics
        """
        logger.info("Starting Kafka publishing")

        # Create Kafka producer with context manager
        with KafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            batch_size=int(get_env("KAFKA_BATCH_SIZE", "1000")),
            linger_ms=int(get_env("KAFKA_LINGER_MS", "100")),
            max_retries=int(get_env("KAFKA_MAX_RETRIES", "3")),
        ) as producer:
            # Publish batch
            stats = producer.publish_batch(domains_by_source)

            self.stats["domains_published"] = stats["overall"]["total"]

            logger.info(
                "Kafka publishing complete",
                malware=stats["overall"]["malware"],
                ads_trackers=stats["overall"]["ads_trackers"],
                total=stats["overall"]["total"],
            )

            return stats

    async def run(self) -> Dict[str, Any]:
        """
        Run the complete ingestion pipeline.

        Returns:
            Pipeline execution statistics

        Raises:
            PipelineException: If pipeline execution fails
        """
        logger.info("Starting ingestion pipeline")

        try:
            # Step 1: Load configuration
            self.load_config()

            # Step 2: Fetch from all sources in parallel
            domains_by_source = await self.fetch_all_sources()

            # Step 3: Validate domains
            validated_domains = self.validate_domains(domains_by_source)

            # Step 4: Publish to Kafka
            publish_stats = self.publish_to_kafka(validated_domains)

            logger.info(
                "Ingestion pipeline complete",
                sources_processed=self.stats["sources_processed"],
                sources_failed=self.stats["sources_failed"],
                domains_fetched=self.stats["domains_fetched"],
                domains_validated=self.stats["domains_validated"],
                domains_published=self.stats["domains_published"],
            )

            return {
                "status": "success",
                "statistics": self.stats,
                "publish_stats": publish_stats,
            }

        except Exception as e:
            logger.error(
                "Ingestion pipeline failed",
                error=str(e),
                error_type=type(e).__name__,
            )
            raise PipelineException(f"Pipeline execution failed: {str(e)}") from e


def main():
    """Main entry point."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Harmful Domain Data Ingestion Service"
    )
    parser.add_argument(
        "--config",
        type=str,
        default="src/config/sources.yaml",
        help="Path to sources configuration file (default: src/config/sources.yaml)",
    )
    parser.add_argument(
        "--kafka-servers",
        type=str,
        default=None,
        help="Kafka bootstrap servers (default: from KAFKA_BOOTSTRAP_SERVERS env var)",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)",
    )
    parser.add_argument(
        "--json-logs",
        action="store_true",
        help="Output logs in JSON format",
    )

    args = parser.parse_args()

    # Setup logging
    global logger
    logger = setup_logging(
        level=args.log_level,
        service_name="ingestion",
        json_format=args.json_logs,
    )

    # Get Kafka bootstrap servers
    kafka_servers = args.kafka_servers or get_env(
        "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"
    )

    logger.info(
        "Starting ingestion service",
        config_path=args.config,
        kafka_servers=kafka_servers,
        log_level=args.log_level,
    )

    # Create and run service
    service = IngestionService(
        config_path=args.config, kafka_bootstrap_servers=kafka_servers
    )

    try:
        # Run async pipeline
        result = asyncio.run(service.run())

        logger.info("Service completed successfully", result=result)
        sys.exit(0)

    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
        sys.exit(0)

    except Exception as e:
        logger.error("Service failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
