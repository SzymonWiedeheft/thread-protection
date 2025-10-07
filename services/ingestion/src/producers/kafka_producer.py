"""Kafka producer for publishing domain data."""

import json
from typing import List, Dict, Any
from kafka import KafkaProducer as KafkaClient
from kafka.errors import KafkaError
import structlog
from schemas import DomainModel, KafkaMessageSchema
from common import KafkaPublishError

logger = structlog.get_logger()


class KafkaProducer:
    """Kafka producer for publishing domain messages."""

    # Topic mapping based on category
    TOPIC_MAP = {
        "malware": "malware_domains",
        "ads_trackers": "ads_trackers_domains",
    }

    def __init__(
        self,
        bootstrap_servers: str = "kafka:9092",
        batch_size: int = 1000,
        linger_ms: int = 100,
        max_retries: int = 3,
    ):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka bootstrap servers (comma-separated)
            batch_size: Number of messages to batch before sending
            linger_ms: Time to wait before sending batch
            max_retries: Maximum number of retries for failed sends
        """
        self.bootstrap_servers = bootstrap_servers
        self.batch_size = batch_size
        self.linger_ms = linger_ms
        self.max_retries = max_retries
        self.producer = None

        logger.info(
            "Initializing Kafka producer",
            bootstrap_servers=bootstrap_servers,
            batch_size=batch_size,
        )

    def connect(self):
        """
        Connect to Kafka.

        Raises:
            KafkaPublishError: If connection fails
        """
        try:
            self.producer = KafkaClient(
                bootstrap_servers=self.bootstrap_servers.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                batch_size=self.batch_size,
                linger_ms=self.linger_ms,
                retries=self.max_retries,
                acks="all",  # Wait for all replicas
                compression_type="gzip",
            )
            logger.info("Kafka producer connected successfully")
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise KafkaPublishError(
                message="Failed to connect to Kafka",
                context={"bootstrap_servers": self.bootstrap_servers},
                original_error=e,
            )

    def close(self):
        """Close Kafka producer connection."""
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed successfully")
            except Exception as e:
                logger.warning("Error closing Kafka producer", error=str(e))

    def get_topic(self, category: str) -> str:
        """
        Get Kafka topic for category.

        Args:
            category: Category (malware or ads_trackers)

        Returns:
            Topic name

        Raises:
            ValueError: If category is invalid
        """
        topic = self.TOPIC_MAP.get(category)
        if not topic:
            raise ValueError(
                f"Invalid category '{category}'. Must be one of {list(self.TOPIC_MAP.keys())}"
            )
        return topic

    def publish(self, domains: List[DomainModel]) -> Dict[str, int]:
        """
        Publish domain models to Kafka.

        Args:
            domains: List of DomainModel objects to publish

        Returns:
            Dictionary with counts: {"malware": X, "ads_trackers": Y, "total": Z}

        Raises:
            KafkaPublishError: If publishing fails
        """
        if not self.producer:
            raise KafkaPublishError(
                message="Kafka producer not connected. Call connect() first.",
                context={"bootstrap_servers": self.bootstrap_servers},
            )

        if not domains:
            logger.warning("No domains to publish")
            return {"malware": 0, "ads_trackers": 0, "total": 0}

        logger.info("Publishing domains to Kafka", total_domains=len(domains))

        # Group by category for statistics
        stats = {"malware": 0, "ads_trackers": 0, "total": 0}
        failed = []

        for domain_model in domains:
            try:
                message = KafkaMessageSchema.from_domain_model(domain_model)
                topic = self.get_topic(domain_model.category)

                key = domain_model.domain

                self.producer.send(
                    topic,
                    key=key,
                    value=message.model_dump(),
                )

                stats[domain_model.category] += 1
                stats["total"] += 1

            except KafkaError as e:
                logger.error(
                    "Failed to publish domain",
                    domain=domain_model.domain,
                    category=domain_model.category,
                    error=str(e),
                )
                failed.append(domain_model.domain)
            except Exception as e:
                logger.error(
                    "Unexpected error publishing domain",
                    domain=domain_model.domain,
                    error=str(e),
                )
                failed.append(domain_model.domain)

        try:
            self.producer.flush()
        except Exception as e:
            logger.error("Failed to flush Kafka producer", error=str(e))
            raise KafkaPublishError(
                message="Failed to flush messages to Kafka",
                context={
                    "bootstrap_servers": self.bootstrap_servers,
                    "attempted_count": len(domains),
                },
                original_error=e,
            )

        if failed:
            logger.warning(
                "Some domains failed to publish",
                failed_count=len(failed),
                total_count=len(domains),
            )

        logger.info(
            "Kafka publishing complete",
            malware_count=stats["malware"],
            ads_trackers_count=stats["ads_trackers"],
            total_count=stats["total"],
            failed_count=len(failed),
        )

        return stats

    def publish_batch(
        self, domains_by_source: Dict[str, List[DomainModel]]
    ) -> Dict[str, Any]:
        """
        Publish domains from multiple sources.

        Args:
            domains_by_source: Dictionary mapping source names to domain lists

        Returns:
            Dictionary with statistics per source and overall
        """
        if not self.producer:
            raise KafkaPublishError(
                message="Kafka producer not connected. Call connect() first.",
                context={"bootstrap_servers": self.bootstrap_servers},
            )

        overall_stats = {"malware": 0, "ads_trackers": 0, "total": 0}
        source_stats = {}

        for source_name, domains in domains_by_source.items():
            logger.info(
                "Publishing batch for source",
                source=source_name,
                count=len(domains),
            )

            stats = self.publish(domains)
            source_stats[source_name] = stats

            overall_stats["malware"] += stats["malware"]
            overall_stats["ads_trackers"] += stats["ads_trackers"]
            overall_stats["total"] += stats["total"]

        logger.info(
            "Batch publishing complete",
            sources_count=len(domains_by_source),
            total_domains=overall_stats["total"],
        )

        return {
            "overall": overall_stats,
            "by_source": source_stats,
        }

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
