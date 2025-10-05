"""Configuration constants for the pipeline."""

from typing import Final

# HTTP Fetcher Defaults
DEFAULT_HTTP_TIMEOUT: Final[int] = 30
DEFAULT_HTTP_RETRIES: Final[int] = 3
DEFAULT_HTTP_BACKOFF: Final[float] = 5.0
MAX_HTTP_TIMEOUT: Final[int] = 300  # 5 minutes
MIN_HTTP_TIMEOUT: Final[int] = 5

# Kafka Producer Defaults
DEFAULT_KAFKA_SERVERS: Final[str] = "kafka:9092"
DEFAULT_KAFKA_BATCH_SIZE: Final[int] = 1000
DEFAULT_KAFKA_LINGER_MS: Final[int] = 100
DEFAULT_KAFKA_MAX_RETRIES: Final[int] = 3
KAFKA_TOPIC_MALWARE: Final[str] = "malware_domains"
KAFKA_TOPIC_ADS_TRACKERS: Final[str] = "ads_trackers_domains"

# Domain Validation
MAX_DOMAIN_LENGTH: Final[int] = 253
MIN_DOMAIN_LENGTH: Final[int] = 4
MAX_LABEL_LENGTH: Final[int] = 63

# Service Defaults
DEFAULT_LOG_LEVEL: Final[str] = "INFO"
DEFAULT_CONFIG_PATH: Final[str] = "src/config/sources.yaml"

# Resource Limits
MAX_CONCURRENT_FETCHES: Final[int] = 10
MAX_DOMAINS_PER_SOURCE: Final[int] = 1_000_000
MAX_BATCH_SIZE: Final[int] = 10_000

# Timeouts (in seconds)
PIPELINE_TIMEOUT: Final[int] = 3600  # 1 hour
VALIDATION_TIMEOUT: Final[int] = 300  # 5 minutes
KAFKA_PUBLISH_TIMEOUT: Final[int] = 600  # 10 minutes
