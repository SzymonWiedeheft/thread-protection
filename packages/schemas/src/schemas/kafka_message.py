"""Kafka message schema."""

from datetime import datetime
from typing import Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class KafkaMessageSchema(BaseModel):
    """Schema for Kafka messages."""

    domain: str = Field(..., description="Domain name")
    category: str = Field(..., description="Category: malware or ads_trackers")
    source: str = Field(..., description="Source name")
    source_format: str = Field(..., description="Format: hosts or adblock")
    raw_entry: str = Field(..., description="Original raw entry")
    ingestion_timestamp: str = Field(..., description="ISO timestamp of ingestion")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Metadata")

    def to_json(self) -> str:
        """Convert to JSON string for Kafka."""
        return self.model_dump_json()

    @classmethod
    def from_domain_model(cls, domain_model: Any) -> "KafkaMessageSchema":
        """Create from DomainModel."""
        return cls(
            domain=domain_model.domain,
            category=domain_model.category,
            source=domain_model.source,
            source_format=domain_model.source_format,
            raw_entry=domain_model.raw_entry,
            ingestion_timestamp=domain_model.ingestion_timestamp.isoformat()
            if isinstance(domain_model.ingestion_timestamp, datetime)
            else domain_model.ingestion_timestamp,
            metadata=domain_model.metadata or {},
        )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "domain": "malicious-site.com",
                "category": "malware",
                "source": "urlhaus",
                "source_format": "adblock",
                "raw_entry": "||malicious-site.com^",
                "ingestion_timestamp": "2025-10-04T14:23:45.123Z",
                "metadata": {
                    "line_number": 12345,
                    "http_status": 200,
                },
            }
        }
    )
