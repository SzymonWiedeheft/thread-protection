"""Domain data model."""

from datetime import datetime, UTC
from typing import Optional
from pydantic import BaseModel, Field, ConfigDict


class DomainModel(BaseModel):
    """Domain data model."""

    domain: str = Field(..., description="Domain name (e.g., malicious-site.com)")
    category: str = Field(..., description="Category: malware or ads_trackers")
    source: str = Field(..., description="Source name (e.g., urlhaus, stevenblack)")
    source_format: str = Field(..., description="Format: hosts or adblock")
    raw_entry: str = Field(..., description="Original raw entry from source")
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Timestamp when data was ingested",
    )
    metadata: Optional[dict] = Field(
        default_factory=dict,
        description="Additional metadata (line_number, file_size, etc.)",
    )

    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "domain": "malicious-site.com",
                "category": "malware",
                "source": "urlhaus",
                "source_format": "adblock",
                "raw_entry": "||malicious-site.com^$important",
                "ingestion_timestamp": "2025-10-04T14:23:45.123Z",
                "metadata": {
                    "line_number": 12345,
                    "file_size_bytes": 12458900,
                    "http_status": 200,
                },
            }
        }
    )
