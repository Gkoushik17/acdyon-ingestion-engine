"""
Data models and schemas using Pydantic V2.
Provides strong typing, normalization validation, and schema drift anomaly detection.
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, HttpUrl, field_validator
import hashlib
import re


class RawJobItem(BaseModel):
    """Raw extracted job listing prior to normalization."""
    source_name: str
    raw_title: Optional[str] = None
    raw_company: Optional[str] = None
    raw_location: Optional[str] = None
    raw_description: Optional[str] = None
    raw_url: Optional[str] = None
    raw_salary: Optional[str] = None
    raw_tags: Optional[List[str]] = Field(default_factory=list)
    raw_published_at: Optional[str] = None
    extracted_at: datetime = Field(default_factory=datetime.utcnow)


class NormalizedJob(BaseModel):
    """Canonical normalized job listing stored in SQLite."""
    id: str = Field(..., description="Deterministic unique SHA256 hash for deduplication")
    source: str = Field(..., description="Origin source identifier (e.g. RemoteOK, WWR, Sandbox)")
    title: str = Field(..., min_length=2, description="Cleaned, standardized job title")
    company: str = Field(..., min_length=1, description="Company name")
    location: str = Field(default="Remote", description="Cleaned location or 'Remote'")
    description_snippet: str = Field(..., description="Sanitized text excerpt without raw HTML tags")
    url: str = Field(..., description="Direct job listing URL")
    salary: Optional[str] = Field(default=None, description="Normalized compensation range if available")
    tags: List[str] = Field(default_factory=list, description="Extracted category tags")
    published_date: Optional[str] = Field(default=None, description="ISO-formatted or sanitized publication date")
    ingested_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())

    @staticmethod
    def generate_id(source: str, title: str, company: str, url: str) -> str:
        """Generates deterministic deduplication hash."""
        slug = f"{source.lower()}:{title.lower().strip()}:{company.lower().strip()}:{url.strip()}"
        return hashlib.sha256(slug.encode("utf-8")).hexdigest()[:16]


class SchemaDriftReport(BaseModel):
    """Telemetry payload created when incoming markup or JSON does not match expectations."""
    source_name: str
    missing_fields: List[str]
    unexpected_structure: bool = False
    drift_score: float = Field(0.0, description="Ratio of unparseable items (0.0 to 1.0)")
    sample_payload_snippet: str
    detected_at: str = Field(default_factory=lambda: datetime.utcnow().isoformat())


class IngestionProgressEvent(BaseModel):
    """Event emitted over Server-Sent Events (SSE) to update the live UI."""
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().strftime("%H:%M:%S.%f")[:-3])
    level: str = Field(default="INFO", description="DEBUG, INFO, WARNING, ERROR, SUCCESS")
    stage: str = Field(..., description="e.g. PACING, FETCHING, CIRCUIT_CHECK, NORMALIZING, PERSISTING")
    source: Optional[str] = None
    message: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class CircuitBreakerStatus(BaseModel):
    """Current state of the target circuit breaker."""
    source: str
    state: str  # CLOSED, OPEN, HALF_OPEN
    failure_count: int
    failure_threshold: int
    recovery_time_seconds: float
    last_failure_reason: Optional[str] = None
    tripped_at: Optional[str] = None
