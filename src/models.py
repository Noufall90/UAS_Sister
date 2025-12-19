from pydantic import BaseModel, Field
from typing import Dict, Any, List, Union
from datetime import datetime
import uuid


class Event(BaseModel):
    """Event model untuk Pub-Sub log aggregator"""
    topic: str = Field(..., min_length=1, max_length=255, description="Topic name")
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique event identifier (UUID atau custom string)"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.utcnow().isoformat() + "Z",
        description="ISO8601 timestamp"
    )
    source: str = Field(..., min_length=1, max_length=255, description="Event source")
    payload: Dict[str, Any] = Field(default_factory=dict, description="Event payload")

    class Config:
        json_schema_extra = {
            "example": {
                "topic": "logs.application",
                "event_id": "evt-12345",
                "timestamp": "2025-12-18T10:30:00Z",
                "source": "service-a",
                "payload": {"level": "INFO", "message": "Application started"}
            }
        }


class PublishRequest(BaseModel):
    """Request model untuk publish event (single atau batch)"""
    events: Union[Event, List[Event]] = Field(..., description="Single event atau list of events")

    def get_events(self) -> List[Event]:
        """Convert to list of events"""
        if isinstance(self.events, list):
            return self.events
        return [self.events]


class PublishResponse(BaseModel):
    """Response dari /publish endpoint"""
    status: str
    count: int
    accepted: int
    rejected: int
    errors: List[Dict[str, Any]] = []


class EventResponse(BaseModel):
    """Response model untuk event yang dikembalikan"""
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]


class StatsResponse(BaseModel):
    """Response dari /stats endpoint"""
    received: int = Field(description="Total event diterima")
    unique_processed: int = Field(description="Event unik yang diproses")
    duplicate_dropped: int = Field(description="Duplikat yang dijatuhkan")
    topics: List[str] = Field(description="Daftar topik yang diproses")
    uptime_seconds: float = Field(description="Uptime dalam detik")
    unique_rate: float = Field(description="Persentase event yang unik")
    duplicate_rate: float = Field(description="Persentase duplikat")


class HealthResponse(BaseModel):
    """Response dari /health endpoint"""
    status: str
    timestamp: str
    version: str = "1.0.0"
