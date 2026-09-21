"""HTTP request and response models for the Phase 1 API."""

from datetime import datetime, timedelta, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from runtime.models import IncidentCreate, ObservationWindow


class HealthResponse(BaseModel):
    """Health state of the API and its local SQLite store."""

    service: str
    status: Literal["ok", "degraded"]
    database: Literal["ok", "error"]
    environment: str


class IncidentRequest(BaseModel):
    """Minimal incident input accepted by POST /incidents."""

    model_config = ConfigDict(str_strip_whitespace=True)

    title: str = Field(min_length=1, max_length=200)
    description: str = Field(min_length=1, max_length=4_000)
    time_window: str = Field(pattern=r"^[1-9][0-9]*[smhd]$")

    @field_validator("time_window")
    @classmethod
    def limit_time_window(cls, value: str) -> str:
        unit = value[-1]
        amount = int(value[:-1])
        seconds = amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        if seconds > 86400:
            raise ValueError("time_window must not exceed 24 hours")
        return value

    def to_runtime_request(self) -> IncidentCreate:
        """Convert the relative input window to an absolute UTC range."""

        unit = self.time_window[-1]
        amount = int(self.time_window[:-1])
        duration = timedelta(
            seconds=amount * {"s": 1, "m": 60, "h": 3600, "d": 86400}[unit]
        )
        end = datetime.now(timezone.utc)
        return IncidentCreate(
            title=self.title,
            description=self.description,
            observation_window=ObservationWindow(start=end - duration, end=end),
        )


class IncidentCreatedResponse(BaseModel):
    """Stable response returned after synchronous diagnosis completes."""

    incident_id: str
