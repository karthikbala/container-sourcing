from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Candidate(BaseModel):
    schema_version: int = 1
    id: str
    container_number: str
    reference: str | None = None
    reference_kind: str | None = None
    source: str
    trip_id: str
    retrieved_at: str
    fetched_at: str | None = None
    ports: list[dict] = Field(default_factory=list)
    events: list[dict] = Field(default_factory=list)
    evidence: dict = Field(default_factory=dict)
    quality_flags: list[str] = Field(default_factory=list)
    portal_verified: bool = False
    api_verified: bool = False


class SourceReference(BaseModel):
    id: str
    reference: str
    carrier: str | None = None
    reference_kind: str = 'bill_of_lading'
    source: str
    retrieved_at: str
    reported_at: str | None = None
    port: str | None = None
    container_hint: str | None = None
    evidence: dict = Field(default_factory=dict)


class Proposal(BaseModel):
    candidate: Candidate
    terminal_code: str
    match_basis: str
    direction: str
    disposition: str
    priority: int
    reasons: list[str] = Field(default_factory=list)
    movement_at: str | None = None

    @property
    def eligible(self) -> bool:
        return self.disposition in {'candidate_for_portal_test', 'needs_terminal_resolution'}
