"""Pydantic request/response models for the local frontend API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GeneratePlansRequest(BaseModel):
    location: str
    radiusKm: float = Field(gt=0)
    transportMode: str
    vibe: str
    budget: str | None = None
    startTime: str | None = None
    durationMinutes: int = Field(gt=0)
    partySize: int = Field(gt=0)
    constraintsNote: str | None = None
    limit: int = Field(default=20, ge=1, le=50)


class PlanStop(BaseModel):
    id: str
    name: str
    description: str | None = None
    time: str | None = None
    transport: str | None = None
    mapsUrl: str | None = None


class TransportLeg(BaseModel):
    mode: str
    durationText: str


class PlanPayload(BaseModel):
    id: str
    title: str
    vibeLine: str
    heroImageUrl: str | None = None
    durationLabel: str
    costBand: str
    weather: str | None = None
    summary: str | None = None
    stops: list[PlanStop]
    transportLegs: list[TransportLeg] = Field(default_factory=list)


class GeneratePlansMeta(BaseModel):
    matchedCount: int
    returnedCount: int
    totalAvailable: int


class GeneratePlansResponse(BaseModel):
    plans: list[PlanPayload]
    warnings: list[str] = Field(default_factory=list)
    meta: GeneratePlansMeta


class PlanDetailResponse(BaseModel):
    plan: PlanPayload


class DateCatalogItem(BaseModel):
    id: str
    label: str
    count: int


class DatesMetadataResponse(BaseModel):
    totalPlans: int
    buckets: list[DateCatalogItem]
    templates: list[DateCatalogItem]
    vibes: list[DateCatalogItem]


class HealthResponse(BaseModel):
    status: str
    plansReady: bool
    plansCount: int
    assetsReady: bool
    imagesCount: int
    source: dict[str, Any]

