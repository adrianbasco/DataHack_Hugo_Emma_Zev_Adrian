"""Pydantic request/response models for the local frontend API."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

from back_end.search.models import (
    LocationInput,
    SearchContext,
    SearchCoordinates,
    SearchRequest,
    StructuredFilters,
    WeatherPreference,
)
from back_end.services.booking import RestaurantBookingRequest

BookingStatusValue = Literal[
    "queued",
    "in_progress",
    "confirmed",
    "declined",
    "no_answer",
    "needs_human_follow_up",
    "failed",
    "unknown",
]


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


class DateTemplateStopPayload(BaseModel):
    type: str
    kind: Literal["connective", "venue"] = "venue"
    note: str | None = None


class DateTemplatePayload(BaseModel):
    id: str
    title: str
    vibes: list[str] = Field(default_factory=list)
    timeOfDay: str
    durationHours: float = Field(gt=0)
    meaningfulVariations: int = Field(ge=0)
    weatherSensitive: bool
    description: str
    stops: list[DateTemplateStopPayload] = Field(default_factory=list)


class TemplatesResponse(BaseModel):
    templates: list[DateTemplatePayload] = Field(default_factory=list)


class PlanStop(BaseModel):
    id: str
    kind: Literal["connective", "venue"] = "venue"
    stopType: str
    name: str
    description: str = ""
    whyItFits: str | None = None
    time: str | None = None
    transport: str | None = None
    mapsUrl: str | None = None
    address: str | None = None
    phoneNumber: str | None = None


class TransportLeg(BaseModel):
    mode: str
    durationText: str


class BookingContextPayload(BaseModel):
    planId: str | None = None
    restaurantName: str | None = None
    restaurantPhoneNumber: str | None = None
    restaurantAddress: str | None = None
    suggestedArrivalTimeIso: str | None = None
    partySize: int | None = None


class PlanPayload(BaseModel):
    id: str
    title: str
    hook: str
    summary: str | None = None
    vibes: list[str] = Field(default_factory=list)
    templateHint: str | None = None
    templateId: str | None = None
    durationLabel: str
    costBand: str
    weather: str | None = None
    heroImageUrl: str | None = None
    mapsVerificationNeeded: bool = False
    constraintsConsidered: list[str] = Field(default_factory=list)
    stops: list[PlanStop]
    transportLegs: list[TransportLeg] = Field(default_factory=list)
    bookingContext: BookingContextPayload | None = None
    source: Literal["api"] = "api"


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


class ClientErrorPayload(BaseModel):
    source: str
    message: str
    stack: str | None = None
    platform: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)


class RestaurantBookingRequestPayload(BaseModel):
    restaurantName: str
    restaurantPhoneNumber: str | None = None
    arrivalTimeIso: str
    partySize: int = Field(gt=0)
    bookingName: str
    customerPhoneNumber: str | None = None
    restaurantAddress: str | None = None
    dietaryConstraints: str | None = None
    accessibilityConstraints: str | None = None
    specialOccasion: str | None = None
    notes: str | None = None
    acceptableTimeWindowMinutes: int | None = Field(default=None, ge=0)
    planId: str | None = None

    def to_internal(self) -> RestaurantBookingRequest:
        try:
            arrival_time = datetime.fromisoformat(self.arrivalTimeIso)
        except ValueError as exc:
            raise ValueError("arrivalTimeIso must be a valid ISO datetime string.") from exc

        return RestaurantBookingRequest(
            restaurant_name=self.restaurantName,
            restaurant_phone_number=self.restaurantPhoneNumber,
            arrival_time=arrival_time,
            party_size=self.partySize,
            booking_name=self.bookingName,
            customer_phone_number=self.customerPhoneNumber,
            restaurant_address=self.restaurantAddress,
            dietary_constraints=self.dietaryConstraints,
            accessibility_constraints=self.accessibilityConstraints,
            special_occasion=self.specialOccasion,
            notes=self.notes,
            acceptable_time_window_minutes=self.acceptableTimeWindowMinutes,
            plan_id=self.planId,
        )


class BlandCallDescriptionPayload(BaseModel):
    provider: Literal["bland_ai"] = "bland_ai"
    phoneNumber: str
    firstSentence: str | None = None
    task: str | None = None
    voice: str | None = None
    model: str | None = None
    language: str | None = None
    timezone: str | None = None
    maxDurationMinutes: int | None = None
    waitForGreeting: bool
    record: bool
    voicemail: dict[str, Any] | None = None
    requestData: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    dispositions: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    summaryPrompt: str | None = None


class RestaurantBookingPreviewResponse(BaseModel):
    bookingContext: BookingContextPayload
    callDescription: BlandCallDescriptionPayload
    liveCallEnabled: bool
    liveCallDisabledReason: str | None = None


class RestaurantBookingJobPayload(BaseModel):
    callId: str
    status: BookingStatusValue
    provider: Literal["bland_ai"]
    restaurantName: str
    restaurantPhoneNumber: str | None = None
    arrivalTimeIso: str
    partySize: int


class RestaurantBookingStatusPayload(BaseModel):
    callId: str
    status: BookingStatusValue
    providerStatus: str | None = None
    queueStatus: str | None = None
    answeredBy: str | None = None
    summary: str | None = None
    errorMessage: str | None = None


class SearchCoordinatesPayload(BaseModel):
    lat: float
    lng: float

    def to_internal(self) -> SearchCoordinates:
        return SearchCoordinates(lat=self.lat, lng=self.lng)


class LocationInputPayload(BaseModel):
    text: str | None = None
    radius_km: float | None = Field(default=None, gt=0)

    def to_internal(self) -> LocationInput:
        return LocationInput(text=self.text, radius_km=self.radius_km)


class SearchContextPayload(BaseModel):
    now_iso: str | None = None
    user_location: SearchCoordinatesPayload | None = None
    exclude_plan_ids: list[str] = Field(default_factory=list)
    limit: int | None = Field(default=None, ge=1, le=50)

    def to_internal(self) -> SearchContext:
        return SearchContext(
            now_iso=self.now_iso,
            user_location=self.user_location.to_internal()
            if self.user_location is not None
            else None,
            exclude_plan_ids=tuple(self.exclude_plan_ids),
            limit=self.limit,
        )


class StructuredFiltersPayload(BaseModel):
    vibes: list[str] = Field(default_factory=list)
    time_of_day: str | None = None
    weather_ok: str | None = None
    location: LocationInputPayload | None = None
    transport_mode: str | None = None
    template_hints: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_weather_ok(self) -> "StructuredFiltersPayload":
        if self.weather_ok is None:
            return self
        if self.weather_ok not in {
            WeatherPreference.INDOORS_ONLY.value,
            WeatherPreference.OUTDOORS_OK.value,
        }:
            raise ValueError(
                "weather_ok must be 'indoors_only' or 'outdoors_ok' when supplied."
            )
        return self

    def is_empty(self) -> bool:
        return not any(
            [
                self.vibes,
                self.time_of_day,
                self.weather_ok,
                self.location is not None
                and (self.location.text is not None or self.location.radius_km is not None),
                self.transport_mode,
                self.template_hints,
            ]
        )

    def to_internal(self) -> StructuredFilters:
        return StructuredFilters(
            vibes=tuple(self.vibes),
            time_of_day=self.time_of_day,
            weather_ok=WeatherPreference(self.weather_ok)
            if self.weather_ok is not None
            else None,
            location=self.location.to_internal() if self.location is not None else None,
            transport_mode=self.transport_mode,
            template_hints=tuple(self.template_hints),
        )


class SearchRequestPayload(BaseModel):
    query: str | None = None
    context: SearchContextPayload | None = None
    overrides: StructuredFiltersPayload | None = None

    @model_validator(mode="after")
    def validate_search_request(self) -> "SearchRequestPayload":
        has_query = self.query is not None and self.query.strip() != ""
        has_overrides = self.overrides is not None and not self.overrides.is_empty()
        if not has_query and not has_overrides:
            raise ValueError("At least one of query or overrides must be provided.")
        return self

    def to_internal(self) -> SearchRequest:
        return SearchRequest(
            query=self.query,
            context=self.context.to_internal() if self.context is not None else None,
            overrides=self.overrides.to_internal() if self.overrides is not None else None,
        )
