"""Restaurant booking service backed by Bland AI phone calls."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any

from back_end.clients.bland import BlandAIClient, BlandCallDetails, BlandCallRequest
from back_end.clients.settings import BlandAISettings

logger = logging.getLogger(__name__)

BOOKING_DISPOSITIONS = (
    "booking_confirmed",
    "restaurant_unavailable",
    "booking_declined",
    "needs_human_follow_up",
    "no_answer",
    "failed",
)
E164_PHONE_RE = re.compile(r"^\+[1-9]\d{6,14}$")
AUSTRALIAN_E164_PHONE_RE = re.compile(r"^\+61[23478]\d{8}$")


class BookingValidationError(ValueError):
    """Raised when a restaurant booking request is incomplete or unsafe."""


class BookingStatus(str, Enum):
    """Internal booking status values returned to callers."""

    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    CONFIRMED = "confirmed"
    DECLINED = "declined"
    NO_ANSWER = "no_answer"
    NEEDS_HUMAN_FOLLOW_UP = "needs_human_follow_up"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class RestaurantBookingRequest:
    """Inputs required to ask a restaurant for a table booking."""

    restaurant_name: str
    arrival_time: datetime
    party_size: int
    booking_name: str
    restaurant_phone_number: str | None = None
    customer_phone_number: str | None = None
    restaurant_address: str | None = None
    dietary_constraints: str | None = None
    accessibility_constraints: str | None = None
    special_occasion: str | None = None
    notes: str | None = None
    acceptable_time_window_minutes: int | None = None
    plan_id: str | None = None


@dataclass(frozen=True)
class RestaurantBookingJob:
    """Booking job returned after Bland AI accepts a call request."""

    call_id: str
    status: BookingStatus
    provider: str
    restaurant_name: str
    restaurant_phone_number: str | None
    arrival_time: datetime
    party_size: int
    request_data: dict[str, Any]


@dataclass(frozen=True)
class RestaurantBookingCallStatus:
    """Current booking status derived from Bland AI call details."""

    call_id: str
    status: BookingStatus
    provider_status: str | None
    queue_status: str | None
    answered_by: str | None
    summary: str | None
    error_message: str | None
    raw_details: BlandCallDetails


class BookingRequestBuilder:
    """Builds constrained Bland AI requests for restaurant reservations."""

    def __init__(self, settings: BlandAISettings) -> None:
        self._settings = settings

    def build(self, request: RestaurantBookingRequest) -> BlandCallRequest:
        _validate_booking_request(request)
        _validate_australian_phone_number(
            self._settings.booking_phone_number,
            "BLAND_AI_BOOKING_PHONE_NUMBER",
        )
        request_data = _booking_request_data(request)
        task = _booking_task(request, request_data)
        first_sentence = (
            f"Heyyy, I am calling on behalf of {request.booking_name} to book "
            f"a table at {request.restaurant_name}."
        )
        return BlandCallRequest(
            phone_number=self._settings.booking_phone_number,
            task=task,
            first_sentence=first_sentence,
            voice=self._settings.default_voice,
            model=self._settings.model,
            language=self._settings.language,
            timezone=self._settings.timezone,
            max_duration=self._settings.max_duration_minutes,
            wait_for_greeting=True,
            record=self._settings.record_calls,
            voicemail={"action": "hangup"},
            request_data=request_data,
            metadata={
                "purpose": "restaurant_booking",
                "provider": "bland_ai",
                "configured_call_target": self._settings.booking_phone_number,
                **({"plan_id": request.plan_id} if request.plan_id else {}),
            },
            dispositions=BOOKING_DISPOSITIONS,
            keywords=(request.restaurant_name, request.booking_name),
            summary_prompt=(
                "Summarize whether the table booking was confirmed, declined, "
                "not answered, or needs human follow-up. Include the confirmed "
                "date, time, party size, booking name, staff name, and "
                "confirmation number if any were provided."
            ),
        )


class BookingService:
    """Starts and inspects restaurant booking calls."""

    def __init__(
        self,
        settings: BlandAISettings | None = None,
        *,
        client: BlandAIClient | None = None,
    ) -> None:
        self._settings = settings or BlandAISettings.from_env()
        self._client = client or BlandAIClient(self._settings)
        self._owns_client = client is None
        self._builder = BookingRequestBuilder(self._settings)

    async def __aenter__(self) -> "BookingService":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def start_restaurant_booking(
        self,
        request: RestaurantBookingRequest,
    ) -> RestaurantBookingJob:
        """Queue a real phone call to make a restaurant booking."""

        call_request = self._builder.build(request)
        queued = await self._client.send_call(call_request)
        logger.info(
            "Queued Bland AI restaurant booking call_id=%s restaurant=%r party_size=%s.",
            queued.call_id,
            request.restaurant_name,
            request.party_size,
        )
        return RestaurantBookingJob(
            call_id=queued.call_id,
            status=BookingStatus.QUEUED,
            provider="bland_ai",
            restaurant_name=request.restaurant_name,
            restaurant_phone_number=request.restaurant_phone_number,
            arrival_time=request.arrival_time,
            party_size=request.party_size,
            request_data=call_request.request_data,
        )

    async def get_booking_status(self, call_id: str) -> RestaurantBookingCallStatus:
        """Fetch and normalize booking call status from Bland AI."""

        details = await self._client.get_call_details(call_id)
        status = _map_bland_status(details)
        error_message = _normalized_status_error_message(details, status)
        if status in {BookingStatus.FAILED, BookingStatus.UNKNOWN}:
            logger.error(
                "Bland AI booking call_id=%s returned normalized_status=%s provider_status=%s "
                "queue_status=%s disposition=%s error=%r.",
                details.call_id,
                status.value,
                details.status,
                details.queue_status,
                details.disposition_tag,
                error_message,
            )
        elif status is BookingStatus.NEEDS_HUMAN_FOLLOW_UP:
            logger.warning(
                "Bland AI booking call_id=%s requires human follow-up. "
                "provider_status=%s disposition=%s summary=%r",
                details.call_id,
                details.status,
                details.disposition_tag,
                details.summary,
            )
        return RestaurantBookingCallStatus(
            call_id=details.call_id,
            status=status,
            provider_status=details.status,
            queue_status=details.queue_status,
            answered_by=details.answered_by,
            summary=details.summary,
            error_message=error_message,
            raw_details=details,
        )


def _validate_booking_request(request: RestaurantBookingRequest) -> None:
    if not request.restaurant_name.strip():
        raise BookingValidationError("restaurant_name must not be empty.")
    if not request.booking_name.strip():
        raise BookingValidationError("booking_name must not be empty.")
    if request.party_size <= 0:
        raise BookingValidationError("party_size must be positive.")
    if request.arrival_time.tzinfo is None or request.arrival_time.utcoffset() is None:
        raise BookingValidationError("arrival_time must be timezone-aware.")
    if request.restaurant_phone_number is not None:
        _validate_australian_phone_number(
            request.restaurant_phone_number,
            "restaurant_phone_number",
        )
    if (
        request.acceptable_time_window_minutes is not None
        and request.acceptable_time_window_minutes < 0
    ):
        raise BookingValidationError(
            "acceptable_time_window_minutes must be >= 0 when provided."
        )
    if request.customer_phone_number is not None:
        _validate_australian_phone_number(
            request.customer_phone_number,
            "customer_phone_number",
        )


def _booking_request_data(request: RestaurantBookingRequest) -> dict[str, Any]:
    data = {
        "restaurant_name": request.restaurant_name.strip(),
        "booking_name": request.booking_name.strip(),
        "party_size": request.party_size,
        "arrival_time_iso": request.arrival_time.isoformat(),
        "arrival_time_local": _format_local_datetime(request.arrival_time),
    }
    optional_fields = {
        "restaurant_phone_number": request.restaurant_phone_number,
        "customer_phone_number": request.customer_phone_number,
        "restaurant_address": request.restaurant_address,
        "dietary_constraints": request.dietary_constraints,
        "accessibility_constraints": request.accessibility_constraints,
        "special_occasion": request.special_occasion,
        "notes": request.notes,
        "acceptable_time_window_minutes": request.acceptable_time_window_minutes,
        "plan_id": request.plan_id,
    }
    for key, value in optional_fields.items():
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                data[key] = stripped
        elif value is not None:
            data[key] = value
    return data


def _booking_task(
    request: RestaurantBookingRequest,
    request_data: dict[str, Any],
) -> str:
    local_time = request_data["arrival_time_local"]
    fallback_instruction = (
        "If that exact time is unavailable, do not book a different time. "
        "Politely ask if the exact time is possible, and if not, end the call "
        "after saying the booking could not be completed."
    )
    if request.acceptable_time_window_minutes is not None:
        minutes = request.acceptable_time_window_minutes
        fallback_instruction = (
            f"If the exact time is unavailable, you may accept the nearest "
            f"same-day time within {minutes} minutes of the requested time. "
            "Do not accept a time outside that window."
        )

    context_lines = [
        "You are making a restaurant reservation.",
        f"Restaurant: {request.restaurant_name.strip()}.",
        f"Book for: {request.party_size} people.",
        f"Requested time: {local_time}.",
        f"Booking name: {request.booking_name.strip()}.",
        fallback_instruction,
        "Ask for a confirmation number or staff name if the booking is accepted.",
        "Repeat the final booking details before ending the call.",
        "Do not provide payment card details, passwords, or sensitive information.",
        "If a card, deposit, or prepayment is required, do not complete the booking.",
        "If the restaurant cannot book the table, end politely without inventing a booking.",
    ]
    optional_context = [
        ("Customer phone", request.customer_phone_number),
        ("Restaurant address", request.restaurant_address),
        ("Dietary constraints", request.dietary_constraints),
        ("Accessibility constraints", request.accessibility_constraints),
        ("Special occasion", request.special_occasion),
        ("Extra notes", request.notes),
    ]
    for label, value in optional_context:
        text = value.strip() if isinstance(value, str) else None
        if text:
            context_lines.append(f"{label}: {text}.")

    task = "\n".join(context_lines)
    if len(task) > 2000:
        logger.error(
            "Bland AI booking task exceeded recommended prompt length: %d chars.",
            len(task),
        )
        raise BookingValidationError(
            "Restaurant booking context is too long for a safe Bland AI task prompt."
        )
    return task


def _map_bland_status(details: BlandCallDetails) -> BookingStatus:
    provider_status = (details.status or "").lower()
    queue_status = (details.queue_status or "").lower()
    disposition = (details.disposition_tag or "").lower()
    answered_by = (details.answered_by or "").lower()

    if disposition == "booking_confirmed":
        return BookingStatus.CONFIRMED
    if disposition == "needs_human_follow_up":
        return BookingStatus.NEEDS_HUMAN_FOLLOW_UP
    if disposition in {"booking_declined", "restaurant_unavailable"}:
        return BookingStatus.DECLINED
    if disposition == "no_answer" or provider_status in {"no-answer", "busy"}:
        return BookingStatus.NO_ANSWER
    if provider_status in {"failed", "canceled"} or queue_status.endswith("_error"):
        return BookingStatus.FAILED
    if details.completed is False or queue_status in {
        "new",
        "queued",
        "allocated",
        "started",
    }:
        return BookingStatus.IN_PROGRESS
    if answered_by in {"no-answer", "voicemail"}:
        return BookingStatus.NO_ANSWER
    if details.completed is True and provider_status == "completed":
        logger.error(
            "Bland AI call_id=%s completed without a recognized booking disposition.",
            details.call_id,
        )
        return BookingStatus.UNKNOWN
    return BookingStatus.UNKNOWN


def _normalized_status_error_message(
    details: BlandCallDetails,
    status: BookingStatus,
) -> str | None:
    if details.error_message:
        return details.error_message
    if status is BookingStatus.FAILED:
        return "Bland AI reported a failed booking call."
    if status is BookingStatus.UNKNOWN:
        return "Bland AI returned an unexpected booking status."
    return None


def _format_local_datetime(value: datetime) -> str:
    hour = value.strftime("%I").lstrip("0") or "0"
    minute = value.strftime("%M")
    suffix = value.strftime("%p")
    timezone_name = value.tzname() or "local time"
    return value.strftime(
        f"%A %d %B %Y at {hour}:{minute} {suffix} {timezone_name}"
    )


def _validate_e164(value: str, field_name: str) -> None:
    if not E164_PHONE_RE.fullmatch(value):
        raise BookingValidationError(
            f"{field_name} must be in E.164 format, got {value!r}."
        )


def _validate_australian_phone_number(value: str, field_name: str) -> None:
    _validate_e164(value, field_name)
    if not AUSTRALIAN_E164_PHONE_RE.fullmatch(value):
        raise BookingValidationError(
            f"{field_name} must be an Australian mobile or geographic number "
            f"in E.164 format, got {value!r}."
        )
