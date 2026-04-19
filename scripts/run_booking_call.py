"""Start or inspect Bland AI restaurant booking calls."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Sequence
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.clients.settings import (  # noqa: E402
    BlandAIConfigurationError,
    BlandAISettings,
)
from back_end.services.booking import (  # noqa: E402
    BookingRequestBuilder,
    BookingService,
    BookingValidationError,
    RestaurantBookingRequest,
)

logger = logging.getLogger("run_booking_call")

DEFAULT_TEST_RESTAURANT_PHONE = "+61491114073"
DEFAULT_CUSTOMER_PHONE = "+61491114073"
DEFAULT_RESTAURANT_NAME = "Test Restaurant"
DEFAULT_BOOKING_NAME = "Emma"
DEFAULT_PARTY_SIZE = 2
DEFAULT_TIME_WINDOW_MINUTES = 30
DEFAULT_TIMEZONE = "Australia/Sydney"
DEFAULT_BOOKING_TIME = time(hour=19, minute=30)
DEFAULT_BOOKING_WEEKDAY = 4  # Friday.


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Place a Bland AI restaurant-booking test call or inspect an "
            "existing call. The start command prints a dry-run payload unless "
            "--place-call is supplied."
        )
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    start = subparsers.add_parser(
        "start",
        help="Build or place a restaurant booking call.",
    )
    start.add_argument(
        "--restaurant-phone",
        default=DEFAULT_TEST_RESTAURANT_PHONE,
        help="Australian restaurant number in E.164 format.",
    )
    start.add_argument("--restaurant-name", default=DEFAULT_RESTAURANT_NAME)
    start.add_argument(
        "--arrival-time",
        default=None,
        help=(
            "ISO datetime for the requested booking. If omitted, the next "
            "Friday at 7:30 PM in --timezone is used."
        ),
    )
    start.add_argument("--timezone", default=DEFAULT_TIMEZONE)
    start.add_argument("--party-size", type=int, default=DEFAULT_PARTY_SIZE)
    start.add_argument("--booking-name", default=DEFAULT_BOOKING_NAME)
    start.add_argument(
        "--customer-phone",
        default=DEFAULT_CUSTOMER_PHONE,
        help="Australian callback number in E.164 format.",
    )
    start.add_argument("--restaurant-address", default=None)
    start.add_argument("--dietary-constraints", default=None)
    start.add_argument("--accessibility-constraints", default=None)
    start.add_argument("--special-occasion", default=None)
    start.add_argument("--notes", default=None)
    start.add_argument(
        "--time-window-minutes",
        type=int,
        default=DEFAULT_TIME_WINDOW_MINUTES,
        help=(
            "Allowed same-day booking-time movement. Use 0 for exact time only."
        ),
    )
    start.add_argument("--plan-id", default="manual-cli-test")
    start.add_argument(
        "--place-call",
        action="store_true",
        help="Actually send the Bland AI call. Without this, no network call is made.",
    )

    status = subparsers.add_parser(
        "status",
        help="Fetch status for a Bland AI call id.",
    )
    status.add_argument("--call-id", required=True)

    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


async def async_main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    _load_repo_env()

    try:
        if args.command == "start":
            return await _start(args)
        if args.command == "status":
            return await _status(args)
    except (BlandAIConfigurationError, BookingValidationError, ValueError) as exc:
        logger.error("Booking call CLI failed: %s", exc)
        return 1
    except Exception:
        logger.exception("Booking call CLI failed unexpectedly.")
        return 1

    logger.error("Unknown booking CLI command: %r", args.command)
    return 1


async def _start(args: argparse.Namespace) -> int:
    request = build_booking_request(args)
    if not args.place_call:
        settings = BlandAISettings(api_key="dry-run-not-used")
        payload = build_call_payload(request, settings)
        logger.info(
            "Dry run only. Add --place-call to queue a real Bland AI call to %s.",
            request.restaurant_phone_number,
        )
        print(
            json.dumps(
                {
                    "dry_run": True,
                    "place_call_required_for_network": True,
                    "call_payload": payload,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    settings = BlandAISettings.from_env()
    async with BookingService(settings) as service:
        job = await service.start_restaurant_booking(request)

    print(
        json.dumps(
            {
                "dry_run": False,
                "provider": job.provider,
                "call_id": job.call_id,
                "status": job.status.value,
                "restaurant_name": job.restaurant_name,
                "restaurant_phone_number": job.restaurant_phone_number,
                "arrival_time": job.arrival_time.isoformat(),
                "party_size": job.party_size,
                "request_data": job.request_data,
            },
            indent=2,
            sort_keys=True,
        )
    )
    logger.info("Queued Bland AI booking call_id=%s.", job.call_id)
    return 0


async def _status(args: argparse.Namespace) -> int:
    settings = BlandAISettings.from_env()
    async with BookingService(settings) as service:
        status = await service.get_booking_status(args.call_id)

    print(
        json.dumps(
            {
                "call_id": status.call_id,
                "status": status.status.value,
                "provider_status": status.provider_status,
                "queue_status": status.queue_status,
                "answered_by": status.answered_by,
                "summary": status.summary,
                "error_message": status.error_message,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def build_booking_request(
    args: argparse.Namespace,
    *,
    now: datetime | None = None,
) -> RestaurantBookingRequest:
    timezone = _load_timezone(args.timezone)
    arrival_time = _parse_arrival_time(args.arrival_time, timezone, now=now)
    if args.time_window_minutes < 0:
        raise BookingValidationError("--time-window-minutes must be >= 0.")

    return RestaurantBookingRequest(
        restaurant_name=args.restaurant_name,
        restaurant_phone_number=args.restaurant_phone,
        arrival_time=arrival_time,
        party_size=args.party_size,
        booking_name=args.booking_name,
        customer_phone_number=args.customer_phone,
        restaurant_address=args.restaurant_address,
        dietary_constraints=args.dietary_constraints,
        accessibility_constraints=args.accessibility_constraints,
        special_occasion=args.special_occasion,
        notes=args.notes,
        acceptable_time_window_minutes=args.time_window_minutes,
        plan_id=args.plan_id,
    )


def build_call_payload(
    request: RestaurantBookingRequest,
    settings: BlandAISettings,
) -> dict[str, object]:
    return BookingRequestBuilder(settings).build(request).to_payload()


def _parse_arrival_time(
    raw_value: str | None,
    timezone: ZoneInfo,
    *,
    now: datetime | None = None,
) -> datetime:
    current_time = now.astimezone(timezone) if now else datetime.now(timezone)
    if raw_value is None:
        value = _default_arrival_time(current_time)
        logger.info(
            "No --arrival-time supplied; using explicit test default %s.",
            value.isoformat(),
        )
    else:
        try:
            value = datetime.fromisoformat(raw_value)
        except ValueError as exc:
            raise ValueError(
                "--arrival-time must be an ISO datetime, for example "
                "2026-05-01T19:30:00."
            ) from exc
        if value.tzinfo is None or value.utcoffset() is None:
            logger.info(
                "Interpreting naive --arrival-time %s in timezone %s.",
                raw_value,
                timezone.key,
            )
            value = value.replace(tzinfo=timezone)
        else:
            value = value.astimezone(timezone)

    if value <= current_time:
        raise BookingValidationError(
            f"--arrival-time must be in the future, got {value.isoformat()}."
        )
    return value


def _default_arrival_time(now: datetime) -> datetime:
    days_until_target = (DEFAULT_BOOKING_WEEKDAY - now.weekday()) % 7
    candidate_date = now.date() + timedelta(days=days_until_target)
    candidate = datetime.combine(
        candidate_date,
        DEFAULT_BOOKING_TIME,
        tzinfo=now.tzinfo,
    )
    if candidate <= now:
        candidate += timedelta(days=7)
    return candidate


def _load_timezone(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"Unknown timezone {name!r}.") from exc


def _load_repo_env() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if not key or key in os.environ:
            continue
        os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
