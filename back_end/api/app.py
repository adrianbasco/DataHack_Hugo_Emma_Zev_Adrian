"""FastAPI app exposing cached plans, deterministic booking previews, and search."""

from __future__ import annotations

import logging
from collections.abc import Callable

from fastapi.encoders import jsonable_encoder
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError

from fastapi.middleware.cors import CORSMiddleware

from back_end.api.models import (
    BlandCallDescriptionPayload,
    ClientErrorPayload,
    DatesMetadataResponse,
    GeneratePlansRequest,
    GeneratePlansResponse,
    HealthResponse,
    PlanDetailResponse,
    PlanPayload,
    RestaurantBookingJobPayload,
    RestaurantBookingPreviewResponse,
    RestaurantBookingRequestPayload,
    RestaurantBookingStatusPayload,
    SearchRequestPayload,
    TemplatesResponse,
)
from back_end.api.service import FrontendApiError, FrontendPlanService
from back_end.clients.settings import (
    BlandAIConfigurationError,
    BlandAISettings,
    bland_ai_booking_phone_number_from_env,
)
from back_end.search import SearchService
from back_end.services.booking import (
    BookingRequestBuilder,
    BookingService,
    BookingValidationError,
)

logger = logging.getLogger(__name__)


def create_app(
    *,
    service: FrontendPlanService | None = None,
    search_service: SearchService | None = None,
    booking_service_factory: Callable[[], BookingService] | None = None,
) -> FastAPI:
    """Create the async API app."""

    plan_service = service or FrontendPlanService()
    search = search_service or SearchService()
    booking_factory = booking_service_factory or BookingService

    app = FastAPI(title="Date Night Frontend API")
    app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ok for dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    app.mount(
        "/static/precache-images",
        StaticFiles(directory=plan_service.assets_dir, check_dir=False),
        name="precache-images",
    )

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> dict:
        return await plan_service.health()

    @app.post("/client-errors")
    async def report_client_error(payload: ClientErrorPayload) -> dict[str, str]:
        logger.error(
            "Frontend client error source=%s platform=%s message=%s context=%s stack=%s",
            payload.source,
            payload.platform,
            payload.message,
            payload.context,
            payload.stack,
        )
        return {"status": "logged"}

    @app.get("/templates", response_model=TemplatesResponse)
    async def list_templates() -> TemplatesResponse:
        try:
            templates = await plan_service.list_templates()
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return TemplatesResponse(templates=templates)

    @app.get("/dates/metadata", response_model=DatesMetadataResponse)
    async def dates_metadata() -> dict:
        try:
            return await plan_service.metadata()
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/dates", response_model=list[PlanPayload])
    async def list_dates(
        request: Request,
        limit: int = Query(default=20, ge=1, le=50),
        bucket_id: str | None = Query(default=None),
        template_id: str | None = Query(default=None),
        vibe: str | None = Query(default=None),
    ) -> list[PlanPayload]:
        try:
            return await plan_service.list_plans(
                limit=limit,
                bucket_id=bucket_id,
                template_id=template_id,
                vibe=vibe,
                public_base_url=_public_base_url(request),
            )
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/dates/{plan_id}", response_model=PlanDetailResponse)
    async def get_date(plan_id: str, request: Request) -> PlanDetailResponse:
        try:
            plan = await plan_service.get_plan(
                plan_id=plan_id,
                public_base_url=_public_base_url(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown plan_id {plan_id!r}.") from exc
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return PlanDetailResponse(plan=plan)

    @app.post("/dates/generate", response_model=GeneratePlansResponse)
    async def generate_dates(
        payload: GeneratePlansRequest,
        request: Request,
    ) -> GeneratePlansResponse:
        try:
            plans, warnings, meta = await plan_service.generate(
                request=payload.model_dump(),
                public_base_url=_public_base_url(request),
            )
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Date generation failed unexpectedly.")
            raise HTTPException(status_code=500, detail=f"Date generation failed: {exc}") from exc
        return GeneratePlansResponse(plans=plans, warnings=warnings, meta=meta)

    @app.post("/dates/search")
    async def search_dates(payload: SearchRequestPayload, request: Request) -> JSONResponse:
        try:
            response = await search.search(payload.to_internal())
        except FileNotFoundError as exc:
            logger.error("Search endpoint dependency missing: %s", exc)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Search endpoint failed unexpectedly.")
            raise HTTPException(status_code=500, detail=f"Search request failed: {exc}") from exc
        content = await _search_response_with_detail_images(
            response=response.to_dict(),
            plan_service=plan_service,
            public_base_url=_public_base_url(request),
        )
        return JSONResponse(content=content)

    @app.post("/booking/restaurants/preview", response_model=RestaurantBookingPreviewResponse)
    async def preview_restaurant_booking(
        payload: RestaurantBookingRequestPayload,
    ) -> RestaurantBookingPreviewResponse:
        try:
            internal_request = payload.to_internal()
            settings = BlandAISettings(
                api_key="dry-run-not-used",
                booking_phone_number=bland_ai_booking_phone_number_from_env(),
            )
            call = BookingRequestBuilder(settings).build(internal_request)
        except (BookingValidationError, ValueError) as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        live_call_enabled = True
        live_call_disabled_reason: str | None = None
        try:
            BlandAISettings.from_env()
        except BlandAIConfigurationError as exc:
            live_call_enabled = False
            live_call_disabled_reason = str(exc)

        call_payload = call.to_payload()
        return RestaurantBookingPreviewResponse(
            bookingContext={
                "planId": internal_request.plan_id,
                "restaurantName": internal_request.restaurant_name,
                "restaurantPhoneNumber": internal_request.restaurant_phone_number,
                "restaurantAddress": internal_request.restaurant_address,
                "suggestedArrivalTimeIso": internal_request.arrival_time.isoformat(),
                "partySize": internal_request.party_size,
            },
            callDescription=BlandCallDescriptionPayload(
                phoneNumber=call.phone_number,
                firstSentence=call.first_sentence,
                task=call.task,
                voice=call.voice,
                model=call.model,
                language=call.language,
                timezone=call.timezone,
                maxDurationMinutes=call.max_duration,
                waitForGreeting=call.wait_for_greeting,
                record=call.record,
                voicemail=call.voicemail,
                requestData=call.request_data,
                metadata=call.metadata,
                dispositions=list(call.dispositions),
                keywords=list(call.keywords),
                summaryPrompt=call.summary_prompt,
            ),
            liveCallEnabled=live_call_enabled,
            liveCallDisabledReason=live_call_disabled_reason,
        )

    @app.post("/booking/restaurants", response_model=RestaurantBookingJobPayload)
    async def create_restaurant_booking(
        payload: RestaurantBookingRequestPayload,
    ) -> RestaurantBookingJobPayload:
        try:
            internal_request = payload.to_internal()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        try:
            async with booking_factory() as booking_service:
                job = await booking_service.start_restaurant_booking(internal_request)
        except BlandAIConfigurationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except BookingValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Restaurant booking call creation failed unexpectedly.")
            raise HTTPException(status_code=502, detail=f"Booking request failed: {exc}") from exc

        return RestaurantBookingJobPayload(
            callId=job.call_id,
            status=job.status.value,
            provider=job.provider,
            restaurantName=job.restaurant_name,
            restaurantPhoneNumber=job.restaurant_phone_number,
            arrivalTimeIso=job.arrival_time.isoformat(),
            partySize=job.party_size,
        )

    @app.get("/booking/restaurants/{call_id}", response_model=RestaurantBookingStatusPayload)
    async def get_restaurant_booking(call_id: str) -> RestaurantBookingStatusPayload:
        try:
            async with booking_factory() as booking_service:
                status = await booking_service.get_booking_status(call_id)
        except BlandAIConfigurationError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Restaurant booking status lookup failed unexpectedly.")
            raise HTTPException(status_code=502, detail=f"Booking status lookup failed: {exc}") from exc

        return RestaurantBookingStatusPayload(
            callId=status.call_id,
            status=status.status.value,
            providerStatus=status.provider_status,
            queueStatus=status.queue_status,
            answeredBy=status.answered_by,
            summary=status.summary,
            errorMessage=status.error_message,
        )

    @app.exception_handler(ValidationError)
    async def validation_error_handler(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={"detail": exc.errors()},
        )

    @app.exception_handler(RequestValidationError)
    async def request_validation_error_handler(
        _: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content={
                "detail": "Invalid request body.",
                "errors": jsonable_encoder(exc.errors()),
            },
        )

    return app


def _public_base_url(request: Request) -> str:
    return str(request.base_url).rstrip("/")


async def _search_response_with_detail_images(
    *,
    response: dict,
    plan_service: FrontendPlanService,
    public_base_url: str,
) -> dict:
    results = response.get("results")
    if not isinstance(results, (list, tuple)):
        logger.error("Search response results payload was not a sequence; cannot enrich card images.")
        return response

    for result in results:
        if not isinstance(result, dict):
            logger.error("Search response contained a non-object result; cannot enrich card image.")
            continue

        card = result.get("card")
        if not isinstance(card, dict):
            logger.error(
                "Search result %r did not contain an object card; cannot enrich image.",
                result.get("plan_id"),
            )
            continue

        if isinstance(card.get("hero_image_url"), str) and card["hero_image_url"].strip():
            continue

        plan_id = result.get("plan_id")
        if not isinstance(plan_id, str) or not plan_id.strip():
            logger.error("Search result without a valid plan_id cannot be enriched with a hero image.")
            continue

        try:
            detail_plan = await plan_service.get_plan(
                plan_id=plan_id,
                public_base_url=public_base_url,
            )
        except KeyError:
            logger.error("Search result plan_id=%s was missing from plan detail data.", plan_id)
            continue
        except FrontendApiError as exc:
            logger.error("Failed to enrich search result image for plan_id=%s: %s", plan_id, exc)
            continue

        if detail_plan.heroImageUrl:
            card["hero_image_url"] = detail_plan.heroImageUrl
        else:
            logger.error("Plan detail payload for plan_id=%s did not include a hero image.", plan_id)

    return response


app = create_app()
