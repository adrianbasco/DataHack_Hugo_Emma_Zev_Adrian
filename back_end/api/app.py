"""FastAPI app serving local cached date plans and image assets."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Annotated

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from back_end.api.models import (
    DatesMetadataResponse,
    GeneratePlansRequest,
    GeneratePlansResponse,
    HealthResponse,
    PlanDetailResponse,
    PlanPayload,
)
from back_end.api.service import FrontendApiError, FrontendPlanService
from back_end.precache.asset_sync import DEFAULT_FRONTEND_API_OUTPUT_PATH, DEFAULT_FRONTEND_IMAGES_DIR

logger = logging.getLogger(__name__)


def create_app(
    *,
    plans_api_path: Path | str | None = None,
    assets_dir: Path | str | None = None,
    service: FrontendPlanService | None = None,
) -> FastAPI:
    resolved_service = service or FrontendPlanService(
        plans_api_path=plans_api_path or DEFAULT_FRONTEND_API_OUTPUT_PATH,
        assets_dir=assets_dir or DEFAULT_FRONTEND_IMAGES_DIR,
    )
    app = FastAPI(title="Date Night Local API", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    resolved_service.assets_dir.mkdir(parents=True, exist_ok=True)
    app.mount(
        "/static/precache-images",
        StaticFiles(directory=str(resolved_service.assets_dir)),
        name="precache-images",
    )

    @app.get("/healthz", response_model=HealthResponse)
    async def healthz() -> HealthResponse:
        return HealthResponse.model_validate(await resolved_service.health())

    @app.get("/dates/metadata", response_model=DatesMetadataResponse)
    async def dates_metadata() -> DatesMetadataResponse:
        try:
            return DatesMetadataResponse.model_validate(await resolved_service.metadata())
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.get("/dates", response_model=list[PlanPayload])
    async def list_dates(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=50)] = 20,
        bucket_id: str | None = None,
        template_id: str | None = None,
        vibe: str | None = None,
    ) -> list[PlanPayload]:
        try:
            return await resolved_service.list_plans(
                limit=limit,
                bucket_id=bucket_id,
                template_id=template_id,
                vibe=vibe,
                public_base_url=_public_base_url(request),
            )
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc

    @app.post("/dates/generate", response_model=GeneratePlansResponse)
    async def generate_dates(
        request: Request,
        payload: GeneratePlansRequest,
    ) -> GeneratePlansResponse:
        try:
            plans, warnings, meta = await resolved_service.generate(
                request=payload.model_dump(),
                public_base_url=_public_base_url(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=400, detail=f"Missing field: {exc}") from exc
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Failed to generate plans from local cache.")
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return GeneratePlansResponse(plans=plans, warnings=warnings, meta=meta)

    @app.get("/dates/{plan_id}", response_model=PlanDetailResponse)
    async def get_date(plan_id: str, request: Request) -> PlanDetailResponse:
        try:
            plan = await resolved_service.get_plan(
                plan_id=plan_id,
                public_base_url=_public_base_url(request),
            )
        except KeyError as exc:
            raise HTTPException(status_code=404, detail=f"Unknown plan_id {plan_id!r}.") from exc
        except FrontendApiError as exc:
            raise HTTPException(status_code=503, detail=str(exc)) from exc
        return PlanDetailResponse(plan=plan)

    return app


def _public_base_url(request: Request) -> str:
    return str(request.base_url)


app = create_app()
