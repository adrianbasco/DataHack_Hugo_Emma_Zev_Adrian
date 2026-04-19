"""Async driver loop for pre-cache plan generation."""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

from back_end.agents.precache_planner import (
    FAILURE_REASON_AGENT_EMPTY,
    FAILURE_REASON_AGENT_LOOP,
    FAILURE_REASON_AGENT_MULTIPLE,
    FAILURE_REASON_DUPLICATE,
    FAILURE_REASON_EMPTY_POOL,
    FAILURE_REASON_OUTPUT_INVALID,
    FAILURE_REASON_VERIFICATION,
    PrecachePlanner,
    PrecachePlannerConfigurationError,
    PrecachePlannerFailure,
    PrecachePlannerRequest,
    PrecachePlannerSuccess,
)
from back_end.domain.models import TravelMode
from back_end.precache.candidate_pools import (
    build_location_candidate_pool,
    plan_budget_for_pair,
)
from back_end.precache.models import LocationBucket, LocationCandidatePool
from back_end.precache.output import (
    DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH,
    DEFAULT_PRECACHE_OUTPUT_PATH,
    PrecacheFailureOutput,
    append_precache_failures,
    append_precache_plans,
    read_existing_signatures,
)

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_RUN_TIMEZONE = "Australia/Sydney"
DEFAULT_MAX_CANDIDATES_PER_BUCKET = 250
TERMINAL_FAILURE_REASON_RETRY_EXHAUSTED = "retry_budget_exhausted"
CELL_STATE_COMPLETED = "completed"
CELL_STATE_EXHAUSTED = "exhausted"
CELL_STATE_FATAL = "fatal"
CELL_STATE_RUNNING = "running"
CELL_STATE_SKIPPED_EXISTING = "skipped_existing"
CELL_STATE_SKIPPED_ZERO_BUDGET = "skipped_zero_budget"
CELL_STATE_TERMINAL_FAILURE = "terminal_failure"
_STATUS_SAFE_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    "._-"
)
_RECOVERABLE_FAILURE_REASONS = frozenset(
    {
        FAILURE_REASON_AGENT_EMPTY,
        FAILURE_REASON_AGENT_LOOP,
        FAILURE_REASON_AGENT_MULTIPLE,
        FAILURE_REASON_OUTPUT_INVALID,
        FAILURE_REASON_VERIFICATION,
    }
)
_TERMINAL_PLANNER_FAILURE_REASONS = frozenset({FAILURE_REASON_EMPTY_POOL})
_TIME_OF_DAY_TO_LOCAL_TIME: dict[str, time] = {
    "morning": time(hour=9, minute=30),
    "midday": time(hour=12, minute=30),
    "afternoon": time(hour=15, minute=30),
    "evening": time(hour=18, minute=30),
    "night": time(hour=21, minute=0),
    "flexible": time(hour=17, minute=0),
}
_TRANSPORT_MODE_ALIASES: dict[str, TravelMode] = {
    "walk": TravelMode.WALK,
    "walking": TravelMode.WALK,
    "transit": TravelMode.TRANSIT,
    "drive": TravelMode.DRIVE,
    "driving": TravelMode.DRIVE,
    "bicycle": TravelMode.BICYCLE,
    "bike": TravelMode.BICYCLE,
    "biking": TravelMode.BICYCLE,
    "cycling": TravelMode.BICYCLE,
}
_MAX_LEG_SECONDS_BY_MODE: dict[TravelMode, int] = {
    TravelMode.WALK: 25 * 60,
    TravelMode.BICYCLE: 25 * 60,
    TravelMode.DRIVE: 30 * 60,
    TravelMode.TRANSIT: 35 * 60,
}


class PlanTimeResolver(Protocol):
    """Resolve one explicit plan time per attempt."""

    def pick(
        self,
        bucket: LocationBucket,
        template: Mapping[str, Any],
        *,
        attempt_index: int,
    ) -> str: ...


class PrecacheDriverConfigurationError(ValueError):
    """Raised when the driver is configured with invalid inputs."""


class PrecacheDriverExecutionError(RuntimeError):
    """Raised when fail-fast is enabled and a cell cannot hit its budget."""


@dataclass(frozen=True)
class RunConfig:
    """Execution settings for the precache driver loop."""

    output_path: Path | str = DEFAULT_PRECACHE_OUTPUT_PATH
    run_dir: Path | str = Path("data/precache/runs/default")
    max_concurrency: int = 1
    retries_per_cell: int = 3
    fail_fast: bool = False
    bucket_filters: tuple[str, ...] = ()
    template_filters: tuple[str, ...] = ()
    failures_path: Path | str | None = None

    def __post_init__(self) -> None:
        output_path = Path(self.output_path)
        run_dir = Path(self.run_dir)
        failures_path = (
            Path(self.failures_path)
            if self.failures_path is not None
            else output_path.parent / DEFAULT_PRECACHE_FAILURE_OUTPUT_PATH.name
        )
        if output_path.suffix != ".parquet":
            raise PrecacheDriverConfigurationError(
                f"output_path must be a .parquet path. Got {output_path}."
            )
        if failures_path.suffix != ".parquet":
            raise PrecacheDriverConfigurationError(
                f"failures_path must be a .parquet path. Got {failures_path}."
            )
        if self.max_concurrency <= 0:
            raise PrecacheDriverConfigurationError("max_concurrency must be positive.")
        if self.retries_per_cell < 0:
            raise PrecacheDriverConfigurationError("retries_per_cell must be >= 0.")
        if not isinstance(self.fail_fast, bool):
            raise PrecacheDriverConfigurationError("fail_fast must be a boolean.")
        if not str(run_dir).strip():
            raise PrecacheDriverConfigurationError("run_dir must be a non-empty path.")

        object.__setattr__(self, "output_path", output_path)
        object.__setattr__(self, "run_dir", run_dir)
        object.__setattr__(self, "failures_path", failures_path)
        object.__setattr__(
            self,
            "bucket_filters",
            _normalize_filter_values(self.bucket_filters, label="bucket_filters"),
        )
        object.__setattr__(
            self,
            "template_filters",
            _normalize_filter_values(self.template_filters, label="template_filters"),
        )


@dataclass(frozen=True)
class DefaultPlanTimeResolver:
    """Deterministically pick future local datetimes from template time_of_day."""

    timezone_name: str = DEFAULT_PRECACHE_RUN_TIMEZONE
    start_date: date | None = None

    def __post_init__(self) -> None:
        timezone = ZoneInfo(self.timezone_name)
        object.__setattr__(self, "_timezone", timezone)
        default_start_date = datetime.now(timezone).date() + timedelta(days=1)
        object.__setattr__(self, "_start_date", self.start_date or default_start_date)

    def pick(
        self,
        bucket: LocationBucket,
        template: Mapping[str, Any],
        *,
        attempt_index: int,
    ) -> str:
        if attempt_index < 0:
            raise PrecacheDriverConfigurationError("attempt_index must be non-negative.")
        template_id = _template_id(template)
        time_of_day = str(template.get("time_of_day") or "").strip().lower()
        local_time = _TIME_OF_DAY_TO_LOCAL_TIME.get(time_of_day)
        if local_time is None:
            raise PrecacheDriverConfigurationError(
                "Unsupported template time_of_day for precache planning: "
                f"template={template_id!r} time_of_day={time_of_day!r}."
            )
        local_dt = datetime.combine(
            self._start_date + timedelta(days=attempt_index),
            local_time,
            tzinfo=self._timezone,
        )
        logger.info(
            "Resolved plan time for bucket=%s template=%s attempt=%d to %s.",
            bucket.bucket_id,
            template_id,
            attempt_index,
            local_dt.isoformat(),
        )
        return local_dt.isoformat()


@dataclass(frozen=True)
class PrecacheDriverResult:
    """Aggregate outcome of one driver run."""

    output_path: Path
    failures_path: Path
    run_dir: Path
    selected_bucket_ids: tuple[str, ...]
    selected_template_ids: tuple[str, ...]
    cell_count: int
    completed_cell_count: int
    skipped_existing_cell_count: int
    skipped_zero_budget_cell_count: int
    exhausted_cell_count: int
    success_count: int
    failure_event_count: int
    duplicate_failure_count: int


@dataclass(frozen=True)
class _CellRunSummary:
    success_count: int = 0
    failure_event_count: int = 0
    duplicate_failure_count: int = 0
    completed: bool = False
    skipped_existing: bool = False
    skipped_zero_budget: bool = False
    exhausted: bool = False


async def run_precache_driver(
    *,
    planner: PrecachePlanner,
    buckets: tuple[LocationBucket, ...],
    templates: tuple[Mapping[str, Any], ...],
    rag_documents_path: Path | str,
    run_config: RunConfig,
    plan_time_resolver: PlanTimeResolver | None = None,
) -> PrecacheDriverResult:
    """Run the bounded precache planner loop across bucket/template cells."""

    if not buckets:
        raise PrecacheDriverConfigurationError("buckets must not be empty.")
    if not templates:
        raise PrecacheDriverConfigurationError("templates must not be empty.")

    rag_path = Path(rag_documents_path)
    if rag_path.suffix != ".parquet":
        raise PrecacheDriverConfigurationError(
            f"rag_documents_path must be a parquet file. Got {rag_path}."
        )

    selected_buckets = _select_buckets(buckets, bucket_filters=run_config.bucket_filters)
    selected_templates = _select_templates(
        templates,
        template_filters=run_config.template_filters,
    )
    resolver = plan_time_resolver or DefaultPlanTimeResolver()
    run_config.run_dir.mkdir(parents=True, exist_ok=True)
    _ensure_run_manifest(
        run_dir=run_config.run_dir,
        run_config=run_config,
        rag_documents_path=rag_path,
        buckets=selected_buckets,
        templates=selected_templates,
        plan_time_resolver=resolver,
    )

    plan_output_lock = asyncio.Lock()
    failure_output_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(run_config.max_concurrency)
    tasks = [
        asyncio.create_task(
            _run_bucket(
                planner=planner,
                bucket=bucket,
                templates=selected_templates,
                rag_documents_path=rag_path,
                run_config=run_config,
                plan_time_resolver=resolver,
                semaphore=semaphore,
                plan_output_lock=plan_output_lock,
                failure_output_lock=failure_output_lock,
            )
        )
        for bucket in selected_buckets
    ]

    try:
        bucket_summaries = await asyncio.gather(*tasks)
    except Exception:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise

    cell_summaries = [summary for bucket_summary in bucket_summaries for summary in bucket_summary]
    return PrecacheDriverResult(
        output_path=run_config.output_path,
        failures_path=run_config.failures_path,
        run_dir=run_config.run_dir,
        selected_bucket_ids=tuple(bucket.bucket_id for bucket in selected_buckets),
        selected_template_ids=tuple(_template_id(template) for template in selected_templates),
        cell_count=len(cell_summaries),
        completed_cell_count=sum(1 for summary in cell_summaries if summary.completed),
        skipped_existing_cell_count=sum(1 for summary in cell_summaries if summary.skipped_existing),
        skipped_zero_budget_cell_count=sum(
            1 for summary in cell_summaries if summary.skipped_zero_budget
        ),
        exhausted_cell_count=sum(1 for summary in cell_summaries if summary.exhausted),
        success_count=sum(summary.success_count for summary in cell_summaries),
        failure_event_count=sum(summary.failure_event_count for summary in cell_summaries),
        duplicate_failure_count=sum(summary.duplicate_failure_count for summary in cell_summaries),
    )


async def _run_bucket(
    *,
    planner: PrecachePlanner,
    bucket: LocationBucket,
    templates: tuple[Mapping[str, Any], ...],
    rag_documents_path: Path,
    run_config: RunConfig,
    plan_time_resolver: PlanTimeResolver,
    semaphore: asyncio.Semaphore,
    plan_output_lock: asyncio.Lock,
    failure_output_lock: asyncio.Lock,
) -> tuple[_CellRunSummary, ...]:
    async with semaphore:
        pool = build_location_candidate_pool(
            rag_documents_path=rag_documents_path,
            bucket=bucket,
            max_candidates=DEFAULT_MAX_CANDIDATES_PER_BUCKET,
        )
        summaries: list[_CellRunSummary] = []
        for template in templates:
            summaries.append(
                await _run_cell(
                    planner=planner,
                    bucket=bucket,
                    template=template,
                    pool=pool,
                    run_config=run_config,
                    plan_time_resolver=plan_time_resolver,
                    plan_output_lock=plan_output_lock,
                    failure_output_lock=failure_output_lock,
                )
            )
        return tuple(summaries)


async def _run_cell(
    *,
    planner: PrecachePlanner,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    pool: LocationCandidatePool,
    run_config: RunConfig,
    plan_time_resolver: PlanTimeResolver,
    plan_output_lock: asyncio.Lock,
    failure_output_lock: asyncio.Lock,
) -> _CellRunSummary:
    template_id = _template_id(template)
    budget = plan_budget_for_pair(
        bucket=bucket,
        template=template,
        candidate_pool=pool,
    )
    if budget == 0:
        logger.warning(
            "Skipping precache cell bucket=%s template=%s because plan budget resolved to 0.",
            bucket.bucket_id,
            template_id,
        )
        _write_cell_status(
            run_dir=run_config.run_dir,
            bucket_id=bucket.bucket_id,
            template_id=template_id,
            status={
                "state": CELL_STATE_SKIPPED_ZERO_BUDGET,
                "bucket_id": bucket.bucket_id,
                "template_id": template_id,
                "budget": 0,
                "pool_size": len(pool.places),
                "pool_target_plan_count": pool.target_plan_count,
                "pool_empty_reason": pool.empty_reason,
                "updated_at": _utc_now_isoformat(),
            },
        )
        return _CellRunSummary(skipped_zero_budget=True)

    async with plan_output_lock:
        signatures = read_existing_signatures(
            bucket.bucket_id,
            template_id,
            output_path=run_config.output_path,
        )

    max_attempts = budget + run_config.retries_per_cell
    success_count = 0
    failure_event_count = 0
    duplicate_failure_count = 0
    recoverable_failure_count = 0
    attempts_completed = 0
    stopped_by_terminal_failure = False
    terminal_failure_reason: str | None = None
    terminal_failure_detail: str | None = None
    last_plan_time_iso: str | None = None
    if len(signatures) >= budget:
        logger.info(
            "Skipping precache cell bucket=%s template=%s because %d existing signatures "
            "already satisfy budget=%d.",
            bucket.bucket_id,
            template_id,
            len(signatures),
            budget,
        )
        _write_cell_status(
            run_dir=run_config.run_dir,
            bucket_id=bucket.bucket_id,
            template_id=template_id,
            status={
                "state": CELL_STATE_SKIPPED_EXISTING,
                "bucket_id": bucket.bucket_id,
                "template_id": template_id,
                "budget": budget,
                "existing_signature_count": len(signatures),
                "max_attempts": max_attempts,
                "pool_size": len(pool.places),
                "updated_at": _utc_now_isoformat(),
            },
        )
        return _CellRunSummary(skipped_existing=True)

    status_payload: dict[str, Any] = {
        "state": CELL_STATE_RUNNING,
        "bucket_id": bucket.bucket_id,
        "template_id": template_id,
        "budget": budget,
        "existing_signature_count": len(signatures),
        "signature_count": len(signatures),
        "max_attempts": max_attempts,
        "pool_size": len(pool.places),
        "pool_target_plan_count": pool.target_plan_count,
        "pool_empty_reason": pool.empty_reason,
        "success_count": 0,
        "failure_event_count": 0,
        "duplicate_failure_count": 0,
        "recoverable_failure_count": 0,
        "attempts_completed": 0,
        "updated_at": _utc_now_isoformat(),
    }
    _write_cell_status(
        run_dir=run_config.run_dir,
        bucket_id=bucket.bucket_id,
        template_id=template_id,
        status=status_payload,
    )

    for attempt_index in range(max_attempts):
        if success_count >= budget:
            break
        plan_time_iso = plan_time_resolver.pick(
            bucket,
            template,
            attempt_index=attempt_index,
        )
        last_plan_time_iso = plan_time_iso
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=pool,
            template=template,
            plan_time_iso=plan_time_iso,
            transport_mode=_transport_mode_for_bucket(bucket),
            max_leg_seconds=_max_leg_seconds_for_bucket(bucket),
            existing_plan_signatures=tuple(sorted(signatures)),
        )
        logger.info(
            "Starting precache attempt bucket=%s template=%s attempt=%d/%d successes=%d/%d.",
            bucket.bucket_id,
            template_id,
            attempt_index + 1,
            max_attempts,
            success_count,
            budget,
        )
        try:
            result = await planner.plan(request)
        except PrecachePlannerConfigurationError:
            status_payload.update(
                {
                    "state": CELL_STATE_FATAL,
                    "last_reason": "planner_configuration_error",
                    "attempts_completed": attempts_completed,
                    "updated_at": _utc_now_isoformat(),
                }
            )
            _write_cell_status(
                run_dir=run_config.run_dir,
                bucket_id=bucket.bucket_id,
                template_id=template_id,
                status=status_payload,
            )
            raise

        attempts_completed += 1
        if isinstance(result, PrecachePlannerSuccess):
            async with plan_output_lock:
                write_result = append_precache_plans(
                    [result.plan],
                    output_path=run_config.output_path,
                )
            signatures.add(result.signature)
            success_count += 1
            status_payload.update(
                {
                    "state": CELL_STATE_RUNNING,
                    "success_count": success_count,
                    "signature_count": len(signatures),
                    "attempts_completed": attempts_completed,
                    "last_reason": result.status,
                    "last_signature": result.signature,
                    "last_plan_id": write_result.plan_ids[0],
                    "updated_at": _utc_now_isoformat(),
                }
            )
            logger.info(
                "Precache success bucket=%s template=%s attempt=%d plan_id=%s successes=%d/%d.",
                bucket.bucket_id,
                template_id,
                attempt_index + 1,
                write_result.plan_ids[0],
                success_count,
                budget,
            )
            _write_cell_status(
                run_dir=run_config.run_dir,
                bucket_id=bucket.bucket_id,
                template_id=template_id,
                status=status_payload,
            )
            continue

        summary = await _handle_planner_failure(
            result=result,
            run_config=run_config,
            bucket=bucket,
            template=template,
            budget=budget,
            attempt_index=attempt_index,
            max_attempts=max_attempts,
            success_count=success_count,
            plan_time_iso=plan_time_iso,
            failure_output_lock=failure_output_lock,
        )
        failure_event_count += summary["failure_event_count"]
        duplicate_failure_count += summary["duplicate_failure_count"]
        recoverable_failure_count += summary["recoverable_failure_count"]
        if result.reason == FAILURE_REASON_DUPLICATE:
            if not result.signature:
                raise PrecacheDriverConfigurationError(
                    "duplicate_signature failure must include a signature."
                )
            signatures.add(result.signature)

        status_payload.update(
            {
                "state": CELL_STATE_RUNNING,
                "success_count": success_count,
                "signature_count": len(signatures),
                "attempts_completed": attempts_completed,
                "failure_event_count": failure_event_count,
                "duplicate_failure_count": duplicate_failure_count,
                "recoverable_failure_count": recoverable_failure_count,
                "last_reason": result.reason,
                "last_detail": result.detail,
                "last_signature": result.signature,
                "updated_at": _utc_now_isoformat(),
            }
        )
        _write_cell_status(
            run_dir=run_config.run_dir,
            bucket_id=bucket.bucket_id,
            template_id=template_id,
            status=status_payload,
        )

        if result.reason in _TERMINAL_PLANNER_FAILURE_REASONS:
            stopped_by_terminal_failure = True
            terminal_failure_reason = result.reason
            terminal_failure_detail = result.detail
            break

    if success_count >= budget:
        status_payload.update(
            {
                "state": CELL_STATE_COMPLETED,
                "success_count": success_count,
                "attempts_completed": attempts_completed,
                "failure_event_count": failure_event_count,
                "duplicate_failure_count": duplicate_failure_count,
                "recoverable_failure_count": recoverable_failure_count,
                "updated_at": _utc_now_isoformat(),
            }
        )
        _write_cell_status(
            run_dir=run_config.run_dir,
            bucket_id=bucket.bucket_id,
            template_id=template_id,
            status=status_payload,
        )
        return _CellRunSummary(
            success_count=success_count,
            failure_event_count=failure_event_count,
            duplicate_failure_count=duplicate_failure_count,
            completed=True,
        )

    if stopped_by_terminal_failure:
        status_payload.update(
            {
                "state": CELL_STATE_TERMINAL_FAILURE,
                "success_count": success_count,
                "attempts_completed": attempts_completed,
                "failure_event_count": failure_event_count,
                "duplicate_failure_count": duplicate_failure_count,
                "recoverable_failure_count": recoverable_failure_count,
                "last_reason": terminal_failure_reason,
                "last_detail": terminal_failure_detail,
                "updated_at": _utc_now_isoformat(),
            }
        )
        _write_cell_status(
            run_dir=run_config.run_dir,
            bucket_id=bucket.bucket_id,
            template_id=template_id,
            status=status_payload,
        )
        if run_config.fail_fast:
            raise PrecacheDriverExecutionError(
                f"Precache cell bucket={bucket.bucket_id} template={template_id} ended with "
                f"terminal planner failure {terminal_failure_reason!r}."
            )
        return _CellRunSummary(
            success_count=success_count,
            failure_event_count=failure_event_count,
            duplicate_failure_count=duplicate_failure_count,
            exhausted=True,
        )

    terminal_detail = (
        f"Cell ended with {success_count}/{budget} successes after "
        f"{attempts_completed}/{max_attempts} attempts."
    )
    async with failure_output_lock:
        append_precache_failures(
            [
                PrecacheFailureOutput(
                    bucket_id=bucket.bucket_id,
                    template_id=template_id,
                    plan_time_iso=last_plan_time_iso
                    or plan_time_resolver.pick(bucket, template, attempt_index=0),
                    attempt_index=attempts_completed,
                    reason=TERMINAL_FAILURE_REASON_RETRY_EXHAUSTED,
                    detail=terminal_detail,
                )
            ],
            output_path=run_config.failures_path,
        )
    failure_event_count += 1
    status_payload.update(
        {
            "state": CELL_STATE_EXHAUSTED,
            "success_count": success_count,
            "attempts_completed": attempts_completed,
            "failure_event_count": failure_event_count,
            "duplicate_failure_count": duplicate_failure_count,
            "recoverable_failure_count": recoverable_failure_count,
            "last_reason": TERMINAL_FAILURE_REASON_RETRY_EXHAUSTED,
            "last_detail": terminal_detail,
            "updated_at": _utc_now_isoformat(),
        }
    )
    _write_cell_status(
        run_dir=run_config.run_dir,
        bucket_id=bucket.bucket_id,
        template_id=template_id,
        status=status_payload,
    )
    logger.error(
        "Precache cell exhausted bucket=%s template=%s successes=%d/%d attempts=%d/%d.",
        bucket.bucket_id,
        template_id,
        success_count,
        budget,
        attempts_completed,
        max_attempts,
    )
    if run_config.fail_fast:
        raise PrecacheDriverExecutionError(
            f"Precache cell bucket={bucket.bucket_id} template={template_id} exhausted its "
            f"retry budget with {success_count}/{budget} successes."
        )
    return _CellRunSummary(
        success_count=success_count,
        failure_event_count=failure_event_count,
        duplicate_failure_count=duplicate_failure_count,
        exhausted=True,
    )


async def _handle_planner_failure(
    *,
    result: PrecachePlannerFailure,
    run_config: RunConfig,
    bucket: LocationBucket,
    template: Mapping[str, Any],
    budget: int,
    attempt_index: int,
    max_attempts: int,
    success_count: int,
    plan_time_iso: str,
    failure_output_lock: asyncio.Lock,
) -> dict[str, int]:
    template_id = _template_id(template)
    if result.reason == FAILURE_REASON_DUPLICATE:
        logger.warning(
            "Precache duplicate bucket=%s template=%s attempt=%d signature=%s.",
            bucket.bucket_id,
            template_id,
            attempt_index + 1,
            result.signature,
        )
        failure_event_count = 1
        duplicate_failure_count = 1
        recoverable_failure_count = 0
    elif result.reason in _RECOVERABLE_FAILURE_REASONS:
        logger.warning(
            "Precache recoverable failure bucket=%s template=%s attempt=%d/%d reason=%s detail=%s",
            bucket.bucket_id,
            template_id,
            attempt_index + 1,
            max_attempts,
            result.reason,
            result.detail,
        )
        failure_event_count = 1
        duplicate_failure_count = 0
        recoverable_failure_count = 1
    elif result.reason in _TERMINAL_PLANNER_FAILURE_REASONS:
        logger.error(
            "Precache terminal planner failure bucket=%s template=%s reason=%s detail=%s",
            bucket.bucket_id,
            template_id,
            result.reason,
            result.detail,
        )
        failure_event_count = 1
        duplicate_failure_count = 0
        recoverable_failure_count = 0
    else:
        raise PrecacheDriverConfigurationError(
            f"Unknown planner failure reason {result.reason!r} for template {template_id!r}."
        )

    async with failure_output_lock:
        append_precache_failures(
            [
                PrecacheFailureOutput(
                    bucket_id=bucket.bucket_id,
                    template_id=template_id,
                    plan_time_iso=plan_time_iso,
                    attempt_index=attempt_index,
                    reason=result.reason,
                    detail=result.detail,
                    signature=result.signature,
                    rejected_ideas=result.rejected_ideas,
                    tool_executions_count=len(result.tool_executions),
                    model=result.model,
                )
            ],
            output_path=run_config.failures_path,
        )
    return {
        "failure_event_count": failure_event_count,
        "duplicate_failure_count": duplicate_failure_count,
        "recoverable_failure_count": recoverable_failure_count,
    }


def _select_buckets(
    buckets: Sequence[LocationBucket],
    *,
    bucket_filters: tuple[str, ...],
) -> tuple[LocationBucket, ...]:
    ordered = tuple(buckets)
    bucket_ids = tuple(bucket.bucket_id for bucket in ordered)
    duplicates = _duplicates(bucket_ids)
    if duplicates:
        raise PrecacheDriverConfigurationError(
            f"Duplicate bucket ids are not allowed: {sorted(duplicates)}."
        )
    if not bucket_filters:
        return ordered
    requested = set(bucket_filters)
    missing = sorted(requested - set(bucket_ids))
    if missing:
        raise PrecacheDriverConfigurationError(
            f"Unknown bucket filters requested: {missing}."
        )
    selected = tuple(bucket for bucket in ordered if bucket.bucket_id in requested)
    if not selected:
        raise PrecacheDriverConfigurationError("Bucket filters selected 0 buckets.")
    return selected


def _select_templates(
    templates: Sequence[Mapping[str, Any]],
    *,
    template_filters: tuple[str, ...],
) -> tuple[Mapping[str, Any], ...]:
    ordered = tuple(templates)
    template_ids = tuple(_template_id(template) for template in ordered)
    duplicates = _duplicates(template_ids)
    if duplicates:
        raise PrecacheDriverConfigurationError(
            f"Duplicate template ids are not allowed: {sorted(duplicates)}."
        )
    if not template_filters:
        return ordered
    requested = set(template_filters)
    missing = sorted(requested - set(template_ids))
    if missing:
        raise PrecacheDriverConfigurationError(
            f"Unknown template filters requested: {missing}."
        )
    selected = tuple(
        template for template in ordered if _template_id(template) in requested
    )
    if not selected:
        raise PrecacheDriverConfigurationError("Template filters selected 0 templates.")
    return selected


def _template_id(template: Mapping[str, Any]) -> str:
    if not isinstance(template, Mapping):
        raise PrecacheDriverConfigurationError("templates must contain mappings.")
    template_id = str(template.get("id") or "").strip()
    if not template_id:
        raise PrecacheDriverConfigurationError("Every template must have a non-empty id.")
    return template_id


def _transport_mode_for_bucket(bucket: LocationBucket) -> TravelMode:
    raw_mode = str(bucket.transport_mode or "").strip().lower()
    travel_mode = _TRANSPORT_MODE_ALIASES.get(raw_mode)
    if travel_mode is None:
        raise PrecacheDriverConfigurationError(
            f"Unsupported bucket transport_mode {bucket.transport_mode!r} for "
            f"bucket {bucket.bucket_id!r}."
        )
    return travel_mode


def _max_leg_seconds_for_bucket(bucket: LocationBucket) -> int:
    travel_mode = _transport_mode_for_bucket(bucket)
    max_leg_seconds = _MAX_LEG_SECONDS_BY_MODE.get(travel_mode)
    if max_leg_seconds is None:
        raise PrecacheDriverConfigurationError(
            f"No max_leg_seconds configured for travel mode {travel_mode.value!r}."
        )
    return max_leg_seconds


def _ensure_run_manifest(
    *,
    run_dir: Path,
    run_config: RunConfig,
    rag_documents_path: Path,
    buckets: Sequence[LocationBucket],
    templates: Sequence[Mapping[str, Any]],
    plan_time_resolver: PlanTimeResolver,
) -> None:
    manifest_path = run_dir / "run_manifest.json"
    manifest = {
        "output_path": str(run_config.output_path),
        "failures_path": str(run_config.failures_path),
        "rag_documents_path": str(rag_documents_path),
        "max_concurrency": run_config.max_concurrency,
        "retries_per_cell": run_config.retries_per_cell,
        "fail_fast": run_config.fail_fast,
        "bucket_filters": list(run_config.bucket_filters),
        "template_filters": list(run_config.template_filters),
        "selected_bucket_ids": [bucket.bucket_id for bucket in buckets],
        "selected_template_ids": [_template_id(template) for template in templates],
        "plan_time_resolver": type(plan_time_resolver).__name__,
    }
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing != manifest:
            raise PrecacheDriverConfigurationError(
                f"Existing precache run manifest at {manifest_path} does not match "
                "the requested run configuration."
            )
        return
    _atomic_write_json(manifest_path, manifest)


def _write_cell_status(
    *,
    run_dir: Path,
    bucket_id: str,
    template_id: str,
    status: Mapping[str, Any],
) -> None:
    path = (
        run_dir
        / "cells"
        / _safe_status_component(bucket_id)
        / f"{_safe_status_component(template_id)}.json"
    )
    _atomic_write_json(path, dict(status))


def _safe_status_component(value: str) -> str:
    normalized = str(value).strip()
    if not normalized:
        raise PrecacheDriverConfigurationError("Status path component must be non-empty.")
    if any(character not in _STATUS_SAFE_CHARS for character in normalized):
        raise PrecacheDriverConfigurationError(
            f"Status path component contains unsupported characters: {value!r}."
        )
    return normalized


def _normalize_filter_values(values: Sequence[str], *, label: str) -> tuple[str, ...]:
    if isinstance(values, str):
        raise PrecacheDriverConfigurationError(
            f"{label} must be a sequence of ids, not a single string."
        )
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = str(raw).strip()
        if not text:
            raise PrecacheDriverConfigurationError(f"{label} must not contain empty values.")
        if text in seen:
            raise PrecacheDriverConfigurationError(
                f"{label} contains duplicate value {text!r}."
            )
        normalized.append(text)
        seen.add(text)
    return tuple(normalized)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    with tempfile.NamedTemporaryFile(
        prefix=f"{path.stem}.",
        suffix=".tmp.json",
        dir=path.parent,
        delete=False,
        mode="w",
        encoding="utf-8",
    ) as handle:
        handle.write(serialized)
        handle.write("\n")
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _utc_now_isoformat() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _duplicates(values: Sequence[str]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


__all__ = [
    "DefaultPlanTimeResolver",
    "PlanTimeResolver",
    "PrecacheDriverConfigurationError",
    "PrecacheDriverExecutionError",
    "PrecacheDriverResult",
    "RunConfig",
    "run_precache_driver",
]
