"""Async driver for the CLI-oriented precache generation flow."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from back_end.agents.precache_planner import (
    PrecachePlanner,
    PrecachePlannerRequest,
    PrecachePlannerSuccess,
)
from back_end.precache.factory import PrecachePlanCell
from back_end.precache.output import append_precache_plans, read_precache_output

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrecacheRunSummary:
    """Structured final summary for one precache CLI run."""

    run_dir: Path
    output_path: Path
    status_path: Path
    started_at_utc: str
    completed_at_utc: str
    requested_plan_count: int
    planner_call_count: int
    successful_plan_count: int
    retry_count: int
    limit: int | None
    limit_reached: bool
    written_plan_ids: tuple[str, ...]
    failure_counts_by_reason: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_dir": str(self.run_dir),
            "output_path": str(self.output_path),
            "status_path": str(self.status_path),
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "requested_plan_count": self.requested_plan_count,
            "planner_call_count": self.planner_call_count,
            "successful_plan_count": self.successful_plan_count,
            "retry_count": self.retry_count,
            "limit": self.limit,
            "limit_reached": self.limit_reached,
            "written_plan_ids": list(self.written_plan_ids),
            "failure_counts_by_reason": dict(self.failure_counts_by_reason),
        }


@dataclass
class _RunState:
    started_at_utc: str
    limit: int | None
    planner_call_count: int = 0
    successful_plan_count: int = 0
    retry_count: int = 0
    written_plan_ids: list[str] | None = None
    failure_counts_by_reason: Counter[str] | None = None

    def __post_init__(self) -> None:
        if self.written_plan_ids is None:
            self.written_plan_ids = []
        if self.failure_counts_by_reason is None:
            self.failure_counts_by_reason = Counter()


def default_run_dir(runs_root: Path) -> Path:
    """Return a timestamped run directory under the configured root."""

    stamp = datetime.now(UTC).strftime("precache-%Y%m%dT%H%M%SZ")
    return runs_root / stamp


async def run(
    *,
    planner: PrecachePlanner,
    cells: tuple[PrecachePlanCell, ...],
    output_path: Path,
    run_dir: Path,
    max_concurrency: int,
    retries_per_cell: int,
    limit: int | None = None,
) -> PrecacheRunSummary:
    """Run the precache planner across a resolved cell matrix."""

    if max_concurrency <= 0:
        raise ValueError("max_concurrency must be positive.")
    if retries_per_cell <= 0:
        raise ValueError("retries_per_cell must be positive.")
    if limit is not None and limit <= 0:
        raise ValueError("limit must be positive when provided.")

    run_dir.mkdir(parents=True, exist_ok=True)
    status_path = run_dir / "status.json"
    output_path = Path(output_path)

    jobs = tuple(
        cell
        for cell in cells
        for _ in range(cell.budget)
        if cell.budget > 0
    )
    state = _RunState(
        started_at_utc=_utc_now_iso(),
        limit=limit,
    )
    signature_index = _load_existing_signature_index(output_path)
    stop_event = asyncio.Event()
    write_lock = asyncio.Lock()
    queue: asyncio.Queue[PrecachePlanCell] = asyncio.Queue()
    for cell in jobs:
        queue.put_nowait(cell)

    _write_status_file(
        status_path,
        {
            "state": "running",
            "run_dir": str(run_dir),
            "output_path": str(output_path),
            "started_at_utc": state.started_at_utc,
            "requested_plan_count": len(jobs),
            "limit": limit,
        },
    )

    async def worker() -> None:
        while not stop_event.is_set():
            try:
                cell = queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await _run_single_job(
                    planner=planner,
                    cell=cell,
                    output_path=output_path,
                    retries_per_cell=retries_per_cell,
                    limit=limit,
                    signature_index=signature_index,
                    state=state,
                    write_lock=write_lock,
                    stop_event=stop_event,
                )
            finally:
                queue.task_done()

    worker_count = min(max_concurrency, len(jobs) or 1)
    workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
    try:
        await asyncio.gather(*workers)
    finally:
        for task in workers:
            if not task.done():
                task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    summary = PrecacheRunSummary(
        run_dir=run_dir,
        output_path=output_path,
        status_path=status_path,
        started_at_utc=state.started_at_utc,
        completed_at_utc=_utc_now_iso(),
        requested_plan_count=len(jobs),
        planner_call_count=state.planner_call_count,
        successful_plan_count=state.successful_plan_count,
        retry_count=state.retry_count,
        limit=limit,
        limit_reached=(limit is not None and state.successful_plan_count >= limit),
        written_plan_ids=tuple(state.written_plan_ids or ()),
        failure_counts_by_reason=dict(state.failure_counts_by_reason or {}),
    )
    _write_status_file(
        status_path,
        {
            "state": "completed",
            **summary.to_dict(),
        },
    )
    return summary


async def _run_single_job(
    *,
    planner: PrecachePlanner,
    cell: PrecachePlanCell,
    output_path: Path,
    retries_per_cell: int,
    limit: int | None,
    signature_index: dict[tuple[str, str], set[str]],
    state: _RunState,
    write_lock: asyncio.Lock,
    stop_event: asyncio.Event,
) -> None:
    pair_key = (cell.bucket_id, cell.template_id)
    for attempt_number in range(1, retries_per_cell + 1):
        async with write_lock:
            if limit is not None and state.successful_plan_count >= limit:
                stop_event.set()
                return
            existing_signatures = tuple(sorted(signature_index[pair_key]))
            if attempt_number > 1:
                state.retry_count += 1

        result = await planner.plan(
            PrecachePlannerRequest(
                bucket=cell.bucket,
                pool=cell.pool,
                template=cell.template,
                plan_time_iso=cell.plan_time.plan_time_iso,
                transport_mode=cell.transport_mode,
                max_leg_seconds=cell.max_leg_seconds,
                existing_plan_signatures=existing_signatures,
            )
        )

        async with write_lock:
            state.planner_call_count += 1
            if limit is not None and state.successful_plan_count >= limit:
                stop_event.set()
                return

            if isinstance(result, PrecachePlannerSuccess):
                if result.signature in signature_index[pair_key]:
                    logger.error(
                        "Planner returned duplicate signature for bucket=%s template=%s; "
                        "attempt=%d/%d signature=%s",
                        cell.bucket_id,
                        cell.template_id,
                        attempt_number,
                        retries_per_cell,
                        result.signature,
                    )
                    state.failure_counts_by_reason["duplicate_signature"] += 1
                    continue

                write_result = append_precache_plans(
                    [result.plan],
                    output_path=output_path,
                )
                signature_index[pair_key].add(result.signature)
                state.successful_plan_count += 1
                state.written_plan_ids.extend(write_result.plan_ids)
                logger.info(
                    "Wrote precache plan for bucket=%s template=%s plan_id=%s "
                    "success=%d/%s",
                    cell.bucket_id,
                    cell.template_id,
                    write_result.plan_ids[0],
                    state.successful_plan_count,
                    limit if limit is not None else "unbounded",
                )
                if limit is not None and state.successful_plan_count >= limit:
                    stop_event.set()
                return

            logger.error(
                "Precache planner failed for bucket=%s template=%s attempt=%d/%d: "
                "reason=%s detail=%s",
                cell.bucket_id,
                cell.template_id,
                attempt_number,
                retries_per_cell,
                result.reason,
                result.detail,
            )
            state.failure_counts_by_reason[result.reason] += 1


def _load_existing_signature_index(
    output_path: Path,
) -> dict[tuple[str, str], set[str]]:
    signatures: dict[tuple[str, str], set[str]] = defaultdict(set)
    if not output_path.exists():
        return signatures
    existing_df = read_precache_output(output_path)
    for row in existing_df.itertuples(index=False):
        signatures[(str(row.bucket_id), str(row.template_id))].add(
            str(row.fsq_place_ids_sorted)
        )
    return signatures


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _write_status_file(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
