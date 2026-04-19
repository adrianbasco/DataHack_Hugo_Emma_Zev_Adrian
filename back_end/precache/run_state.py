"""Durable manifest, event log, and resumable status for precache runs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from back_end.llm.models import AgentToolExecution
from back_end.query.settings import REPO_ROOT

logger = logging.getLogger(__name__)

DEFAULT_PRECACHE_RUNS_ROOT = REPO_ROOT / "data" / "precache" / "runs"

RUN_MANIFEST_FILENAME = "run_manifest.json"
EVENTS_FILENAME = "events.jsonl"
STATUS_FILENAME = "status.json"
TOOL_EXECUTIONS_DIRNAME = "tool_executions"

EVENT_ATTEMPT_STARTED = "attempt_started"
EVENT_SUCCESS = "success"
EVENT_FAILURE = "failure"
EVENT_DUPLICATE_SIGNATURE = "duplicate_signature"
EVENT_RETRY = "retry"
EVENT_CELL_COMPLETE = "cell_complete"

KNOWN_EVENT_TYPES = frozenset(
    {
        EVENT_ATTEMPT_STARTED,
        EVENT_SUCCESS,
        EVENT_FAILURE,
        EVENT_DUPLICATE_SIGNATURE,
        EVENT_RETRY,
        EVENT_CELL_COMPLETE,
    }
)


class PrecacheRunStateError(RuntimeError):
    """Raised when the precache run state is invalid or cannot be updated safely."""


@dataclass(frozen=True)
class PrecacheCell:
    """One planned (bucket, template) cell for precache generation."""

    bucket_id: str
    template_id: str

    def __post_init__(self) -> None:
        if not _nonempty_string(self.bucket_id):
            raise PrecacheRunStateError("PrecacheCell.bucket_id must be a non-empty string.")
        if not _nonempty_string(self.template_id):
            raise PrecacheRunStateError("PrecacheCell.template_id must be a non-empty string.")

    @property
    def cell_id(self) -> str:
        return f"{self.bucket_id}::{self.template_id}"

    @property
    def transcript_basename(self) -> str:
        digest = hashlib.sha256(self.cell_id.encode("utf-8")).hexdigest()[:10]
        bucket_slug = _safe_slug(self.bucket_id)
        template_slug = _safe_slug(self.template_id)
        return f"{bucket_slug}__{template_slug}__{digest}"

    def to_manifest_dict(self) -> dict[str, str]:
        return {
            "cell_id": self.cell_id,
            "bucket_id": self.bucket_id,
            "template_id": self.template_id,
        }

    @classmethod
    def from_value(cls, value: PrecacheCell | Mapping[str, Any]) -> PrecacheCell:
        if isinstance(value, cls):
            return value
        if not isinstance(value, Mapping):
            raise PrecacheRunStateError(
                "Each precache cell must be a PrecacheCell or mapping."
            )
        return cls(
            bucket_id=_required_text(value.get("bucket_id"), "cell.bucket_id"),
            template_id=_required_text(value.get("template_id"), "cell.template_id"),
        )


@dataclass(frozen=True)
class PrecacheRunPaths:
    """Filesystem layout for one precache run."""

    run_dir: Path
    manifest_path: Path
    events_path: Path
    status_path: Path
    tool_executions_dir: Path


@dataclass(frozen=True)
class PrecacheRunSnapshot:
    """Derived summary rebuilt from manifest + authoritative event log."""

    run_id: str
    cells_total: int
    cells_complete: int
    plans_written: int
    failures: int
    duplicates_avoided: int
    attempts_started: int
    retries: int
    cost_usd_total: float
    completed_cell_ids: tuple[str, ...]
    pending_cell_ids: tuple[str, ...]
    state: str
    updated_at: str
    last_event_at: str | None = None


@dataclass
class PrecacheRunState:
    """Handle manifest/event/status updates for a single precache run."""

    run_id: str
    paths: PrecacheRunPaths
    cells: tuple[PrecacheCell, ...]
    input_config: dict[str, Any]
    planner_model: str
    git_sha: str | None
    started_at_utc: str
    ended_at_utc: str | None
    cell_list_hash: str

    @classmethod
    def resolve_or_create(
        cls,
        *,
        cells: Sequence[PrecacheCell | Mapping[str, Any]],
        input_config: Mapping[str, Any],
        planner_model: str,
        git_sha: str | None = None,
        runs_root: Path | str = DEFAULT_PRECACHE_RUNS_ROOT,
        started_at_utc: str | None = None,
    ) -> PrecacheRunState:
        """Reuse an unfinished matching run or create a new one."""

        normalized_cells = _normalize_cells(cells)
        normalized_input_config = _normalized_json_mapping(
            input_config,
            field_name="input_config",
        )
        clean_planner_model = _required_text(planner_model, "planner_model")
        clean_git_sha = _optional_text(git_sha)
        clean_started_at = (
            _normalize_timestamp(started_at_utc, field_name="started_at_utc")
            if started_at_utc is not None
            else _utc_now_isoformat()
        )
        root = Path(runs_root)
        root.mkdir(parents=True, exist_ok=True)
        cell_list_hash = _cell_list_hash(normalized_cells)

        resumable_run_dir = _find_resumable_run_dir(
            runs_root=root,
            cells=normalized_cells,
            input_config=normalized_input_config,
            planner_model=clean_planner_model,
            cell_list_hash=cell_list_hash,
        )
        if resumable_run_dir is not None:
            logger.info("Resuming incomplete precache run at %s.", resumable_run_dir)
            return cls.load(run_dir=resumable_run_dir)

        run_id = make_precache_run_id(
            cells=normalized_cells,
            started_at_utc=clean_started_at,
        )
        run_dir = root / run_id
        if run_dir.exists():
            logger.error("Refusing to overwrite existing precache run dir at %s.", run_dir)
            raise PrecacheRunStateError(
                f"Precache run dir already exists at {run_dir}; refusing to overwrite it."
            )

        paths = _paths_for_run_dir(run_dir)
        run_dir.mkdir(parents=True, exist_ok=False)
        manifest = {
            "run_id": run_id,
            "run_dir": str(run_dir),
            "started_at_utc": clean_started_at,
            "ended_at_utc": None,
            "planner_model": clean_planner_model,
            "git_sha": clean_git_sha,
            "cell_list_hash": cell_list_hash,
            "cells_total": len(normalized_cells),
            "input_config": normalized_input_config,
            "cells": [cell.to_manifest_dict() for cell in normalized_cells],
        }
        _atomic_write_json(paths.manifest_path, manifest)
        if not paths.events_path.exists():
            paths.events_path.touch()
        initial_status = {
            "run_id": run_id,
            "state": "running",
            "cells_total": len(normalized_cells),
            "cells_complete": 0,
            "plans_written": 0,
            "failures": 0,
            "duplicates_avoided": 0,
            "attempts_started": 0,
            "retries": 0,
            "cost_usd_total": 0.0,
            "completed_cell_ids": [],
            "pending_cell_ids": [cell.cell_id for cell in normalized_cells],
            "updated_at": clean_started_at,
            "last_event_at": None,
        }
        _atomic_write_json(paths.status_path, initial_status)
        return cls(
            run_id=run_id,
            paths=paths,
            cells=normalized_cells,
            input_config=normalized_input_config,
            planner_model=clean_planner_model,
            git_sha=clean_git_sha,
            started_at_utc=clean_started_at,
            ended_at_utc=None,
            cell_list_hash=cell_list_hash,
        )

    @classmethod
    def load(cls, *, run_dir: Path | str) -> PrecacheRunState:
        """Load an existing run from its manifest."""

        root = Path(run_dir)
        paths = _paths_for_run_dir(root)
        manifest = _load_manifest(paths.manifest_path)
        return cls(
            run_id=_required_text(manifest.get("run_id"), "run_manifest.run_id"),
            paths=paths,
            cells=_cells_from_manifest(manifest),
            input_config=_normalized_json_mapping(
                manifest.get("input_config"),
                field_name="run_manifest.input_config",
            ),
            planner_model=_required_text(
                manifest.get("planner_model"),
                "run_manifest.planner_model",
            ),
            git_sha=_optional_text(manifest.get("git_sha")),
            started_at_utc=_normalize_timestamp(
                manifest.get("started_at_utc"),
                field_name="run_manifest.started_at_utc",
            ),
            ended_at_utc=(
                _normalize_timestamp(
                    manifest.get("ended_at_utc"),
                    field_name="run_manifest.ended_at_utc",
                )
                if manifest.get("ended_at_utc") is not None
                else None
            ),
            cell_list_hash=_required_text(
                manifest.get("cell_list_hash"),
                "run_manifest.cell_list_hash",
            ),
        )

    def snapshot(self) -> PrecacheRunSnapshot:
        """Rebuild and persist the current derived status."""

        return rebuild_precache_run_status(self.paths.run_dir)

    def remaining_cells(self) -> tuple[PrecacheCell, ...]:
        """Return cells not yet marked complete."""

        snapshot = self.snapshot()
        completed = set(snapshot.completed_cell_ids)
        return tuple(cell for cell in self.cells if cell.cell_id not in completed)

    def record_attempt_started(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        signature: str | None = None,
        attempt_number: int | None = None,
        cost_usd: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        return self.record_event(
            EVENT_ATTEMPT_STARTED,
            cell,
            signature=signature,
            attempt_number=attempt_number,
            cost_usd=cost_usd,
            metadata=metadata,
            occurred_at_utc=occurred_at_utc,
        )

    def record_success(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        signature: str,
        cost_usd: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        return self.record_event(
            EVENT_SUCCESS,
            cell,
            signature=signature,
            cost_usd=cost_usd,
            metadata=metadata,
            occurred_at_utc=occurred_at_utc,
        )

    def record_failure(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        reason: str,
        signature: str | None = None,
        cost_usd: float | int | None = None,
        detail: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        payload = dict(metadata or {})
        if detail is not None:
            payload["detail"] = _required_text(detail, "detail")
        return self.record_event(
            EVENT_FAILURE,
            cell,
            reason=reason,
            signature=signature,
            cost_usd=cost_usd,
            metadata=payload or None,
            occurred_at_utc=occurred_at_utc,
        )

    def record_duplicate_signature(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        signature: str,
        reason: str,
        cost_usd: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        return self.record_event(
            EVENT_DUPLICATE_SIGNATURE,
            cell,
            signature=signature,
            reason=reason,
            cost_usd=cost_usd,
            metadata=metadata,
            occurred_at_utc=occurred_at_utc,
        )

    def record_retry(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        reason: str,
        signature: str | None = None,
        attempt_number: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        return self.record_event(
            EVENT_RETRY,
            cell,
            reason=reason,
            signature=signature,
            attempt_number=attempt_number,
            metadata=metadata,
            occurred_at_utc=occurred_at_utc,
        )

    def record_cell_complete(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        result: str,
        reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        payload = dict(metadata or {})
        payload["result"] = _required_text(result, "result")
        return self.record_event(
            EVENT_CELL_COMPLETE,
            cell,
            reason=reason,
            metadata=payload,
            occurred_at_utc=occurred_at_utc,
        )

    def record_event(
        self,
        event_type: str,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        signature: str | None = None,
        reason: str | None = None,
        attempt_number: int | None = None,
        cost_usd: float | int | None = None,
        metadata: Mapping[str, Any] | None = None,
        occurred_at_utc: str | None = None,
    ) -> PrecacheRunSnapshot:
        """Append one event and rebuild the rolling summary."""

        clean_event_type = _required_text(event_type, "event_type")
        if clean_event_type not in KNOWN_EVENT_TYPES:
            logger.error("Unknown precache run event type=%s.", clean_event_type)
            raise PrecacheRunStateError(f"Unknown precache run event type {clean_event_type!r}.")

        clean_cell = self._normalize_known_cell(cell)
        event: dict[str, Any] = {
            "event_type": clean_event_type,
            "occurred_at_utc": (
                _normalize_timestamp(occurred_at_utc, field_name="occurred_at_utc")
                if occurred_at_utc is not None
                else _utc_now_isoformat()
            ),
            "cell_id": clean_cell.cell_id,
            "bucket_id": clean_cell.bucket_id,
            "template_id": clean_cell.template_id,
        }
        if signature is not None:
            event["signature"] = _required_text(signature, "signature")
        if reason is not None:
            event["reason"] = _required_text(reason, "reason")
        if attempt_number is not None:
            clean_attempt_number = _required_positive_int(
                attempt_number,
                "attempt_number",
            )
            event["attempt_number"] = clean_attempt_number
        if cost_usd is not None:
            event["cost_usd"] = _required_nonnegative_float(cost_usd, "cost_usd")
        if metadata is not None:
            event["metadata"] = _normalized_json_mapping(metadata, field_name="metadata")

        _validate_event_shape(event)
        _append_jsonl_line(self.paths.events_path, event)
        return self.snapshot()

    def append_tool_executions(
        self,
        cell: PrecacheCell | Mapping[str, Any],
        *,
        tool_executions: Sequence[AgentToolExecution],
        attempt_number: int | None = None,
        occurred_at_utc: str | None = None,
    ) -> Path:
        """Append raw tool execution JSONL for one cell."""

        clean_cell = self._normalize_known_cell(cell)
        if isinstance(tool_executions, (str, bytes)):
            raise PrecacheRunStateError("tool_executions must be a sequence of AgentToolExecution.")
        self.paths.tool_executions_dir.mkdir(parents=True, exist_ok=True)
        transcript_path = (
            self.paths.tool_executions_dir / f"{clean_cell.transcript_basename}.jsonl"
        )
        clean_attempt_number = (
            _required_positive_int(attempt_number, "attempt_number")
            if attempt_number is not None
            else None
        )
        recorded_at = (
            _normalize_timestamp(occurred_at_utc, field_name="occurred_at_utc")
            if occurred_at_utc is not None
            else _utc_now_isoformat()
        )
        with transcript_path.open("a", encoding="utf-8") as handle:
            for execution in tool_executions:
                if not isinstance(execution, AgentToolExecution):
                    raise PrecacheRunStateError(
                        "tool_executions must contain only AgentToolExecution values."
                    )
                payload = {
                    "recorded_at_utc": recorded_at,
                    "cell_id": clean_cell.cell_id,
                    "bucket_id": clean_cell.bucket_id,
                    "template_id": clean_cell.template_id,
                    "attempt_number": clean_attempt_number,
                    "call_id": execution.call_id,
                    "tool_name": execution.tool_name,
                    "arguments": execution.arguments,
                    "output_text": execution.output_text,
                    "tool_message": execution.tool_message.to_api_dict(),
                }
                handle.write(_json_dumps(payload))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        return transcript_path

    def mark_finished(self, *, ended_at_utc: str | None = None) -> PrecacheRunSnapshot:
        """Persist the run end time once orchestration has finished."""

        manifest = _load_manifest(self.paths.manifest_path)
        clean_ended_at = (
            _normalize_timestamp(ended_at_utc, field_name="ended_at_utc")
            if ended_at_utc is not None
            else _utc_now_isoformat()
        )
        manifest["ended_at_utc"] = clean_ended_at
        _atomic_write_json(self.paths.manifest_path, manifest)
        self.ended_at_utc = clean_ended_at
        return self.snapshot()

    def _normalize_known_cell(self, cell: PrecacheCell | Mapping[str, Any]) -> PrecacheCell:
        clean_cell = PrecacheCell.from_value(cell)
        if clean_cell.cell_id not in {known.cell_id for known in self.cells}:
            logger.error(
                "Refusing to write event for unknown cell_id=%s in run=%s.",
                clean_cell.cell_id,
                self.run_id,
            )
            raise PrecacheRunStateError(
                f"Cell {clean_cell.cell_id!r} does not belong to run {self.run_id!r}."
            )
        return clean_cell


def make_precache_run_id(
    *,
    cells: Sequence[PrecacheCell | Mapping[str, Any]],
    started_at_utc: str | None = None,
) -> str:
    """Return a filesystem-safe run id: timestamp plus short cell-list hash."""

    normalized_cells = _normalize_cells(cells)
    clean_started_at = (
        _normalize_timestamp(started_at_utc, field_name="started_at_utc")
        if started_at_utc is not None
        else _utc_now_isoformat()
    )
    timestamp = _timestamp_for_run_id(clean_started_at)
    return f"{timestamp}-{_cell_list_hash(normalized_cells)}"


def rebuild_precache_run_status(run_dir: Path | str) -> PrecacheRunSnapshot:
    """Rebuild `status.json` from `events.jsonl` and return the derived snapshot."""

    root = Path(run_dir)
    paths = _paths_for_run_dir(root)
    manifest = _load_manifest(paths.manifest_path)
    run_id = _required_text(manifest.get("run_id"), "run_manifest.run_id")
    cells = _cells_from_manifest(manifest)
    cell_order = [cell.cell_id for cell in cells]
    known_cells = {cell.cell_id: cell for cell in cells}
    if not paths.events_path.exists():
        paths.events_path.touch()

    completed_cells: set[str] = set()
    plans_written = 0
    failures = 0
    duplicates_avoided = 0
    attempts_started = 0
    retries = 0
    cost_usd_total = 0.0
    updated_at = _utc_now_isoformat()
    last_event_at: str | None = None

    for line_number, event in enumerate(_read_jsonl(paths.events_path), start=1):
        _validate_event_shape(event)
        cell_id = _required_text(event.get("cell_id"), "event.cell_id")
        if cell_id not in known_cells:
            logger.error(
                "Run %s contains event for unknown cell_id=%s at line=%d.",
                run_id,
                cell_id,
                line_number,
            )
            raise PrecacheRunStateError(
                f"Event line {line_number} references unknown cell_id {cell_id!r}."
            )
        expected_cell = known_cells[cell_id]
        if event.get("bucket_id") != expected_cell.bucket_id:
            raise PrecacheRunStateError(
                f"Event line {line_number} bucket_id does not match cell_id {cell_id!r}."
            )
        if event.get("template_id") != expected_cell.template_id:
            raise PrecacheRunStateError(
                f"Event line {line_number} template_id does not match cell_id {cell_id!r}."
            )

        event_type = _required_text(event.get("event_type"), "event.event_type")
        last_event_at = _normalize_timestamp(
            event.get("occurred_at_utc"),
            field_name="event.occurred_at_utc",
        )
        updated_at = last_event_at
        cost_usd_total += float(event.get("cost_usd", 0.0))

        if event_type == EVENT_ATTEMPT_STARTED:
            attempts_started += 1
        elif event_type == EVENT_SUCCESS:
            plans_written += 1
        elif event_type == EVENT_FAILURE:
            failures += 1
        elif event_type == EVENT_DUPLICATE_SIGNATURE:
            duplicates_avoided += 1
        elif event_type == EVENT_RETRY:
            retries += 1
        elif event_type == EVENT_CELL_COMPLETE:
            if cell_id in completed_cells:
                logger.error(
                    "Run %s logged duplicate cell_complete for cell_id=%s at line=%d.",
                    run_id,
                    cell_id,
                    line_number,
                )
                raise PrecacheRunStateError(
                    f"Event line {line_number} repeats cell_complete for {cell_id!r}."
                )
            completed_cells.add(cell_id)

    completed_cell_ids = tuple(cell_id for cell_id in cell_order if cell_id in completed_cells)
    pending_cell_ids = tuple(cell_id for cell_id in cell_order if cell_id not in completed_cells)
    state = "completed" if len(completed_cell_ids) == len(cell_order) else "running"
    snapshot = PrecacheRunSnapshot(
        run_id=run_id,
        cells_total=len(cell_order),
        cells_complete=len(completed_cell_ids),
        plans_written=plans_written,
        failures=failures,
        duplicates_avoided=duplicates_avoided,
        attempts_started=attempts_started,
        retries=retries,
        cost_usd_total=round(cost_usd_total, 6),
        completed_cell_ids=completed_cell_ids,
        pending_cell_ids=pending_cell_ids,
        state=state,
        updated_at=updated_at,
        last_event_at=last_event_at,
    )
    _atomic_write_json(paths.status_path, _snapshot_to_status_payload(snapshot))
    return snapshot


def _find_resumable_run_dir(
    *,
    runs_root: Path,
    cells: tuple[PrecacheCell, ...],
    input_config: dict[str, Any],
    planner_model: str,
    cell_list_hash: str,
) -> Path | None:
    if not runs_root.exists():
        return None

    matching_run_dirs: list[Path] = []
    expected_cell_payload = [cell.to_manifest_dict() for cell in cells]
    for manifest_path in runs_root.glob(f"*/{RUN_MANIFEST_FILENAME}"):
        manifest = _load_manifest(manifest_path)
        if manifest.get("cell_list_hash") != cell_list_hash:
            continue
        if manifest.get("planner_model") != planner_model:
            continue
        if _normalized_json_mapping(
            manifest.get("input_config"),
            field_name="run_manifest.input_config",
        ) != input_config:
            continue
        if manifest.get("cells") != expected_cell_payload:
            continue

        run_dir = manifest_path.parent
        snapshot = rebuild_precache_run_status(run_dir)
        if snapshot.state == "completed":
            continue
        matching_run_dirs.append(run_dir)

    if len(matching_run_dirs) > 1:
        logger.error(
            "Found multiple incomplete matching precache runs for cell_list_hash=%s: %s",
            cell_list_hash,
            matching_run_dirs,
        )
        raise PrecacheRunStateError(
            "Found multiple incomplete matching precache runs; refusing to guess which one to "
            f"resume for cell_list_hash={cell_list_hash}."
        )
    return matching_run_dirs[0] if matching_run_dirs else None


def _paths_for_run_dir(run_dir: Path) -> PrecacheRunPaths:
    return PrecacheRunPaths(
        run_dir=run_dir,
        manifest_path=run_dir / RUN_MANIFEST_FILENAME,
        events_path=run_dir / EVENTS_FILENAME,
        status_path=run_dir / STATUS_FILENAME,
        tool_executions_dir=run_dir / TOOL_EXECUTIONS_DIRNAME,
    )


def _load_manifest(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Precache run manifest not found at {path}.")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.error("Failed to parse precache run manifest at %s.", path)
        raise PrecacheRunStateError(f"Precache run manifest at {path} is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise PrecacheRunStateError(f"Precache run manifest at {path} must be a JSON object.")
    return payload


def _cells_from_manifest(manifest: Mapping[str, Any]) -> tuple[PrecacheCell, ...]:
    raw_cells = manifest.get("cells")
    if isinstance(raw_cells, (str, bytes)) or not isinstance(raw_cells, Sequence):
        raise PrecacheRunStateError("run_manifest.cells must be a sequence of cell objects.")
    cells = tuple(PrecacheCell.from_value(cell) for cell in raw_cells)
    if len(cells) != len({cell.cell_id for cell in cells}):
        raise PrecacheRunStateError("run_manifest.cells contains duplicate cell ids.")
    expected_total = manifest.get("cells_total")
    if expected_total is not None and int(expected_total) != len(cells):
        raise PrecacheRunStateError("run_manifest.cells_total does not match run_manifest.cells.")
    return cells


def _normalize_cells(
    cells: Sequence[PrecacheCell | Mapping[str, Any]],
) -> tuple[PrecacheCell, ...]:
    if isinstance(cells, (str, bytes)) or not isinstance(cells, Sequence):
        raise PrecacheRunStateError("cells must be a non-empty sequence of cell definitions.")
    normalized = tuple(PrecacheCell.from_value(cell) for cell in cells)
    if not normalized:
        raise PrecacheRunStateError("cells must contain at least one cell.")
    duplicates = sorted(_duplicates(cell.cell_id for cell in normalized))
    if duplicates:
        logger.error("Resolved precache cell list contains duplicate cell_ids=%s.", duplicates)
        raise PrecacheRunStateError(
            f"Resolved precache cell list contains duplicate cell_ids {duplicates}."
        )
    return normalized


def _cell_list_hash(cells: Sequence[PrecacheCell]) -> str:
    payload = [cell.to_manifest_dict() for cell in cells]
    return hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()[:12]


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                logger.error("Encountered blank line in %s at line=%d.", path, line_number)
                raise PrecacheRunStateError(f"Blank line in {path} at line {line_number}.")
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                logger.error("Failed to parse %s line=%d as JSON.", path, line_number)
                raise PrecacheRunStateError(
                    f"Invalid JSON in {path} at line {line_number}."
                ) from exc
            if not isinstance(payload, dict):
                raise PrecacheRunStateError(
                    f"Expected JSON object in {path} at line {line_number}."
                )
            events.append(payload)
    return events


def _validate_event_shape(event: Mapping[str, Any]) -> None:
    event_type = _required_text(event.get("event_type"), "event.event_type")
    if event_type not in KNOWN_EVENT_TYPES:
        raise PrecacheRunStateError(f"Unknown event type {event_type!r}.")
    _normalize_timestamp(event.get("occurred_at_utc"), field_name="event.occurred_at_utc")
    cell_id = _required_text(event.get("cell_id"), "event.cell_id")
    bucket_id = _required_text(event.get("bucket_id"), "event.bucket_id")
    template_id = _required_text(event.get("template_id"), "event.template_id")
    expected_cell_id = PrecacheCell(bucket_id=bucket_id, template_id=template_id).cell_id
    if cell_id != expected_cell_id:
        raise PrecacheRunStateError(
            f"event.cell_id {cell_id!r} does not match bucket/template pair {expected_cell_id!r}."
        )
    if event_type in {EVENT_SUCCESS, EVENT_DUPLICATE_SIGNATURE}:
        _required_text(event.get("signature"), "event.signature")
    if event_type in {EVENT_FAILURE, EVENT_DUPLICATE_SIGNATURE, EVENT_RETRY}:
        _required_text(event.get("reason"), "event.reason")
    if event_type == EVENT_CELL_COMPLETE:
        metadata = event.get("metadata")
        if not isinstance(metadata, Mapping):
            raise PrecacheRunStateError("cell_complete events must include metadata.result.")
        _required_text(metadata.get("result"), "event.metadata.result")
    if "attempt_number" in event:
        _required_positive_int(event["attempt_number"], "event.attempt_number")
    if "cost_usd" in event:
        _required_nonnegative_float(event["cost_usd"], "event.cost_usd")
    if "metadata" in event and event["metadata"] is not None:
        _normalized_json_mapping(event["metadata"], field_name="event.metadata")


def _snapshot_to_status_payload(snapshot: PrecacheRunSnapshot) -> dict[str, Any]:
    return {
        "run_id": snapshot.run_id,
        "state": snapshot.state,
        "cells_total": snapshot.cells_total,
        "cells_complete": snapshot.cells_complete,
        "plans_written": snapshot.plans_written,
        "failures": snapshot.failures,
        "duplicates_avoided": snapshot.duplicates_avoided,
        "attempts_started": snapshot.attempts_started,
        "retries": snapshot.retries,
        "cost_usd_total": snapshot.cost_usd_total,
        "completed_cell_ids": list(snapshot.completed_cell_ids),
        "pending_cell_ids": list(snapshot.pending_cell_ids),
        "updated_at": snapshot.updated_at,
        "last_event_at": snapshot.last_event_at,
    }


def _append_jsonl_line(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(_json_dumps(payload))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        delete=False,
    ) as handle:
        handle.write(serialized)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
        temp_path = Path(handle.name)
    temp_path.replace(path)


def _normalized_json_mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PrecacheRunStateError(f"{field_name} must be a mapping.")
    normalized = json.loads(_json_dumps(dict(value)))
    if not isinstance(normalized, dict):
        raise PrecacheRunStateError(f"{field_name} must normalize to a JSON object.")
    return normalized


def _json_dumps(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _utc_now_isoformat() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _timestamp_for_run_id(timestamp: str) -> str:
    parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H-%M-%SZ")


def _normalize_timestamp(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise PrecacheRunStateError(f"{field_name} must be a non-empty ISO timestamp string.")
    text = value.strip()
    if not text:
        raise PrecacheRunStateError(f"{field_name} must be a non-empty ISO timestamp string.")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        logger.error("%s is not a valid ISO timestamp: %r.", field_name, value)
        raise PrecacheRunStateError(f"{field_name} must be a valid ISO timestamp.") from exc
    if parsed.tzinfo is None:
        raise PrecacheRunStateError(f"{field_name} must include timezone information.")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _required_text(value: Any, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise PrecacheRunStateError(f"{field_name} must be a non-empty string.")
    return text


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_nonnegative_float(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise PrecacheRunStateError(f"{field_name} must be numeric, got bool.")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheRunStateError(f"{field_name} must be numeric.") from exc
    if parsed < 0:
        raise PrecacheRunStateError(f"{field_name} must be non-negative.")
    return parsed


def _required_positive_int(value: Any, field_name: str) -> int:
    if isinstance(value, bool):
        raise PrecacheRunStateError(f"{field_name} must be an integer, got bool.")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise PrecacheRunStateError(f"{field_name} must be an integer.") from exc
    if parsed <= 0:
        raise PrecacheRunStateError(f"{field_name} must be positive.")
    return parsed


def _nonempty_string(value: Any) -> bool:
    return _optional_text(value) is not None


def _duplicates(values: Sequence[str] | Any) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return duplicates


def _safe_slug(value: str) -> str:
    characters = [
        character.lower()
        if character.isalnum()
        else "-"
        for character in value.strip()
    ]
    collapsed = "".join(characters)
    while "--" in collapsed:
        collapsed = collapsed.replace("--", "-")
    return collapsed.strip("-") or "cell"
