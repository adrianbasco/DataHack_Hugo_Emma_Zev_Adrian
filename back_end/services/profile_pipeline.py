"""Chunked, resumable orchestration helpers for website profile enrichment."""

from __future__ import annotations

import json
import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from back_end.services.profile_sharding import build_shard_keys
from back_end.services.profile_sharding import shard_suffix

logger = logging.getLogger(__name__)


class _WebsiteProfileClient(Protocol):
    async def enrich_dataframe(self, places: pd.DataFrame) -> pd.DataFrame: ...


@dataclass(frozen=True)
class ChunkedRunLayout:
    run_dir: Path
    shard_dir: Path
    chunk_dir: Path
    manifest_path: Path
    shard_plan_path: Path
    shard_status_path: Path
    shard_metrics_path: Path
    shard_output_path: Path


def default_run_dir(
    *,
    input_path: Path,
    backend: str,
    worker_count: int,
    shard_key: str,
    chunk_size: int,
) -> Path:
    return (
        input_path.parent
        / "website_profile_runs"
        / f"{input_path.stem}_{backend}_{worker_count}w_{shard_key}_chunks-{chunk_size}"
    )


def run_layout(
    *,
    run_dir: Path,
    shard_count: int,
    shard_index: int,
) -> ChunkedRunLayout:
    shard_label = shard_suffix(shard_count=shard_count, shard_index=shard_index).lstrip("_")
    shard_dir = run_dir / shard_label
    return ChunkedRunLayout(
        run_dir=run_dir,
        shard_dir=shard_dir,
        chunk_dir=shard_dir / "chunks",
        manifest_path=run_dir / "run_manifest.json",
        shard_plan_path=shard_dir / "plan.parquet",
        shard_status_path=shard_dir / "status.json",
        shard_metrics_path=shard_dir / "chunk_metrics.parquet",
        shard_output_path=shard_dir / "output.parquet",
    )


def ensure_run_manifest(
    *,
    run_dir: Path,
    input_path: Path,
    backend: str,
    shard_count: int,
    shard_key: str,
    chunk_size: int,
    overwrite: bool,
) -> Path:
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = run_dir / "run_manifest.json"
    manifest = {
        "input_path": str(input_path),
        "backend": backend,
        "shard_count": shard_count,
        "shard_key": shard_key,
        "chunk_size": chunk_size,
    }
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if existing != manifest and not overwrite:
            raise ValueError(
                f"Existing run manifest at {manifest_path} does not match current run settings."
            )
    _atomic_write_json(manifest_path, manifest)
    return manifest_path


def build_chunk_plan(
    places: pd.DataFrame,
    *,
    chunk_size: int,
    shard_key: str,
) -> pd.DataFrame:
    if chunk_size <= 0:
        raise ValueError(f"chunk_size must be positive. Got {chunk_size}.")
    if "fsq_place_id" not in places.columns:
        raise ValueError("places DataFrame must contain fsq_place_id for chunk planning.")

    shard_keys = build_shard_keys(places, shard_key=shard_key)
    plan = places.loc[:, ["fsq_place_id"]].copy()
    plan["_chunk_group_key"] = shard_keys.astype(str)
    plan = plan.sort_values(["_chunk_group_key", "fsq_place_id"], kind="stable").reset_index(drop=True)

    unique_keys = plan["_chunk_group_key"].drop_duplicates().tolist()
    key_to_chunk = {key: index // chunk_size for index, key in enumerate(unique_keys)}
    plan["chunk_index"] = plan["_chunk_group_key"].map(key_to_chunk)
    return plan


def prepare_shard_plan(
    places: pd.DataFrame,
    *,
    layout: ChunkedRunLayout,
    chunk_size: int,
    shard_key: str,
    overwrite: bool,
) -> pd.DataFrame:
    layout.shard_dir.mkdir(parents=True, exist_ok=True)
    layout.chunk_dir.mkdir(parents=True, exist_ok=True)

    if layout.shard_plan_path.exists() and not overwrite:
        return pd.read_parquet(layout.shard_plan_path)

    plan = build_chunk_plan(
        places,
        chunk_size=chunk_size,
        shard_key=shard_key,
    )
    _atomic_write_parquet(plan, path=layout.shard_plan_path, overwrite=True)
    return plan


async def run_chunked_profile_enrichment(
    *,
    places: pd.DataFrame,
    client: _WebsiteProfileClient,
    layout: ChunkedRunLayout,
    chunk_size: int,
    shard_key: str,
    overwrite: bool,
    progress_every: int | None = None,
) -> Path:
    if places.empty:
        raise ValueError("Refusing to run chunked enrichment over 0 places.")
    if progress_every is not None and progress_every <= 0:
        raise ValueError(f"progress_every must be positive when provided. Got {progress_every}.")

    plan = prepare_shard_plan(
        places,
        layout=layout,
        chunk_size=chunk_size,
        shard_key=shard_key,
        overwrite=overwrite,
    )
    chunk_count = int(plan["chunk_index"].max()) + 1
    total_row_count = len(places)
    processed_row_count = 0
    next_progress_mark = progress_every
    chunk_metrics = _load_chunk_metrics(layout=layout)
    places_by_id = places.set_index("fsq_place_id", drop=False)

    _write_shard_status(
        layout=layout,
        status={
            "state": "running",
            "processed_row_count": processed_row_count,
            "total_row_count": total_row_count,
            "completed_chunk_count": int(chunk_metrics["chunk_index"].nunique()),
            "total_chunk_count": chunk_count,
            "updated_at": _utc_now_isoformat(),
        },
    )

    for chunk_index in range(chunk_count):
        chunk_ids = plan.loc[plan["chunk_index"] == chunk_index, "fsq_place_id"].tolist()
        if not chunk_ids:
            raise ValueError(f"Chunk {chunk_index} in {layout.shard_plan_path} contains 0 rows.")

        chunk_path = chunk_output_path(layout=layout, chunk_index=chunk_index)
        if chunk_path.exists() and not overwrite:
            if validate_chunk_output(chunk_path=chunk_path, expected_place_ids=chunk_ids):
                processed_row_count += len(chunk_ids)
                next_progress_mark = _log_progress_marks(
                    processed_row_count=processed_row_count,
                    total_row_count=total_row_count,
                    next_progress_mark=next_progress_mark,
                    progress_every=progress_every,
                )
                chunk_metrics = _record_chunk_metric(
                    layout=layout,
                    chunk_index=chunk_index,
                    row_count=len(chunk_ids),
                    elapsed_seconds=None,
                    status="skipped_existing",
                    metrics_df=chunk_metrics,
                )
                logger.info(
                    "Skipping completed shard chunk %d/%d at %s (%d/%d rows already complete).",
                    chunk_index + 1,
                    chunk_count,
                    chunk_path,
                    processed_row_count,
                    total_row_count,
                )
                _write_shard_status(
                    layout=layout,
                    status={
                        "state": "running",
                        "processed_row_count": processed_row_count,
                        "total_row_count": total_row_count,
                        "completed_chunk_count": int(chunk_metrics["chunk_index"].nunique()),
                        "total_chunk_count": chunk_count,
                        "last_completed_chunk_index": chunk_index,
                        "updated_at": _utc_now_isoformat(),
                    },
                )
                continue
            raise ValueError(
                f"Existing chunk output at {chunk_path} did not match the expected place ids."
            )

        chunk_places = places_by_id.loc[chunk_ids].copy().reset_index(drop=True)
        logger.info(
            "Processing shard chunk %d/%d with %d rows.",
            chunk_index + 1,
            chunk_count,
            len(chunk_places),
        )
        started_at = time.perf_counter()
        enriched = await client.enrich_dataframe(chunk_places)
        elapsed_seconds = time.perf_counter() - started_at
        status_summary = _enrichment_status_summary(enriched)
        _atomic_write_parquet(enriched, path=chunk_path, overwrite=True)
        processed_row_count += len(chunk_places)
        next_progress_mark = _log_progress_marks(
            processed_row_count=processed_row_count,
            total_row_count=total_row_count,
            next_progress_mark=next_progress_mark,
            progress_every=progress_every,
        )
        chunk_metrics = _record_chunk_metric(
            layout=layout,
            chunk_index=chunk_index,
            row_count=len(chunk_places),
            elapsed_seconds=elapsed_seconds,
            status="completed",
            status_summary=status_summary,
            metrics_df=chunk_metrics,
        )
        logger.info(
            "Completed shard chunk %d/%d: %d rows in %.2fs (%.2f rows/s, %d/%d rows done). Statuses: %s",
            chunk_index + 1,
            chunk_count,
            len(chunk_places),
            elapsed_seconds,
            len(chunk_places) / elapsed_seconds if elapsed_seconds > 0 else float("inf"),
            processed_row_count,
            total_row_count,
            status_summary,
        )
        _write_shard_status(
            layout=layout,
            status={
                "state": "running",
                "processed_row_count": processed_row_count,
                "total_row_count": total_row_count,
                "completed_chunk_count": int(chunk_metrics["chunk_index"].nunique()),
                "total_chunk_count": chunk_count,
                "last_completed_chunk_index": chunk_index,
                "last_chunk_elapsed_seconds": elapsed_seconds,
                "updated_at": _utc_now_isoformat(),
            },
        )

    shard_output_path = merge_completed_chunks(
        layout=layout,
        overwrite=overwrite,
    )
    _write_shard_status(
        layout=layout,
        status={
            "state": "completed",
            "processed_row_count": total_row_count,
            "total_row_count": total_row_count,
            "completed_chunk_count": chunk_count,
            "total_chunk_count": chunk_count,
            "shard_output_path": str(shard_output_path),
            "updated_at": _utc_now_isoformat(),
        },
    )
    return shard_output_path


def _log_progress_marks(
    *,
    processed_row_count: int,
    total_row_count: int,
    next_progress_mark: int | None,
    progress_every: int | None,
) -> int | None:
    if progress_every is None or next_progress_mark is None:
        return next_progress_mark
    while processed_row_count >= next_progress_mark:
        logger.info(
            "Progress: processed at least %d/%d rows.",
            min(next_progress_mark, total_row_count),
            total_row_count,
        )
        next_progress_mark += progress_every
    return next_progress_mark


def merge_completed_chunks(
    *,
    layout: ChunkedRunLayout,
    overwrite: bool,
) -> Path:
    if layout.shard_output_path.exists() and not overwrite:
        logger.info(
            "Shard output already exists at %s. Leaving it unchanged.",
            layout.shard_output_path,
        )
        return layout.shard_output_path

    if not layout.shard_plan_path.exists():
        raise FileNotFoundError(f"Shard plan not found at {layout.shard_plan_path}.")

    plan = pd.read_parquet(layout.shard_plan_path)
    chunk_indexes = sorted(plan["chunk_index"].unique().tolist())
    chunk_paths: list[Path] = []
    seen_place_ids: set[str] = set()
    for chunk_index in chunk_indexes:
        chunk_path = chunk_output_path(layout=layout, chunk_index=int(chunk_index))
        if not chunk_path.exists():
            raise FileNotFoundError(f"Expected completed chunk parquet at {chunk_path}.")
        chunk_ids = plan.loc[plan["chunk_index"] == chunk_index, "fsq_place_id"].tolist()
        if not validate_chunk_output(chunk_path=chunk_path, expected_place_ids=chunk_ids):
            raise ValueError(
                f"Chunk parquet at {chunk_path} does not match the expected place ids."
            )
        place_ids = pd.read_parquet(chunk_path, columns=["fsq_place_id"])["fsq_place_id"].astype(str)
        overlap = seen_place_ids.intersection(place_ids)
        if overlap:
            raise ValueError(
                f"Shard merge for {layout.shard_dir} produced duplicate fsq_place_id values."
            )
        seen_place_ids.update(place_ids)
        chunk_paths.append(chunk_path)

    _stream_merge_parquets(
        source_paths=chunk_paths,
        output_path=layout.shard_output_path,
        overwrite=True,
    )
    return layout.shard_output_path


def stream_merge_shard_outputs(
    *,
    shard_paths: list[Path],
    output_path: Path,
    overwrite: bool,
) -> None:
    seen_place_ids: set[str] = set()
    for shard_path in shard_paths:
        place_ids = pd.read_parquet(shard_path, columns=["fsq_place_id"])["fsq_place_id"].astype(str)
        overlap = seen_place_ids.intersection(place_ids)
        if overlap:
            raise ValueError("Merged shard output contains duplicate fsq_place_id values.")
        seen_place_ids.update(place_ids)
    _stream_merge_parquets(
        source_paths=shard_paths,
        output_path=output_path,
        overwrite=overwrite,
    )


def chunk_output_path(*, layout: ChunkedRunLayout, chunk_index: int) -> Path:
    return layout.chunk_dir / f"chunk-{chunk_index:05d}.parquet"


def validate_chunk_output(
    *,
    chunk_path: Path,
    expected_place_ids: list[str],
) -> bool:
    actual = pd.read_parquet(chunk_path, columns=["fsq_place_id"])["fsq_place_id"].astype(str)
    return sorted(actual.tolist()) == sorted(str(place_id) for place_id in expected_place_ids)


def _load_chunk_metrics(*, layout: ChunkedRunLayout) -> pd.DataFrame:
    if not layout.shard_metrics_path.exists():
        return pd.DataFrame(
            columns=[
                "chunk_index",
                "row_count",
                "elapsed_seconds",
                "rows_per_second",
                "status",
                "enrichment_status_summary",
                "updated_at",
            ]
        )
    return pd.read_parquet(layout.shard_metrics_path)


def _record_chunk_metric(
    *,
    layout: ChunkedRunLayout,
    chunk_index: int,
    row_count: int,
    elapsed_seconds: float | None,
    status: str,
    metrics_df: pd.DataFrame,
    status_summary: str | None = None,
) -> pd.DataFrame:
    existing = metrics_df.loc[metrics_df["chunk_index"] == chunk_index]
    if status == "skipped_existing" and not existing.empty:
        return metrics_df

    updated = metrics_df.loc[metrics_df["chunk_index"] != chunk_index].copy()
    updated = pd.concat(
        [
            updated,
            pd.DataFrame(
                [
                    {
                        "chunk_index": chunk_index,
                        "row_count": row_count,
                        "elapsed_seconds": elapsed_seconds,
                        "rows_per_second": (
                            row_count / elapsed_seconds
                            if elapsed_seconds is not None and elapsed_seconds > 0
                            else None
                        ),
                        "status": status,
                        "enrichment_status_summary": status_summary,
                        "updated_at": _utc_now_isoformat(),
                    }
                ]
            ),
        ],
        ignore_index=True,
    )
    updated.sort_values("chunk_index", inplace=True, kind="stable")
    updated.reset_index(drop=True, inplace=True)
    _atomic_write_parquet(updated, path=layout.shard_metrics_path, overwrite=True)
    return updated


def _enrichment_status_summary(enriched: pd.DataFrame) -> str:
    status_columns = [
        column
        for column in (
            "no_website_profile_status",
            "profile_enrichment_status",
            "crawl4ai_enrichment_status",
            "website_enrichment_status",
        )
        if column in enriched.columns
    ]
    if not status_columns:
        return "no_status_column"
    status_column = status_columns[0]
    counts = enriched[status_column].fillna("missing").astype(str).value_counts()
    return ", ".join(f"{status}={count}" for status, count in counts.items())


def _write_shard_status(*, layout: ChunkedRunLayout, status: dict[str, object]) -> None:
    _atomic_write_json(layout.shard_status_path, status)


def _stream_merge_parquets(
    *,
    source_paths: list[Path],
    output_path: Path,
    overwrite: bool,
) -> None:
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file {output_path}. Pass overwrite=True to replace it."
        )
    tmp_path: Path | None = None
    writer: pq.ParquetWriter | None = None
    target_schema: pa.Schema | None = None
    try:
        target_schema = _unified_parquet_schema(source_paths)
        with tempfile.NamedTemporaryFile(
            prefix=f"{output_path.stem}.",
            suffix=".tmp.parquet",
            dir=output_path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)

        for source_path in source_paths:
            frame = pd.read_parquet(source_path)
            table = pa.Table.from_pandas(frame, preserve_index=False)
            table = table.cast(target_schema, safe=False)
            if writer is None:
                writer = pq.ParquetWriter(tmp_path, target_schema)
            writer.write_table(table)
        if writer is None:
            raise ValueError(f"Refusing to write empty merged parquet to {output_path}.")
        writer.close()
        writer = None
        os.replace(tmp_path, output_path)
    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise


def _unified_parquet_schema(source_paths: list[Path]) -> pa.Schema:
    """Build a merge schema that tolerates all-null columns in early chunks."""

    schemas: list[pa.Schema] = []
    for source_path in source_paths:
        table = pa.Table.from_pandas(pd.read_parquet(source_path), preserve_index=False)
        schemas.append(table.schema)
    if not schemas:
        raise ValueError("Refusing to merge 0 parquet sources.")
    try:
        return pa.unify_schemas(schemas)
    except (pa.ArrowInvalid, pa.ArrowTypeError):
        # A null-only chunk cannot define the eventual logical type, and pandas
        # can infer int64 in one chunk but double in another when nulls are
        # present. Pick a stable widening type per field.
        ordered_names: list[str] = []
        types_by_name: dict[str, list[pa.DataType]] = {}
        for schema in schemas:
            for field in schema:
                if field.name not in ordered_names:
                    ordered_names.append(field.name)
                types_by_name.setdefault(field.name, []).append(field.type)
        return pa.schema(
            [
                pa.field(name, _widen_arrow_type(types_by_name[name]))
                for name in ordered_names
            ]
        )


def _widen_arrow_type(types: list[pa.DataType]) -> pa.DataType:
    non_null_types = [data_type for data_type in types if not pa.types.is_null(data_type)]
    if not non_null_types:
        return pa.string()
    if any(pa.types.is_string(data_type) or pa.types.is_large_string(data_type) for data_type in non_null_types):
        return pa.large_string()
    if any(pa.types.is_floating(data_type) for data_type in non_null_types):
        return pa.float64()
    if all(pa.types.is_integer(data_type) for data_type in non_null_types):
        return pa.int64()
    if all(pa.types.is_boolean(data_type) for data_type in non_null_types):
        return pa.bool_()
    if all(pa.types.is_list(data_type) or pa.types.is_large_list(data_type) for data_type in non_null_types):
        value_types = [
            data_type.value_type
            for data_type in non_null_types
            if pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
        ]
        return pa.large_list(_widen_arrow_type(value_types))
    return non_null_types[0]


def _atomic_write_parquet(df: pd.DataFrame, *, path: Path, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(
            f"Refusing to overwrite existing file {path}. Pass overwrite=True to replace it."
        )
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"{path.stem}.",
            suffix=".tmp.parquet",
            dir=path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
        df.to_parquet(tmp_path, index=False)
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise


def _atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            prefix=f"{path.stem}.",
            suffix=".tmp.json",
            dir=path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        os.replace(tmp_path, path)
    except Exception:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()
        raise


def _utc_now_isoformat() -> str:
    return datetime.now(UTC).isoformat()
