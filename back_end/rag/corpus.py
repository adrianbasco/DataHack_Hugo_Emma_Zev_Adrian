"""Build a read-only-derived RAG corpus from valid scraped shard chunks."""

from __future__ import annotations

import hashlib
import logging
import os
import tempfile
from pathlib import Path
from typing import Iterable

import pandas as pd

from back_end.query.settings import REPO_ROOT
from back_end.rag.models import RagCorpusBuildResult, RagPlaceDocument
from back_end.rag.settings import RagSettings, load_rag_settings, make_run_id

logger = logging.getLogger(__name__)

DOCUMENT_FILENAME = "place_documents.parquet"
MANIFEST_FILENAME = "manifest.parquet"

CANDIDATE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "name",
    "latitude",
    "longitude",
    "locality",
    "region",
    "postcode",
    "fsq_category_labels",
    "date_closed",
)

PROFILE_REQUIRED_COLUMNS: tuple[str, ...] = (
    "fsq_place_id",
    "crawl4ai_enrichment_status",
    "crawl4ai_rich_profile_text",
    "crawl4ai_quality_score",
    "crawl4ai_template_stop_tags",
    "crawl4ai_ambience_tags",
    "crawl4ai_setting_tags",
    "crawl4ai_activity_tags",
    "crawl4ai_drink_tags",
    "crawl4ai_booking_signals",
    "crawl4ai_evidence_snippets",
)


class RagCorpusError(RuntimeError):
    """Raised when the RAG corpus cannot be built safely."""


def discover_profile_chunk_paths(profile_runs_root: Path | str) -> tuple[Path, ...]:
    """Return scraped profile chunk parquets below the run root."""

    root = Path(profile_runs_root)
    if not root.exists():
        raise FileNotFoundError(f"Profile runs root not found at {root}.")
    chunk_paths = tuple(
        sorted(path for path in root.rglob("*.parquet") if path.parent.name == "chunks")
    )
    if not chunk_paths:
        raise FileNotFoundError(f"No scraped profile chunk parquet files found under {root}.")
    return chunk_paths


def build_and_write_rag_corpus(
    *,
    settings: RagSettings | None = None,
    candidate_parquet_path: Path | str | None = None,
    profile_chunk_paths: Iterable[Path | str] | None = None,
    output_dir: Path | str | None = None,
    run_id: str | None = None,
    min_profile_quality_score: int | None = None,
    overwrite: bool = False,
) -> RagCorpusBuildResult:
    """Build a corpus and write it into a new derived RAG run directory."""

    settings = settings or load_rag_settings()
    run_id = run_id or make_run_id("rag-corpus")
    output_dir = Path(output_dir) if output_dir is not None else settings.rag_runs_root / run_id
    _prepare_output_dir(output_dir, overwrite=overwrite)
    chunk_paths = tuple(
        Path(path)
        for path in (
            profile_chunk_paths or discover_profile_chunk_paths(settings.profile_runs_root)
        )
    )

    documents, manifest = build_rag_corpus_documents(
        candidate_parquet_path=candidate_parquet_path or settings.candidate_parquet_path,
        profile_chunk_paths=chunk_paths,
        run_id=run_id,
        min_profile_quality_score=(
            settings.min_profile_quality_score
            if min_profile_quality_score is None
            else min_profile_quality_score
        ),
    )

    documents_path = output_dir / DOCUMENT_FILENAME
    manifest_path = output_dir / MANIFEST_FILENAME
    _atomic_write_parquet(documents, documents_path, overwrite=overwrite)
    _atomic_write_parquet(manifest, manifest_path, overwrite=overwrite)

    excluded_count = int(
        manifest.loc[manifest["metric"].eq("excluded_rows"), "value"].sum()
    )
    result = RagCorpusBuildResult(
        output_dir=output_dir,
        documents_path=documents_path,
        manifest_path=manifest_path,
        document_count=len(documents),
        excluded_count=excluded_count,
        source_chunk_count=len(chunk_paths),
    )
    logger.info(
        "Built RAG corpus with %d documents and %d excluded rows at %s.",
        result.document_count,
        result.excluded_count,
        output_dir,
    )
    return result


def build_rag_corpus_documents(
    *,
    candidate_parquet_path: Path | str,
    profile_chunk_paths: Iterable[Path | str],
    run_id: str,
    min_profile_quality_score: int = 1,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return valid place documents and a manifest without writing files."""

    if min_profile_quality_score < 0:
        raise ValueError("min_profile_quality_score must be non-negative.")

    candidate_path = _require_parquet(candidate_parquet_path, must_exist=True)
    chunk_paths = tuple(_require_parquet(path, must_exist=True) for path in profile_chunk_paths)
    if not chunk_paths:
        raise ValueError("profile_chunk_paths must contain at least one parquet file.")

    candidates = pd.read_parquet(candidate_path, columns=list(CANDIDATE_REQUIRED_COLUMNS))
    _validate_columns(candidates, CANDIDATE_REQUIRED_COLUMNS, source=candidate_path)
    duplicate_candidate_count = int(candidates["fsq_place_id"].duplicated().sum())
    if duplicate_candidate_count:
        raise RagCorpusError(
            f"Candidate parquet {candidate_path} contains {duplicate_candidate_count} "
            "duplicate fsq_place_id values."
        )
    candidates = candidates.copy()
    candidates["fsq_place_id"] = candidates["fsq_place_id"].astype(str)
    candidate_by_id = candidates.set_index("fsq_place_id", drop=False)

    manifest_rows: list[dict[str, object]] = [
        _manifest("input", "candidate_parquet_rows", len(candidates), str(candidate_path)),
        _manifest("input", "source_chunk_count", len(chunk_paths), None),
    ]

    profile_frames: list[pd.DataFrame] = []
    for chunk_path in chunk_paths:
        frame = pd.read_parquet(chunk_path)
        _validate_columns(frame, PROFILE_REQUIRED_COLUMNS, source=chunk_path)
        frame = frame.loc[:, list(PROFILE_REQUIRED_COLUMNS)].copy()
        frame["source_chunk_path"] = str(chunk_path)
        profile_frames.append(frame)
        manifest_rows.append(
            _manifest("input", "source_chunk_rows", len(frame), str(chunk_path))
        )
        for status, count in (
            frame["crawl4ai_enrichment_status"]
            .fillna("<missing>")
            .astype(str)
            .value_counts()
            .sort_index()
            .items()
        ):
            manifest_rows.append(
                _manifest("status", f"crawl4ai_status:{status}", int(count), str(chunk_path))
            )

    profiles = pd.concat(profile_frames, ignore_index=True)
    profiles["fsq_place_id"] = profiles["fsq_place_id"].fillna("").astype(str).str.strip()

    checks = {
        "missing_fsq_place_id": profiles["fsq_place_id"].eq(""),
        "non_ok_scrape_status": profiles["crawl4ai_enrichment_status"].astype(str).ne("ok"),
        "empty_rich_profile_text": ~profiles["crawl4ai_rich_profile_text"].map(
            _has_text
        ),
        "quality_score_below_threshold": pd.to_numeric(
            profiles["crawl4ai_quality_score"], errors="coerce"
        ).fillna(-1)
        < min_profile_quality_score,
        "not_in_candidate_parquet": ~profiles["fsq_place_id"].isin(candidate_by_id.index),
    }
    for reason, mask in checks.items():
        count = int(mask.sum())
        if count:
            manifest_rows.append(_manifest("exclusion", reason, count, None))

    valid_mask = pd.Series(True, index=profiles.index)
    for mask in checks.values():
        valid_mask &= ~mask
    valid_profiles = profiles.loc[valid_mask].copy()

    duplicate_valid_count = int(valid_profiles["fsq_place_id"].duplicated().sum())
    if duplicate_valid_count:
        manifest_rows.append(
            _manifest("dedupe", "duplicate_valid_scraped_rows", duplicate_valid_count, None)
        )
        valid_profiles["_quality"] = pd.to_numeric(
            valid_profiles["crawl4ai_quality_score"], errors="coerce"
        ).fillna(0)
        valid_profiles["_profile_len"] = valid_profiles["crawl4ai_rich_profile_text"].astype(
            str
        ).str.len()
        valid_profiles = (
            valid_profiles.sort_values(
                ["fsq_place_id", "_quality", "_profile_len", "source_chunk_path"],
                ascending=[True, False, False, True],
                kind="stable",
            )
            .drop_duplicates("fsq_place_id", keep="first")
            .drop(columns=["_quality", "_profile_len"])
        )

    documents: list[RagPlaceDocument] = []
    invalid_coordinate_count = 0
    closed_candidate_count = 0
    for row in valid_profiles.itertuples(index=False):
        place_id = str(row.fsq_place_id)
        candidate = candidate_by_id.loc[place_id]
        if not _is_missing(candidate["date_closed"]):
            closed_candidate_count += 1
            continue
        latitude = _as_float(candidate["latitude"])
        longitude = _as_float(candidate["longitude"])
        if latitude is None or longitude is None:
            invalid_coordinate_count += 1
            continue

        document_text = _build_document_text(row, candidate)
        document_hash = _document_hash(place_id=place_id, document_text=document_text)
        documents.append(
            RagPlaceDocument(
                fsq_place_id=place_id,
                name=str(candidate["name"]),
                latitude=latitude,
                longitude=longitude,
                locality=_optional_text(candidate.get("locality")),
                region=_optional_text(candidate.get("region")),
                postcode=_optional_text(candidate.get("postcode")),
                fsq_category_labels=_string_tuple(candidate.get("fsq_category_labels")),
                crawl4ai_quality_score=int(
                    pd.to_numeric(row.crawl4ai_quality_score, errors="coerce")
                ),
                crawl4ai_template_stop_tags=_string_tuple(
                    row.crawl4ai_template_stop_tags
                ),
                crawl4ai_ambience_tags=_string_tuple(row.crawl4ai_ambience_tags),
                crawl4ai_setting_tags=_string_tuple(row.crawl4ai_setting_tags),
                crawl4ai_activity_tags=_string_tuple(row.crawl4ai_activity_tags),
                crawl4ai_drink_tags=_string_tuple(row.crawl4ai_drink_tags),
                crawl4ai_booking_signals=_string_tuple(row.crawl4ai_booking_signals),
                crawl4ai_evidence_snippets=_string_tuple(
                    row.crawl4ai_evidence_snippets
                ),
                source_chunk_path=str(row.source_chunk_path),
                source_run_id=run_id,
                document_text=document_text,
                document_hash=document_hash,
            )
        )

    if invalid_coordinate_count:
        manifest_rows.append(
            _manifest("exclusion", "invalid_candidate_coordinates", invalid_coordinate_count, None)
        )
    if closed_candidate_count:
        manifest_rows.append(
            _manifest("exclusion", "closed_candidate_place", closed_candidate_count, None)
        )
    if not documents:
        raise RagCorpusError(
            "RAG corpus build produced 0 valid scraped documents. Refusing to write "
            "an empty semantic corpus."
        )

    document_frame = pd.DataFrame([document.to_dict() for document in documents])
    excluded_rows = len(profiles) - len(document_frame)
    manifest_rows.append(_manifest("output", "document_rows", len(document_frame), None))
    manifest_rows.append(_manifest("output", "excluded_rows", excluded_rows, None))
    manifest = pd.DataFrame(manifest_rows)
    return document_frame, manifest


def _build_document_text(row: object, candidate: pd.Series) -> str:
    evidence = _string_tuple(row.crawl4ai_evidence_snippets)
    profile_text = str(row.crawl4ai_rich_profile_text).strip()
    parts = [
        f"Name: {candidate['name']}",
        "Area: "
        + _join_non_empty(
            [
                candidate.get("locality"),
                candidate.get("region"),
                candidate.get("postcode"),
            ]
        ),
        f"Categories: {_join_non_empty(_string_tuple(candidate.get('fsq_category_labels')))}",
        f"Date stop tags: {_join_non_empty(_string_tuple(row.crawl4ai_template_stop_tags))}",
        f"Ambience: {_join_non_empty(_string_tuple(row.crawl4ai_ambience_tags))}",
        f"Setting: {_join_non_empty(_string_tuple(row.crawl4ai_setting_tags))}",
        f"Activities: {_join_non_empty(_string_tuple(row.crawl4ai_activity_tags))}",
        f"Drinks: {_join_non_empty(_string_tuple(row.crawl4ai_drink_tags))}",
        f"Booking signals: {_join_non_empty(_string_tuple(row.crawl4ai_booking_signals))}",
    ]
    if evidence:
        parts.append("Evidence snippets:")
        parts.extend(f"- {snippet}" for snippet in evidence[:8])
    parts.append("Website profile:")
    parts.append(profile_text)
    return "\n".join(part for part in parts if part.strip())


def _document_hash(*, place_id: str, document_text: str) -> str:
    """Return a stable unique hash for one place document.

    Chain venues can produce identical scraped text across multiple FSQ places.
    The embedding table is keyed by fsq_place_id/document_hash, but duplicate
    document_hash values are still ambiguous for reuse checks, so include the
    local place id in the hash input.
    """

    return hashlib.sha256(
        f"fsq_place_id:{place_id}\n{document_text}".encode("utf-8")
    ).hexdigest()


def _prepare_output_dir(output_dir: Path, *, overwrite: bool) -> None:
    _reject_protected_output_path(output_dir)
    if output_dir.exists():
        if not output_dir.is_dir():
            raise FileExistsError(f"RAG output path exists and is not a directory: {output_dir}")
        if any(output_dir.iterdir()) and not overwrite:
            raise FileExistsError(
                f"Refusing to write into non-empty RAG output directory {output_dir}. "
                "Pass overwrite=True to replace derived RAG outputs."
            )
    output_dir.mkdir(parents=True, exist_ok=True)


def _reject_protected_output_path(path: Path) -> None:
    resolved = path.resolve()
    protected_dirs = (
        REPO_ROOT / "data" / "website_profile_runs",
        REPO_ROOT / "data" / "au_places_shards",
    )
    for protected in protected_dirs:
        protected_resolved = protected.resolve()
        if resolved == protected_resolved or protected_resolved in resolved.parents:
            raise RagCorpusError(f"Refusing to write RAG outputs under protected path {protected}.")


def _atomic_write_parquet(df: pd.DataFrame, path: Path, *, overwrite: bool) -> None:
    _reject_protected_output_path(path)
    if path.suffix != ".parquet":
        raise ValueError(f"RAG output must be a parquet file. Got {path}.")
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing RAG parquet {path}.")
    path.parent.mkdir(parents=True, exist_ok=True)

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


def _validate_columns(df: pd.DataFrame, required: Iterable[str], *, source: Path) -> None:
    missing = sorted(set(required) - set(df.columns))
    if missing:
        raise RagCorpusError(
            f"Parquet {source} is missing required RAG columns {missing}. "
            f"Got columns: {sorted(df.columns)}."
        )


def _require_parquet(path: Path | str, *, must_exist: bool) -> Path:
    resolved = Path(path)
    if resolved.suffix != ".parquet":
        raise ValueError(f"Expected a .parquet path. Got {resolved}.")
    if must_exist and not resolved.exists():
        raise FileNotFoundError(f"Parquet file not found at {resolved}.")
    return resolved


def _manifest(kind: str, metric: str, value: int, detail: str | None) -> dict[str, object]:
    return {"kind": kind, "metric": metric, "value": int(value), "detail": detail}


def _has_text(value: object) -> bool:
    return _optional_text(value) is not None


def _optional_text(value: object) -> str | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    return text or None


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _as_float(value: object) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if pd.isna(result):
        return None
    return result


def _string_tuple(value: object) -> tuple[str, ...]:
    if _is_missing(value):
        return ()
    if isinstance(value, str):
        values = [value]
    else:
        try:
            values = list(value)  # type: ignore[arg-type]
        except TypeError:
            values = [value]
    result = tuple(str(item).strip() for item in values if str(item).strip())
    return result


def _join_non_empty(values: Iterable[object]) -> str:
    return ", ".join(str(value).strip() for value in values if _optional_text(value))
