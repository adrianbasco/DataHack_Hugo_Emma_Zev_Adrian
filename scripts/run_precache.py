"""MVP driver: generate N differentiated date ideas for one template + bucket.

Usage:
    python -m scripts.run_precache \
        --template-id dinner_then_dessert \
        --bucket-id sydney_cbd \
        --count 3 \
        --rag-run rag-corpus-agent-smoke

This is the hackathon-grade runner. It wraps PrecachePlanner in a simple loop
that accumulates signatures between calls so every new plan must differ from
the previous ones by at least one venue. It also loosens Google Maps match
thresholds to demo-friendly defaults and writes a per-run event log under
``data/precache/runs/<run_id>/``.
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import logging
import os
import signal
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.agents.date_idea_agent import (  # noqa: E402
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
)
from back_end.agents.precache_planner import (  # noqa: E402
    FAILURE_REASON_DUPLICATE,
    PrecachePlanner,
    PrecachePlannerFailure,
    PrecachePlannerRequest,
    PrecachePlannerSuccess,
)
from back_end.clients.api_trace import ApiTraceLogger  # noqa: E402
from back_end.clients.maps import GoogleMapsClient  # noqa: E402
from back_end.clients.openrouter import OpenRouterClient  # noqa: E402
from back_end.clients.settings import MapsSettings, OpenRouterSettings  # noqa: E402
from back_end.domain.models import TravelMode  # noqa: E402
from back_end.precache.candidate_pools import (  # noqa: E402
    build_location_candidate_pool,
    load_location_buckets,
)
from back_end.precache.output import (  # noqa: E402
    DEFAULT_PRECACHE_OUTPUT_PATH,
    append_precache_plans,
)
from back_end.rag.embeddings import (  # noqa: E402
    LOCAL_HASHING_EMBEDDING_MODEL_PREFIX,
    LocalHashingEmbeddingClient,
    default_local_embedding_client,
)
from back_end.rag.retriever import load_date_templates  # noqa: E402
from back_end.rag.settings import load_rag_settings  # noqa: E402
from back_end.rag.vector_store import ExactVectorStore  # noqa: E402

logger = logging.getLogger("run_precache")

SYDNEY_TZ = ZoneInfo("Australia/Sydney")

TIME_OF_DAY_HOUR: dict[str, int] = {
    "morning": 10,
    "midday": 12,
    "afternoon": 14,
    "evening": 19,
    "night": 21,
    "flexible": 18,
}

# Weekend-biased day picker: mornings/middays → Saturday, afternoons+ → Friday.
TIME_OF_DAY_WEEKDAY: dict[str, int] = {
    "morning": 5,
    "midday": 5,
    "afternoon": 5,
    "evening": 4,
    "night": 4,
    "flexible": 5,
}

TRANSPORT_MODE_ALIASES: dict[str, TravelMode] = {
    "walk": TravelMode.WALK,
    "walking": TravelMode.WALK,
    "drive": TravelMode.DRIVE,
    "driving": TravelMode.DRIVE,
    "bicycle": TravelMode.BICYCLE,
    "bike": TravelMode.BICYCLE,
    "cycling": TravelMode.BICYCLE,
    "transit": TravelMode.TRANSIT,
    "public_transport": TravelMode.TRANSIT,
}

# Hackathon-friendly defaults. Production values (see MapsSettings) are
# stricter: 0.92 name similarity + 250m is realistic for billing auditability,
# but in practice Foursquare names drift enough from Google names ("Felix Bar &
# Bistro" vs "Felix", "RivaReno Gelato" vs "RivaReno Gelato Italiano",
# "Employees Only" vs "Employees Only Sydney") that the demo fails constantly.
DEMO_MIN_NAME_SIMILARITY = 0.72
DEMO_MAX_MATCH_DISTANCE_METERS = 350.0
DEMO_MIN_PLACE_RATING = 3.5
DEMO_MIN_USER_RATING_COUNT = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate N differentiated, Maps-verified date ideas for a given "
            "(template, bucket) pair and append them to the pre-cache parquet."
        )
    )
    parser.add_argument("--template-id", required=True)
    parser.add_argument("--bucket-id", default="sydney_cbd")
    parser.add_argument("--count", type=int, default=3)
    parser.add_argument(
        "--rag-run",
        default="rag-corpus-agent-smoke",
        help="RAG run id under data/rag/runs.",
    )
    parser.add_argument("--documents", type=Path, default=None)
    parser.add_argument("--embeddings", type=Path, default=None)
    parser.add_argument(
        "--buckets-path",
        type=Path,
        default=REPO_ROOT / "config" / "location_buckets.yaml",
    )
    parser.add_argument(
        "--templates-path",
        type=Path,
        default=REPO_ROOT / "config" / "date_templates.yaml",
    )
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_PRECACHE_OUTPUT_PATH,
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Where to write events.jsonl + summary.json; defaults to "
        "data/precache/runs/<timestamp-hash>/",
    )
    parser.add_argument(
        "--transport-mode",
        default=None,
        help="Override bucket default (walking / driving / bicycle / transit).",
    )
    parser.add_argument("--max-leg-seconds", type=int, default=1200)
    parser.add_argument(
        "--max-pool-places",
        type=int,
        default=400,
        help="Upper bound on pool size used for this run.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=0,
        help="Max planner calls (defaults to count * 2 + 2).",
    )
    parser.add_argument("--model", default=DEFAULT_DATE_IDEA_AGENT_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument(
        "--min-name-similarity",
        type=float,
        default=DEMO_MIN_NAME_SIMILARITY,
        help=f"Google Maps name-match threshold (default {DEMO_MIN_NAME_SIMILARITY}).",
    )
    parser.add_argument(
        "--max-match-distance-meters",
        type=float,
        default=DEMO_MAX_MATCH_DISTANCE_METERS,
        help=(
            "Max distance between RAG candidate and Google match "
            f"(default {DEMO_MAX_MATCH_DISTANCE_METERS})."
        ),
    )
    parser.add_argument(
        "--min-place-rating",
        type=float,
        default=DEMO_MIN_PLACE_RATING,
        help=f"Minimum Google rating accepted (default {DEMO_MIN_PLACE_RATING}).",
    )
    parser.add_argument(
        "--min-user-rating-count",
        type=int,
        default=DEMO_MIN_USER_RATING_COUNT,
        help=f"Minimum user-rating count accepted (default {DEMO_MIN_USER_RATING_COUNT}).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve pool + plan time and print, without calling the LLM or Maps.",
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Do not write plans to parquet; just print them.",
    )
    return parser.parse_args()


async def async_main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    _load_repo_env()

    template = _find_template(args.templates_path, args.template_id)
    bucket = _find_bucket(args.buckets_path, args.bucket_id)
    documents_path, embeddings_path = _resolve_rag_paths(args)

    rag_documents = _enrich_rag_documents_with_addresses(pd.read_parquet(documents_path))
    vector_store = ExactVectorStore(
        documents=rag_documents,
        embeddings=pd.read_parquet(embeddings_path),
    )
    embedding_client = _embedding_client_for_existing_embeddings(embeddings_path)

    pool = build_location_candidate_pool(
        rag_documents_path=documents_path,
        bucket=bucket,
        max_candidates=args.max_pool_places,
    )
    transport_mode = _resolve_transport_mode(args.transport_mode or bucket.transport_mode)
    plan_time_iso = _resolve_plan_time_iso(template)
    max_attempts = args.max_attempts or (args.count * 2 + 2)

    run_dir = args.run_dir or _default_run_dir(
        template_id=args.template_id,
        bucket_id=args.bucket_id,
    )
    events = _EventLog(run_dir=run_dir, dry_run=args.dry_run)

    _print_setup_summary(
        template=template,
        bucket=bucket,
        pool_size=len(pool.allowed_place_ids),
        pool_target=pool.target_plan_count,
        plan_time_iso=plan_time_iso,
        transport_mode=transport_mode,
        count=args.count,
        max_attempts=max_attempts,
        model=args.model,
        output_path=args.output_path,
        dry_run=args.dry_run,
        no_persist=args.no_persist,
        run_dir=run_dir,
        api_trace_path=run_dir / "api_trace.jsonl",
        name_similarity=args.min_name_similarity,
        match_distance=args.max_match_distance_meters,
        min_place_rating=args.min_place_rating,
        min_user_rating_count=args.min_user_rating_count,
    )
    if args.dry_run:
        return 0
    if not pool.allowed_place_ids:
        logger.error(
            "Pool is empty for bucket=%s: %s",
            bucket.bucket_id,
            pool.empty_reason,
        )
        events.write(
            "run_aborted",
            reason="empty_pool",
            detail=pool.empty_reason or "(no reason)",
        )
        return 2

    maps_settings = _build_maps_settings(args)
    successes: list[PrecachePlannerSuccess] = []
    signatures: list[str] = []
    failures: list[tuple[str, str]] = []
    failed_place_ids: dict[str, str] = {}
    attempts = 0
    interrupted = False

    def _handle_sigint(*_args: Any) -> None:
        nonlocal interrupted
        interrupted = True
        logger.warning(
            "Received SIGINT; will stop after the current attempt completes."
        )

    loop = asyncio.get_running_loop()
    try:
        loop.add_signal_handler(signal.SIGINT, _handle_sigint)
    except (NotImplementedError, RuntimeError):
        pass

    events.write(
        "run_started",
        template_id=args.template_id,
        bucket_id=args.bucket_id,
        target_count=args.count,
        max_attempts=max_attempts,
        pool_size=len(pool.allowed_place_ids),
        plan_time_iso=plan_time_iso,
        transport_mode=transport_mode.value,
        model=args.model,
    )

    try:
        api_trace_logger = ApiTraceLogger(run_dir / "api_trace.jsonl")
        async with (
            OpenRouterClient(
                OpenRouterSettings.from_env(),
                trace_logger=api_trace_logger,
            ) as llm_client,
            GoogleMapsClient(
                maps_settings,
                trace_logger=api_trace_logger,
            ) as maps_client,
        ):
            planner = PrecachePlanner(
                llm_client=llm_client,
                maps_client=maps_client,
                vector_store=vector_store,
                embedding_client=embedding_client,
                rag_documents=rag_documents,
                model=args.model,
                reasoning_effort=args.reasoning_effort,
            )
            while len(successes) < args.count and attempts < max_attempts:
                if interrupted:
                    break
                attempts += 1
                attempt_label = f"attempt {attempts}/{max_attempts}"
                logger.info(
                    "Planner %s (successes=%d/%d, existing_signatures=%d)",
                    attempt_label,
                    len(successes),
                    args.count,
                    len(signatures),
                )
                started_at = time.monotonic()
                events.write(
                    "attempt_started",
                    attempt=attempts,
                    signatures_so_far=len(signatures),
                )
                request = PrecachePlannerRequest(
                    bucket=bucket,
                    pool=pool,
                    template=template,
                    plan_time_iso=plan_time_iso,
                    transport_mode=transport_mode,
                    max_leg_seconds=args.max_leg_seconds,
                    existing_plan_signatures=tuple(signatures),
                    excluded_place_ids=tuple(sorted(failed_place_ids)),
                    maps_cache_path=run_dir / "maps_resolution_cache.parquet",
                )
                result = await planner.plan(request)
                duration_s = round(time.monotonic() - started_at, 2)

                if isinstance(result, PrecachePlannerSuccess):
                    successes.append(result)
                    signatures.append(result.signature)
                    if not args.no_persist:
                        append_precache_plans(
                            [result.plan],
                            output_path=args.output_path,
                        )
                    _print_plan(index=len(successes), total=args.count, result=result)
                    events.write(
                        "attempt_success",
                        attempt=attempts,
                        duration_s=duration_s,
                        signature=result.signature,
                        title=result.idea.title,
                        tools=_tool_summary(result.tool_executions),
                    )
                    continue

                assert isinstance(result, PrecachePlannerFailure)
                failures.append((result.reason, result.detail))
                new_failed_ids = _failed_place_ids_from_result(result)
                failed_place_ids.update(new_failed_ids)
                logger.warning(
                    "Planner failure on %s (%.1fs): reason=%s detail=%s",
                    attempt_label,
                    duration_s,
                    result.reason,
                    result.detail,
                )
                events.write(
                    "attempt_failure",
                    attempt=attempts,
                    duration_s=duration_s,
                    reason=result.reason,
                    detail=result.detail[:500],
                    rejected_ideas=list(result.rejected_ideas or ()),
                    failed_place_ids=new_failed_ids,
                    tools=_tool_summary(result.tool_executions),
                )
                if result.reason == FAILURE_REASON_DUPLICATE and result.signature:
                    signatures.append(result.signature)
    finally:
        try:
            loop.remove_signal_handler(signal.SIGINT)
        except (NotImplementedError, RuntimeError, ValueError):
            pass

    events.write(
        "run_complete",
        produced=len(successes),
        requested=args.count,
        attempts_used=attempts,
        interrupted=interrupted,
    )
    _print_final_summary(
        successes=successes,
        failures=failures,
        attempts_used=attempts,
        requested=args.count,
        output_path=args.output_path,
        no_persist=args.no_persist,
        interrupted=interrupted,
        run_dir=run_dir,
        api_trace_path=run_dir / "api_trace.jsonl",
    )
    events.write_summary(
        successes=successes,
        failures=failures,
        attempts_used=attempts,
        requested=args.count,
        interrupted=interrupted,
    )
    if len(successes) < args.count and failures:
        events.write_failure_summary(
            template_id=args.template_id,
            bucket_id=args.bucket_id,
            requested=args.count,
            produced=len(successes),
            failed_place_ids=failed_place_ids,
            failures=failures,
        )
    if interrupted and not successes:
        return 130
    return 0 if len(successes) >= args.count else 1


def _find_template(path: Path, template_id: str) -> dict[str, Any]:
    templates = load_date_templates(path)
    for template in templates:
        if str(template.get("id")) == template_id:
            return dict(template)
    known = ", ".join(str(t.get("id")) for t in templates)
    raise SystemExit(
        f"Template {template_id!r} not found in {path}. Known ids: {known}."
    )


def _find_bucket(path: Path, bucket_id: str):
    for bucket in load_location_buckets(path):
        if bucket.bucket_id == bucket_id:
            return bucket
    known = ", ".join(b.bucket_id for b in load_location_buckets(path))
    raise SystemExit(
        f"Bucket {bucket_id!r} not found in {path}. Known ids: {known}."
    )


def _resolve_rag_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.documents and args.embeddings:
        docs = args.documents
        embs = args.embeddings
    else:
        run_dir = load_rag_settings().rag_runs_root / args.rag_run
        docs = args.documents or run_dir / "place_documents.parquet"
        embs = args.embeddings or run_dir / "place_embeddings.parquet"
    missing = [str(p) for p in (docs, embs) if not p.exists()]
    if missing:
        raise SystemExit(
            "Missing RAG parquet file(s): " + ", ".join(missing)
        )
    return docs, embs


def _embedding_client_for_existing_embeddings(embeddings_path: Path):
    metadata = pd.read_parquet(embeddings_path, columns=["embedding_model"])
    if metadata.empty:
        raise SystemExit(f"Embedding parquet {embeddings_path} is empty.")
    models = set(metadata["embedding_model"].astype(str))
    if len(models) != 1:
        raise SystemExit(
            f"Embedding parquet {embeddings_path} contains mixed models: "
            f"{sorted(models)}."
        )
    model = next(iter(models))
    if model.startswith(f"{LOCAL_HASHING_EMBEDDING_MODEL_PREFIX}:"):
        dimension = int(model.rsplit(":", maxsplit=1)[-1])
        return LocalHashingEmbeddingClient(dimension=dimension)
    return default_local_embedding_client()


def _resolve_transport_mode(value: str) -> TravelMode:
    key = value.strip().lower()
    if key in TRANSPORT_MODE_ALIASES:
        return TRANSPORT_MODE_ALIASES[key]
    try:
        return TravelMode(value.strip().upper())
    except ValueError as exc:
        raise SystemExit(
            f"Unknown transport_mode {value!r}. "
            f"Try one of: {sorted(TRANSPORT_MODE_ALIASES)}."
        ) from exc


def _resolve_plan_time_iso(template: dict[str, Any]) -> str:
    time_of_day = str(template.get("time_of_day") or "flexible").strip().lower()
    hour = TIME_OF_DAY_HOUR.get(time_of_day, TIME_OF_DAY_HOUR["flexible"])
    target_weekday = TIME_OF_DAY_WEEKDAY.get(time_of_day, 5)
    now = datetime.now(SYDNEY_TZ)
    days_ahead = (target_weekday - now.weekday()) % 7
    if days_ahead < 2:
        days_ahead += 7
    target_date = (now + timedelta(days=days_ahead)).date()
    plan_dt = datetime(
        year=target_date.year,
        month=target_date.month,
        day=target_date.day,
        hour=hour,
        minute=0,
        second=0,
        tzinfo=SYDNEY_TZ,
    )
    return plan_dt.isoformat()


def _build_maps_settings(args: argparse.Namespace) -> MapsSettings:
    base = MapsSettings.from_env()
    return dataclasses.replace(
        base,
        min_name_similarity=args.min_name_similarity,
        max_match_distance_meters=args.max_match_distance_meters,
        min_place_rating=args.min_place_rating,
        min_user_rating_count=args.min_user_rating_count,
    )


def _enrich_rag_documents_with_addresses(rag_documents: pd.DataFrame) -> pd.DataFrame:
    if "address" in rag_documents.columns:
        return rag_documents
    places_path = REPO_ROOT / "data" / "au_places.parquet"
    if not places_path.exists():
        logger.error(
            "Cannot enrich RAG documents with street addresses because %s is missing.",
            places_path,
        )
        return rag_documents
    places = pd.read_parquet(places_path)
    required = {"fsq_place_id", "address"}
    missing = required - set(places.columns)
    if missing:
        logger.error(
            "Cannot enrich RAG documents with street addresses; %s is missing columns %s.",
            places_path,
            sorted(missing),
        )
        return rag_documents
    address_lookup = places.loc[:, ["fsq_place_id", "address"]].drop_duplicates(
        subset=["fsq_place_id"],
        keep="first",
    )
    enriched = rag_documents.merge(
        address_lookup,
        on="fsq_place_id",
        how="left",
        validate="many_to_one",
    )
    matched = int(enriched["address"].notna().sum())
    logger.info(
        "Enriched RAG documents with street addresses for %d/%d places.",
        matched,
        len(enriched),
    )
    return enriched


def _default_run_dir(*, template_id: str, bucket_id: str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    digest = hashlib.sha256(f"{template_id}:{bucket_id}:{stamp}".encode()).hexdigest()[:8]
    return REPO_ROOT / "data" / "precache" / "runs" / f"{stamp}-{template_id}-{bucket_id}-{digest}"


class _EventLog:
    def __init__(self, *, run_dir: Path, dry_run: bool) -> None:
        self._run_dir = run_dir
        self._enabled = not dry_run
        self._events_path = run_dir / "events.jsonl"
        self._summary_path = run_dir / "summary.json"
        if self._enabled:
            run_dir.mkdir(parents=True, exist_ok=True)

    def write(self, event_type: str, /, **fields: Any) -> None:
        if not self._enabled:
            return
        record = {
            "ts": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "event": event_type,
            **fields,
        }
        with self._events_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":"), sort_keys=True))
            handle.write("\n")

    def write_summary(
        self,
        *,
        successes: list[PrecachePlannerSuccess],
        failures: list[tuple[str, str]],
        attempts_used: int,
        requested: int,
        interrupted: bool,
    ) -> None:
        if not self._enabled:
            return
        by_reason: dict[str, int] = {}
        for reason, _detail in failures:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        payload = {
            "requested": requested,
            "produced": len(successes),
            "attempts_used": attempts_used,
            "interrupted": interrupted,
            "failures_by_reason": by_reason,
            "plan_signatures": [s.signature for s in successes],
            "plan_titles": [s.idea.title for s in successes],
        }
        self._summary_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def write_failure_summary(
        self,
        *,
        template_id: str,
        bucket_id: str,
        requested: int,
        produced: int,
        failed_place_ids: dict[str, str],
        failures: list[tuple[str, str]],
    ) -> None:
        if not self._enabled:
            return
        payload = {
            "template_id": template_id,
            "bucket_id": bucket_id,
            "reason": "precache_run_finished_without_requested_count",
            "requested": requested,
            "produced": produced,
            "failed_place_ids": failed_place_ids,
            "failures": [
                {"reason": reason, "detail": detail}
                for reason, detail in failures[-5:]
            ],
        }
        (self._run_dir / "failure_summary.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    @property
    def run_dir(self) -> Path:
        return self._run_dir


def _tool_summary(tool_executions: tuple[Any, ...]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for execution in tool_executions:
        name = getattr(execution, "tool_name", None) or "unknown"
        counts[name] = counts.get(name, 0) + 1
    return counts


def _failed_place_ids_from_result(result: PrecachePlannerFailure) -> dict[str, str]:
    verification = result.verification
    if not isinstance(verification, dict):
        return {}
    failed: dict[str, str] = {}
    for stop in verification.get("stops_verification", []):
        if not isinstance(stop, dict):
            continue
        fsq_place_id = stop.get("fsq_place_id")
        if not isinstance(fsq_place_id, str) or not fsq_place_id:
            continue
        if stop.get("ok") is True:
            continue
        reason = stop.get("failure_reason") or stop.get("open_failure_reason")
        failed[fsq_place_id] = str(reason or "verification_failed")
    return failed


def _print_setup_summary(
    *,
    template: dict[str, Any],
    bucket,
    pool_size: int,
    pool_target: int,
    plan_time_iso: str,
    transport_mode: TravelMode,
    count: int,
    max_attempts: int,
    model: str,
    output_path: Path,
    dry_run: bool,
    no_persist: bool,
    run_dir: Path,
    api_trace_path: Path,
    name_similarity: float,
    match_distance: float,
    min_place_rating: float,
    min_user_rating_count: int,
) -> None:
    header = "=" * 72
    print(header)
    print("PRECACHE RUN")
    print(header)
    print(f"  template_id      : {template.get('id')}")
    print(f"  template_title   : {template.get('title')}")
    print(f"  template_stops   : {len(template.get('stops') or [])}")
    print(f"  time_of_day      : {template.get('time_of_day')}")
    print(f"  bucket_id        : {bucket.bucket_id}")
    print(f"  bucket_label     : {bucket.label}")
    print(f"  pool_size        : {pool_size} (target plans = {pool_target})")
    print(f"  plan_time_iso    : {plan_time_iso}")
    print(f"  transport_mode   : {transport_mode.value}")
    print(f"  target plans     : {count} (max_attempts = {max_attempts})")
    print(f"  model            : {model}")
    print(f"  output_path      : {output_path if not no_persist else '(not persisted)'}")
    print(f"  run_dir          : {run_dir if not dry_run else '(dry run, no log)'}")
    print(f"  api_trace_path   : {api_trace_path if not dry_run else '(dry run, no log)'}")
    print(
        f"  maps thresholds  : name_sim>={name_similarity} "
        f"max_distance={match_distance}m min_rating={min_place_rating} "
        f"min_rating_count={min_user_rating_count}"
    )
    if dry_run:
        print("  DRY RUN          : skipping all LLM + Maps calls")
    print(header)


def _print_plan(
    *,
    index: int,
    total: int,
    result: PrecachePlannerSuccess,
) -> None:
    idea = result.idea
    bar = "-" * 72
    print(bar)
    print(f"PLAN {index}/{total} — {idea.title}")
    print(bar)
    if idea.hook:
        print(idea.hook)
        print()
    for i, stop in enumerate(idea.stops, start=1):
        tag = f"[{stop.kind}:{stop.stop_type}]"
        name = stop.name or "(connective)"
        print(f"  Stop {i} {tag} {name}")
        if stop.description:
            print(f"         {stop.description}")
    print()
    print(f"  signature : {result.signature}")
    print(f"  model     : {result.model}")
    print(f"  tools     : {_format_tool_counts(result.tool_executions)}")
    feas = result.verification.get("feasibility", {}) if isinstance(result.verification, dict) else {}
    print(
        "  verified  : "
        f"venues_matched={feas.get('all_venues_matched')} "
        f"open_at_plan_time={feas.get('all_open_at_plan_time')} "
        f"legs_ok={feas.get('all_legs_under_threshold')}"
    )


def _print_final_summary(
    *,
    successes: list[PrecachePlannerSuccess],
    failures: list[tuple[str, str]],
    attempts_used: int,
    requested: int,
    output_path: Path,
    no_persist: bool,
    interrupted: bool,
    run_dir: Path,
    api_trace_path: Path,
) -> None:
    print("=" * 72)
    print("FINAL SUMMARY")
    print("=" * 72)
    if interrupted:
        print("  INTERRUPTED     : stopped by SIGINT")
    print(f"  requested_plans : {requested}")
    print(f"  produced_plans  : {len(successes)}")
    print(f"  attempts_used   : {attempts_used}")
    if failures:
        by_reason: dict[str, int] = {}
        for reason, _ in failures:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        print("  failures_by_reason:")
        for reason, count in sorted(by_reason.items(), key=lambda item: -item[1]):
            print(f"    - {reason}: {count}")
    if successes and not no_persist:
        print(f"  output_path     : {output_path}")
    if successes:
        print("  plan_titles:")
        for result in successes:
            print(f"    - {result.idea.title}")
    print(f"  run_dir         : {run_dir}")
    print(f"  api_trace_path  : {api_trace_path}")
    print("=" * 72)


def _format_tool_counts(tool_executions: tuple[Any, ...]) -> str:
    counts = _tool_summary(tool_executions)
    if not counts:
        return "(none)"
    return ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))


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
        if value in {"", "YOUR-OPENROUTER-MODEL"}:
            continue
        os.environ[key] = value


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
