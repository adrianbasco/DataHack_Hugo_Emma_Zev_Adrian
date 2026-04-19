"""Batch-generate cached date cards across templates with spatial diversity."""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import hashlib
import json
import logging
import math
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from back_end.agents.date_idea_agent import (  # noqa: E402
    DEFAULT_DATE_IDEA_AGENT_MODEL,
    DEFAULT_REASONING_EFFORT,
)
from back_end.agents.precache_planner import PrecachePlanner, PrecachePlannerRequest  # noqa: E402
from back_end.clients.api_trace import ApiTraceLogger  # noqa: E402
from back_end.clients.maps import GoogleMapsClient  # noqa: E402
from back_end.clients.openrouter import OpenRouterClient  # noqa: E402
from back_end.clients.settings import OpenRouterSettings  # noqa: E402
from back_end.precache.candidate_pools import (  # noqa: E402
    _haversine_km,
    build_location_candidate_pool_from_documents,
    load_location_buckets,
)
from back_end.precache.models import CandidatePoolPlace, LocationBucket, LocationCandidatePool  # noqa: E402
from back_end.precache.output import (  # noqa: E402
    DEFAULT_PRECACHE_OUTPUT_PATH,
    append_precache_plans,
    read_precache_output,
)
from back_end.rag.retriever import load_date_templates  # noqa: E402
from back_end.rag.vector_store import ExactVectorStore  # noqa: E402
from scripts.run_precache import (  # noqa: E402
    _build_maps_settings,
    _embedding_client_for_existing_embeddings,
    _enrich_rag_documents_with_addresses,
    _failed_place_ids_from_result,
    _load_repo_env,
    _print_plan,
    _resolve_plan_time_iso,
    _resolve_rag_paths,
    _resolve_transport_mode,
)

logger = logging.getLogger("build_date_cache")

DEFAULT_BUCKET_CYCLES = 1
DEFAULT_VARIANT_POOL_RATIO = 0.55
DEFAULT_VARIANT_POOL_MIN_PLACES = 120

STOP_TAG_WEIGHTS: dict[str, dict[str, float]] = {
    "beach": {"beach": 7.0, "waterfront": 2.0},
    "ferry_ride": {"ferry": 8.0, "waterfront": 3.0},
    "harbor_or_pier": {"waterfront": 5.0, "destination": 2.0},
    "boardwalk_or_lookout": {"waterfront": 4.0, "destination": 2.0},
    "botanical_garden": {"destination": 3.0, "dense": 1.0},
    "brewery_or_bar": {"breweries": 6.0, "nightlife": 3.0, "casual": 1.0},
    "bar": {"nightlife": 3.0, "casual": 1.0},
    "cocktail_bar": {"nightlife": 4.0, "dense": 1.0},
    "wine_bar": {"nightlife": 3.0, "dining": 2.0},
    "aquarium": {"waterfront": 3.0, "destination": 2.0},
    "bookstore": {"dense": 2.0, "destination": 1.0},
    "museum": {"dense": 2.0, "destination": 2.0},
    "art_gallery": {"dense": 2.0, "destination": 2.0},
    "restaurant": {"dining": 3.0, "dense": 1.0},
    "brunch_restaurant": {"dining": 3.0, "casual": 1.0},
    "casual_restaurant": {"dining": 3.0, "casual": 1.0},
    "dessert_shop": {"dining": 2.0, "dense": 1.0},
    "bakery": {"casual": 2.0, "dense": 1.0},
    "cafe": {"casual": 2.0, "dense": 1.0},
}

VIBE_TAG_WEIGHTS: dict[str, dict[str, float]] = {
    "nightlife": {"nightlife": 3.0, "dense": 1.0},
    "foodie": {"dining": 3.0, "dense": 1.0},
    "romantic": {"destination": 2.0, "waterfront": 2.0},
    "outdoorsy": {"beach": 3.0, "waterfront": 2.0},
    "casual": {"casual": 2.0},
    "nerdy": {"dense": 1.5, "destination": 1.0},
}


@dataclass(frozen=True)
class _GenerationSlot:
    bucket: LocationBucket
    cycle_index: int
    score: float


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a batch of cached, card-ready date plans across all date templates."
        )
    )
    parser.add_argument(
        "--template-ids",
        default=None,
        help="Optional comma-separated template ids. Defaults to all templates.",
    )
    parser.add_argument(
        "--bucket-mode",
        choices=("auto", "fixed"),
        default="auto",
        help="Auto-select best-fit buckets per template, or force a single bucket.",
    )
    parser.add_argument(
        "--bucket-id",
        default="sydney_cbd",
        help="Bucket id used when --bucket-mode=fixed.",
    )
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
        "--rag-run",
        default="rag-corpus-agent-smoke",
        help="RAG run id under data/rag/runs.",
    )
    parser.add_argument("--documents", type=Path, default=None)
    parser.add_argument("--embeddings", type=Path, default=None)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=DEFAULT_PRECACHE_OUTPUT_PATH,
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="Directory for api_trace.jsonl and summary.json.",
    )
    parser.add_argument("--model", default=DEFAULT_DATE_IDEA_AGENT_MODEL)
    parser.add_argument("--reasoning-effort", default=DEFAULT_REASONING_EFFORT)
    parser.add_argument("--max-leg-seconds", type=int, default=1200)
    parser.add_argument("--max-pool-places", type=int, default=400)
    parser.add_argument(
        "--count-per-template",
        type=int,
        default=0,
        help="Override auto count. Must be between 1 and 4 when provided.",
    )
    parser.add_argument("--min-per-template", type=int, default=1)
    parser.add_argument("--max-per-template", type=int, default=4)
    parser.add_argument(
        "--bucket-cycles",
        type=int,
        default=DEFAULT_BUCKET_CYCLES,
        help="How many spatially distinct passes to try per ranked bucket.",
    )
    parser.add_argument(
        "--variant-pool-ratio",
        type=float,
        default=DEFAULT_VARIANT_POOL_RATIO,
        help="Fraction of each bucket pool kept for one spatial seed.",
    )
    parser.add_argument(
        "--variant-pool-min-places",
        type=int,
        default=DEFAULT_VARIANT_POOL_MIN_PLACES,
    )
    parser.add_argument(
        "--transport-mode",
        default=None,
        help="Override bucket transport mode for every run.",
    )
    parser.add_argument(
        "--min-name-similarity",
        type=float,
        default=0.72,
    )
    parser.add_argument(
        "--max-match-distance-meters",
        type=float,
        default=350.0,
    )
    parser.add_argument(
        "--min-place-rating",
        type=float,
        default=3.5,
    )
    parser.add_argument(
        "--min-user-rating-count",
        type=int,
        default=0,
    )
    parser.add_argument(
        "--no-persist",
        action="store_true",
        help="Generate and print without writing plans.parquet.",
    )
    return parser.parse_args()


async def async_main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    args = parse_args()
    _validate_args(args)
    _load_repo_env()

    templates = _selected_templates(args.templates_path, args.template_ids)
    buckets = load_location_buckets(args.buckets_path)
    buckets_by_id = {bucket.bucket_id: bucket for bucket in buckets}
    if args.bucket_mode == "fixed" and args.bucket_id not in buckets_by_id:
        raise SystemExit(
            f"Unknown bucket_id {args.bucket_id!r}. Known: {sorted(buckets_by_id)}"
        )

    documents_path, embeddings_path = _resolve_rag_paths(args)
    rag_documents = _enrich_rag_documents_with_addresses(pd.read_parquet(documents_path))
    embeddings = pd.read_parquet(embeddings_path)
    vector_store = ExactVectorStore(documents=rag_documents, embeddings=embeddings)
    embedding_client = _embedding_client_for_existing_embeddings(embeddings_path)
    prebuilt_pools = {
        bucket.bucket_id: build_location_candidate_pool_from_documents(
            rag_documents=rag_documents,
            bucket=bucket,
            max_candidates=args.max_pool_places,
        )
        for bucket in buckets
    }
    existing_signatures_by_template = _load_existing_signatures_by_template(args.output_path)

    run_dir = args.run_dir or _default_batch_run_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    summary_path = run_dir / "summary.json"
    batch_summary: list[dict[str, Any]] = []

    maps_settings = _build_maps_settings(args)
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

        for template in templates:
            template_id = str(template.get("id") or "")
            target_count = _target_count_for_template(
                template,
                override=args.count_per_template,
                minimum=args.min_per_template,
                maximum=args.max_per_template,
            )
            slots = _generation_slots_for_template(
                template=template,
                buckets=buckets,
                prebuilt_pools=prebuilt_pools,
                fixed_bucket=buckets_by_id.get(args.bucket_id)
                if args.bucket_mode == "fixed"
                else None,
                cycles=args.bucket_cycles,
                bucket_limit=max(4, target_count * 2),
            )
            template_result = await _generate_template_dates(
                planner=planner,
                template=template,
                target_count=target_count,
                slots=slots,
                prebuilt_pools=prebuilt_pools,
                output_path=args.output_path,
                no_persist=args.no_persist,
                transport_mode_override=args.transport_mode,
                max_leg_seconds=args.max_leg_seconds,
                run_dir=run_dir,
                existing_signatures=existing_signatures_by_template.get(template_id, set()),
                variant_pool_ratio=args.variant_pool_ratio,
                variant_pool_min_places=args.variant_pool_min_places,
            )
            batch_summary.append(template_result)

    summary_payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "output_path": None if args.no_persist else str(args.output_path),
        "template_count": len(templates),
        "results": batch_summary,
    }
    summary_path.write_text(
        json.dumps(summary_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print("=" * 80)
    print("BATCH SUMMARY")
    print("=" * 80)
    for item in batch_summary:
        print(
            f"  {item['template_id']}: produced {item['produced_count']}/"
            f"{item['target_count']} across buckets {item['bucket_ids_used']}"
        )
    print(f"  summary_path : {summary_path}")
    print(f"  api_trace    : {run_dir / 'api_trace.jsonl'}")
    print("=" * 80)
    return 0


async def _generate_template_dates(
    *,
    planner: PrecachePlanner,
    template: dict[str, Any],
    target_count: int,
    slots: list[_GenerationSlot],
    prebuilt_pools: dict[str, LocationCandidatePool],
    output_path: Path,
    no_persist: bool,
    transport_mode_override: str | None,
    max_leg_seconds: int,
    run_dir: Path,
    existing_signatures: set[str],
    variant_pool_ratio: float,
    variant_pool_min_places: int,
) -> dict[str, Any]:
    template_id = str(template.get("id") or "")
    print(
        f"[{template_id}] target={target_count} title={template.get('title')} "
        f"slots={len(slots)}"
    )
    successes = 0
    signatures = set(existing_signatures)
    used_place_ids: set[str] = set()
    failed_place_ids: dict[str, str] = {}
    bucket_ids_used: list[str] = []
    failures: list[dict[str, str]] = []

    for slot in slots:
        if successes >= target_count:
            break
        bucket = slot.bucket
        base_pool = prebuilt_pools[bucket.bucket_id]
        variant_pool = _diversified_pool(
            base_pool,
            seed=_stable_seed(template_id, bucket.bucket_id, slot.cycle_index),
            keep_ratio=variant_pool_ratio,
            min_places=variant_pool_min_places,
        )
        if not variant_pool.places:
            logger.error(
                "Template=%s bucket=%s cycle=%d produced an empty diversified pool.",
                template_id,
                bucket.bucket_id,
                slot.cycle_index,
            )
            failures.append(
                {
                    "bucket_id": bucket.bucket_id,
                    "reason": "empty_diversified_pool",
                    "detail": "No candidates survived the spatial diversity filter.",
                }
            )
            continue

        transport_mode = _resolve_transport_mode(
            transport_mode_override or bucket.transport_mode
        )
        request = PrecachePlannerRequest(
            bucket=bucket,
            pool=variant_pool,
            template=template,
            plan_time_iso=_resolve_plan_time_iso(template),
            transport_mode=transport_mode,
            max_leg_seconds=max_leg_seconds,
            existing_plan_signatures=tuple(sorted(signatures)),
            excluded_place_ids=tuple(sorted(set(failed_place_ids) | used_place_ids)),
            maps_cache_path=run_dir / "maps_resolution_cache.parquet",
        )
        print(
            f"[{template_id}] trying bucket={bucket.bucket_id} cycle={slot.cycle_index + 1} "
            f"variant_pool={len(variant_pool.places)}"
        )
        result = await planner.plan(request)
        if result.status == "success":
            signatures.add(result.signature)
            used_place_ids.update(_venue_ids_from_success(result))
            bucket_ids_used.append(bucket.bucket_id)
            successes += 1
            if not no_persist:
                append_precache_plans([result.plan], output_path=output_path)
            print(
                f"[{template_id}] generated {successes}/{target_count} "
                f"bucket={bucket.bucket_id} title={result.idea.title}"
            )
            _print_plan(index=successes, total=target_count, result=result)
            continue

        failed_place_ids.update(_failed_place_ids_from_result(result))
        detail = result.detail
        failures.append(
            {
                "bucket_id": bucket.bucket_id,
                "reason": result.reason,
                "detail": detail,
            }
        )
        print(
            f"[{template_id}] failed bucket={bucket.bucket_id} "
            f"reason={result.reason} detail={detail}"
        )

    return {
        "template_id": template_id,
        "target_count": target_count,
        "produced_count": successes,
        "bucket_ids_used": bucket_ids_used,
        "failure_count": len(failures),
        "failures": failures[-10:],
    }


def _selected_templates(path: Path, selected_ids: str | None) -> list[dict[str, Any]]:
    templates = list(load_date_templates(path))
    if selected_ids is None:
        return templates
    wanted = {part.strip() for part in selected_ids.split(",") if part.strip()}
    selected = [template for template in templates if str(template.get("id")) in wanted]
    missing = sorted(wanted - {str(template.get("id")) for template in selected})
    if missing:
        raise SystemExit(f"Unknown template ids: {missing}")
    return selected


def _target_count_for_template(
    template: dict[str, Any],
    *,
    override: int,
    minimum: int,
    maximum: int,
) -> int:
    if override:
        return override
    meaningful = int(template.get("meaningful_variations") or 1)
    if meaningful <= 3:
        target = 1
    elif meaningful <= 8:
        target = 2
    elif meaningful <= 14:
        target = 3
    else:
        target = 4
    return max(minimum, min(maximum, target))


def _generation_slots_for_template(
    *,
    template: dict[str, Any],
    buckets: list[LocationBucket] | tuple[LocationBucket, ...],
    prebuilt_pools: dict[str, LocationCandidatePool],
    fixed_bucket: LocationBucket | None,
    cycles: int,
    bucket_limit: int,
) -> list[_GenerationSlot]:
    ranked = (
        [fixed_bucket]
        if fixed_bucket is not None
        else _rank_buckets_for_template(template, buckets, prebuilt_pools=prebuilt_pools)
    )
    if fixed_bucket is None:
        ranked = ranked[:bucket_limit]
    slots: list[_GenerationSlot] = []
    for cycle_index in range(cycles):
        for bucket in ranked:
            score = (
                _bucket_score(
                    template,
                    bucket,
                    pool=prebuilt_pools.get(bucket.bucket_id),
                )
                - cycle_index * 0.25
            )
            slots.append(
                _GenerationSlot(
                    bucket=bucket,
                    cycle_index=cycle_index,
                    score=score,
                )
            )
    slots.sort(key=lambda slot: (-slot.score, slot.cycle_index, slot.bucket.bucket_id))
    return slots


def _rank_buckets_for_template(
    template: dict[str, Any],
    buckets: list[LocationBucket] | tuple[LocationBucket, ...],
    *,
    prebuilt_pools: dict[str, LocationCandidatePool],
) -> list[LocationBucket]:
    ranked = sorted(
        buckets,
        key=lambda bucket: (
            -_bucket_score(template, bucket, pool=prebuilt_pools.get(bucket.bucket_id)),
            bucket.bucket_id,
        ),
    )
    return ranked


def _bucket_score(
    template: dict[str, Any],
    bucket: LocationBucket,
    *,
    pool: LocationCandidatePool | None = None,
) -> float:
    bucket_tags = set(bucket.tags)
    score = float(bucket.strategic_boost)
    stop_types = [
        str(stop.get("type") or "").strip()
        for stop in template.get("stops", [])
        if isinstance(stop, dict)
    ]
    for stop_type in stop_types:
        for tag, weight in STOP_TAG_WEIGHTS.get(stop_type, {}).items():
            if tag in bucket_tags:
                score += weight
    vibe_values = template.get("vibe") or []
    if isinstance(vibe_values, str):
        vibe_values = [vibe_values]
    for vibe in vibe_values:
        for tag, weight in VIBE_TAG_WEIGHTS.get(str(vibe), {}).items():
            if tag in bucket_tags:
                score += weight
    if "destination" in bucket_tags:
        score += 0.5
    if "dense" in bucket_tags:
        score += 0.25
    if pool is not None:
        score += _bucket_supply_score(template, pool)
    return score


def _bucket_supply_score(
    template: dict[str, Any],
    pool: LocationCandidatePool,
) -> float:
    required_stop_counts = _required_venue_stop_counts(template)
    if not required_stop_counts:
        logger.error(
            "Template=%s has no venue stops; supply scoring will be neutral.",
            template.get("id", "unknown"),
        )
        return 0.0

    pool_tag_counts: Counter[str] = Counter()
    quality_by_tag: dict[str, list[int]] = {}
    for place in pool.places:
        relevant_tags = set(place.template_stop_tags) & set(required_stop_counts)
        for tag in relevant_tags:
            pool_tag_counts[tag] += 1
            quality_by_tag.setdefault(tag, []).append(place.quality_score)

    missing_stop_types = [
        stop_type
        for stop_type in required_stop_counts
        if pool_tag_counts[stop_type] <= 0
    ]
    if missing_stop_types:
        return -100.0 - 25.0 * len(missing_stop_types)

    effective_capacity = min(
        pool_tag_counts[stop_type] // required_count
        for stop_type, required_count in required_stop_counts.items()
    )
    score = min(18.0, effective_capacity * 4.0)
    score += min(8.0, math.log1p(len(pool.places)) * 1.5)
    score += min(6.0, pool.target_plan_count / 4.0)

    scarcity_penalty = 0.0
    quality_bonus = 0.0
    coverage_volume_bonus = 0.0
    for stop_type, required_count in required_stop_counts.items():
        available_count = pool_tag_counts[stop_type]
        target_depth = max(required_count * 4, 4)
        if available_count < target_depth:
            scarcity_penalty += (target_depth - available_count) * 1.4
        coverage_volume_bonus += min(4.0, math.log1p(available_count) * 1.2)
        top_scores = sorted(quality_by_tag.get(stop_type, ()), reverse=True)[:5]
        if top_scores:
            quality_bonus += sum(top_scores) / len(top_scores) / 8.0

    return score + min(12.0, coverage_volume_bonus + quality_bonus) - scarcity_penalty


def _required_venue_stop_counts(template: dict[str, Any]) -> Counter[str]:
    stop_counts: Counter[str] = Counter()
    for stop in template.get("stops", []):
        if not isinstance(stop, dict):
            continue
        stop_type = str(stop.get("type") or "").strip()
        if not stop_type:
            continue
        if stop.get("kind") == "connective":
            continue
        stop_counts[stop_type] += 1
    return stop_counts


def _diversified_pool(
    pool: LocationCandidatePool,
    *,
    seed: int,
    keep_ratio: float,
    min_places: int,
) -> LocationCandidatePool:
    places = list(pool.places)
    if len(places) <= min_places:
        return pool
    anchor_lat, anchor_lng = _variant_anchor(
        bucket=pool.bucket,
        seed=seed,
    )
    keep_count = min(
        len(places),
        max(min_places, int(math.ceil(len(places) * keep_ratio))),
    )
    ranked = sorted(
        places,
        key=lambda place: (
            _haversine_km(anchor_lat, anchor_lng, place.latitude, place.longitude),
            place.distance_km,
            -place.quality_score,
            place.name,
            place.fsq_place_id,
        ),
    )
    return LocationCandidatePool(
        bucket=pool.bucket,
        places=tuple(ranked[:keep_count]),
        target_plan_count=pool.target_plan_count,
        empty_reason=pool.empty_reason,
    )


def _variant_anchor(*, bucket: LocationBucket, seed: int) -> tuple[float, float]:
    angle = (seed % 360) * math.pi / 180.0
    radial_fraction = 0.18 + ((seed // 360) % 5) * 0.12
    radial_km = max(0.15, min(bucket.radius_km * 0.75, bucket.radius_km * radial_fraction))
    lat_delta = (radial_km * math.cos(angle)) / 111.0
    lon_scale = max(0.15, math.cos(math.radians(bucket.latitude)))
    lon_delta = (radial_km * math.sin(angle)) / (111.0 * lon_scale)
    return bucket.latitude + lat_delta, bucket.longitude + lon_delta


def _stable_seed(template_id: str, bucket_id: str, cycle_index: int) -> int:
    digest = hashlib.sha256(f"{template_id}:{bucket_id}:{cycle_index}".encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def _load_existing_signatures_by_template(output_path: Path) -> dict[str, set[str]]:
    if not output_path.exists():
        return {}
    df = read_precache_output(output_path)
    signatures: dict[str, set[str]] = {}
    for row in df.loc[:, ["template_id", "fsq_place_ids_sorted"]].itertuples(index=False):
        template_id = str(row.template_id)
        signatures.setdefault(template_id, set()).add(str(row.fsq_place_ids_sorted))
    return signatures


def _venue_ids_from_success(result: Any) -> set[str]:
    out: set[str] = set()
    for stop in result.idea.stops:
        if stop.kind != "venue":
            continue
        if stop.fsq_place_id:
            out.add(stop.fsq_place_id)
    return out


def _default_batch_run_dir() -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return REPO_ROOT / "data" / "precache" / "runs" / f"{stamp}-batch-cache"


def _validate_args(args: argparse.Namespace) -> None:
    if args.count_per_template and not 1 <= args.count_per_template <= 4:
        raise SystemExit("--count-per-template must be between 1 and 4.")
    if args.min_per_template <= 0 or args.max_per_template <= 0:
        raise SystemExit("--min-per-template and --max-per-template must be positive.")
    if args.min_per_template > args.max_per_template:
        raise SystemExit("--min-per-template cannot exceed --max-per-template.")
    if not 0 < args.variant_pool_ratio <= 1:
        raise SystemExit("--variant-pool-ratio must be in (0, 1].")
    if args.variant_pool_min_places <= 0:
        raise SystemExit("--variant-pool-min-places must be positive.")
    if args.bucket_cycles <= 0:
        raise SystemExit("--bucket-cycles must be positive.")


if __name__ == "__main__":
    raise SystemExit(asyncio.run(async_main()))
