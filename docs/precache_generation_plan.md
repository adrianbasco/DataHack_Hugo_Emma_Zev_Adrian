**LLM GENERATED**

# Date Plan Pre-Cache Generation Plan

## Goal

Generate a large cache of grounded date plans before users ask for them. Each
cached plan should be tied to:

- a location bucket or destination cluster
- a date template
- a vibe / time-of-day / budget shape
- local RAG evidence
- later, Google Maps feasibility checks

The LLM should not search the whole Sydney corpus every time. It should work
inside a bounded candidate pool for one location and one template, then the
backend verifies the result.

## Cache Unit

The primary cache key should be:

```text
location_bucket_id + template_id + vibe + time_of_day + budget + transport_mode
```

Example:

```text
newtown_enmore + drinks_dinner_dessert + romantic_foodie + evening + $$ + walk
```

One cache key can store multiple plans if the area has enough strong venues.
Dense/date-worthy areas get more generated plans than weak areas.

## Location Strategy

Use two kinds of location buckets:

1. **Origin buckets**
   Common places users start from, such as Sydney CBD, Newtown, Surry Hills,
   Bondi, Manly, Parramatta, Chatswood, Marrickville, and Paddington.

2. **Destination clusters**
   Areas worth travelling to because venue density is high, such as Darling
   Harbour, Potts Point, Enmore, Circular Quay, Barangaroo, Manly Wharf, Bondi
   Beach, and Oxford Street.

Every bucket has:

- stable id
- display label
- latitude / longitude anchor
- default radius km
- transport mode
- minimum generated plan count
- maximum generated plan count
- optional strategic boost

The generator computes a target plan count from local venue density and quality,
then clamps it between minimum and maximum.

## Generation Pipeline

1. **Build RAG corpus**
   Read only parquet-derived source data and write:

   ```text
   data/rag/runs/<run-id>/place_documents.parquet
   ```

2. **Build embeddings**
   For the demo path, use the explicit in-process hashing backend:

   ```text
   data/rag/runs/<run-id>/place_embeddings.parquet
   ```

   A stronger semantic embedding model can replace this later.

3. **Load location buckets**
   Read bucket definitions from YAML or generate them from high-density clusters.

4. **Build a scoped candidate pool**
   For each bucket:

   - load `place_documents.parquet`
   - filter by distance from the bucket anchor
   - drop places without coordinates
   - score by profile quality and distance
   - compute a target plan count from density
   - pass only those FSQ place ids to the RAG tool

5. **Pick a template**
   Load one template from `config/date_templates.yaml`. The LLM gets exactly
   that template shape and should fill only its venue stops.

6. **Run the LLM planner**
   The LLM can call `search_rag_places`, but that tool is scoped to the current
   candidate pool. The LLM should not see or use venues outside the pool.

7. **Validate LLM output**
   The backend rejects:

   - malformed JSON
   - invented FSQ place ids
   - duplicate venue ids in one plan
   - missing required stop fields
   - venue names that do not match retrieved RAG results

   A single schema-repair call is allowed for formatting errors, but not for
   invented venues.

8. **Google Maps verification**
   This is a separate deterministic pass:

   - resolve FSQ venue to Google Place
   - fetch opening hours, rating, review count, Maps URL, and photos
   - compute routes between consecutive venue stops
   - reject plans with excessive travel time or impossible sequencing
   - attach verified route and place metadata

9. **Write cache**
   Store generated plans as parquet and optionally JSONL:

   ```text
   data/date_plan_cache/<run-id>/plans.parquet
   data/date_plan_cache/<run-id>/plans.jsonl
   ```

## Maps Verification Rules

The LLM must not decide route feasibility. Google Maps does.

Initial deterministic rules:

- walking plan: reject any consecutive venue leg over 25 minutes walking
- transit plan: reject any consecutive venue leg over 35 minutes transit
- driving plan: reject any consecutive venue leg over 30 minutes driving
- reject if any required venue cannot be confidently matched in Google Places
- reject if a venue is permanently closed
- reject if confirmed opening hours conflict with the target time window
- reject if total route time makes the plan exceed the duration band

Connective stops are narrative only until Maps verification. A waterfront walk
or scenic stroll can remain if the venue-to-venue route is feasible.

## Location Search Tool

Add a small deterministic location tool for the planner/verification stage:

```text
search_location_context(query, origin_bucket_id)
```

It should return nearby candidate areas and connective possibilities:

- nearest waterfront / harbour / pier-ish areas from local candidates
- nearest parks/gardens/lookouts if present in the scoped pool
- approximate distance from origin

This tool should not invent places. It should only summarize local parquet/RAG
metadata and later Maps route results.

## First Implementation Milestone

Build the pre-cache foundation without Maps:

- location bucket models
- candidate pool generation from RAG documents
- scoped RAG tool
- template-specific agent context
- JSON plan output with `maps_verification_needed=true`
- tests for location scope and candidate-pool failure modes

## Second Implementation Milestone

Add Google Maps verification:

- resolve each venue to one Google Place
- compute route legs
- reject infeasible plans loudly
- store verification metadata

## Third Implementation Milestone

Batch cache generation:

```bash
python scripts/generate_date_plan_cache.py \
  --rag-run rag-corpus-agent-smoke \
  --locations config/location_buckets.yaml \
  --output-dir data/date_plan_cache/smoke \
  --templates all \
  --vibes romantic,foodie,casual,outdoorsy,active,nightlife
```

This script should be restartable and should never silently overwrite a cache
unless `--overwrite` is supplied.
