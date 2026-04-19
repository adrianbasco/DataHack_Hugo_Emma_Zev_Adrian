**LLM GENERATED**

# Frontend API Interface

## Purpose

This repo exposes a local async API for the app to query:

- backend-owned date templates from `config/date_templates.yaml`
- cached date plans from `data/precache/frontend/plans_api.parquet`
- locally stored card images from `data/precache/frontend/images/`
- deterministic booking previews for the Bland AI phone flow
- live Bland AI booking create / status endpoints
- cached-card search via `/dates/search`

The booking-preview flow is rule-based. It does not use an LLM.

## Prerequisites

Refresh the frontend export first:

```bash
source /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/.venv/bin/activate
python -m scripts.sync_precache_frontend_assets
```

That writes:

- `data/precache/frontend/plans_api.parquet`
- `data/precache/frontend/image_assets.parquet`
- `data/precache/frontend/images/`

Start the API:

```bash
source /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/.venv/bin/activate
python -m scripts.run_frontend_api
```

Default base URL:

```text
http://127.0.0.1:8000
```

## Frontend Config

Web defaults to `http://127.0.0.1:8000`.

Native builds should set:

```text
EXPO_PUBLIC_API_BASE_URL=http://<your-machine-ip>:8000
```

The frontend client lives in [date-night-app/lib/api.ts](/Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/date-night-app/lib/api.ts).

The Expo frontend does not silently switch to bundled demo templates or bundled demo plans when these endpoints fail. Contract or transport errors are surfaced explicitly.

## Endpoints

### `GET /healthz`

Returns API and asset readiness.

Example response:

```json
{
  "status": "ok",
  "plansReady": true,
  "plansCount": 89,
  "assetsReady": true,
  "imagesCount": 124,
  "source": {
    "plansApiPath": ".../data/precache/frontend/plans_api.parquet",
    "assetsDir": ".../data/precache/frontend/images",
    "plansApiExists": true
  }
}
```

### `GET /templates`

Returns the frontend template browser contract from `config/date_templates.yaml`.

Response shape:

```json
{
  "templates": [
    {
      "id": "sunset_lookout_and_dinner",
      "title": "Sunset lookout, then dinner",
      "vibes": ["romantic", "outdoorsy"],
      "timeOfDay": "evening",
      "durationHours": 3.5,
      "meaningfulVariations": 16,
      "weatherSensitive": true,
      "description": "Catch the sunset from a scenic lookout, then a proper dinner after dark.",
      "stops": [
        {
          "type": "scenic_lookout",
          "kind": "connective",
          "note": "sunset from a scenic lookout"
        },
        {
          "type": "restaurant",
          "kind": "venue",
          "note": null
        }
      ]
    }
  ]
}
```

Notes:

- This is the backend source of truth for the template library screen.
- Field names are camelCase in the API response even though the YAML source uses snake_case.
- If the YAML file is missing or invalid, the endpoint returns `503` rather than substituting fallback templates.

### `GET /dates/metadata`

Returns counts for browsing/filter UI.

Example response:

```json
{
  "totalPlans": 89,
  "buckets": [{ "id": "sydney_cbd", "label": "Sydney Cbd", "count": 8 }],
  "templates": [{ "id": "drinks_dinner_dessert", "label": "Drinks Dinner Dessert", "count": 5 }],
  "vibes": [{ "id": "romantic", "label": "Romantic", "count": 42 }]
}
```

### `GET /dates`

Returns app-ready plans.

Query params:

- `limit`: `1..50`
- `bucket_id`: optional
- `template_id`: optional
- `vibe`: optional free-text vibe filter

Example:

```bash
curl "http://127.0.0.1:8000/dates?limit=3&bucket_id=sydney_cbd"
```

Response shape:

```json
[
  {
    "id": "ff6c...",
    "title": "Cocktails, French Bistro & Gelato in the CBD",
    "hook": "A Maps-verified date night plan.",
    "summary": "The classic — pre-dinner drinks, a proper dinner, a walk to somewhere sweet.",
    "vibes": ["romantic", "foodie"],
    "templateHint": "Drinks, dinner, dessert",
    "templateId": "drinks_dinner_dessert",
    "durationLabel": "4 hours",
    "costBand": "Unspecified",
    "weather": null,
    "heroImageUrl": "http://127.0.0.1:8000/static/precache-images/...",
    "mapsVerificationNeeded": false,
    "constraintsConsidered": [],
    "stops": [
      {
        "id": "5bfd...",
        "kind": "venue",
        "stopType": "restaurant",
        "name": "Restaurant Hubert",
        "description": "Restaurant Hubert",
        "whyItFits": "Matches the requested template stop.",
        "time": "8:15 PM",
        "transport": "Walk · 9 min",
        "mapsUrl": "https://maps.google.com/?cid=...",
        "address": "15 Bligh St",
        "phoneNumber": null
      }
    ],
    "transportLegs": [
      {
        "mode": "Walk",
        "durationText": "9 min"
      }
    ],
    "bookingContext": {
      "planId": "ff6c...",
      "restaurantName": "Restaurant Hubert",
      "restaurantPhoneNumber": null,
      "restaurantAddress": "15 Bligh St",
      "suggestedArrivalTimeIso": "2026-04-24T20:15:00+10:00",
      "partySize": 2
    },
    "source": "api"
  }
]
```

### `GET /dates/{plan_id}`

Returns one app-ready plan object by `plan_id`.

Example:

```bash
curl "http://127.0.0.1:8000/dates/ff6cfff2fc5a180da0cb4aff420e8f288c1b6bce1a34c9728906eaeb5c55425b"
```

Response shape:

```json
{
  "plan": {
    "id": "ff6c...",
    "title": "Cocktails, French Bistro & Gelato in the CBD",
    "hook": "A Maps-verified date night plan.",
    "summary": "...",
    "vibes": ["romantic", "foodie"],
    "templateHint": "Drinks, dinner, dessert",
    "templateId": "drinks_dinner_dessert",
    "durationLabel": "4 hours",
    "costBand": "Unspecified",
    "heroImageUrl": "http://127.0.0.1:8000/static/precache-images/...",
    "stops": [],
    "transportLegs": [],
    "bookingContext": {
      "restaurantName": "Restaurant Hubert",
      "restaurantPhoneNumber": null
    },
    "source": "api"
  }
}
```

### `POST /dates/generate`

Ranks the locally exported plans against the request and returns the best matches.

Request body:

```json
{
  "location": "Sydney",
  "radiusKm": 5,
  "transportMode": "walking",
  "vibe": "romantic foodie",
  "budget": "$$",
  "startTime": "19:00",
  "durationMinutes": 180,
  "partySize": 2,
  "constraintsNote": "",
  "limit": 3
}
```

Response shape:

```json
{
  "plans": [],
  "warnings": [
    "Budget filtering is not applied because the local cached plans do not include reliable price data."
  ],
  "meta": {
    "matchedCount": 12,
    "returnedCount": 3,
    "totalAvailable": 89
  }
}
```

Notes:

- This does not call the LLM.
- It ranks from the local exported snapshot only.
- If a locality is ambiguous, the API may retry as `..., NSW` and emit a warning.
- The returned `bookingContext.partySize` is populated from the request `partySize`.

### `POST /dates/search`

Runs cached-card retrieval against the structured search pipeline.

Request body:

```json
{
  "query": "romantic dinner in surry hills tonight",
  "context": {
    "now_iso": "2026-04-19T18:00:00+10:00",
    "user_location": {
      "lat": -33.883,
      "lng": 151.211
    },
    "exclude_plan_ids": ["abc123"],
    "limit": 5
  },
  "overrides": {
    "vibes": ["romantic", "foodie"],
    "time_of_day": "evening",
    "weather_ok": "indoors_only",
    "location": {
      "text": "Surry Hills, NSW",
      "radius_km": 6
    },
    "transport_mode": "walking",
    "template_hints": ["dessert", "rooftop"]
  }
}
```

Contract notes:

- `query` is optional if `overrides` is provided.
- `context` is ambient state only. It should not contain user-chosen filters.
- `overrides` uses the same filter schema the parser produces.
- Precedence is:
  `overrides` > parsed query output > derived from `context` > unset
- `weather_ok` accepts:
  - `indoors_only`
  - `outdoors_ok`
- `transport_mode` accepts:
  - `walking`
  - `public_transport`
  - `driving`
- `time_of_day` accepts:
  - `morning`
  - `midday`
  - `afternoon`
  - `evening`
  - `night`
  - `flexible`

Minimal request:

```json
{
  "query": "bookstore date in newtown tonight"
}
```

Override-only request:

```json
{
  "overrides": {
    "vibes": ["romantic"],
    "location": {
      "text": "Sydney, NSW",
      "radius_km": 8
    }
  }
}
```

Representative response:

```json
{
  "parsed": {
    "vibes": {
      "value": ["romantic", "foodie"],
      "source": "override"
    },
    "time_of_day": {
      "value": "evening",
      "source": "override"
    },
    "weather_ok": {
      "value": "indoors_only",
      "source": "override"
    },
    "location": {
      "value": {
        "text": "Surry Hills, NSW",
        "radius_km": 6.0,
        "anchor_latitude": -33.883,
        "anchor_longitude": 151.212,
        "resolved_label": "Surry Hills, NSW"
      },
      "source": "override"
    },
    "transport_mode": {
      "value": "walking",
      "source": "override"
    },
    "template_hints": {
      "value": ["dessert", "rooftop"],
      "source": "override"
    },
    "free_text_residual": {
      "value": "romantic dinner",
      "source": "parsed"
    },
    "warnings": [],
    "auto_applied_notes": [
      "Applied indoors-only prefilter before weather lookup."
    ]
  },
  "results": [
    {
      "plan_id": "cde397d17421ed21aec0149891335a05c41718e91888bc55f100d77c90dfadb1",
      "score": 2.314,
      "match_reasons": [
        "Matched query terms: dinner, romantic",
        "Matched template hints: dessert",
        "Nearby bucket: Surry Hills (1.8km)",
        "Vibes: romantic, foodie"
      ],
      "score_breakdown": {
        "lexical": 1.884,
        "template_bonus": 0.18,
        "location_bonus": 0.25,
        "total": 2.314
      },
      "card": {
        "plan_title": "Candlelit Italian Dinner then Showboats on the Harbour"
      }
    }
  ],
  "diagnostics": {
    "total_matched_before_limit": 7,
    "filter_stage_counts": [
      {
        "stage": "ready_cards",
        "before": 90,
        "after": 87,
        "rejected": 3,
        "status": "applied",
        "detail": "Dropped plans without a valid cached card payload."
      }
    ],
    "weather_gate_stats": {
      "evaluated": 0,
      "rejected": 0,
      "upstream_failures": 0,
      "skipped_indoors_only": 0,
      "cache_hits": 0,
      "groups": 0
    },
    "unsupported_constraints": [],
    "warnings": []
  }
}
```

Response notes:

- `results[].card` is the cached `card_json` payload returned verbatim.
- `parsed` is the frontend-facing debug block for chip rendering and parser inspection.
- `diagnostics.filter_stage_counts` is intended for debugging, not UI rendering.
- Weather-sensitive plans may be rejected at search time. Upstream weather failures are surfaced in warnings instead of being hidden.

### `POST /booking/restaurants/preview`

Builds a deterministic Bland AI call description without placing a call.

This is the endpoint the frontend should use before attempting a real booking create request so it can inspect exactly what will be sent to the phone agent and whether live Bland AI calls are configured.

Request body:

```json
{
  "restaurantName": "Restaurant Hubert",
  "restaurantPhoneNumber": "+61290000000",
  "restaurantAddress": "15 Bligh St",
  "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
  "partySize": 2,
  "bookingName": "Emma",
  "planId": "ff6c..."
}
```

`restaurantPhoneNumber` is optional context. The outbound Bland AI call is not routed to this value.

Response shape:

```json
{
  "bookingContext": {
    "planId": "ff6c...",
    "restaurantName": "Restaurant Hubert",
    "restaurantPhoneNumber": "+61290000000",
    "restaurantAddress": "15 Bligh St",
    "suggestedArrivalTimeIso": "2026-04-24T20:15:00+10:00",
    "partySize": 2
  },
  "callDescription": {
    "provider": "bland_ai",
    "phoneNumber": "+61491114073",
    "firstSentence": "Heyyy, I am calling on behalf of Emma to book a table at Restaurant Hubert.",
    "task": "You are making a restaurant reservation.\nRestaurant: Restaurant Hubert.\n...",
    "voice": null,
    "model": "base",
    "language": "en-AU",
    "timezone": "Australia/Sydney",
    "maxDurationMinutes": 8,
    "waitForGreeting": true,
    "record": false,
    "voicemail": { "action": "hangup" },
    "requestData": {
      "restaurant_name": "Restaurant Hubert",
      "restaurant_phone_number": "+61290000000",
      "booking_name": "Emma",
      "party_size": 2,
      "arrival_time_iso": "2026-04-24T20:15:00+10:00",
      "plan_id": "ff6c..."
    },
    "metadata": {
      "purpose": "restaurant_booking",
      "provider": "bland_ai",
      "configured_call_target": "+61491114073",
      "plan_id": "ff6c..."
    },
    "dispositions": [
      "booking_confirmed",
      "restaurant_unavailable",
      "booking_declined",
      "needs_human_follow_up",
      "no_answer",
      "failed"
    ],
    "keywords": ["Restaurant Hubert", "Emma"],
    "summaryPrompt": "Summarize whether the table booking was confirmed, declined, not answered, or needs human follow-up. ..."
  },
  "liveCallEnabled": false,
  "liveCallDisabledReason": "BLAND_AI_API_KEY is required for Bland AI client calls."
}
```

### `POST /booking/restaurants`

Queues a real Bland AI outbound call.

Request body matches `POST /booking/restaurants/preview`.

Response shape:

```json
{
  "callId": "call_123",
  "status": "queued",
  "provider": "bland_ai",
  "restaurantName": "Restaurant Hubert",
  "restaurantPhoneNumber": "+61491114073",
  "arrivalTimeIso": "2026-04-24T20:15:00+10:00",
  "partySize": 2
}
```

### `GET /booking/restaurants/{call_id}`

Fetches the normalized Bland AI booking status.

Response shape:

```json
{
  "callId": "call_123",
  "status": "confirmed",
  "providerStatus": "completed",
  "queueStatus": "complete",
  "answeredBy": "human",
  "summary": "Booking confirmed.",
  "errorMessage": null
}
```

Normalized status values:

- `queued`
- `in_progress`
- `confirmed`
- `declined`
- `no_answer`
- `needs_human_follow_up`
- `failed`
- `unknown`

## Booking Context Notes

- `bookingContext.restaurantPhoneNumber` is only populated when the selected stop contains a trustworthy E.164 phone number.
- The backend no longer fabricates a demo phone number.
- Bland AI calls are always placed to `BLAND_AI_BOOKING_PHONE_NUMBER`, defaulting to `+61491114073` when the env var is not set.
- The request `restaurantPhoneNumber` is preserved as restaurant context in `callDescription.requestData`, but it is not used as the outbound Bland call target.
- If the selected stop contains a non-E.164 phone number, the API logs a warning and omits the prefill so the frontend can require manual correction.
- The restaurant choice remains deterministic:
  - first stop whose `stop_type` contains `restaurant`
  - otherwise first stop with booking signals such as `booking` or `third_party_booking`

## Static Images

Local images are served at:

```text
/static/precache-images/<relative_path_from_manifest>
```

Example:

```text
http://127.0.0.1:8000/static/precache-images/chij-fzc3qcvemsrnx9stdt7cmm/2fc61b160f659514c004bc4a98e65a12ec526aa0e8ebf301628c43ffff9587b1.jpg
```

The API already returns fully qualified `heroImageUrl` values.

## Failure Behavior

Important non-happy-path behavior:

- If `config/date_templates.yaml` is missing or invalid, `/templates` returns `503`.
- If `plans_api.parquet` is missing, `/healthz` reports degraded and plan endpoints return `503`.
- If an image file is missing, rerun `python -m scripts.sync_precache_frontend_assets`.
- If native app requests fail immediately, verify `EXPO_PUBLIC_API_BASE_URL`.
- If `/dates/generate` returns no plans, the request matched no locally exported cached plans. It is not a fallback to live plan generation.
- The frontend only shows a cached plan-detail copy when a live `GET /dates/{plan_id}` request fails and that exact plan already exists in local generated/saved storage.
- If `BLAND_AI_API_KEY` is not configured, real booking create / status endpoints return `503`.
- `POST /booking/restaurants/preview` still works without `BLAND_AI_API_KEY` because it is a dry-run payload builder only, and the response sets `liveCallEnabled=false` with an explicit reason.
- If Bland AI returns an unexpected completed outcome, the normalized booking status becomes `unknown` and the API logs the unexpected provider state loudly instead of assuming success.

## Useful Commands

Refresh exported plans and images:

```bash
source /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/.venv/bin/activate
python -m scripts.sync_precache_frontend_assets
```

Run the API:

```bash
source /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/.venv/bin/activate
python -m scripts.run_frontend_api
```

Run the API/backend checks used during integration:

```bash
source /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/.venv/bin/activate
python -m unittest tests.api.test_app tests.services.test_booking tests.scripts.test_run_booking_call
cd /Users/eriksreinfelds/Documents/GitHub/DataHack_Hugo_Emma_Zev_Adrian/date-night-app && npm run test && npm run typecheck
```
