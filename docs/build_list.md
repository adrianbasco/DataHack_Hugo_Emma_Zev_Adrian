# Build List

This document breaks the project into a practical implementation order without
rewriting the original brief or architecture. It is derived from
`docs/project_brief_full.md`, so if the brief changes, this doc should change
with it.

## MVP First

The first shippable version should focus on one complete loop:

1. collect user inputs
2. filter local places from the parquet dataset
3. enrich candidate places with Google Maps
4. generate candidate date plans from those enriched places
5. verify each plan with Google Maps and weather where relevant
6. show the valid results in a swipeable UI
7. let the user save and share a plan
8. let the user trigger restaurant booking when applicable

Because this is a hackathon demo, the build should optimize for a convincing
end-to-end story, not perfect generality.

## Phase 1: Data Foundation

- Load `data/au_places.parquet` and `data/categories.parquet`.
- Reuse the existing vibe-to-category allowlist work already in
  `back_end/catalog/categories.py`.
- Build a places repository/query layer on top of the parquet dataset.
- Validate required columns loudly at load time.
- Do not add any CSV fallback.

Subcomponents to build:

- `Settings` / config loader
  Reads environment variables and app config in one place. Should fail loudly
  if required keys like `MAPS_API_KEY` or `OPENROUTER_API_KEY` are missing for a
  code path that needs them.
- `PlacesRepository`
  Loads `au_places.parquet`, validates the schema, and exposes query helpers.
- `CategoriesRepository`
  Loads `categories.parquet` and supports the existing vibe expansion logic.
- `PlaceRecord` / typed place models
  One normalized internal shape for places so the rest of the system is not
  passing raw pandas rows around.
- `DatasetHealthCheck`
  Startup or test-time validation that loudly reports missing files, missing
  columns, duplicate IDs, or malformed category fields.
- `AnonymousSessionId`
  A lightweight anonymous session identifier for saved dates and booking state.
  Accounts are out of scope, but saved plans still need somewhere to hang.

## Phase 2: Search and Filtering

- Filter places by the selected vibe/category allowlist.
- Filter by location using suburb, postcode, or lat/lon radius logic.
- Filter by budget where the source data supports it.
- Handle empty result sets explicitly.
- Drop malformed rows rather than guessing what they mean.

Subcomponents to build:

- `PlaceFilterService`
  The main deterministic filtering service that combines all hard constraints.
- `LocationFilter`
  Handles suburb, postcode, and coordinate-radius matching.
- `BudgetFilter`
  Applies budget filtering only when the underlying place data actually supports
  it. If budget data is absent or ambiguous, that should be explicit rather than
  guessed.
- `ConstraintNormalizer`
  Normalizes user inputs such as radius, transport mode, and party size into one
  backend shape.
- `CandidatePoolBuilder`
  Produces the bounded set of candidate places passed downstream to Maps and the
  planner.
- `TypedLocationResolver`
  Parses suburb or postcode input into the internal location shape used by
  filtering.
- `CurrentLocationInferenceService` (optional)
  If the team wants "current location" without browser permission, this is a
  separate service for inference, for example from IP. It should be optional
  and never silently replace explicit typed input.

## Phase 3: Place Enrichment

- Verify shortlisted places against Google Maps so obviously stale places do not
  enter planning.
- Pull opening hours and check them against the requested time window.
- Pull place ratings and drop weak candidates before they ever reach the LLM.
- Resolve Maps links and photos for places that survive enrichment.
- Produce one enriched candidate pool for the planner.

Subcomponents to build:

- `GoogleMapsClient`
  A thin HTTP client around the Google Maps APIs you need. It should own auth,
  timeouts, retries if any, response parsing, and loud error reporting. This is
  definitely an MVP client.
- `PlaceEnrichmentService`
  The main per-place enrichment pass. It should do one clear job: turn a raw
  candidate place into an enriched, verified candidate or reject it with an
  explicit reason.
- `PlaceExistenceChecker`
  Uses `GoogleMapsClient` to confirm the place still exists and resolves
  canonical Maps metadata.
- `OpeningHoursChecker`
  Verifies that the place is plausibly open during the requested date window.
- `PlaceRatingFilter`
  Applies an explicit minimum rating threshold or equivalent rule. If rating is
  missing, that should be handled explicitly rather than guessed around.
- `PlacePhotoService`
  Resolves image URLs or photo references for the detail view.
- `MapsLinkBuilder`
  Produces stable Google Maps links for the UI.
- `CandidatePoolStore`
  Stores the enriched candidate pool passed to the planner so downstream steps
  can reference stable place IDs without recomputing enrichment.

## Phase 4: Dietary and Menu Enrichment

- If the user supplied dietary constraints and a candidate includes a
  restaurant, try to gather menu information before planning.
- Prefer menu data from Maps if available.
- Fall back to a light scrape of the restaurant's own site if Maps has no menu.
- If no menu can be found, surface that explicitly and continue with a weaker
  dietary check rather than pretending the menu was verified.

Subcomponents to build:

- `MenuLookupService`
  Orchestrates menu acquisition for restaurants.
- `MapsMenuExtractor`
  Attempts to pull menu data from Google Maps or associated place metadata.
- `RestaurantWebsiteFinder`
  Resolves the official restaurant site if menu scraping is needed.
- `MenuScraper`
  Lightweight best-effort scraper for the restaurant's own site.
- `DietaryContextBuilder`
  Packages menu data, dietary constraints, and restaurant metadata for the LLM.
- `MenuLookupResult`
  An explicit status model such as `found`, `not_found`, `scrape_failed`, so the
  rest of the system does not infer success from missing data.

## Phase 5: Planning Layer

- Build a constrained planner input from the filtered place set.
- Require the LLM to choose only from supplied place IDs.
- Generate multiple candidate itineraries rather than one result.
- Validate model output strictly before returning anything to the client.
- Log rejected plans and rejection reasons loudly.

Subcomponents to build:

- `OpenRouterClient`
  A thin client for OpenRouter. It should own auth, model selection,
  timeouts, request IDs, and raw response parsing. This is definitely an MVP
  client.
- `PromptBuilder`
  Converts user constraints plus the enriched candidate pool into a structured
  prompt.
- `PlannerService`
  Calls `OpenRouterClient` and asks for multiple itinerary candidates.
- `PlannerResponseValidator`
  Verifies that the model output conforms to the expected schema and only uses
  supplied place IDs.
- `PlanAssembler`
  Converts validated model output into internal itinerary objects.
- `PlanRejectionRecorder`
  Records why a plan was rejected: malformed LLM output, invented place,
  infeasible travel time, missing Maps match, and so on.
- `PlanHookModel`
  Captures the title and one-line vibe summary shown on the swipe card.
- `NarrativeStepModel`
  Allows non-venue connective tissue from the LLM without pretending those
  narrative steps are mapped places.

Other clients to consider:

- `EmbeddingClient` or reranking client
  Not required for MVP. Only build this later if simple filtering plus planning
  is not good enough.

## Phase 6: Plan Feasibility and Weather

- Compute travel-time legs between candidate stops.
- Validate that each plan fits the user's time window and transport mode.
- Check weather for the requested date/time window.
- Reject outdoor or active plans that the forecast would obviously ruin.

Subcomponents to build:

- `RouteMatrixService`
  Uses `GoogleMapsClient` to fetch travel times between stops for walking,
  transit, or driving.
- `TransportLegAssembler`
  Converts Maps route output into the transport leg detail shown in the UI.
- `ItineraryFeasibilityChecker`
  Validates that a proposed itinerary fits the requested time window and travel
  mode. This should reject plans explicitly rather than trying to quietly fix
  them.
- `WeatherClient`
  Thin client for the chosen weather provider. Owns auth if needed, timeouts,
  response parsing, and loud failure handling.
- `WeatherEvaluationService`
  Evaluates whether the forecast should reject outdoor or active plans.
- `WeatherBadgeBuilder`
  Produces the small forecast summary shown on cards or detail views.

## Phase 7: Back-End API

- Stand up the FastAPI app and typed request/response models.
- Implement `POST /dates/generate`.
- Stream verified plans as they are ready, preferably with SSE.
- Add any short-lived server-side plan state needed for swipe/detail flows.
- Return explicit validation and upstream failure responses.
- Support saved dates and booking-job status lookups.

Subcomponents to build:

- `FastAPI app` bootstrap
  App startup, dependency wiring, and configuration loading.
- Request and response schemas
  Typed models for generate requests, streamed plan events, itinerary detail
  payloads, saved-plan payloads, booking payloads, and error payloads.
- `DateGenerationOrchestrator`
  The core application service that coordinates filtering, enrichment, planning,
  and final feasibility checks.
- `PlanStore`
  Short-lived server-side storage for generated plans so swipe/detail flows can
  look up a plan by ID.
- `SavedDatesStore`
  Persistence for right-swiped plans. Could be local-first or server-backed, but
  the interface should be explicit.
- `BookingJobStore`
  Tracks booking jobs and their async status.
- `SSEStreamAdapter`
  Streams `plan`, `complete`, and `error` events to the client.
- `ErrorMapper`
  Converts internal exceptions into explicit `400` or `502` responses without
  hiding the real failing dependency.
- `SharePayloadBuilder`
  Produces the shareable itinerary payload or link content.

## Phase 8: Front End

- Build the input form for location, vibe, radius, budget, and time window.
- Build the Tinder-like swipe deck.
- Build the itinerary detail view for accepted cards.
- Show place images, travel legs, and Maps links.
- Add explicit loading, empty, and error states.
- Add saved dates and share flows.
- Add booking UI states for restaurant plans.

Subcomponents to build:

- input form state and validation
- SSE or streamed-plan client
- swipe deck UI
- itinerary detail page or panel
- saved dates view
- empty-state view
- upstream-error view
- plan card components for venue summary, timeline summary, and maps actions
- share button / share payload UI
- booking action UI
- booking status UI

## Phase 9: Booking Agent

- Trigger a restaurant booking flow from the detail view.
- Hand the agent the restaurant name, arrival time, party size, and dietary /
  accessibility constraints.
- Treat booking as asynchronous from the user's perspective.
- Surface confirmed / declined / no-answer outcomes explicitly.

Subcomponents to build:

- `BookingAgentClient`
  Thin client for the voice / phone platform. This is now in-scope, not stretch.
- `BookingRequestBuilder`
  Packages plan context into the agent request.
- `BookingService`
  Starts booking jobs and updates their state.
- `BookingResultModel`
  Explicit result states such as `confirmed`, `declined`, `no_answer`,
  `failed`.
- `BookingStatusPoller` or webhook handler
  Whichever integration pattern the phone platform needs.
- `RestaurantStopDetector`
  Determines whether a saved plan contains a restaurant and is eligible for
  booking.

## Phase 10: Testing and Observability

- Add tests for parquet loading and schema validation.
- Add tests for category expansion and place filtering.
- Add tests for place enrichment, planner output validation, and route
  feasibility.
- Add tests for weather rejection and dietary/menu fallback behaviour.
- Add tests for booking state transitions.
- Add tests for Maps verification failure handling.
- Add loud structured logging around bad upstream outputs and rejected plans.

Subcomponents to build:

- unit tests for repositories and filters
- unit tests for `GoogleMapsClient` response parsing and failure paths
- unit tests for `OpenRouterClient` response parsing and failure paths
- unit tests for `WeatherClient` and `BookingAgentClient` failure paths
- planner schema-validation tests
- orchestration tests for end-to-end happy path and non-happy paths
- structured logger setup
- request correlation IDs so one failed generation can be traced across clients
- health or smoke checks for config and external dependency readiness
- explicit rejection reason taxonomy so empty states and logs stay consistent

## Stretch

- Better ranking and personalization from swipe history.
- More advanced sequencing like dinner plus activity plus dessert.
- Better saved-date persistence if anonymous session storage feels too flimsy.

Stretch-only subcomponents:

- persistence layer beyond in-memory `PlanStore`
  Only needed if short-lived process memory becomes a real limitation.
- analytics or recommendation service
  Only needed once basic swipe feedback exists.
