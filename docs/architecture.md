# Front End ↔ Back End Architecture

## Shape

- **Back end**: Python FastAPI service. Stateless JSON REST API.
- **Front end**: SPA that calls the API over `fetch`.
- **Contract**: Pydantic models on the server define the schema. FastAPI's OpenAPI spec is used to generate TS types for the front end — one source of truth.

## Boundary

The front end never touches the places dataset, Google Maps keys, the LLM, or any secrets. It only sends user inputs and renders what the back end returns.

## Core endpoints

- `POST /dates/generate` — body: user inputs (location, radius, vibe, budget, time window, party size, constraints). Returns candidate date plans (the Tinder deck), verified against Maps for feasibility.
- `POST /dates/{id}/swipe` — records the choice; right-swipe returns the full itinerary (timeline, images, Maps links).
- `POST /dates/{id}/book` — triggers the restaurant-booking agent. Async: returns a job id, FE polls `GET /jobs/{id}`.

`/dates/generate` should stream via SSE so cards appear as they're verified, rather than blocking on one long request.

## Request flow

1. FE collects inputs → `POST /dates/generate`.
2. BE filters the parquet dataset by location/radius/category/budget.
3. BE asks the LLM to assemble candidate plans from that filtered set.
4. BE verifies each plan with Google Maps.
5. BE streams verified plans back as cards.
6. FE renders the deck. Right-swipe → detail view → optional booking.

## Non-happy path

- Validation errors → `400` with `{ error, field, message }`. FE surfaces these directly.
- Upstream failure (LLM, Maps, booking) → `502` naming which dependency failed. No fabricated results, no un-verified plans returned as if they were verified.
- Empty result set → `200` with `{ plans: [], reason }`. FE shows an explicit empty state, not a spinner.
