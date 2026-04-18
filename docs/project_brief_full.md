**LLM GENERATED**

# Date Night — Full Product Brief

This is the expanded version of `project_brief.md`. It captures the same vision
in more detail and folds in the decisions made during the scoping discussion.
It is the single place to look for "what exactly are we building".

## Product in one line

Tinder, but for date night ideas. The user describes the shape of the date
they want, the app generates a swipeable deck of concrete, end-to-end plans,
and a right-swipe turns into something they can actually execute (itinerary,
maps links, booked restaurant where relevant).

## Constraints

- We must use the data in `data/` (the Foursquare AU places parquet + the
  categories parquet) for at least some part of the application. This dataset
  is the source of candidate places.
- This is a hackathon demo, so scope is ruthlessly trimmed to what makes the
  demo compelling.

## User inputs

Collected in a single form on the front end and sent to the back end as one
typed request.

- **Time**: Rough time of day, and amount of time that the date will take. 
- **Location**: suburb or postcode, typed. Geolocation is not in scope; if we
  want current location we infer it (e.g. IP-based) rather than asking for a
  browser permission.
- **Travel radius**: in km, plus transport mode (walking / public transport /
  driving).
- **Vibe / date type**: casual, romantic, active, foodie, nerdy, outdoorsy,
  etc. Fed into the embedding retrieval query and the curator LLM's prompt
  — not mapped to a hand-curated category allowlist.
- **Budget**: $ / $$ / $$$ / $$$$.
- **Time window**: start time + duration (e.g. "Saturday 6pm, 3 hours").
- **Party size**: defaults to 2, adjustable (e.g. double date).
- **Dietary / accessibility constraints**: free-text, passed into the LLM
  prompt and the booking agent context.

## End-to-end flow

The pipeline is designed around a simple principle: do the cheapest filter
first, and never spend an expensive call (Maps enrichment, LLM tokens) on
something that was never going to be a date in the first place.

1. User fills out the input form and submits.
2. **Deterministic cut (parquet, free).** Back end filters the places
   parquet by location + radius, opening hours against the time window,
   and a minimal hard-no category list (petrol stations, medical, hardware,
   automotive, supermarkets, storage — categorically never a date). This
   is cost hygiene, not taste curation. Typical pool: 200–500 candidates.
3. **Embedding retrieval (cheap, no per-request LLM tokens).** Back end
   embeds the user's intent ("romantic Saturday evening date spot, $$,
   Fitzroy") and pulls the top-K (~60) nearest candidates from a
   pre-computed place embedding index built offline from the FSQ dataset.
   This is a soft vibe filter — it uses the embedding model's implicit
   world knowledge of what categories of place go with what kinds of date,
   without us writing any taste rules down.
4. **Maps enrichment (bounded, paid).** Back end hits Google Maps for the
   top-K only, pulling rating + review count, a couple of top review
   snippets, Places attributes (`reservable`, `romantic`, `outdoorSeating`,
   `liveMusic`, etc.), confirmed hours, price tier, photo refs, and a
   canonical Maps URL.
5. **LLM curator rerank (cheap, batched, no tools).** Back end sends the
   enriched candidates to a small model that scores each for date-worthiness
   given the user's specific vibe / time / budget / dietary free-text. This
   is where taste lives. Survivors become the curated pool the planners
   work from.
6. **Anchor seeding (deterministic, MMR).** Back end picks N diverse anchor
   venues from the curated pool using max-marginal-relevance: quality term
   × diversity penalty across category, geography, and price tier. Anchors
   are guaranteed to be different kinds of night out, not N variants of the
   same thing.
7. **Planner agents (one per anchor, in parallel, tool-using).** Each agent
   is a tool-using LLM that builds one full date around its assigned
   anchor — drinks → dinner → dessert, activity → food → nightcap, etc.
   Agents pull additional stops from the curated pool via a tool, check
   opening hours and travel times via Maps tools, and check the weather.
   Agents don't see each other; diversity is already baked in by anchor
   selection.
8. **Deterministic feasibility pass.** Travel times between stops, time
   window fit, transport leg detail, weather gate for outdoor vibes,
   schema validation. Plans that fail are rejected loudly, not silently
   patched.
9. Verified plans stream back to the front end as cards. The user swipes.
10. Right-swipe → plan lands in "saved dates", with a detail view
    (timeline, photos, maps links, transport steps) and a share button.
11. If the plan includes a restaurant, the user can trigger the booking
    agent from the detail view. The agent calls the restaurant and books.

## The LLM's job

The LLM shows up in two distinct places with very different roles. Keeping
them separate matters; conflating them is where bad design happens.

### 1. Curator (cheap, no tools, batched)

After Maps enrichment, the curator LLM scores each of the ~60 candidates
0–3 for date-worthiness given the user's specific vibe, time, budget, and
any dietary / accessibility free-text. It sees per-place enriched data —
name, fine-grained category, rating, review count, top review snippets,
Places attributes, price tier — and its only job is *"is this a date spot
for this user, for this occasion, yes or no, how strongly"*.

- This is where taste judgement lives. No hand-curated allowlist.
- No tools. One small-model call per batch of 30–50 candidates.
- Output: a pruned curated pool (drop score ≤ 1), plus the scores so the
  anchor selector can use them as the quality term.

### 2. Planner agent (one per anchor, tool-using, bounded)

Given one anchor venue and the curated pool, a planner agent builds a
single full date around that anchor. Tools:

- `find_complementary_places(anchor, vibe, type_hint)` — queries the
  curated pool for neighbours to stitch in.
- `get_place_details(place_id)` — Maps enrichment on demand.
- `get_route(from, to, mode, depart_time)` — Maps Routes.
- `get_weather(datetime, location)` — weather check.
- `finalize_plan(stops, hook, description)` — emits the plan.

Rules for the planner agent:

- Venue stops come only from the curated pool, always via tool results.
  Invented venues are rejected.
- Non-venue connective tissue is allowed and encouraged as narrative
  colour ("walk along the river between stops", "grab the tram on
  Swanston") — that kind of thing cannot be anchored to a Foursquare ID
  and is what makes the date feel like a date, not a spreadsheet.
- Travel times and transport leg detail (mode, line, departure) are not
  produced by the LLM — they come from the `get_route` tool, because LLMs
  hallucinate them.
- Each plan gets a short LLM-written hook (title + one-line vibe) for the
  swipe card and stop-level descriptions for the detail view.
- Each agent has a hard iteration cap and wall-clock timeout, and fails
  loudly on exceeding either.

## Google Maps's job

Google Maps is the ground-truth layer. It is called in three places, and
cost is bounded by the pool being small by the time Maps is hit:

- **Bulk enrichment (top-K only)**: after embedding retrieval narrows the
  pool to ~60, the back end pulls existence, opening hours, rating +
  review count, review snippets, Places attributes, price tier, photo
  refs, and a canonical Maps URL for each candidate. This is the
  expensive pass but it's capped at K, not the full 200–500 filtered
  pool.
- **On-demand during planning**: planner agents can call Maps via tools
  for extra detail while they stitch a plan — usually `get_place_details`
  for a venue the curator surfaced but that needs more info, and
  `get_route` for travel legs.
- **Final feasibility pass**: travel time between each pair of
  consecutive stops for the chosen plan, plus transport leg detail (mode,
  line, departure) for the UI. If the plan does not fit the time window
  it is rejected, not silently trimmed.

## Place embedding index

A pre-computed vector index over the FSQ places parquet. Built once,
offline, not per request.

- **Input per place**: `"{name} — {fsq_category_path} — {suburb}"`, plus
  any other short signal the parquet gives us (price tier, popularity).
  The leaf category ("Neapolitan Pizza Restaurant", "Jazz Club", "Wine
  Bar") carries most of the information; the top-level categories are
  too coarse.
- **Model**: an open embedding model (e.g. `bge-small-en` or similar)
  run at index-build time. No runtime dependency on a paid embedding API.
- **Query-time cost**: one embedding of the user's intent string, plus a
  nearest-neighbour lookup. Milliseconds.
- **Role in the pipeline**: a soft vibe filter that cheaply reduces the
  geo/time-filtered pool (~200–500) to the top-K (~60) before the paid
  Maps enrichment pass. It is not the taste call — the curator LLM is.
  It just has to be good enough to keep the obviously-right stuff in the
  top-K and the obviously-wrong stuff out.
- **Failure mode**: places with uninformative names/categories may not
  cluster well in embedding space. Mitigation: widen K (e.g. 100), and
  optionally blend the embedding score with popularity/rating so
  well-known places get a bump into the enrichment pool even if their
  text description is weak.

## Plan diversity via anchor seeding

The problem: if you spin up N planner agents on the same curated pool,
they converge on the same obvious "best" venues. Diversity is a property
of how the inputs are sliced, not a property of the LLM.

- **Anchor selection is deterministic.** Given the curated pool with
  curator scores, the back end picks N anchors by max-marginal-relevance:
  each new anchor maximises `curator_score × bayesian_rating − λ ·
  similarity(already_picked)`.
- **Similarity is a weighted combo of**: category overlap, geographic
  distance (suburb or lat/lon clusters), and price tier.
- **Each planner agent receives one anchor**, the curated pool (with
  peer anchors in a blocklist so they aren't reused as the same role),
  and the user's constraints. Agents don't know about each other.
- **Safety net**: after agents return, compute Jaccard overlap on venue
  IDs across pairs of plans. If any pair exceeds a threshold (~60%), the
  lower-scoring plan is dropped or re-run with an explicit "avoid these
  venues" instruction.
- **Small-pool behaviour**: if MMR cannot find N sufficiently-diverse
  anchors (small city, narrow vibe), the system generates fewer cards
  rather than force-fit ones that aren't really different.

## Weather

A weather API is called for the user's date/time window.

- Used to filter out plans whose outdoor / active stops would be ruined by
  the forecast (heavy rain, thunderstorm, extreme heat).
- Not needed for fully indoor plans, but cheap enough that we can always
  check and display the forecast on the card as a nice-to-have.

## Restaurant booking agent (in scope)

This is a headline feature for the demo, not a stretch goal.

- Triggered from the detail view after the user right-swipes a plan that
  contains a restaurant.
- The agent places a real phone call to the restaurant and makes the
  booking.
- Context handed to the agent: restaurant name, arrival time, party size,
  dietary / accessibility free-text.
- The booking is asynchronous from the user's perspective: the UI shows
  "booking in progress" and updates when the agent returns a result
  (confirmed, declined, no answer).

## Dietary constraints and menus

If the user has dietary constraints and the plan includes a restaurant, we
try to make the recommendation actually respect those constraints:

- Pull the restaurant's menu from Google Maps if it exposes one.
- Otherwise, attempt a light web scrape of the restaurant's own site.
- Pass the menu into the LLM alongside the dietary free-text so the
  recommendation is sanity-checked before the user ever sees the card.

This is best-effort — if we can't find a menu, we fall back to the LLM's
general knowledge plus the dietary free-text, and that is stated explicitly
rather than hidden.

## What a plan looks like (card + detail view)

**Swipe card (compact):**
- Hero image (pulled from Google Maps).
- LLM-written title (hook) and one-line vibe.
- The stops, named, in order.
- Total duration and approximate cost band.
- Weather badge if relevant.

**Detail view (after right-swipe):**
- Scrollable timeline of stops with images and LLM-written descriptions.
- Transport legs between stops, with mode / line / duration from Maps.
- Google Maps link per stop (tap → native navigation).
- Share button → produces a link / message containing the itinerary so the
  user can send it to their partner.
- "Book restaurant" action if applicable.
- "Save" / already-saved state.

## Persistence

Saved dates need to survive at least the user's session, so there is some
amount of persistence behind "saved dates". The MVP direction is a lightweight
server-side store keyed by an anonymous session. Plan status: planned in
`docs/plans/saved-dates-persistence.md`. Accounts are explicitly out of scope,
so the implementation cannot require sign-in.

## Front end

- Single-page app.
- Input form up front.
- Swipe deck (Tinder-like) streams cards as they are verified.
- Right-swipe → saved to "saved dates" + detail view.
- Left-swipe → dismiss.
- Saved dates view lists previously right-swiped plans with their detail
  views.
- Every plan (in the deck or in saved dates) has a share button.
- Explicit loading, empty, and error states — no silent spinners.

## Non-goals (explicitly out of scope for this build)

Listing these so they don't creep back in mid-build.

- **Mutual matching** ("both partners swipe right"). Single-user generator
  only. Sharing is one-way: you send the plan to your partner outside the
  app.
- **User accounts / auth**. Anonymous session only.
- **Partner-side flow** (partner preferences, joint sessions, notifications).
- **Browser geolocation permission**. Location is typed or inferred.
- **Time-sensitive / live events** (concerts, markets, one-off gigs). The
  Foursquare dataset is static and we're not wiring up an events feed.
- **Hand-curated date-worthiness allowlist.** Taste judgement is delegated
  to the curator LLM, grounded in Maps-enriched data. The only curated
  list is the hard-no category list (petrol stations, medical, etc.),
  which is cost hygiene, not taste.
- **Fully agentic end-to-end planning.** The planner agents have tools,
  but the candidate curation and diversity selection are deliberately
  kept deterministic / cheap-LLM. A single agent choosing every venue
  from scratch with tool calls was ruled out on cost and latency.
- **Analytics / swipe-history learning**. No telemetry pipeline.
- **Persistence beyond saved dates.** Anything fancier than "remember the
  plans the user liked" is not in scope.

## External dependencies

- Foursquare OS Places (AU subset), already downloaded as parquet.
- Google Maps APIs: Places (existence, hours, rating + review count,
  review snippets, attributes, photos), Routes / Distance Matrix (travel
  times, transport legs), and whatever is needed for stable Maps URLs.
- OpenRouter (or equivalent) for both the curator LLM (cheap small
  model) and the planner agents (capable tool-using model). Two different
  model tiers, one provider.
- An open embedding model run at index build time (e.g. `bge-small-en`
  via sentence-transformers). No paid embedding API dependency at
  runtime.
- A vector index for nearest-neighbour lookup over the place embeddings
  (FAISS, or a lightweight in-process alternative — at AU scale the
  index fits comfortably in memory).
- A weather API (TBD which provider).
- A voice / phone agent platform for the restaurant booking call (TBD
  which provider).
- Optional: a light web scraper for restaurant menus when Maps doesn't
  expose them.

## Failure modes worth stating up front

- **No candidate places after the deterministic cut** → explicit empty
  state with a suggestion (e.g. expand radius).
- **Embedding retrieval returns nothing useful** (tiny pool, weird vibe)
  → widen K, and if that still fails, surface an explicit "no vibe
  matches in this area" state.
- **Curator LLM rejects everything** → surface that directly rather than
  silently passing low-scored candidates through.
- **Maps rejects too many candidates at enrichment** → explicit empty
  state naming Maps as the reason rather than returning a half-baked
  deck.
- **Anchor selector can't find N diverse anchors** → generate fewer
  cards, do not force-fit near-duplicates.
- **Planner agent exceeds iteration cap or timeout** → that plan is
  dropped and the failure is logged loudly. Other agents' plans still
  return.
- **Planner invents a venue** → plan rejected, not silently rewritten.
- **Plan doesn't fit time window** → plan rejected, not silently
  trimmed.
- **Weather kills an outdoor plan** → plan rejected, reason surfaced.
- **Booking agent fails (no answer, declined)** → surfaced explicitly in
  the UI, date is still saved, user can retry or pick another plan.
