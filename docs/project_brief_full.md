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
  etc. Mapped to a curated allowlist of Foursquare category IDs.
- **Budget**: $ / $$ / $$$ / $$$$.
- **Time window**: start time + duration (e.g. "Saturday 6pm, 3 hours").
- **Party size**: defaults to 2, adjustable (e.g. double date).
- **Dietary / accessibility constraints**: free-text, passed into the LLM
  prompt and the booking agent context.

## End-to-end flow

1. User fills out the input form and submits.
2. Back end filters the parquet dataset by vibe → category allowlist,
   location + radius, and budget where the data supports it.
   Plan status: planned in `docs/plans/backend-search-and-filtering.md`.
3. For the filtered shortlist, the back end hits Google Maps in a single
   enrichment pass per place:
   - confirm the place still exists,
   - pull opening hours and check them against the user's time window,
   - pull the place rating so low-rated places can be dropped,
   - resolve photos and a canonical Maps link.
   Plan status: planned in `docs/plans/backend-place-enrichment.md`.
4. The back end sends the surviving candidate pool to the LLM and asks it to
   propose multiple full date plans (not single venues — proper sequences
   like drinks → dinner → dessert, or activity → food → nightcap).
   Plan status: planned in `docs/plans/llm-planning-layer.md`.
5. For each proposed plan, the back end verifies feasibility through Google
   Maps: travel times between stops, transport leg detail, and whether the
   whole thing fits the time window.
6. For outdoor / active vibes, the back end checks a weather API over the
   date's time window and rejects plans the forecast would obviously ruin.
7. Verified plans stream back to the front end as cards. The user swipes.
8. Right-swipe → plan lands in "saved dates", with a detail view (timeline,
   photos, maps links, transport steps) and a share button.
9. If the plan includes a restaurant, the user can trigger the booking agent
   from the detail view. The agent calls the restaurant and books.

## The LLM's job

The LLM is not a search engine. It is the creative layer.

- It receives the enriched candidate pool (places that already survived
  existence, opening hours, and rating checks) plus the user's constraints.
- It is prompted to use its own knowledge of the area and its creativity to
  stitch stops into a coherent series of events — a date with a narrative
  arc, not a ranked list of venues.
- It must only choose venue stops from the supplied candidate pool. Venues
  it invents are rejected.
- It may add non-venue connective tissue ("walk along the river between
  stops", "grab the tram on Swanston") as narrative colour, because that
  kind of thing cannot be anchored to a Foursquare ID.
- For each plan it also produces a short hook: a title and a one-line vibe
  summary that becomes the headline on the swipe card.
- Travel times and transport leg detail (mode, line, departure) are **not**
  taken from the LLM — those come from Google Maps, because LLMs hallucinate
  them.

## Google Maps's job

Google Maps is the ground-truth layer. It is called in two places:

- **Per place (enrichment pass)**: existence, opening hours, rating, photos,
  canonical Maps URL. Preferably in one request per place to keep API cost
  down.
- **Per plan (feasibility pass)**: travel time between each pair of
  consecutive stops, plus transport leg detail for the UI. If the plan does
  not fit the time window, it is rejected rather than silently trimmed.

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
- **Plan diversity algorithm** across a deck. Separate design question; not
  tackled in this build.
- **Analytics / swipe-history learning**. No telemetry pipeline.
- **Persistence beyond saved dates.** Anything fancier than "remember the
  plans the user liked" is not in scope.

## External dependencies

- Foursquare OS Places (AU subset), already downloaded as parquet.
- Google Maps APIs: Places (existence, hours, rating, photos), Routes /
  Distance Matrix (travel times, transport legs), and whatever is needed for
  stable Maps URLs.
- OpenRouter (or equivalent) for the planning LLM.
- A weather API (TBD which provider).
- A voice / phone agent platform for the restaurant booking call (TBD which
  provider).
- Optional: a light web scraper for restaurant menus when Maps doesn't
  expose them.

## Failure modes worth stating up front

- **No candidate places after filtering** → explicit empty state with a
  suggestion (e.g. expand radius).
- **Maps rejects too many candidates** → explicit empty state naming Maps as
  the reason rather than returning a half-baked deck.
- **LLM invents a venue** → plan rejected, not silently rewritten.
- **Plan doesn't fit time window** → plan rejected, not silently trimmed.
- **Weather kills an outdoor plan** → plan rejected, reason surfaced.
- **Booking agent fails (no answer, declined)** → surfaced explicitly in the
  UI, date is still saved, user can retry or pick another plan.
