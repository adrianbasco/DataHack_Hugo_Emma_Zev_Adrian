# Name: Date Night

Essentially, tinder, except for date night ideas.

Constraint: We must use the data in the data directory for at least some part of our application

### Back end (python)

USER INPUTS:
- Location: suburb or postcode (typed). App can also ping users location
- Travel radius: in km, or "walking / public transport / driving".
- Date type / vibe: e.g. casual, romantic, active, foodie, nerdy, outdoorsy. Maps to a curated subset of Foursquare category IDs.
- Budget $ / $$ / $$$ / $$$$.
- Time window: start time + duration (e.g. "Saturday 6pm, 3 hours").
- Party size: defaults to 2, though adjustable (eg double date)
- Dietary / accessibility constraints: free‑text, passed into the LLM prompt.

* The app will pull a users location. Or ask them where they want to go on a date. Possibly ask them a set of questions about where they want to go on a date as well.
	* User specifiable parameters:
	* Date type preference
	* Location range
	* Activity type preferences
	* Budget
* search around their local area for things to do using the dataset. 
    * Filter by surrounding postcodes possibly. 
* verify that the places exist with google maps api. In the same lookup, check opening hours against the selected time window and pull the place rating so low-quality or closed places are dropped before the LLM ever sees them.
* feed these to an LLM to find a good combination of activities to turn into a date
    * get the LLM to propose a plan. eg, go to place 1, train from place 1 to place 2, arrive at place 2, bus from place 2 to place 3, arrive at place 3 etc
    * prompt the LLM to use its own knowledge of the area and its creativity to stitch the stops into a coherent series of events, not just an ordered list of venues
* this plan will then be fed into google maps and checked for it's feasibility. to make sure the times line up. LLMs might halucinate travel times etc. So verify each step of the plan on google maps individually. The concrete transport leg detail (mode, line, departure) comes from google maps, not the LLM.
* for outdoor / active vibes, check a weather api over the date's time window and reject plans that the forecast would obviously ruin.
* if confirmed, the plan will marked as valid
* images will be pulled from the google maps api, and a scrollable timeline will be shown to the user using these images, and possibly written descriptions by the LLM. Google maps links to each place can be shown so that the user can simply press on a link, and google maps will navigate them to where they need to go
* each plan also gets a short LLM-written hook (title + one-line vibe) so the swipe deck reads like dates, not like a list of venues.

Restaurant booking (in scope)
* If the date involves a restaurant, an agent will call the place and make the booking. Feed it party size, arrival time, and the dietary / accessibility free-text from the user.
* If there are dietary constraints, try to grab the restaurant's menu (from google maps if it has it, otherwise a light web scrape) and pass it into the LLM so the chosen restaurant actually fits the constraints.


### Front end
* Tinder like ui
* Right-swipe sends the plan to a "saved dates" view the user can reopen later.
* Each plan has a share button so the user can send the itinerary (timeline, images, maps links) to their partner.
