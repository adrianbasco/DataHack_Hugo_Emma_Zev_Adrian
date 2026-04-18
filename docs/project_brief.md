# Name: Date Night

Essentially, tinder, except for date night ideas.

### Back end
* The app will pull a users location. Or ask them where they want to go on a date. Possibly ask them a set of questions about where they want to go on a date as well.
	* User specifiable parameters:
	* Date type preference
	* Location range
	* Activity type preferences
	* Budget
* search around their local area for things to do using the dataset. 
    * Filter by surrounding postcodes possibly. 
* feed these to an LLM to find a good combination of activities to turn into a date
* verify that the places exist with google maps api
* get the LLM to propose a plan. eg, go to place 1, train from place 1 to place 2, arrive at place 2, bus from place 2 to place 3, arrive at place 3 etc
* this plan will then be fed into google maps and checked for it's feasibility. to make sure the times line up
* if confirmed, the plan will marked as valid
* images will be pulled from the google maps api, and a scrollable timeline will be shown to the user using these images, and possibly written descriptions by the LLM. Google maps links to each place can be shown so that the user can simply press on a link, and google maps will navigate them to where they need to go

Note
* If the date involves a restaurant, then an agent may call up the place, and make the booking for them. note, will need to feed in context to the agent. booking for x many people


### Front end
* Tinder like ui

