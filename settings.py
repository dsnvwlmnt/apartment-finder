"""
Default settings. Should be overridden in a private.py or config/private.py file.
"""

import os

## Search Filters
SEARCH_FILTERS = {'query': '',          # The search terms you want to use.
                  'min_bedrooms': 0,    # The minimum number of bedrooms you want.
                  'min_bathrooms': 0}   # The minimum number of bathrooms you want.

# The minimum rent you want to pay per month.
MIN_PRICE = 0

# The maximum rent you want to pay per month.
MAX_PRICE = 9999999

# The maximum price per room you want to pay per month. Filtered in the code.
MAX_PER_ROOM = 999999

# Additional custom filter. If None, it's ignored. Otherwise, SEARCH_FILTERS.min_bedrooms is overrode with 3, and for results with exactly 3 bedrooms it will filter by maximum price per room you want to pay per month, defined here.
MAX_PER_ROOM_3BR = None

## Location preferences

# The Craigslist site you want to search on.
# For instance, https://sfbay.craigslist.org is SF and the Bay Area.
# You only need the beginning of the URL.
CRAIGSLIST_SITE = 'sfbay'

# What Craigslist subdirectories to search on.
# For instance, https://sfbay.craigslist.org/eby/ is the East Bay, and https://sfbay.craigslist.org/sfc/ is San Francisco.
# You only need the last three letters of the URLs.
AREAS = ['eby', 'sfc', 'sby', 'nby']

# A list of neighborhoods and coordinates that you want to look for apartments in.  Any listing that has coordinates
# attached will be checked to see which area it is in.  If there's a match, it will be annotated with the area
# name.  If no match, the neighborhood field, which is a string, will be checked to see if it matches
# anything in NEIGHBORHOODS.
BOXES = {
    'adams_point': [
        [37.80789, -122.25000],
        [37.81589,	-122.26081],
    ],
    'piedmont': [
        [37.82240, -122.24768],
        [37.83237, -122.25386],
    ],
    'rockridge': [
        [37.83826, -122.24073],
        [37.84680, -122.25944],
    ],
    'berkeley': [
        [37.86226, -122.25043],
        [37.86781, -122.26502],
    ],
    'north_berkeley': [
        [37.86425, -122.26330],
        [37.87655, -122.28974],
    ],
    'pac_heights': [
        [37.79124, -122.42381],
        [37.79850, -122.44784],
    ],
    'lower_pac_heights': [
        [37.78554, -122.42878],
        [37.78873, -122.44544],
    ],
    'haight': [
        [37.77059, -122.42688],
        [37.77086, -122.45401],
    ],
    'sunset': [
        [37.75451, -122.46422],
        [37.76258, -122.50825],
    ],
    'richmond': [
        [37.77188, -122.47263],
        [37.78029, -122.51005],
    ],
    'presidio': [
        [37.77805, -122.43959],
        [37.78829, -122.47151],
    ]
}

# A list of neighborhood names to look for in the Craigslist neighborhood name field. If a listing doesn't fall into
# one of the boxes you defined, it will be checked to see if the neighborhood name it was listed under matches one
# of these.  This is less accurate than the boxes, because it relies on the owner to set the right neighborhood,
# but it also catches listings that don't have coordinates (many listings are missing this info).
NEIGHBORHOODS = ['berkeley north', 'berkeley', 'rockridge', 'adams point', 'oakland lake merritt', 'cow hollow', 'piedmont', 'pac hts', 'pacific heights', 'lower haight', 'inner sunset', 'outer sunset', 'presidio', 'palo alto', 'richmond / seacliff', 'haight ashbury', 'alameda', 'twin peaks', 'noe valley', 'bernal heights', 'glen park', 'sunset', 'mission district', 'potrero hill', 'dogpatch']

## Transit preferences

# The farthest you want to live from a transit stop.
MAX_TRANSIT_DIST = 2 # kilometers

# Transit stations you want to check against.  Every coordinate here will be checked against each listing,
# and the closest station name will be added to the result and posted into Slack.
TRANSIT_STATIONS = {
    'oakland_19th_bart': [37.8118051,-122.2720873],
    'macarthur_bart': [37.8265657,-122.2686705],
    'rockridge_bart': [37.841286,-122.2566329],
    'downtown_berkeley_bart': [37.8629541,-122.276594],
    'north_berkeley_bart': [37.8713411,-122.2849758]
}

## Search type preferences

# The Craigslist section underneath housing that you want to search in.
# For instance, https://sfbay.craigslist.org/search/apa find apartments for rent.
# https://sfbay.craigslist.org/search/sub finds sublets.
# You only need the last 3 letters of the URLs.
CRAIGSLIST_HOUSING_SECTION = 'apa'

## System settings

# How long we should sleep between scrapes of Craigslist.
# Too fast may get rate limited.
# Too slow may miss listings.
# If set to 0 we randomize the interval.
SLEEP_INTERVAL = 20 * 60 # 20 minutes

# How long we should pause scraping nightly.
OVERNIGHT_SLEEP_INTERVAL = 9 * 60 * 60 # 9 hours

# What time (military time) we want to resume scraping. Scraping pauses at 00:00 (midnight) local time.
WAKEUP_TIME = 9

# Which slack channel to post the listings into.
SLACK_CHANNEL = '#housing'

# The token that allows us to connect to slack.
# Should be put in private.py, or set as an environment variable.
SLACK_TOKEN = os.getenv('SLACK_TOKEN', '')

# Google sheet ID to output to.
SHEET_ID = None

# Any private settings are imported here.
try:
    from private import *
except Exception:
    pass

# Any external private settings are imported here.
try:
    from config.private import *
except Exception:
    pass
