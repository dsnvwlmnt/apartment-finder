import sys
import time
import traceback
from craigslist import CraigslistHousing
from dateutil.parser import parse
from slackclient import SlackClient
from httplib2 import Http
from apiclient import discovery
from sheet import get_credentials, post_listings_to_sheet
import settings
import database as db
from util import post_listing_to_slack, find_points_of_interest

def scrape_area(area, cl_bugged=False):
    """
    Scrapes craigslist for a geographic area and finds the latest listings.
    :param area:
    :return: A list of results.
    """

    if not cl_bugged:
        settings.SEARCH_FILTERS['min_price'] = MIN_PRICE
		settings.SEARCH_FILTERS['max_price'] = MAX_PRICE

    if settings.MAX_PER_ROOM_3BR is not None:
        settings.SEARCH_FILTERS['min_bedrooms'] = 3

    try:
        cl_h = CraigslistHousing(site=settings.CRAIGSLIST_SITE,
                                 area=area,
                                 category=settings.CRAIGSLIST_HOUSING_SECTION,
                                 filters=settings.SEARCH_FILTERS)
        results = []
        if settings.DEBUG:
            input('Generator initial call (press any key)...')
        if settings.DEBUG:
            gen = cl_h.get_results(sort_by='newest', geotagged=True, limit=1)
        else:
            gen = cl_h.get_results(sort_by='newest', geotagged=True)
    except (ConnectionError, ValueError):
        print('{}: Pause connection polling for ' \
              '3 minutes.'.format(time.ctime()))
        time.sleep(180)

    while True:
        if settings.DEBUG:
            input('Generator next call (press any key)...')
        try:
            result = next(gen)
        except StopIteration:
            break
        # Catch errors we want to stop hitting the CL servers with:
        except (ConnectionError, ValueError):
            break
        except Exception:
            continue

        if settings.DEBUG:
            input('Query listing on db (press any key)...')
        listing = db.query_cl_id(result['id'])
        # Don't store the listing if it already exists.
        if listing:
            continue
#skip for now:
#            if result["where"] is None:
#                # If there is no string identifying which neighborhood the 
#                # result is from, skip it.
#                continue
#when re-add this use bshlenk's version  if listing or result["where"] is None:
#                                            continue

        lat = 0
        lon = 0
        if result["geotag"] is not None:
            # Assign the coordinates.
            lat = result["geotag"][0]
            lon = result["geotag"][1]

#skip for now:
#            # Annotate the result with information about the area it's in and
#            # points of interest near it.
#            geo_data = find_points_of_interest(result["geotag"], 
#                                               result["where"])
#            result.update(geo_data)
#        else:
#indent these back into else, when re-adding the else above
        result["area"] = ""
        result["bart"] = ""
#might also need to set "area_found", "near_bart", "bart_dist"

        price = 0
        if cl_bugged:
            print('{}: Craigslist prices are bugged, implement+test ' \
                  'beautifulsoup price scraping!'.format(time.ctime()))
            # TODO: get the price from title or post body using beautifulsoup.
            # Until this is coded, you'll get no new results when CL is bugged.
            # Which is fine since you wouldn't get any results anyway.
            if price < settings.MIN_PRICE or price > settings.MAX_PRICE
                continue
        else:
            # Try parsing the price.
            try:
                price = float(result["price"].replace("$", ""))
            except Exception:
                if settings.DEBUG:
                    input('Did not get a numeric price (press any key)...')
                pass

        # Skip the ad if we can't find a non-zero price.
        if price == 0:
            continue

        bedrooms = int(result["bedrooms"])
        if bedrooms > 0:
            price_per_bedroom = price / bedrooms
            # if $/room is too high, skip ad
            if (  (bedrooms == 3
                   and price_per_bedroom > settings.MAX_PER_ROOM_3BR
                  )
                   or price_per_bedroom > settings.MAX_PER_ROOM
               ):
                continue

        if settings.DEBUG:
            input('Create listing and add to db (press any key)...')
        # Create the listing object.
        listing = Listing(link=result["url"],
                          created=parse(result["datetime"]),
                          lat=lat,
                          lon=lon,
                          name=result["name"],
                          price=price,
                          location=result["where"],
                          cl_id=result["id"],
                          area=result["area"],
                          bart_stop=result["bart"],
                          bedrooms=bedrooms)

        # Save the listing so we don't grab it again.
        db.add(listing)

        # Return the result if it's near a bart station, or if it is in an area
        # we defined.
#skip for now            if len(result["bart"]) > 0 or len(result["area"]) > 0:
        results.append(result)

    return results

def do_scrape():
    """
    Runs the craigslist scraper, and posts data to slack and Google sheets.
    """

    if settings.DEBUG:
        input('lastrun num_results (press any key)...: ' + db.query_last_run())
    # If the last run gave 0 results, test for CL price bug (can't filter by
    # price, because no price in title). For e.g., if CL is bugged, this URL
    # will give 0 results:
    # https://vancouver.craigslist.ca/search/van/apa?postedToday=1&min_price=1
    if db.query_last_run() == 0:
        cl_test = CraigslistHousing(
                                site=settings.CRAIGSLIST_SITE,
                                area=settings.AREAS[0],
                                category=settings.CRAIGSLIST_HOUSING_SECTION,
                                filters={'posted_today': True, 'min_price': 1})
        if settings.DEBUG:
            input('Calling test generator (press any key)...')
        gen_test = cl_test.get_results(limit=1)
        try:
            result_test = next(gen_test)
        except StopIteration:
            cl_bugged = True

    # Get all the results from craigslist.
    all_results = []
    for area in settings.AREAS:
        all_results += scrape_area(area, cl_bugged)

    print("{}: Got {} results".format(time.ctime(), len(all_results)))

    if settings.SHEET_ID is not None:
        # Authenticate and create Google sheet service.
        credentials = get_credentials()
        http = credentials.authorize(Http())
        discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                        'version=v4')
        service = discovery.build('sheets', 'v4', http=http,
                                  discoveryServiceUrl=discoveryUrl)

        # Post all results to sheet simultaneously, to avoid Google api quotas
        post_listings_to_sheet(service, all_results)

    if settings.SLACK_TOKEN != '':
        # Create a slack client.
        sc = SlackClient(settings.SLACK_TOKEN)

        if settings.DEBUG:
            input('Going to loop post_listing_to_slack (press any key)...')
        # Post each result to slack.
        for result in all_results:
            post_listing_to_slack(sc, result)

    return len(all_results)
