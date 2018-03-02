from craigslist import CraigslistHousing
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean
from sqlalchemy.orm import sessionmaker
from dateutil.parser import parse
from util import post_listing_to_slack, find_points_of_interest
from slackclient import SlackClient
import sys
import time
import settings
import traceback
from httplib2 import Http
from apiclient import discovery
from sheet import get_credentials, post_listings_to_sheet

engine = create_engine('sqlite:///listings.db', echo=False)

Base = declarative_base()

class Listing(Base):
    """
    A table to store data on craigslist listings.
    """

    __tablename__ = 'listings'

    id = Column(Integer, primary_key=True)
    link = Column(String, unique=True)
    created = Column(DateTime)
    geotag = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    name = Column(String)
    price = Column(Float)
    location = Column(String)
    cl_id = Column(Integer, unique=True)
    area = Column(String)
    bart_stop = Column(String)
    bedrooms = Column(Integer)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

def scrape_area(area, cl_bugged):
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
        if DEBUG:
            gen = cl_h.get_results(sort_by='newest', geotagged=True, limit=1)
        else:
            gen = cl_h.get_results(sort_by='newest', geotagged=True)
    except (ConnectionError, ValueError):
        print('{}: Pause connection polling for ' \
              '3 minutes.'.format(time.ctime()))
        time.sleep(180)

    while True:
        try:
            result = next(gen)
        except StopIteration:
            break
        # Catch errors we want to stop hitting the CL servers with:
        except (ConnectionError, ValueError):
            break
        except Exception:
            continue

        listing = session.query(Listing).filter_by(cl_id=result["id"]).first()
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
            # TODO: get the price from title or post body using beautifulsoup
            if price < settings.MIN_PRICE or price > settings.MAX_PRICE
                continue
        else:
            # Try parsing the price.
            try:
                price = float(result["price"].replace("$", ""))
            except Exception:
                pass

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
        session.add(listing)
        session.commit()

        # Return the result if it's near a bart station, or if it is in an area
        # we defined.
#skip for now            if len(result["bart"]) > 0 or len(result["area"]) > 0:
        results.append(result)

    return results

def do_scrape():
    """
    Runs the craigslist scraper, and posts data to slack and Google sheets.
    """

    # Check for CL price bug: can't filter by price, because no price in title.
    # For e.g., if CL is bugged, this URL will give 0 results:
    # https://vancouver.craigslist.ca/search/van/apa?postedToday=1&min_price=1
    cl_bugged = False
    cl_test = CraigslistHousing(site=settings.CRAIGSLIST_SITE,
                                area=settings.AREAS[0],
                                category=settings.CRAIGSLIST_HOUSING_SECTION,
                                filters={'posted_today': True, 'min_price': 1})
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

    # Create a slack client.
    sc = SlackClient(settings.SLACK_TOKEN)

    # Post each result to slack.
    for result in all_results:
        post_listing_to_slack(sc, result)

def main():
    print("{}: Starting scrape cycle".format(time.ctime()))
    try:
        do_scrape()
    except KeyboardInterrupt:
        print("Exiting....")
        sys.exit(1)
    except Exception as exc:
        print("Error with the scraping:", sys.exc_info()[0])
        traceback.print_exc()
    else:
        print("{}: Successfully finished scraping".format(time.ctime()))

if __name__ == "__main__":
    main()
