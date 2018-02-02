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

#this is done for each area e.g. bnc, nvn, van
def scrape_area(area):
    """
    Scrapes craigslist for a certain geographic area, and finds the latest listings.
    :param area:
    :return: A list of results.
    """

#    input("site: " + settings.CRAIGSLIST_SITE + "; area: " + area + "; category: " + settings.CRAIGSLIST_HOUSING_SECTION + "; maxprice: " + str(settings.MAX_PRICE) + "; min price: " + str(settings.MIN_PRICE))

#orig    cl_h = CraigslistHousing(site=settings.CRAIGSLIST_SITE, area=area, category=settings.CRAIGSLIST_HOUSING_SECTION,
#orig                             filters={'max_price': settings.MAX_PRICE, "min_price": settings.MIN_PRICE})
    try:
        cl_h = CraigslistHousing(site=settings.CRAIGSLIST_SITE, area=area, category=settings.CRAIGSLIST_HOUSING_SECTION,
                                filters=settings.SEARCH_FILTERS)
    except (MaxRetryError, ConnectionError) as e:
        print('Caught exception: ' + e)
        time.sleep(300)     # pause for 5 minutes

    results = []
#orig    gen = cl_h.get_results(sort_by='newest', geotagged=True, limit=20)
    gen = cl_h.get_results(sort_by='newest', geotagged=True)
    while True:
        try:
            result = next(gen)
        except StopIteration:
            break
        except Exception as e:
            print('{}: Exception: {}'.format(time.ctime(), e), file=sys.stderr)
            continue
        listing = session.query(Listing).filter_by(cl_id=result["id"]).first()

        # Don't store the listing if it already exists.
        if listing is None:
#'where' is the neighborhood for US craigslist in brackets in the title (can't filter by this on .ca but there's still neighborhood info in brackets based on whatever ppl input, not sure if standardized, i think no)
#            input("id: " + str(result["id"]) + "; name: " + str(result["name"]) + "; url: " + str(result["url"]) + "; datetime: " + str(result["datetime"]) + "; price: " + str(result["price"]) + "; where: " + str(result["where"]) + "; has_image: " + str(result["has_image"]) + "; has_map: " + str(result["has_map"]) + "; geotag: " + str(result["geotag"]) + "; footage: " + str(result["area"]) + "; bedrooms: " + str(result["bedrooms"]))
#skip for now:
#            if result["where"] is None:
#                input("we are inside where = none")

                # If there is no string identifying which neighborhood the result is from, skip it.
#skip for now:
#                continue

            lat = 0
            lon = 0
            if result["geotag"] is not None:
                # Assign the coordinates.
                lat = result["geotag"][0]
                lon = result["geotag"][1]

                # Annotate the result with information about the area it's in and points of interest near it.
#skip for now:
#                geo_data = find_points_of_interest(result["geotag"], result["where"])
#                result.update(geo_data)
#            else:
#indent back into else, if re-add the if/else above
            result["area"] = ""
            result["bart"] = ""
#might also need to set "area_found", "near_bart", "bart_dist"

            # Try parsing the price.
            price = 0
            try:
                price = float(result["price"].replace("$", ""))
            except Exception:
                pass
#2800.0            input("price: " + str(price))

#            input("price: " + str(price) + "; bedrooms: " + str(result["bedrooms"]) + "; maxperroom: " + str(settings.MAX_PER_ROOM))
            bedrooms = int(result["bedrooms"])
            if bedrooms > 0:
#                input("inside bedrooms > 0")
                price_per_bedroom = price / bedrooms
#1487.5                input(str(price_per_bedroom))
# if $/room is too high, skip ad
                if price_per_bedroom > settings.MAX_PER_ROOM:
                    continue

#other results data i could use: id, datetime, has_image, has_map, geotag
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


            # Return the result if it's near a bart station, or if it is in an area we defined.
#skip for now            if len(result["bart"]) > 0 or len(result["area"]) > 0:
#add a tab if i re-add if condition
            results.append(result)

    return results

def do_scrape():
    """
    Runs the craigslist scraper, and posts data to slack.
    """

    # Get all the results from craigslist.
    all_results = []
    for area in settings.AREAS:
        all_results += scrape_area(area)

    print("{}: Got {} results".format(time.ctime(), len(all_results)))

    # Create a slack client.
    sc = SlackClient(settings.SLACK_TOKEN)

    # Authenticate and create Google sheet service.
    credentials = get_credentials()
    http = credentials.authorize(Http())
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    service = discovery.build('sheets', 'v4', http=http,
                              discoveryServiceUrl=discoveryUrl)

# tmp test
#    post_listing_to_sheet(service, result)
#this does a1 a2 a3 in 3 rows:
#    post_listing_to_sheet(service, [["valuea1"], ["valuea2"], ["valuea3"]])
#this doesn't work
#    post_listing_to_sheet(service, ['testA1', 'testA2'])
#this works
#    post_listing_to_sheet(service, [['testA1', 'testA2', 'testA3']])
# tmp test

    # Post each result to slack.
    for result in all_results:
        post_listing_to_slack(sc, result)
#        post_listing_to_sheet(service, result)

    # Post all results to sheet at the same time, to avoid google api quotas.
    post_listings_to_sheet(service, all_results)
