import time
import sys
import traceback
import random
import datetime as dt
import settings
import database as db
from scraper import do_scrape

def scrape():
    print("{}: Starting scrape cycle".format(time.ctime()))
    try:
        if settings.DEBUG:
            input('Calling do_scrape (press any key)...')
        do_scrape()
    except KeyboardInterrupt:
        print("Exiting....")
        sys.exit(1)
    except Exception as exc:
        print("Error with the scraping:", sys.exc_info()[0])
        traceback.print_exc()
        sys.exit(1)
    else:
        print("{}: Successfully finished scraping".format(time.ctime()))
        sys.exit(0)

def main():
    if settings.RUN_ONCE:
        scrape()
    else:
        while True:
            if dt.datetime.now().hour >= settings.WAKEUP_TIME:
                scrape()
                if settings.DEBUG:
                    input('Going to sleep (press any key)...')
                if settings.SLEEP_INTERVAL == 0:
                    # randomize polling time for Windows (no cron)
                    skip_flag = random.randint(1,1000)
                    if skip_flag > 222:
                        #normal minimum sleep 80% of the time 5-48min
                        time.sleep(random.randint(321,2863))
                    elif skip_flag > 55:
                        #skip for an extra ~45min 15% of the time
                        time.sleep(random.randint(321+2700,2863+2700))
                    elif skip_flag > 18:
                        #skip for an extra ~125min 4% of the time
                        time.sleep(random.randint(321+7500,2863+7500))
                    else:
                        #skip for an extra ~83min 2% of the time
                        time.sleep(random.randint(321+5000,2863+5000))
                else:
                    time.sleep(settings.SLEEP_INTERVAL)
            else:
                if settings.SLEEP_INTERVAL == 0:
                    print("{}: Overnight sleep".format(time.ctime()))
                    # Calculate relative sleep length in case you launch the
                    # script during the "night"
                    time.sleep(settings.OVERNIGHT_SLEEP_INTERVAL
                               - dt.datetime.now().hour*60*60)

if __name__ == "__main__":
    main()
