from scraper import do_scrape
import settings
import time
import sys
import traceback
import random
import datetime as dt

def main():
    input('This version should be run from scraper.py. Otherwise, press any key to continue.')
    while True:
        if dt.datetime.now().hour >= settings.WAKEUP_TIME:
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
                time.sleep(settings.OVERNIGHT_SLEEP_INTERVAL)

if __name__ == "__main__":
    main()
