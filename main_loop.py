import time
import sys
import traceback
import random
import urllib.request
import datetime as dt
from dateutil.parser import parse
import settings
import database as db
from scraper import do_scrape

def scrape(run_log):
    print('{}: Starting scrape cycle'.format(time.ctime()))
    try:
        run_log.num_results = do_scrape()
    except KeyboardInterrupt:
        print('Exiting...')
        run_log.time_end = parse(time.ctime())
        run_log.ip_end = urllib.request.urlopen('https://ident.me')\
                            .read().decode('utf8')
        run_log.exit_code = 1
        run_log.status_message = 'KeyboardInterrupt'
        if settings.DEBUG:
            input('run_log start time: {}'.format(run_log.time_start))
        db.add(run_log)
        sys.exit(1)
    except Exception as exc:
        print('Error with the scraping:', sys.exc_info()[0])
        traceback.print_exc()
        run_log.time_end = parse(time.ctime())
        run_log.ip_end = urllib.request.urlopen('https://ident.me')\
                            .read().decode('utf8')
        run_log.exit_code = -1
        run_log.status_message = 'Error with the scraping:'\
                                 + sys.exc_info()[0]
        if settings.DEBUG:
            input('run_log start time: {}'.format(run_log.time_start))
        db.add(run_log)
        sys.exit(-1)
    else:
        print('{}: Successfully finished scraping'.format(time.ctime()))
        run_log.time_end = parse(time.ctime())
        run_log.ip_end = urllib.request.urlopen('https://ident.me')\
                            .read().decode('utf8')
        run_log.exit_code = 0
        run_log.status_message = 'Successfully finished scraping'
        db.add(run_log)
        sys.exit(0)

def main():
    run_log = db.RunLog(debug=settings.DEBUG,
                        run_once=settings.RUN_ONCE,
                        time_start=parse(time.ctime()),
                        ip_start=urllib.request.urlopen('https://ident.me')\
                            .read().decode('utf8'))

    if settings.RUN_ONCE:
        scrape(run_log)
    else:
        while True:
            if dt.datetime.now().hour >= settings.WAKEUP_TIME:
                scrape(run_log)
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
