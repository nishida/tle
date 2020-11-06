#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
import time
import sys
import subprocess
from subprocess import PIPE
import requests
from setup_logger import setup_logger
import spacetrackaccount

MAX_ERROR = 3
MAX_RETRY = 2
# Limit API queries to less than 30 requests per minute / 300 requests per hour
MIN_INTERVAL = 12 # sec

def getdata(st, norad_cat_id, logger = None):
    from pprint import pprint
    for i in range(MAX_RETRY + 1):
        if i > 0:
            logger.warning('Retry {}/{} for NORAD Catalog Number {}'.format(i, MAX_RETRY, norad_cat_id))
            logger.debug('Sleep: %f secs', MIN_INTERVAL * 2 ** i)
            time.sleep(MIN_INTERVAL * 2 ** i)
        elif getdata.lasttime > 0:
            delta = time.monotonic() - getdata.lasttime
            if delta < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - delta)

        getdata.lasttime = time.monotonic()

        try:
            data = st.tle(norad_cat_id=norad_cat_id, orderby=['norad_cat_id', 'epoch'], format='json')

        except requests.HTTPError as e:
            # Critical error. Don't retry
            logger.debug('Response Time: %f secs', time.monotonic() - getdata.lasttime)
            logger.critical('HTTPError: ' + str(e))
            break

        except (requests.ConnectionError, requests.Timeout) as e:
            # Retry
            logger.debug('Response Time: %f secs', time.monotonic() - getdata.lasttime)
            logger.warning('ConnectionError: ' + str(e))

        else:
            # Success
            logger.debug('Response Time: %f secs', time.monotonic() - getdata.lasttime)
            return data

    return None

getdata.lasttime = 0

def savedata(data, filename, compress = True, logger = None):
    with open(filename, 'w') as fp:
        fp.write(data)

    if not compress:
        return True

    proc = subprocess.Popen(['xz', '-9', filename], stdout=PIPE, stderr=PIPE, text=True)
    (stdout, stderr) = proc.communicate()
    if stdout != '' or stderr != '':
        logger.critical(stdout + stderr)
        return False
    else:
        return True

def main():
    logger = setup_logger('download_tle_satcat_json')

    if len(sys.argv) == 3:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
    elif len(sys.argv) == 2:
        start = int(sys.argv[1])
        end = int(sys.argv[1])
    else:
        logger.critical('Invalid number of arguments!')
        print('Usage: {} NORAD_Catalog_Number [NORAD_Catalog_Number]'.format(sys.argv[0]))
        sys.exit(0)

    if end < start:
        start, end = end, start

    nsats = end - start + 1

    print('Start: {}'.format(start))
    print('End: {}'.format(end))
    print('Number of Satellites: {}'.format(nsats))

    st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

    error_count = 0

    for i in range(start, end + 1):
        logger.info('Downloading NORAD Catalog Number {} ({}/{})'.format(i, i - start + 1, nsats))

        data = getdata(st, i, logger=logger)

        if data is None:
            logger.warning("Error: Fail to download data for NORAD Catalog Number {}".format(i))
            error_count += 1
            if error_count >= MAX_ERROR:
                logger.critical("The number of errors reaches its Maximum Error Count")
                sys.exit(0)
        else:
            filename = 'download/{}.json'.format(i)
            result = savedata(data, filename, logger = logger)
            if not result:
                logger.critical("Error: Fail to save data for NORAD Catalog Number {}".format(i))
                sys.exit(0)

    sys.exit(1)

if __name__ == '__main__':
    main()

