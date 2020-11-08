#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
import time
import sys
import math
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
            logger.error('HTTPError: ' + str(e))
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
        logger.error(stdout + stderr)
        return False
    else:
        return True

def main():
    logger = setup_logger('download_tle_satcat_json')

    if len(sys.argv) == 4:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
        unit = int(sys.argv[3])
    elif len(sys.argv) == 3:
        start = int(sys.argv[1])
        end = int(sys.argv[2])
        unit = 1
    elif len(sys.argv) == 2:
        start = int(sys.argv[1])
        end = int(sys.argv[1])
        unit = 1
    else:
        logger.critical('Invalid number of arguments!')
        print('Usage: {} NORAD_Catalog_Number [NORAD_Catalog_Number [Unit]]'.format(sys.argv[0]))
        sys.exit(1)

    if end < start:
        start, end = end, start

    nsats = end - start + 1
    nfiles = math.ceil(nsats / unit)

    logger.info('Start: {}'.format(start))
    logger.info('End: {}'.format(end))
    logger.info('Number of Satellites: {}'.format(nsats))
    logger.info('Number of Files: {}'.format(nfiles))

    st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

    starttime = time.monotonic()
    tsize = 0
    tfiles = 0
    error_count = 0

    for i in range(0, nfiles):
        if unit == 1:
            norad_cat_id = start + i
        else:
            norad_cat_id = op.inclusive_range(start + i * unit, min(start + (i + 1) * unit - 1, end))

        logger.info('Downloading NORAD Catalog Number {} ({}/{})'.format(norad_cat_id, i + 1, nfiles))

        data = getdata(st, norad_cat_id, logger=logger)

        if data is None:
            logger.error("Error: Fail to download data for NORAD Catalog Number {}".format(norad_cat_id))
            error_count += 1
        else:
            tsize += len(data)
            tfiles += 1
            filename = 'download/{}.json'.format(norad_cat_id)
            result = savedata(data, filename, logger = logger)
            if not result:
                logger.error("Error: Fail to save data for NORAD Catalog Number {}".format(norad_cat_id))
                error_count += 1

        if error_count >= MAX_ERROR:
            break

    if error_count > 0:
        logger.error("The number of errors is {}".format(error_count))
    if error_count >= MAX_ERROR:
        logger.critical("The number of errors reaches its Maximum Error Count")

    logger.info("Downloaded: {} files, {} bytes in {} sec".format(tfiles , tsize, int(time.monotonic() - starttime)))
    sys.exit(0 if error_count == 0 else 1)

if __name__ == '__main__':
    main()

