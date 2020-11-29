#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
from datetime import datetime
import time
import sys
import os
import math
import argparse
import subprocess
from subprocess import PIPE
import requests
from setup_logger import setup_logger
import spacetrackaccount

MAX_RETRY = 2
# Limit API queries to less than 30 requests per minute / 300 requests per hour
MIN_INTERVAL = 12 # sec

def getdata(st, norad_cat_id = None, logger = None):
    for i in range(MAX_RETRY + 1):
        if i > 0:
            logger.warning('Retry {}/{}'.format(i, MAX_RETRY))
            logger.debug('Sleep: %f secs', MIN_INTERVAL * 2 ** i)
            time.sleep(MIN_INTERVAL * 2 ** i)
        elif getdata.lasttime > 0:
            delta = time.monotonic() - getdata.lasttime
            if delta < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - delta)

        getdata.lasttime = time.monotonic()

        try:
            if norad_cat_id is None or len(norad_cat_id) == 0:
                data = st.satcat(orderby=['norad_cat_id'], format='json')
            else:
                data = st.satcat(norad_cat_id=norad_cat_id, orderby=['norad_cat_id'], format='json')

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

def savedata(data, filename, compress = True, force = False, logger = None):
    if os.path.exists(filename) and not force:
        logger.error(filename + ' already exists')
        return False

    with open(filename, 'w') as fp:
        fp.write(data)

    if not compress:
        return True
    program = ['xz', '-9', filename]
    if force:
        program.insert(1, '-f')
    proc = subprocess.Popen(program, stdout=PIPE, stderr=PIPE, text=True)
    (stdout, stderr) = proc.communicate()
    if stdout != '' or stderr != '':
        logger.error(stdout + stderr)
        return False
    else:
        return True

def main():
    logger = setup_logger('download_satcat')

    parser = argparse.ArgumentParser(description='Download SATCAT data of specified NORAD Catalog Number.')
    parser.add_argument('CAT_ID', type=str, nargs='*', help='NORAD Catalog Number.')
    parser.add_argument('-o', '--output', type=str, help='Output file.')
    parser.add_argument('-f', '--force', action='store_true', help='If the outpu file already exists, overwrite it.')
    args = parser.parse_args()
    force = args.force

    norad_cat_id = op._stringify_predicate_value(args.CAT_ID)

    if len(norad_cat_id) > 0:
        logger.info('Downloading NORAD Catalog Number: {}'.format(norad_cat_id))
    else:
        logger.info('Downloading NORAD Catalog Number: All')

    if args.output is None:
        filename = 'download/satcat-{:%Y%m%d%H%M%S}.json'.format(datetime.now())
    else:
        filename = args.output
    logger.info('Filename: {}'.format(filename))

    st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

    starttime = time.monotonic()
    tsize = 0
    tfiles = 0
    error_count = 0

    data = getdata(st, norad_cat_id, logger=logger)

    if data is None:
        logger.error("Error: Fail to download data")
        error_count += 1
    else:
        tsize += len(data)
        tfiles += 1
        result = savedata(data, filename, force = force, logger = logger)
        if not result:
            logger.error("Error: Fail to save data")
            error_count += 1

    if error_count > 0:
        logger.error("The number of errors is {}".format(error_count))

    logger.info("Downloaded: {} files, {} bytes in {} sec".format(tfiles , tsize, int(time.monotonic() - starttime)))
    sys.exit(0 if error_count == 0 else 1)

if __name__ == '__main__':
    main()

