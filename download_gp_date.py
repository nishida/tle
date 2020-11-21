#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
import dateutil.parser
from datetime import datetime, timedelta
import time
import sys
import math
import argparse
import subprocess
from subprocess import PIPE
import requests
from setup_logger import setup_logger
import spacetrackaccount

MAX_ERROR = 3
MAX_RETRY = 2
# Limit API queries to less than 30 requests per minute / 300 requests per hour
MIN_INTERVAL = 12 # sec

def getdata(st, epoch, date_type = 'EPOCH', logger = None):
    for i in range(MAX_RETRY + 1):
        if i > 0:
            logger.warning('Retry {}/{} for {}'.format(i, MAX_RETRY, epoch))
            logger.debug('Sleep: %f secs', MIN_INTERVAL * 2 ** i)
            time.sleep(MIN_INTERVAL * 2 ** i)
        elif getdata.lasttime > 0:
            delta = time.monotonic() - getdata.lasttime
            if delta < MIN_INTERVAL:
                time.sleep(MIN_INTERVAL - delta)

        getdata.lasttime = time.monotonic()

        try:
            if date_type == 'EPOCH':
                data = st.gp_history(epoch=epoch, orderby=['norad_cat_id', 'epoch'], format='json')
            elif date_type =='CREATION_DATE':
                data = st.gp_history(creation_date=epoch, orderby=['norad_cat_id', 'creation_date'], format='json')
            else:
                raise Exception('Unknown date_type')

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
    logger = setup_logger('download_gp_date')

    parser = argparse.ArgumentParser(description='Download GP data of specified date.')
    parser.add_argument('START', type=str, help='Start Date (YYYY-MM-DD).')
    parser.add_argument('END', type=str, nargs='?', help='End Date (YYYY-MM-DD). Default: same as START')
    parser.add_argument('CHUNK', type=int, nargs='?', default=1, help='Chunk size (days). Default: 1')
    parser.add_argument('-c', '--creationdate', action='store_true', help='Use CREATION_DATE intead of EPOCH')
    args = parser.parse_args()

    start = dateutil.parser.parse(args.START, yearfirst=True)
    end = dateutil.parser.parse(args.END, yearfirst=True) if args.END is not None else start
    unit = args.CHUNK
    date_type = 'EPOCH' if not args.creationdate else 'CREATION_DATE'

    start = datetime(start.year, start.month, start.day)
    end = datetime(end.year, end.month, end.day)
    
    if end < start:
        start, end = end, start

    ndays = (end - start).days + 1
    nfiles = math.ceil(ndays / unit)

    logger.info('Start: ' +  start.strftime('%Y-%m-%d'))
    logger.info('End: ' + end.strftime('%Y-%m-%d'))
    logger.info('Number of Days: {}'.format(str(ndays)))
    logger.info('Number of Files: {}'.format(nfiles))

    st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

    starttime = time.monotonic()
    tsize = 0
    tfiles = 0
    error_count = 0

    for i in range(0, nfiles):
        day1 = start + timedelta(days = i * unit)
        day2 = min(day1 + timedelta(days = unit - 1), end)
        epoch = op.inclusive_range(day1.strftime('%Y-%m-%d'), (day2 + timedelta(days = 1)).strftime('%Y-%m-%d'))

        if day1 == day2:
            epoch_to_show = day1.strftime('%Y-%m-%d')
            filename = 'download/{}.json'.format(day1.strftime('%Y%m%d'))
        else:
            epoch_to_show = '{}--{}'.format(day1.strftime('%Y-%m-%d'), day2.strftime('%Y-%m-%d'))
            filename = 'download/{}-{}.json'.format(day1.strftime('%Y%m%d'), day2.strftime('%Y%m%d'))

        logger.info('Downloading {} ({}/{})'.format(epoch_to_show, i + 1, nfiles))

        data = getdata(st, epoch, date_type = date_type, logger = logger)
    
        if data is None:
            logger.error("Error: Fail to download data for " + epoch_to_show)
            error_count += 1
        else:
            tsize += len(data)
            tfiles += 1
            result = savedata(data, filename, logger = logger)
            if not result:
                logger.error("Error: Fail to save data for {}".format(epoch_to_show))
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

