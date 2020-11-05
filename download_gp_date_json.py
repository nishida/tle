#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
from dateutil import parser
from datetime import datetime, timedelta
import time
import sys
import subprocess
from subprocess import PIPE
import spacetrackaccount

# Limit API queries to less than 30 requests per minute / 300 requests per hour
mininterval = 12 # sec

def showusage():
    print('Usage: {} YYYY-MM-DD [YYYY-MM-DD]'.format(sys.argv[0]))

if len(sys.argv) == 3:
    start = parser.parse(sys.argv[1], yearfirst=True)
    end = parser.parse(sys.argv[2], yearfirst=True)
elif len(sys.argv) == 2:
    start = parser.parse(sys.argv[1], yearfirst=True)
    end = start
else:
    print('Invalid number of arguments!')
    showusage()
    sys.exit(0)


start = datetime(start.year, start.month, start.day)
end = datetime(end.year, end.month, end.day)

if end < start:
	start, end = end, start

ndays = (end - start).days + 1

print('Start:', start.strftime('%Y-%m-%d'))
print('End:', end.strftime('%Y-%m-%d'))
print('Number of Days:', ndays)

st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

def mycallback(until):
    duration = int(round(until - time.monotonic()))
    print('Sleeping for {:d} seconds.'.format(duration))

st.callback = mycallback

for day in (start + timedelta(days=i) for i in range(ndays)):
    print('Downloading', day.strftime('%Y-%m-%d'))

    nextday = day + timedelta(days=1)

    time1 = time.monotonic()
    data = st.gp_history(epoch=[op.inclusive_range(day.strftime('%Y-%m-%d'), nextday.strftime('%Y-%m-%d'))], 
        orderby=['norad_cat_id', 'epoch'], format='json')
    file = 'download/{}.json'.format(day.strftime('%Y%m%d'))
    with open(file, 'w') as fp:
        fp.write(data)

    proc = subprocess.Popen(['xz', '-9', file], stdout=PIPE, stderr=PIPE, text=True)
    (stdout, stderr) = proc.communicate()
    if stdout != '' or stderr != '':
        print(stdout, stderr)
        sys.exit(0)

    delta = time.monotonic() - time1
    if delta < mininterval and day != (start + timedelta(days = (ndays - 1))):
        time.sleep(mininterval - delta)

sys.exit(1)

