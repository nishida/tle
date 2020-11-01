#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
import time
import sys
import subprocess
from subprocess import PIPE
import spacetrackaccount

if len(sys.argv) == 3 :
    start = int(sys.argv[1])
    end = int(sys.argv[2])
elif len(sys.argv) == 2 :
    start = int(sys.argv[1])
    end = int(sys.argv[1])
else :
    print('Invalid number of arguments!')
    sys.exit(0)

st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

def mycallback(until):
    duration = int(round(until - time.monotonic()))
    print('Sleeping for {:d} seconds.'.format(duration))

st.callback = mycallback

for i in range(start, end + 1):
    print(i)
    data = st.tle(norad_cat_id=i, orderby=['norad_cat_id', 'epoch'], format='csv')

    if len(data) != 0:
        file = 'download/{}.csv'.format(i)
        with open(file, 'w') as fp:
            fp.write(data)
        proc = subprocess.Popen(['xz', '-9', file], stdout=PIPE, stderr=PIPE, text=True)

    # Limit API queries to less than 30 requests per minute / 300 requests per hour
    if i != end :
        time.sleep(12)

    if len(data) != 0:
        (stdout, stderr) = proc.communicate()
        if stdout != '' or stderr != '' :
            print(stdout, stderr)
            sys.exit(0)

sys.exit(1)

