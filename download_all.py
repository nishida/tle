#!/usr/bin/env python
# -*- coding: utf-8 -*-

import spacetrack.operators as op
from spacetrack import SpaceTrackClient
import pandas as pd
import json
import time
import sys
import spacetrackaccount

if len(sys.argv) != 3:
    print('Invalid number of arguments!')
    sys.exit(0)

start = int(sys.argv[1])
end = int(sys.argv[2])

st = SpaceTrackClient(spacetrackaccount.userid, spacetrackaccount.password)

def mycallback(until):
    duration = int(round(until - time.monotonic()))
    print('Sleeping for {:d} seconds.'.format(duration))

st.callback = mycallback

for i in range(start, end + 1):
    print(i)
    data = st.tle(norad_cat_id=i, orderby=['norad_cat_id', 'epoch'], format='json')
    file = 'download/{}.json'.format(i)
    with open(file, 'w') as fp:
        fp.write(data)
    # Limit API queries to less than 30 requests per minute / 300 requests per hour
    time.sleep(12)

sys.exit(1)
