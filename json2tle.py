#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import sys
import re

if len(sys.argv) < 2:
    print('Invalid number of arguments!')
    sys.exit(0)

p = re.compile(r'(?:\.json)?(?:\.gz|\.bz2|\.xz|\.zip)?$')

for jsonfile in sys.argv[1:]: 
    print('Input:', jsonfile)
    tlefile = p.sub('', jsonfile) + '.tle'
    df = pd.read_json(jsonfile, orient='records', , dtype='object')
    print(len(df), 'records read')
    if len(df) == 0:
        print('No output')
    else:
        print('Output:', tlefile)
        with open(tlefile, 'w') as fp:
            for line0, line1, line2 in zip(df['TLE_LINE0'], df['TLE_LINE1'], df['TLE_LINE2']):
                fp.write(line0 + "\n")
                fp.write(line1 + "\n")
                fp.write(line2 + "\n")

sys.exit(1)

