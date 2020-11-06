#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import json
import csv
import sys
import re

if len(sys.argv) < 2:
    print('Invalid number of arguments!')
    sys.exit(0)

p = re.compile(r'(?:\.json)?(?:\.gz|\.bz2|\.xz|\.zip)?$')

for jsonfile in sys.argv[1:]: 
    print('Input:', jsonfile)
    csvfile = p.sub('', jsonfile) + '.csv'
    df = pd.read_json(jsonfile, orient='records', dtype='object')
    print(len(df), 'records read')
    if len(df) == 0:
        print('No output')
    else:
        print('Output:', csvfile)
        df.to_csv(csvfile, index=False, quoting=csv.QUOTE_NONNUMERIC)

sys.exit(1)

