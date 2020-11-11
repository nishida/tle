#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
import argparse

parser = argparse.ArgumentParser(description='Convert JSON to Parquet.')

parser.add_argument('JSON_file', type=str, nargs='+', help='Input JSON files.')
parser.add_argument('Parquet_file', type=str, nargs=1, help='Output Parquet file.')
parser.add_argument('-c', '--compression', type=str, default='snappy', help='Name of the compression to use. Use None for no compression.')

args = parser.parse_args()

jsonfiles = args.JSON_file
parquetfile = args.Parquet_file[0]
compression = args.compression

if os.path.isfile(parquetfile):
    print('error: {} already exists'.format(parquetfile), file=sys.stderr)
    sys.exit(1)

dtype = {'CCSDS_OMM_VERS': str,  'COMMENT': str,  'CREATION_DATE': 'datetime64[ns]',  'ORIGINATOR': str, 
       'OBJECT_NAME': str,  'OBJECT_ID': str,  'CENTER_NAME': str,  'REF_FRAME': str, 
       'TIME_SYSTEM': str,  'MEAN_ELEMENT_THEORY': str,  'EPOCH': 'datetime64[ns]',  'MEAN_MOTION': 'float64', 
       'ECCENTRICITY': 'float64',  'INCLINATION': 'float64',  'RA_OF_ASC_NODE': 'float64', 
       'ARG_OF_PERICENTER': 'float64',  'MEAN_ANOMALY': 'float64',  'EPHEMERIS_TYPE': 'int8', 
       'CLASSIFICATION_TYPE': str,  'NORAD_CAT_ID': 'uint32',  'ELEMENT_SET_NO': 'uint16', 
       'REV_AT_EPOCH': 'uint32',  'BSTAR': 'float64',  'MEAN_MOTION_DOT': 'float64',  'MEAN_MOTION_DDOT': 'float64', 
       'SEMIMAJOR_AXIS': 'float64',  'PERIOD': 'float64',  'APOAPSIS': 'float64',  'PERIAPSIS': 'float64',  'OBJECT_TYPE': str, 
       'RCS_SIZE': str,  'COUNTRY_CODE': str,  'LAUNCH_DATE': 'datetime64[ns]',  'SITE': str,  'DECAY_DATE': 'datetime64[ns]', 
       'FILE': 'uint64',  'GP_ID': 'uint32',  'TLE_LINE0': str,  'TLE_LINE1': str,  'TLE_LINE2': str}

convert_dates = ['EPOCH', 'CREATION_DATE', 'LAUNCH_DATE', 'DECAY_DATE']

df_list = []

n = len(jsonfiles)

for i, jsonfile in enumerate(jsonfiles): 
    print('Input({}/{}): {}'.format(i + 1, n, jsonfile))
    if not os.path.isfile(jsonfile):
        print('error: {} not found'.format(jsonfile), file=sys.stderr)
        sys.exit(1)

    df_list.append(
        pd.read_json(
            jsonfile, convert_dates = convert_dates, dtype = dtype, precise_float = True, orient = 'records'
        
        )
    )

df = pd.concat(df_list)
len1 = len(df)
df.drop_duplicates(subset = ['GP_ID'], ignore_index = True, inplace=True)
len2 = len(df)
if len1 != len2:
    print('{} duplicated records removed'.format(len1 - len2))

print('Output: {}'.format(parquetfile))

if compression == "None":
    df.to_parquet(parquetfile, compression=None)
else:
    df.to_parquet(parquetfile, compression=compression)

sys.exit(0)

