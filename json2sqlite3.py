#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
import argparse
import sqlite3
from setup_logger import setup_logger

def main():
    logger = setup_logger('json2sqlite3')

    # JSONの各カラムの型
    dtype = {'CCSDS_OMM_VERS': object,  'COMMENT': object,  'CREATION_DATE': 'datetime64[ns]',  'ORIGINATOR': object, 
           'OBJECT_NAME': object,  'OBJECT_ID': object,  'CENTER_NAME': object,  'REF_FRAME': object, 
           'TIME_SYSTEM': object,  'MEAN_ELEMENT_THEORY': object,  'EPOCH': 'datetime64[ns]',  'MEAN_MOTION': 'float64', 
           'ECCENTRICITY': 'float64',  'INCLINATION': 'float64',  'RA_OF_ASC_NODE': 'float64', 
           'ARG_OF_PERICENTER': 'float64',  'MEAN_ANOMALY': 'float64',  'EPHEMERIS_TYPE': 'int8', 
           'CLASSIFICATION_TYPE': object,  'NORAD_CAT_ID': 'uint32',  'ELEMENT_SET_NO': 'uint16', 
           'REV_AT_EPOCH': 'uint32',  'BSTAR': 'float64',  'MEAN_MOTION_DOT': 'float64',  'MEAN_MOTION_DDOT': 'float64', 
           'SEMIMAJOR_AXIS': 'float64',  'PERIOD': 'float64',  'APOAPSIS': 'float64',  'PERIAPSIS': 'float64',  'OBJECT_TYPE': object, 
           'RCS_SIZE': object,  'COUNTRY_CODE': object,  'LAUNCH_DATE': 'datetime64[ns]',  'SITE': object,  'DECAY_DATE': 'datetime64[ns]', 
           'FILE': 'uint64',  'GP_ID': 'uint32',  'TLE_LINE0': object,  'TLE_LINE1': object,  'TLE_LINE2': object}

    # JSONで日時として扱うカラム
    convert_dates = ['EPOCH', 'CREATION_DATE', 'LAUNCH_DATE', 'DECAY_DATE']

    # DBに保存するカラム名 (TLEなし)
    columns_out_without_tle = ['CREATION_DATE', 'EPOCH', 'OBJECT_ID', 'MEAN_MOTION', 'ECCENTRICITY', 'INCLINATION', 'RA_OF_ASC_NODE',
        'ARG_OF_PERICENTER', 'MEAN_ANOMALY', 'NORAD_CAT_ID', 'REV_AT_EPOCH', 'BSTAR', 'SEMIMAJOR_AXIS',
        'PERIOD', 'APOAPSIS', 'PERIAPSIS', 'GP_ID']

    # DBに保存するカラム名 (TLEつき)
    columns_out_with_tle = columns_out_without_tle + ['TLE_LINE0', 'TLE_LINE1', 'TLE_LINE2']

    # テーブル作成 (テーブル名は後で入れる)
    create_table_without_tle = '''CREATE TABLE IF NOT EXISTS {} (
        CREATION_DATE timestamp, EPOCH timestamp, OBJECT_ID text,
        MEAN_MOTION real, ECCENTRICITY real, INCLINATION real, RA_OF_ASC_NODE real, ARG_OF_PERICENTER real, MEAN_ANOMALY real,
        NORAD_CAT_ID integer, REV_AT_EPOCH integer, BSTAR real, SEMIMAJOR_AXIS real, PERIOD real, APOAPSIS real, PERIAPSIS real,
        GP_ID integer primary key)'''
    create_table_with_tle = '''CREATE TABLE IF NOT EXISTS {} (
        CREATION_DATE timestamp, EPOCH timestamp, OBJECT_ID text,
        MEAN_MOTION real, ECCENTRICITY real, INCLINATION real, RA_OF_ASC_NODE real, ARG_OF_PERICENTER real, MEAN_ANOMALY real,
        NORAD_CAT_ID integer, REV_AT_EPOCH integer, BSTAR real, SEMIMAJOR_AXIS real, PERIOD real, APOAPSIS real, PERIAPSIS real,
        GP_ID integer  primary key, TLE_LINE0 text, TLE_LINE1 text, TLE_LINE2 text)'''

    # レコード追加 (テーブル名は後で入れる)
    insert_record_without_tle = '''INSERT INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(GP_ID) DO NOTHING'''
    insert_record_with_tle = '''INSERT INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(GP_ID) DO NOTHING'''
    insert_record_without_tle_compat = '''REPLACE INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
    insert_record_with_tle_compat = '''REPLACE INTO {} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

    # indexをつけるカラム
    columns_with_index = ['EPOCH', 'NORAD_CAT_ID']

    parser = argparse.ArgumentParser(description='Convert JSON or Parquet to SQLite3.')

    parser.add_argument('FILE', type=str, nargs='+', help='Input JSON / Parquet files.')
    parser.add_argument('DATABASE', type=str, help='SQLite3 Database file.')
    parser.add_argument('TABLE', type=str, help='Table name.')
    parser.add_argument('-t', '--with_tle', action='store_true', help='Include TLE lines')
    parser.add_argument('-d', '--drop_table', action='store_true', help='Drop table if exists')
    parser.add_argument('-i', '--drop_index', action='store_true', help='Drop index temporarily before insert records')
    parser.add_argument('-r', '--replace_record', action='store_true', help='Use REPLACE statment instead of UPSERT')

    args = parser.parse_args()

    infiles = args.FILE
    dbfile = args.DATABASE
    table = args.TABLE
    with_tle = args.with_tle
    drop_table = args.drop_table
    drop_index = args.drop_index
    replace_record = args.replace_record

    columns_out = columns_out_with_tle if with_tle else columns_out_without_tle
    create_table = create_table_with_tle.format(table) if with_tle else create_table_without_tle.format(table)

    version = sqlite3.sqlite_version.split('.')
    if not replace_record and (int(version[0]) > 3 or (int(version[0]) == 3 and int(version[1]) >=24)):
        # SQLite3 3.24以降ではUPSERT (ON CONFLICT句) を使う
        insert_record = insert_record_with_tle.format(table) if with_tle else insert_record_without_tle.format(table)
    else:
        # 古い SQLite3 の場合と、--replace_record が指定された場合にはREPLACEを使う
        if not replace_record:
            logger.warning("SQLite3 version {} doesn't support UPSERT. REPLACE is used.".format(sqlite3.sqlite_version))
        insert_record = insert_record_with_tle_compat.format(table) if with_tle else insert_record_without_tle_compat.format(table)

    logger.debug("connecting to {}".format(dbfile))
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    if drop_table:
        cur.execute('DROP TABLE IF EXISTS {}'.format(table))
        logger.debug("table {} is dropped".format(table))

    cur.execute(create_table)

    if drop_index:
        for column in columns_with_index:
            cur.execute('DROP INDEX IF EXISTS index_{0}_{1}'.format(table, column))

    n = len(infiles)

    for i, infile in enumerate(infiles): 
        logger.info('Input({}/{}): {}'.format(i + 1, n, infile))
        if not os.path.isfile(infile):
            logger.critical('error: {} not found'.format(infile))
            sys.exit(1)
        try:
            df = pd.read_parquet(infile, columns = columns_out)
        except OSError as e:
            df = pd.read_json(infile, convert_dates = convert_dates, dtype = dtype, precise_float = True, orient = 'records')
            if len(df) != 0:
                df = df[columns_out]

        logger.debug('{} records read'.format(len(df)))
        if len(df) == 0:
            continue
        df['CREATION_DATE'] = df['CREATION_DATE'].astype(str)
        df['EPOCH'] = df['EPOCH'].astype(str)
        cur.executemany(insert_record, df.values.tolist())

    for column in columns_with_index:
        cur.execute('CREATE INDEX IF NOT EXISTS index_{0}_{1} ON {0} ({1})'.format(table, column))

    con.commit()
    con.close()

    sys.exit(0)

if __name__ == '__main__':
    main()
