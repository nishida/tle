#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pandas as pd
import os
import sys
import argparse
import sqlite3
from setup_logger import setup_logger

def main():
    logger = setup_logger('satcat2qlite3')

    # JSONの各カラムの型 (https://www.space-track.org/basicspacedata/modeldef/class/satcat/format/html)
    dtype = {'INTLDES': object, 'NORAD_CAT_ID': 'uint32', 'OBJECT_TYPE': object, 'SATNAME': object,
        'COUNTRY': object, 'LAUNCH': 'datetime64[ns]', 'SITE': object, 'DECAY': 'datetime64[ns]',
        'PERIOD': 'float64', 'INCLINATION': 'float64', 'APOGEE': 'uint64', 'PERIGEE': 'uint64',
        'COMMENT': object, 'COMMENTCODE': 'uint8', 'RCSVALUE': 'int32', 'RCS_SIZE': object,
        'FILE': 'uint16', 'LAUNCH_YEAR': 'uint16', 'LAUNCH_NUM': 'uint16', 'LAUNCH_PIECE': object,
        'CURRENT': object, 'OBJECT_NAME': object, 'OBJECT_ID': object, 'OBJECT_NUMBER': 'uint32'}

    # JSONで日時として扱うカラム
    convert_dates = ['LAUNCH', 'DECAY']

    # DBに保存するカラム名
    columns_out = ['NORAD_CAT_ID', 'OBJECT_TYPE', 'COUNTRY', 'LAUNCH', 'DECAY', 'PERIOD', 'INCLINATION',
        'APOGEE', 'PERIGEE', 'COMMENT', 'RCSVALUE', 'RCS_SIZE', 'CURRENT', 'OBJECT_NAME', 'OBJECT_ID']

    # テーブル作成 (テーブル名は後で入れる)
    create_table = '''CREATE TABLE IF NOT EXISTS {} (
        NORAD_CAT_ID integer primary key, OBJECT_TYPE text, COUNTRY text, LAUNCH timestamp, DECAY timestamp, 
        PERIOD real, INCLINATION real, APOGEE integer, PERIGEE integer, COMMENT text, RCSVALUE integer, 
        RCS_SIZE text, CURRENT text, OBJECT_NAME text, OBJECT_ID text)'''

    # indexをつけるカラム
    columns_with_index = ['OBJECT_TYPE', 'LAUNCH', 'DECAY', 'CURRENT']

    parser = argparse.ArgumentParser(description='Convert SATCAT JSON to SQLite3.')

    parser.add_argument('FILE', type=str, help='Input JSON file.')
    parser.add_argument('DATABASE', type=str, help='SQLite3 Database file.')
    parser.add_argument('TABLE', type=str, help='Table name.')

    args = parser.parse_args()

    infile = args.FILE
    dbfile = args.DATABASE
    table = args.TABLE


    logger.info('Input: {}'.format(infile))
    if not os.path.isfile(infile):
        logger.critical('error: {} not found'.format(infile))
        sys.exit(1)
    df = pd.read_json(infile, convert_dates = convert_dates, dtype = dtype, precise_float = True, orient = 'records')

    if len(df) == 0:
        logger.error('error: No valid data in {}'.format(infile))
        sys.exit(1)

    logger.debug('{} records read'.format(len(df)))

    logger.debug("connecting to {}".format(dbfile))
    con = sqlite3.connect(dbfile)
    cur = con.cursor()

    cur.execute('DROP TABLE IF EXISTS {}'.format(table))
    cur.execute(create_table.format(table))
    df[columns_out].to_sql(table, con, if_exists='append', index=None)

    for column in columns_with_index:
        cur.execute('CREATE INDEX IF NOT EXISTS index_{0}_{1} ON {0} ({1})'.format(table, column))

    con.commit()
    con.close()

    sys.exit(0)

if __name__ == '__main__':
    main()
