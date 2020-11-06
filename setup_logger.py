#!/usr/bin/env python
# -*- coding: utf-8 -*-

from logging import getLogger, StreamHandler, Formatter, FileHandler, DEBUG, INFO
from datetime import datetime

def setup_logger(name, logfile = None, shlevel = INFO, fhlevel = DEBUG):

    if logfile is None:
        logfile = 'log/{}-{:%Y%m%d%H%M%S}'.format(name, datetime.now())

    logger = getLogger(name)
    logger.setLevel(DEBUG)

    sh = StreamHandler()
    sh.setLevel(shlevel)
    formatter = Formatter('%(asctime)s [%(levelname)s] %(message)s')
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    fh = FileHandler(logfile)
    fh.setLevel(fhlevel)
    fh_formatter = Formatter('%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d (%(funcName)s) %(message)s')
    fh.setFormatter(fh_formatter)
    logger.addHandler(fh)
    return logger

