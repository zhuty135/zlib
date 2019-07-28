#!/usr/bin/python3

import pandas as pd
import numpy as np
import shutil
import os

from datetime import date, datetime, timedelta
from zipline.utils.calendars import get_calendar

def get_prev_business_date(d,n,fmt="%Y-%m-%d",VERBOSE=False):
    if d is None:
        today = date.today()
        if VERBOSE: print('today',today)
    else:
        today = pd.to_datetime(d)
    edate = today.strftime(fmt)
    sdate = (today - timedelta(7)).strftime(fmt)
    if VERBOSE: print(sdate)

    t=get_calendar('XSHG').all_sessions
    pydate_array=t.to_pydatetime()
    date_only_array =  np.vectorize(lambda s: s.strftime(fmt))(pydate_array)
    if VERBOSE: print(date_only_array > sdate)
    if VERBOSE: print(date_only_array < edate)
    date_only_array = date_only_array[ date_only_array < edate]
    if VERBOSE: print(date_only_array[-100:] )

    return date_only_array[n]


if __name__ == '__main__':
    get_prev_business_date(None,-1,VERBOSE=True)
    calobj = ZCalendar( period_begin = pd.Timestamp( '2011-12-30' ), period_end = pd.Timestamp(date.today()), oflag=True)

    #calobj._create_cal_file(oflag=False)
