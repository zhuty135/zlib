#!/usr/local/anaconda3/bin/python3.9
###!/usr/bin/python3

import pandas as pd
import numpy as np
import shutil
import os

from datetime import date, datetime, timedelta
from zipline.utils.calendar_utils import get_calendar




def get_business_date_list(fmt="%Y-%m-%d",caltype='XSHG'):
    t=get_calendar(caltype).all_sessions
    pydate_array=t.to_pydatetime()
    return np.vectorize(lambda s: s.strftime(fmt))(pydate_array)

def get_prev_business_date(d,n,fmt="%Y-%m-%d",caltype='XSHG',VERBOSE=False):
    if d is None:
        today = date.today()
        if VERBOSE: print('today',today)
    else:
        today = pd.to_datetime(d)
    edate = today.strftime(fmt)
    sdate = (today - timedelta(7)).strftime(fmt)
    if VERBOSE: print(sdate,edate)

    date_only_array  = get_business_date_list(caltype=caltype)

    if VERBOSE: print(date_only_array > sdate)
    if VERBOSE: print(date_only_array < edate)
    date_only_array = date_only_array[ date_only_array < edate]
    if VERBOSE: print(date_only_array,n)#[n] )

    return date_only_array[n]


def get_config(cfg = 'token'):
    import configparser
    import pwd
    uname = pwd.getpwuid(os.getuid()).pw_name
    cp = configparser.ConfigParser()
    cp.read('/work/'+uname+'/project/factors/config/databasic.cfg')
    if cfg in ('token','ix_symb'):
        sect = 'tushare'
    else:
        sect = ''
    print('sect',sect)
    result = eval(cp.get(sect,cfg))
    return result



if __name__ == '__main__':
    get_prev_business_date(None,-1,VERBOSE=True)
    calobj = ZCalendar( period_begin = pd.Timestamp( '2011-12-30' ), period_end = pd.Timestamp(date.today()), oflag=True)

    #calobj._create_cal_file(oflag=False)
