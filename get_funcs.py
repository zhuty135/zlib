import talib as ta 
import numpy as np
import pandas as pd
from math import sqrt 
from dateutil.relativedelta import relativedelta
from datetime import datetime
from zipline.api import get_datetime,symbol
from zipline.utils.math_utils import  nanargmax,  nanargmin,nanmax, nanmean,  nanstd,  nansum,  nanmin
import configparser
cp = configparser.ConfigParser()
import os
import pwd
uname = pwd.getpwuid(os.getuid()).pw_name

u_sect = os.environ['MODEL_SELECT'] 
sect = 'base'
m_select = u_sect
cp = configparser.ConfigParser()
m_path = '/work/' + uname + '/project/gitrepos/slib/config/' + m_select + '.cfg'
print(m_path)
cp.read(m_path)

VERBOSE = cp.getboolean(sect,'verbose')
DAY_HACK_ENABLED = eval(os.environ['DAY_HACK_ENABLED'])#eval(cp.get(sect,'day_hack_enabled'))
ATR_ENABLED = cp.getboolean(sect,'ATR_ENABLED')
#BB_SHRINK = eval(cp.get(sect,'bb_shrink'))
#BB_BUF_ZONE = eval(cp.get(sect,'bb_buf_zone'))
SEC_ENABLED = eval(os.environ['SEC_ENABLED'])#eval(cp.get(sect,'SEC_ENABLED'))
LONG_ONLY =  eval(cp.get(u_sect,'long_only'))
MB_HACK = cp.getint('mb','mb_hack')

def get_symb_data(data, s, fld, bc, freq) :
    return data.history(symbol(s), fields=fld, bar_count=bc, frequency=freq) if DAY_HACK_ENABLED else data.history(symbol(s),fields=fld, bar_count=bc+1, frequency=freq)[:-1]

def get_atr(context, data, s='000016.SH' , barcount = 5):
    highs =  get_symb_data(data, s, 'high', barcount, '1d' )
    lows  =  get_symb_data(data, s, 'low' , barcount, '1d' )
    closes = get_symb_data(data, s, 'high', barcount, '1d' )
    vol = ta.TRANGE(np.array(highs), np.array(lows), np.array(closes))[-1] if ATR_ENABLED else closes.std()
    return vol

def testday (context, data):
    from zipline.utils.tradingcalendar import trading_day
    from pandas import date_range

    tdate = get_datetime()
    tdate_mon = tdate.strftime('%m' )

    week_later = tdate + relativedelta(weeks=+1)
    week_before = tdate + relativedelta(weeks=-1)
    print('week_later', week_later.strftime('%Y-%m'), context.last_reb_month)
    sdate = week_later.strftime('%Y-%m' ) + '-01'
    edate = week_later.strftime('%Y-%m' ) + '-15'
    next_month_days = date_range(sdate, edate, freq=trading_day, tz='Asia/Shanghai' )
    d1 = pd.to_datetime(tdate.strftime("%Y-%m-%d"))
    d2 = pd.to_datetime(datetime.strftime(next_month_days[0],"%Y-%m-%d"))
    d_diff= pd.Timedelta(d2-d1).days
    first_w_of_m = 0

    if (d_diff < 5 and d_diff > 0) or (context.last_reb_month != tdate.strftime('%Y-%m' )) :
        print('mend' , tdate, next_month_days[0])
        print('mdiff' ,type(week_later), (next_month_days[0]))
        first_w_of_m = 1
    print('first_w_of_m',first_w_of_m, d_diff, d1, d2)

    return np.sign(first_w_of_m)

def get_returns(context, data, asset, n):
    
    dh = data.history(asset, 'price' , bar_count=n+1, frequency="1d" )
    return dh[-1]/dh[-n-1]-dh[-1]/dh[-2]

def get_sea(context, data):
    largecap_ret1w = context.pipeline_data.loc[symbol('000016.SH'), 'wrets' ]
    smallcap_ret1w = context.pipeline_data.loc[symbol('000905.SH'), 'wrets' ]

    vol = get_atr(context, data)
    print('atr' , vol, context.seavol)
    dispersion = largecap_ret1w - smallcap_ret1w
    first_w_of_m = testday(context, data)
    last_week_return = context.pipeline_data.loc[symbol('000300.SH'),'wrets']
    last_3m_return =   context.pipeline_data.loc[symbol('000300.SH'),'returns_5M']
    eq_idx_dret = context.pipeline_data.loc[symbol('000300.SH' ), 'drets']
    eq_idx_wvol = context.pipeline_data.loc[symbol('000300.SH' ), 'volatility']/sqrt(242)#beat context.seavol
    bondid = 'H11077.SH' #'H00140.SH' `
    yld_wvol = context.pipeline_data.loc[symbol(bondid), 'volatility']/sqrt(242)*sqrt(5)
    yld_wret = context.pipeline_data.loc[symbol(bondid), 'wrets']
    rb_wret  = context.pipeline_data.loc[symbol('CFRBPA.JZ'), 'wrets' ]
    au_wret  = context.pipeline_data.loc[symbol('CFAUPA.JZ'), 'wrets' ]
    print ('seatest' ,  get_datetime())
    for asset in context.pipeline_data.index:
        context.seadf.loc[asset, 'vol' ] = 1 if vol > context.seavol else -1# .22 (a11), .88(1+0),
        context.seadf.loc[asset, 'bond'] = np.sign(yld_wret) #0.2sr
        context.seadf.loc[asset, 'mom' ] = 1 if last_3m_return > 0 else -1 # .88 (a11),
        context.seadf.loc[asset, 'sea' ] = first_w_of_m#.26sr
        context.seadf.loc[asset, 'com' ] = 1 if rb_wret < au_wret else -1
        context.seadf.loc[asset, 'dis' ] = 1 if dispersion > .005 else -1# 0. 87(000300), 1.4(+1) -0.08 (-1)
        context.seadf.loc[asset, 'check' ] = (np.sign(eq_idx_dret)) if abs(eq_idx_dret) > eq_idx_wvol else 0# -1 120180814 •
    sea_score = context.seadf.sum(axis=1)
    print ('vol:', vol, context.seavol)
    print ('bond:', yld_wret, yld_wvol)
    print ('mom:', last_3m_return)
    print ('com:' , rb_wret, au_wret)
    print ('dis1D:', context.pipeline_data.loc[symbol('000016.SH'), 'drets' ] , context.pipeline_data.loc[symbol('000905.SH'),'drets'])
    print ('dis6D:', context.pipeline_data.loc[symbol('000016.SH'), 'returns_6D' ] , context.pipeline_data.loc[symbol('000905.SH'),'returns_6D'])
    print ('dis1W:', context.pipeline_data.loc[symbol('000016.SH'), 'wrets' ] , context.pipeline_data.loc[symbol('000905.SH'),'wrets'])
    print ('get_returns', largecap_ret1w, smallcap_ret1w)
    print ('seaoverall:',get_datetime().strftime('%Y-%m-%d'),',',sea_score.loc[symbol('000300.SH')], '\n' , context.seadf.loc[symbol('000300.SH')])
    #test_dis = context.seadf.loc[symbol('000300.SH'), 'dis' ]
    toggle = 1#if (sea score. loc[symbol('000300.SH')) - test_dis)*test_dis > 0 else .5
    return (np.sign(sea_score), toggle)

def get_index_bands(context, data, assets_weight, n) :
    price_history = data.history(context.pipeline_data.index, fields="price", bar_count=n, frequency="1d")
    return_history = price_history/price_history.shift(1)-1
    weighted_return_history = return_history * assets_weight
    #print price_history, return_history, (assets_weight).shape
    weighted_return_history['sum'] = weighted_return_history.sum (axis = 1 )
    weighted_return_history['cumsum' ] = (1 + weighted_return_history['sum'] ).cumprod() - 1
    idxretmean = np.nanmean (weighted_return_history['cumsum' ] )
    idxretstd = np.nanstd (weighted_return_history['cumsum' ] )
    if VERBOSE: print('weighted_return_history:\n' , weighted_return_history)
    upper = idxretmean + 2*idxretstd
    lower = idxretmean - 1*idxretstd
    return(weighted_return_history['cumsum'][-1], upper, idxretmean, lower, idxretstd*np.sqrt(252))


def get_index_vol(context, data, assets_weight,n) :
    price_history = data.history(context.pipeline_data.index, fields="price", bar_count=n, frequency="1d")
    print('price_history', n, '\n',price_history)
    return_history = price_history/price_history.shift(1)-1
    weighted_return_history = return_history * assets_weight
    weighted_return_history['sum'] = weighted_return_history.sum(axis =1)
    return (np.nanstd(weighted_return_history['sum'] )*np.sqrt(252))


def get_pipe_weights(context, data):
    pipdf = context.new_pipeline_assets_weight.fillna(0).copy()
    print('context.new_pipeline_assets_weight', context.new_pipeline_assets_weight)
    wdflist= [context.new_pipeline_assets_weight, context.new_assets_weight, context.last_1w_weight, context.last_2w_weight]
    wdfcat = pd.concat (wdflist, axis=1).fillna(0).round(3)
    wsdfcat = pd.concat([(wdfcat.sum(axis=1)/4).round(3), wdfcat], axis=1)
    wsdfcat.columns = ['wsum' ,'p', 'cur', '-1w' ,'-2w' ]
    print(wdfcat , wsdfcat)
    pre_w = (context.new_pipeline_assets_weight.fillna(0) + context.new_assets_weight.fillna(0) )
    pre_v = 0.25*(pre_w.fillna(0)+context.last_2w_weight.fillna(0) + context.last_1w_weight.fillna(0) )
    print("checkdate='%s'" % (get_datetime().strftime('%Y-%m-%d')))
    print('cosportfolio= {')
    for k in wsdfcat.index:
        kstrp = str(k).replace("[","|").replace("]","|").split('|')[1]
        print("'%s':%s," % ( kstrp, wsdfcat.loc[k,'p'].round(3) if DAY_HACK_ENABLED else wsdfcat.loc[k,'wsum'].round(3)))
    print('}')



def get_band_lev(x, uband, lband, cur_lev) : #20190304
    print('calc bb begin', cur_lev, x, uband, lband)
    band_sign = 1 if uband > lband else -1
    x, uband, lband = x * band_sign, uband * band_sign, lband * band_sign
    updated_lev = cur_lev
    if x >= uband:
        updated_lev = band_sign#        cur_lev = band_sign  20190301
    elif x < lband:
        updated_lev = 0
        print('need to set zero', cur_lev)
    print('calc bb final', updated_lev, x, uband, lband)
    return updated_lev

def get_comp_bband(context, data, bb_shrink, bb_buf_zone) :
    cur_px = data.current(context.pipeline_data.index , 'close' )
    dh = data.history(context.pipeline_data.index, fields="close", bar_count=3, frequency="1d")
    dhl = data.history(context.pipeline_data.index, fields="price", bar_count=2, frequency="1d")
    print('cur_px', cur_px)
    print('\nt-1', dhl.iloc[-1,:])
    print('\nt-2', dh.iloc[-2,:])
    mid = context.bbmid_old
    vol = context.bbvol_old
    pre_cls = dh.iloc[-1,:] if DAY_HACK_ENABLED else dh.iloc[-2, :] #jz hack
    print('\npre_cls',pre_cls)
    up2 = mid + vol*bb_shrink# context. pipeline data(' upper' .1 *le SIERT
    lo2 = mid - vol*bb_shrink#context. pipeline data(' lower.] *le SWAY
    up1 = mid + vol*bb_buf_zone
    lo1 = mid - vol*bb_buf_zone
    for i in context.pipeline_data.index:
        print('jzLONG_ONLY',eval(cp.get(u_sect,'long_only')))
        print('bb:', i, pre_cls[i], up2[i], up1[i], mid[i], lo1[i], lo2[i])
        if False: #SEC_ENABLED:  20190226 for 'eb'
            context.comp_levs.loc[i,'up2'] = get_band_lev(pre_cls[i],up2[i],mid[i],context.comp_levs.loc[i,'up2'])
            context.comp_levs.loc[i,'up1'] = get_band_lev(pre_cls[i],mid[i],lo2[i],context.comp_levs.loc[i,'up1'])
            context.comp_levs.loc[i,'lo1'] = 0
            context.comp_levs.loc[i,'lo2'] = 0
        elif LONG_ONLY:
            print('JZMB')
            context.comp_levs.loc[i,'up2'] = get_band_lev(pre_cls[i], up2[i], mid[i], context.comp_levs.loc[i, 'up2' ] )
            context.comp_levs.loc[i,'up1'] = get_band_lev(pre_cls[i], mid[i], lo1[i], context.comp_levs.loc[i, 'up1' ] )#20190227 for mb
            context.comp_levs.loc[i,'lo1'] = 0
            context.comp_levs.loc[i,'lo2'] = 0
        else:
            print("JZEB:",bb_buf_zone)
            context.comp_levs.loc[i,'up2'] = get_band_lev(pre_cls[i],up2[i],up1[i],context.comp_levs.loc[i,'up2'])
            context.comp_levs.loc[i,'up1'] = get_band_lev(pre_cls[i],up1[i],mid[i],context.comp_levs.loc[i,'up1'])
            context.comp_levs.loc[i,'lo1'] = get_band_lev(pre_cls[i],lo1[i],mid[i],context.comp_levs.loc[i,'lo1'])
            context.comp_levs.loc[i,'lo2'] = get_band_lev(pre_cls[i],lo2[i],lo1[i],context.comp_levs.loc[i,'lo2'])
    comp_levs = context.comp_levs.sum (axis=1)
    return comp_levs#context. cowp basic lev 1- context. camp double lev

def get_dma(context,data,nfast,nslow):
    print('DAY_HACK_ENABLED',DAY_HACK_ENABLED)
    offset = 0 if DAY_HACK_ENABLED else 1
    dh = data.history(context.pipeline_data.index, fields='price', bar_count=nslow+offset, frequency='1d')
    cur_px = data.current(context.pipeline_data.index,'close') if DAY_HACK_ENABLED else dh.iloc[-1-offset,:]
    startdate = -offset-nfast
    f_mavg = dh.iloc[startdate:,:] if DAY_HACK_ENABLED else dh.iloc[startdate:-1,:]
    startdate = -offset-nslow
    s_mavg = dh.iloc[startdate:,:] if DAY_HACK_ENABLED else dh.iloc[startdate:-1,:]
    print('fma/sma/curpx',f_mavg,s_mavg)#TF
    print('mamean',f_mavg.mean()[0],s_mavg.mean()[0],cur_px)#TF
    dma_up = 1*((f_mavg.mean() >= s_mavg.mean()) & (cur_px >= s_mavg.mean())) 
    dma_dn = -1*((f_mavg.mean() < s_mavg.mean()) & (cur_px <  s_mavg.mean())) 
    print('dmaup/dn:',dma_up[0],dma_dn[0])
    return dma_up + dma_dn

def get_adj_w(a, w, symb_list,context, m='mb',nd=18):
    flag0 = m in ('mb',)  and a in symb_list#
    flag1  = context.bb_day_count_df.loc[a,'nd'] > nd# int(current_date.strftime('%m')) < 7
    if flag1 and a == symbol('CB.PI') : print('jznd', a, w, flag0, context.bb_day_count_df.loc[a,'nd'] ,15)
    vol_threshold = 0.003 if a == symbol('BD.PI') else -1
    flag2 =   context.pipeline_data.loc[a,'volatility'] < vol_threshold

    if flag0 and flag2 : print('jfuz',vol_threshold,context.pipeline_data.loc[a,'volatility']  )
    ndzero_threshold = 0 if a == symbol('BD.PI') else 8
    flag3 =  context.bb_day_count_df.loc[a,'ndzero'] > ndzero_threshold #CB: 10=0.58; 20=0.6
    chipinfactor =  (min(16, context.bb_day_count_df.loc[a,'ndzero']))/16#new debug
    if a in symb_list: print('flag3', context.bb_day_count_df.loc[a,'ndzero'])
    mbflag =  flag0 and ( flag3 or  flag2 )#or flag1)
    if mbflag : print('mbflag=',a,'chipinfactor=',chipinfactor, context.bb_day_count_df.loc[a,'nd'] ,w)
    if mbflag and w > 1 and flag1: print('xyd')
    myvol =  context.pipeline_data.loc[a,'volatility']
    adj_w =  MB_HACK*chipinfactor if mbflag else (1 if flag0 and w > 1 and flag1 else w)#(np.sign(w) if a == symbol('BD.PI') else w)# and w*context. last lw weight (asset) 0 else w/2
    return adj_w

