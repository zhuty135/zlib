#!/usr/local/anaconda3/bin/python3.9
##!/usr/bin/python3
import sys
import pwd
import os
import re
uname = pwd.getpwuid(os.getuid()).pw_name

import pandas as pd
import numpy as np
import math
from empyrical import sharpe_ratio, annual_return,max_drawdown, cum_returns, annual_volatility 
from zutils_p39 import get_business_date_list, get_prev_business_date
from datetime import datetime,date, timedelta


def get_max_dd(cumret):
    dd = (np.maximum.accumulate(cumret) - cumret)/np.maximum.accumulate(cumret)
    return np.max(dd)

def get_sharpe(returns, rf=0, days=252):
    volatility = returns.std()
    sharpe_ratio = (returns.mean() - rf) / volatility * np.sqrt(days)
    return sharpe_ratio

def result_stats(perf,verbose=False):
    if isinstance(perf,str):
        perf = pd.read_pickle(perf)
        
    prets = perf['returns']
    asr = sharpe_ratio(returns = prets)
    aret = annual_return( returns = prets, period = 'daily')
    avol = annual_volatility( returns = prets, period = 'daily')
    maxdd  = max_drawdown(prets)#perf['max_drawdown']       
    txns = perf['weight']#perf['transactions']
    tdf = pd.DataFrame() 
    for index, value in txns.items():
        #if verbose: print(index,value)
        if isinstance(value,dict):
            for k,v in value.items():
                if verbose == 2: print(k,v)
                tdf = tdf.append(pd.DataFrame({'icker':[k],'dt':[index],'weight':[v]}))

    #tdf.set_index('dt',inplace=True)
    #tdf.sort_index(inplace=True)
    num_of_txns = 0
    if not tdf.empty :
        tdf.sort_values(by=['dt'],inplace=True)
        tdf.reset_index(inplace=True)
        #tdf.to_csv('/tmp/tdf.csv')
        a = np.sign(tdf['weight'])
        num_of_txns = len(np.where(np.diff(np.sign(a)))[0])

    #num_of_txns = perf['transactions'].size
    if verbose:
        print('asr',asr)
        print('aret',aret)
        print('avol',avol)
        print('maxdd',maxdd)#,get_max_dd(perf['portfolio_value']))
        print('num_of_txns',num_of_txns)
    return asr, aret, avol,maxdd, num_of_txns 

def prob(x):
    return np.sum(x>0)/len(x)

def fake_data(df,f):
    #if df.shape[1] < 2:

    df.columns = ['open']
    df["high"] = df.iloc[:,0]
    df["low"]  = df.iloc[:,0]
    df["close"] = df.iloc[:,0]
    df["volume"] = np.sign(df.iloc[:,0])*1e9
    df["adjusted"] = df.iloc[:,0]
    dt_fmt='%Y/%m/%d'

    sd = df.index[0].strftime(dt_fmt)
    #ed = df.index[-1].strftime(dt_fmt)
    ed = date.today().strftime(dt_fmt)
    bd_list = get_business_date_list(fmt=dt_fmt)
    print(sd,ed,type(bd_list))
    short_bd_list = pd.to_datetime(bd_list[(bd_list >= sd) & (bd_list <= ed)])
    newdf = df.copy(deep=True)
    #print('newdf\n',newdf)
    ''' 
    newdf = newdf.reindex(short_bd_list).ffill(limit=10)
    df = newdf
    ''' 
    newdf = newdf.reindex(short_bd_list)
    df = newdf#hack 20220605 df.append(newdf)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    if f in ['PMI','PMI_NewOrder','PMI_GoodsInventory','PMI_MaterialInventory','PMI_Production','PMI_NewExportOrder']:
        df.ffill(inplace=True)
    else:
        df.ffill(limit=3,inplace=True)
        df.bfill(limit=3,inplace=True)
    

    #print('test',df)
    return df

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

def cal_macro():
    ofile_list = ['M1ADVPPI','ODSCHG','PMI','PMI_NewOrder','PMI_GoodsInventory','PMI_MaterialInventory','PMI_Production','PMI_NewExportOrder','EDMCHG','MGPCHG', 'DEBTCHG_YEAR','PPI_CHG','SHIBOR3M', 'CREDITCURVE','YIELDCURVE','M2_CHG','ADDVALUE_CHG','USCNYIELD','DEBTCHG_F','TFTPA.PO','M1M2_CHG','CPI_PPI_CHG', 'PPI_CHG3','M1M2_CHG3','ODSCHG3','CONSUMER_CHG','M1_PPI_CHG'] 
    columns = ['PMI','PMI_Production','PMI_NewOrder','PMI_NewExportOrder','PMI_GoodsInventory','PMI_MaterialInventory','M1','M2','CPI','PPI','RMBloan','Industrial_added_value','SHIBOR3M',]

    rpath = '/work/' + uname + '/data/raw/'
    rfile = rpath + 'macroraw.csv'
    df = pd.read_csv(rfile,index_col = 0,parse_dates=True)
    lastdt = df.index[-1]
    lastdt = last_day_of_month(lastdt +  timedelta(days=7))
    df.loc[lastdt] = df.iloc[-1]

    for f in ofile_list:
        ofile = '/work/' + uname + '/output/macro/'+ f + '.csv'
        df_ratio = None
        df_dif = None
        if f in ['ODSCHG',]:
            df_ratio = df['PMI_NewOrder'][:-1]/df['PMI_GoodsInventory'][:-1]
            df_dif = pd.DataFrame(df_ratio.diff())
        elif f in ['PMI_GoodsInventory','PMI_NewOrder','PMI_MaterialInventory','PMI_Production','PMI_NewExportOrder']:
            df_ratio = df[f]
            df_dif = pd.DataFrame(df_ratio)
        elif f in ['M1ADVPPI',]:
            df_ratio = df['M1'][:-1] - df['PPI'][:-1] - df['Industrial_added_value'][:-1]
            df_dif = pd.DataFrame(df_ratio.diff(periods=12))
        elif f in ['EDMCHG',]:
            df_ratio = df['PMI_NewExportOrder'][:-1]/df['PMI_MaterialInventory'][:-1]
            df_dif = pd.DataFrame(df_ratio.diff())
        elif f in ['MGPCHG',]:
            #df_ratio = df['PMI_MaterialInventory'][:-1]/df['PMI_NewOrder'][:-1]
            df_ratio = df['PMI_MaterialInventory'][:-1]/df['PMI_GoodsInventory'][:-1]
            #df_ratio = df['PMI_MaterialInventory'][:-1]/df['PMI_Production'][:-1]
            #PMI,PMI_Production,PMI_NewOrder,PMI_NewExportOrder,PMI_GoodsInventory,PMI_MaterialInventory,
            df_dif = pd.DataFrame(df_ratio.diff())
        elif f in ['ADDVALUE_CHG',]:
            df_ratio = df['Industrial_added_value'].shift(1)
            df_dif = pd.DataFrame(df_ratio.diff(periods=12))
        elif f in ['DEBTCHG_YEAR',]:
            df_ratio = df['RMBloan'].shift(1)
            df_dif = pd.DataFrame(df_ratio.diff(periods=12))
        elif f in ['M1M2_CHG',]:
            df_ratio = df['M1'].shift(1) - df['M2'].shift(1) 
            df_dif = pd.DataFrame(df_ratio.diff(periods=1))
        elif f in ['M1_PPI_CHG',]:
            df_ratio = df['M1'].shift(1) - df['PPI'].shift(1) 
            df_dif = pd.DataFrame(df_ratio.diff(periods=1))
        elif f in ['M1M2_CHG3',]:
            df_ratio = df['M1'].shift(1)
            df_dif = pd.DataFrame(df_ratio.diff(periods=12))
        elif f in ['CPI_PPI_CHG',]:
            df_ratio = df['CPI'].shift(1) - df['PPI'].shift(1) 
            df_dif = pd.DataFrame(df_ratio.diff(periods=1))

        elif f in ['SHIBOR3M',]:
            sfile = rpath + f + '.csv' 
            sdf = pd.read_csv(sfile,index_col = 0,parse_dates=True)
            sdf_ratio = sdf['SHIBOR3M']
            df_dif = pd.DataFrame(sdf_ratio)
        else:
            continue


        odf = fake_data(df_dif,f)
        odf.index.rename('date',inplace=True)
        print(odf)
        if eval(os.environ['OUTPUTFLAG']):
            odf.to_csv(ofile,date_format='%Y/%m/%d')
            print('Next step is: cp ' + ofile +  ' /work/jzhu/project/ql/data/')
        else:
            print('no file output')

def get_tickers():
    if os.environ['ASSETTYPE'] == 'cfpa':
        #tickers = ['CFCUPA.PO','CFAUPA.PO','CFMAPA.PO','CFRUPA.PO','CFIPA.PO','CFAGPA.PO','CFNIPA.PO','CFYPA.PO', 'CFPPPA.PO','CFPBPA.PO','CFSRPA.PO','CFTAPA.PO','CFMPA.PO','CFCPA.PO','CFRBPA.PO', 'CFCFPA.PO','CFJDPA.PO','CFALPA.PO','CFZCPA.PO','CFZNPA.PO','CFPPA.PO','CFOIPA.PO', 'CFLPA.PO','CFAPA.PO','CFVPA.PO','CFJPA.PO','CFJMPA.PO','CFFGPA.PO','TFTFPA.PO','TFTSPA.PO','TFTPA.PO']
        tickers = ['CFSMPA.PO','CFFUPA.PO','CFCUPA.PO', 'CFAUPA.PO','CFMAPA.PO', 'CFRUPA.PO', 'CFIPA.PO','CFAGPA.PO', 'CFSRPA.PO','CFTAPA.PO','CFMPA.PO','CFCPA.PO', 'CFRBPA.PO','CFCFPA.PO','CFJDPA.PO','CFNIPA.PO','CFYPA.PO','CFALPA.PO', 'CFPBPA.PO', 'CFPPPA.PO', 'CFZCPA.PO', 'CFAPA.PO','CFFGPA.PO','CFLPA.PO','CFOIPA.PO','CFPPA.PO','CFJPA.PO','CFBUPA.PO','CFSNPA.PO', 'CFJMPA.PO','CFCSPA.PO','CFHCPA.PO', 'CFRMPA.PO','CFZNPA.PO','CFVPA.PO','CFAPPA.PO','CFSCPA.PO','CFCICA.PO', 'CFPMSA.PO', 'CFFMSA.PO', 'CYNMSA.PO','CYNHSA.PO', 'CFOPSA.PO', 'CYYLSA.PO', 'CFSCSA.PO','CFCGSA.PO','TFTFPA.PO','TFTSPA.PO','TFTPA.PO','IFIFPA.PO','IFICPA.PO','IFIHPA.PO',]
    elif os.environ['ASSETTYPE'] == 'spgs' :
        tickers = ['SPGSAG.TR',  'SPGSCL.TR',  'SPGSFC.TR',  'SPGSHU.TR',  'SPGSIL.TR',  'SPGSKW.TR',  'SPGSLV.TR',  'SPGSRE.TR',  'SPGSSO.TR', 'SPGSBR.TR',  'SPGSCN.TR',  'SPGSGC.TR',  'SPGSIA.TR',  'SPGSIN.TR',  'SPGSLC.TR',  'SPGSNG.TR',  'SPGSSB.TR',  'SPGSWH.TR', 'SPGSCC.TR',  'SPGSCT.TR',  'SPGSGO.TR',  'SPGSIC.TR',  'SPGSIZ.TR',  'SPGSLE.TR',  'SPGSPM.TR',  'SPGSSF.TR', 'SPGSCI.TR',  'SPGSEN.TR',  'SPGSHO.TR',  'SPGSIK.TR',  'SPGSKC.TR',  'SPGSLH.TR',  'SPGSPT.TR',  'SPGSSI.TR',]
    elif os.environ['ASSETTYPE'] == 'iv' :
        tickers = ['510050_iv_1m1000.PO','510300_iv_1m1000.PO','sr_iv_1m1000.PO','m_iv_1m1000.PO','c_iv_1m1000.PO','cf_iv_1m1000.PO','cu_iv_1m1000.PO','ma_iv_1m1000.PO','al_iv_1m1000.PO','zc_iv_1m1000.PO','zn_iv_1m1000.PO','ta_iv_1m1000.PO','v_iv_1m1000.PO','pp_iv_1m1000.PO','ru_iv_1m1000.PO','l_iv_1m1000.PO','rm_iv_1m1000.PO','i_iv_1m1000.PO', 
                    '510050_iv_6m1000.PO','510300_iv_6m1000.PO','sr_iv_6m1000.PO','m_iv_6m1000.PO','c_iv_6m1000.PO','cf_iv_6m1000.PO','cu_iv_6m1000.PO','ma_iv_6m1000.PO','al_iv_6m1000.PO','zc_iv_6m1000.PO','zn_iv_6m1000.PO','ta_iv_6m1000.PO','v_iv_6m1000.PO','pp_iv_6m1000.PO','ru_iv_6m1000.PO','l_iv_6m1000.PO','rm_iv_6m1000.PO','i_iv_6m1000.PO',]
    elif os.environ['ASSETTYPE'] == 'dig' :
        tickers = ['USO.P','UUP.P','TLT.O','VIG.P', 'VBR.P','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EWY.P','VXX.BAT','KWEB.P','ARKK.P', 'XLB.P', 'XLC.P', 'XLI.P', 'XLE.P','XLF.P','XLP.P', 'XLU.P','XLV.P','XLY.P','EFA.P','EEM.P','IYR.P','SPY.P','LIT.P','TAN.P','SNSR.O','BOTZ.O','IWF.P','IWM.P','SKYY.O','HYG.P','GSG.P','EWU.P','EWQ.P','EWG.P','EWJ.P','EWS.P','EWA.P','EWZ.P','FXY.P','FXE.P','FXB.P', 'THD.P','TBT.P', ] # 'VNM.P'
    elif os.environ['ASSETTYPE'] == 'idxetf' :
        #tickers = ['VIX.GI','USO.P','USDCNH.FX','XLK.P','SPGSCL.TR','UUP.P','IBOVESPA.GI','N225.GI','NDX.GI','HSI.HI','TLT.O','VIG.P', 'VBR.P','SOX.GI','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EURUSD.FX','INDA.BAT','AS51.GI','STI.GI','EWY.P']
        tickers = ['VIX.GI','USO.P','USDCNH.FX','SPGSCL.TR','UUP.P','IBOVESPA.GI','N225.GI','NDX.GI','HSI.HI','TLT.O','VIG.P', 'VBR.P','SOX.GI','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EURUSD.FX','INDA.BAT','AS51.GI','STI.GI','EWY.P','VXX.BAT','KWEB.P','ARKK.P','ARKG.P','GDAXI.GI', 'XLB.P', 'XLC.P', 'XLI.P', 'XLE.P','XLF.P','XLP.P', 'XLU.P','XLV.P','XLY.P','EFA.P','EEM.P','IYR.P','SPY.P','LIT.P','TAN.P','SNSR.O','BOTZ.O','IWF.P','IWM.P','FTSE.GI','SKYY.O','HYG.P','GSG.P','FCHI.GI','VNINDEX.GI','SETI.GI','EWU.P','EWQ.P','EWG.P','EWJ.P','EWS.P','EWA.P','EWZ.P','FXY.P','FXE.P','FXB.P','VNM.P','THD.P','TBT.P', ] 
    elif os.environ['ASSETTYPE'] == 'shsz':
        tickers = ['000016.SH','000905.SH', '000300.SH']#'399300.SZ',
    elif os.environ['ASSETTYPE'] == 'nh':
        tickers = [ 'NH0001.NHF', 'NH0017.NHF', 'NH0016.NHF', 'NH0015.NHF', 'NH0014.NHF', 'NH0013.NHF', 'NH0012.NHF', 'NH0011.NHF', 'NH0010.NHF', 'NH0009.NHF', 'NH0008.NHF', 'NH0007.NHF', 'NH0006.NHF', 'NH0005.NHF', 'NH0004.NHF', 'NH0003.NHF', 'NH0002.NHF', 'NH0035.NHF', 'NH0034.NHF', 'NH0033.NHF', 'NH0032.NHF', 'NH0031.NHF', 'NH0030.NHF', 'NH0029.NHF', 'NH0028.NHF', 'NH0027.NHF', 'NH0026.NHF', 'NH0025.NHF', 'NH0024.NHF', 'NH0023.NHF', 'NH0022.NHF', 'NH0021.NHF', 'NH0020.NHF', 'NH0019.NHF', 'NH0018.NHF', 'NH0055.NHF', 'NH0054.NHF', 'NH0053.NHF', 'NH0052.NHF', 'NH0051.NHF', 'NH0050.NHF', 'NH0049.NHF', 'NH0048.NHF', 'NH0047.NHF', 'NH0046.NHF', 'NH0045.NHF', 'NH0044.NHF', 'NH0043.NHF', 'NH0042.NHF', 'NH0041.NHF', 'NH0040.NHF', 'NH0039.NHF', 'NH0038.NHF', 'NH0037.NHF', 'NH0036.NHF', 'NHSN.NHF', 'NHSM.NHF', 'NHSF.NHF', 'NHNI.NHF', 'NHLR.NHF', 'NHCS.NHF', 'NH0800.NHF', 'NH0700.NHF', 'NH0600.NHF', 'NH0500.NHF', 'NH0400.NHF', 'NH0300.NHF', 'NH0200.NHF', 'NH0100.NHF', 'NH0057.NHF', 'NH0056.NHF', 'W00109SPT.NM',]
    else:
        tickers = []
        tickers.append(os.environ['ASSETTYPE'] )
    return tickers 

def cal_prob():
    tickers = get_tickers() 

    agg_df = None#pd.DataFrame()

    for ticker in tickers:
        ipath = '/work/' + uname + '/data/pol/work/jzhu/input/'
        if re.match(r'.*\.TR$',ticker):
            ipath += 'global/'
        elif re.match(r'.*\.NHF$',ticker) or  re.match(r'.*\.NM$',ticker) :
            ipath += 'nh/'
        elif re.match(r'.*\.GI$',ticker) or  re.match(r'.*\.P$',ticker) or  re.match(r'.*\.HI$',ticker) or  re.match(r'.*\.O$',ticker) or  re.match(r'.*\.FX$',ticker):
            ipath += 'idxetf/'
        elif re.match(r'.*iv.*\.PO$',ticker):
            ipath += 'iv/'
        else:
            ipath = '/work/' + uname + '/data/pol/'
            ipath += 'Index/'#'shared/spgs/'
        print(ipath)

        data = pd.read_csv(ipath + ticker + '.csv')###bond index
        data.columns=[name.lower() for name in list(data.columns)]
        data['DATETIME'] = data['date'].apply(pd.to_datetime)
        
        data.index = data['date'].apply(pd.to_datetime)
        #data = data[data.index<=pd.to_datetime('2018/4/1')]
        #data['close'].plot()
        ##calendar dates
        data['weekday'] = data['DATETIME'].apply(lambda x:x.weekday())+1
        data['day'] = data['DATETIME'].apply(lambda x:x.day)
        data['month'] = data['DATETIME'].apply(lambda x:x.month)
        data['year'] = data['DATETIME'].apply(lambda x:x.year)
        data = data[data['weekday'].isin([1,2,3,4,5])]
        data['chg'] = data['close']/data['close'].shift(1)-1.
        data['chg'].iat[0]=0.
        dayall = data[['year','month','chg']].groupby(['year','month']).std()
        dayall = dayall.reset_index()
        dayall['std_chg'] = dayall['chg']- dayall['chg'].shift(1)
        dayall['std_chg'].iat[0]=0.
        ##
        monthbegin = data.groupby(['year','month']).head(1)
        monthend = data.groupby(['year','month']).tail(1)
        
        
        monthall = monthend.copy()
        monthall['month_chg']=np.array(monthend['close'])/np.array(monthend['close'].shift(1))-1.
        monthall['month_chg'].iloc[0]= monthend['close'].iloc[0]/monthbegin['close'].iloc[0]-1.
        #
        monthall_1 = monthbegin.copy()
        monthall_1['month_chg_1']=np.array(monthbegin['close'].shift(-1))/np.array(monthbegin['close'])-1.
        monthall_1['month_chg_1'].iloc[-1]=monthend['close'].iloc[-1]/monthbegin['close'].iloc[-1]-1.
    
    
        output = np.round(monthall[['month','month_chg']].groupby(['month']).median(),2)
        output.columns=['median']
        output['prob'] = np.round(monthall[['month','month_chg']].groupby(['month']).apply(prob)['month_chg'],2)
        output['std']= np.round(monthall[['month','month_chg']].groupby(['month']).std(),2)
        
        ##std change probability
        output['std_prob'] = np.round(dayall[['month','std_chg']].groupby(['month']).apply(prob)['std_chg'],2)
    
        output['median_begin']=np.round(monthall_1[['month','month_chg_1']].groupby(['month']).median(),2)
        output['prob_begin'] = np.round(monthall_1[['month','month_chg_1']].groupby(['month']).apply(prob)['month_chg_1'],2)
        output['std_begin']= np.round(monthall_1[['month','month_chg_1']].groupby(['month']).std(),2)
        if eval(os.environ['OUTPUTFLAG']):
            output.to_csv('/work/jzhu/output/cal/calendar_'+ticker + '.csv' + os.environ['DERIVED'])
        else:
            print(ticker,'\n',output)
        if agg_df is None:
            agg_df = output['std']
        else:
            agg_df = pd.concat([agg_df,output['std']],axis=1)
    agg_df.columns = tickers
    if True:#eval(os.environ['OUTPUTFLAG']):
        agg_df.to_csv('/work/jzhu/output/cal/calendar_agg.csv')
    print(agg_df)
    print(agg_df.mean(axis=1))
    print(agg_df.mean(axis=0))

import talib
def convert_to_w(df):
    print(df['date'])
    df['date'] = pd.to_datetime(df['date'])

    #df['Week_Number'] = df['date'].dt.week
    df['Week_Number'] = df['date'].dt.isocalendar().week#[0]
    #df['Year'] = df['date'].dt.year
    df['Year'] = df['date'].dt.isocalendar().year#[0]


    df2 = df.groupby(['Year','Week_Number']).agg({'open':'first', 'high':'max', 'low':'min', 'close':'last','volume':'sum'})

    if True:
        df2.to_csv('/tmp/Weekly_OHLC.csv')
    return(df2)

def get_csv_data(ticker, wflag):
    dw = None
    ipath = None
    if True:
        ipath = '/work/' + uname + '/data/pol/work/jzhu/input/'
        if re.match(r'.*\.TR$',ticker):
            ipath += 'global/'
        elif re.match(r'.*\.NHF$',ticker) or re.match(r'.*\.NM$',ticker) :
            ipath += 'nh/'
        elif re.match(r'.*\.PO$',ticker):
            ipath = '/work/' + uname + '/data/pol/'
            if  re.match(r'.*iv.*\.PO$',ticker):
                ipath += 'work/jzhu/input/iv/'
            else:
                ipath += 'Index/'
        elif re.match(r'.*\.SH$',ticker) or re.match(r'.*\.SZ$',ticker):
            #ipath = '/work/jzhu/project/ql/data/'
            ipath = '/work/jzhu/data/pol/work/jzhu/input/moredata/'
        else:
            ipath += 'idxetf/'
        ifile = ipath + ticker + '.csv'
        print('input',ifile)
        df = pd.read_csv(ifile)#,index_col = 0,parse_dates=True)
        df.columns=[name.lower() for name in list(df.columns)]
        if os.environ['DERIVED'] in ['iv30','gex','dpi']  :
            df['open'] = df[  os.environ['DERIVED']  ] 
            df['high'] = df[  os.environ['DERIVED']  ] 
            df['low'] = df[  os.environ['DERIVED']  ] 
            df['close'] = df[  os.environ['DERIVED']  ] 
        #print('jzzz',df['close'].values)
        #assert(0)

        if wflag:
            dw = convert_to_w(df)
            dw.dropna(inplace=True)
            
        else:
            dw = df

    return dw 

def cal_bb(wflag=True):
    tickers = get_tickers()
    for ticker in tickers:
        tp = eval(os.environ['BWLEN'])
        dw = get_csv_data(ticker,wflag)
        #print('jzzz',dw)#,dw['close'].values)
        #assert(0)
        dw['BolU'], dw['BolM'], dw['BolL'] = talib.BBANDS(
            np.double(dw['close'].values),
            timeperiod=tp,
            nbdevup=1,
            nbdevdn=1,
            matype=0)

        if eval(os.environ['OUTPUTFLAG']):
            opath = '/work/jzhu/output/cal/bw' + os.environ['BWLEN'] + '_' +ticker +'.csv'
            print('output to:', opath)
            dw.to_csv(opath)
        else:
            print(ticker,'\n')

def cal_kdj(wflag=True):
    tickers = get_tickers()
    for ticker in tickers:
        dw = get_csv_data(ticker,wflag)
        #print(type(dw['high'].values))

        dw['k'], dw['d'] = talib.STOCH(
            np.double(dw['high'].values), 
            np.double(dw['low'].values), 
            np.double(dw['close'].values),
            fastk_period=9,
            slowk_period=3,
            slowk_matype=0,
            slowd_period=3,
            slowd_matype=0)
        dw['j'] = 3*dw['k']-2*dw['d']
        if eval(os.environ['OUTPUTFLAG']):
            opath = '/work/jzhu/output/cal/jw_'+ticker +'.csv' +  os.environ['DERIVED']
            print('output to:', opath)
            dw.to_csv(opath)
        else:
            print(ticker,'\n')
            print(dw['k'],dw['d'])
            print(dw['j'])
    
def main():
    import getopt, sys
    try:
        opts, args = getopt.getopt(sys.argv[1:],"d:m:p:t:l:ov",["mode=", "help"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)
    verbose = False
    runmode = 'cal_prob'
    params = '()' 
    os.environ['ASSETTYPE'] = 'cfpa'
    os.environ['OUTPUTFLAG'] = 'False'
    os.environ['BWLEN'] = '20' 
    os.environ['DERIVED']  = ''
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-m","--mode"):
            runmode = a 
        elif o in ("-o"):
            os.environ['OUTPUTFLAG'] = 'True'
        elif o in ("-p"):
            params = a
        elif o in ("-t"):
            os.environ['ASSETTYPE'] = a
        elif o in ("-l"):
            os.environ['BWLEN'] = a
        elif o in ("-d"):
            os.environ['DERIVED']  = a

    func = runmode + params 
    if runmode in ('result_stats'):
        func = runmode + "('" +  params + "',verbose=" + str(verbose) +')'

    if verbose: print('running function:',func)
    eval(func)

if __name__ == '__main__':
   main() 
