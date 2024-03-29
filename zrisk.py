#!/usr/local/anaconda3/bin/python3.9
import xlrd

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

pz_code= {'NH0100.NHF':'NH0100.NHF','a':'NH0001.NHF', 'y':'NH0002.NHF', 'c':'NH0003.NHF', 'l':'NH0004.NHF', 'cf':'NH0006.NHF', 'sr':'NH0007.NHF', 'au':'NH0008.NHF', 'zn':'NH0009.NHF', 'fu':'NH0010.NHF', 'ta':'NH0011.NHF', 'cu':'NH0012.NHF', 'al':'NH0013.NHF', 'ru':'NH0014.NHF', 'm':'NH0015.NHF', 'rb':'NH0016.NHF', 'wr':'NH0017.NHF', 'v':'NH0019.NHF', 'oi':'NH0020.NHF', 'p':'NH0021.NHF', 'k':'NH0022.NHF', 'pb':'NH0023.NHF', 'ma':'NH0024.NHF', 'ag':'NH0025.NHF', 'fg':'NH0026.NHF', 'rs':'NH0027.NHF', 'rm':'NH0028.NHF', 'jm':'NH0029.NHF', 'zc':'NH0030.NHF', 'bu':'NH0031.NHF', 'i':'NH0032.NHF', 'jd':'NH0033.NHF', 'pp':'NH0037.NHF', 'b':'NH0047.NHF', 'pg':'NH0052.NHF', 'cs':'NHCS.NHF','sc':'NH0041.NHF'}
def lgk(res):
    landa = 0.9049
    N = 30

    for i in range(31,len(res)):
        d = res.index[i]

        vega_2 = np.log(res.open.iloc[i]/res.close.iloc[i-1])**2+            0.5* np.log(res.high.iloc[i]/res.low.iloc[i])**2 -            0.39 *np.log(res.close.iloc[i]/res.open.iloc[i])**2

        if i== N+1:
            r_bar = np.log(res.close).diff().iloc[:i].mean()
            sig_2 = ((np.log(res.close).diff() - r_bar)**2).iloc[:i].sum()/(N-1)

        else:
            ln_sig_2 = (1-landa) * np.log(res.vega_2.iloc[i-1]) + landa* np.log(res.sig.iloc[i-1]**2)
            sig_2 = np.exp(ln_sig_2)

        res.loc[d,'vega_2'] = vega_2
        res.loc[d,'sig'] = np.sqrt(sig_2)
    output = res.sig*np.sqrt(252)
    print('zlib_lgk', output.iloc[-10:])
    if np.isnan(output.iloc[-10:].sum()):
        print('lgk bad data:', res.close)
        assert(0)
    return(output)


def prob(x):
    return np.sum(x>0)/len(x)

def fake_data(df):
    prmean = df['NAV'].rolling(window=21, center=False).mean().round(3)
    prstd  = df['NAV'].rolling(window=21, center=False).std().round(3)
    upper = (prmean + 1*prstd).round(3)
    lower = (prmean - 1*prstd).round(3)
    print('std',prstd,lower)
    df = pd.concat([prmean, upper, lower, df.round(3)], axis = 1)
    df.columns = ['open','high','low','close']
    df.index =  df.index.strftime("%Y/%m/%d")
    print('df',df)
    df.index.name = 'date'
    '''
    df.columns = ['open']
    df["high"] = df.iloc[:,0]
    df["low"]  = df.iloc[:,0]
    df["close"] = df.iloc[:,0]
    df["volume"] = np.sign(df.iloc[:,0])*1e9
    df["adjusted"] = df.iloc[:,0]

    dt_fmt='%Y/%m/%d'

    sd = df.index[0]#.strftime(dt_fmt)
    #ed = df.index[-1].strftime(dt_fmt)
    ed = date.today().strftime(dt_fmt)
    bd_list = get_business_date_list(fmt=dt_fmt,caltype='NYSE')
    print(sd,ed,(bd_list))
    short_bd_list = pd.to_datetime(bd_list[(bd_list >= sd) & (bd_list <= ed)])
    newdf = df.copy(deep=True)
    #print('newdf\n',newdf)
    newdf = newdf.reindex(short_bd_list).ffill(limit=10)
    df = newdf
    newdf = newdf.reindex(short_bd_list)
    #pd.DataFrame(bd_list).to_csv('/tmp/bd_list.txt')
    df = newdf # df.append(newdf)
    df.sort_index(inplace=True)
    df = df[~df.index.duplicated(keep='first')]
    #20220525 df.ffill(limit=3,inplace=True)
    #20220525 df.bfill(limit=3,inplace=True)
    df.ffill(inplace=True)
    
    '''

    #print('test',df)
    return df

def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + timedelta(days=4)  # this will never fail
    return next_month - timedelta(days=next_month.day)

def cal_pos():
    #ofile_list = ['103_port','102_port','wenjian3_port']
    ofile_list = ['103_port']
    iv_list = ['al','au','c','cf','cu','i','l','m','ma','p','pp','pg','rm','ru','sc','sr','ta','v','zc','zn']

    columns = ['id','market value',]# 
    for f in ofile_list:
        rpath = '/work/' + uname + '/input/yf/risk/'
        enddt = pd.to_datetime(get_prev_business_date(date.today(), -1), utc=True).strftime("%Y%m%d")

        rfile = rpath + f + '_' + enddt + '.csv'
        #df = pd.read_csv(rfile,index_col = 0,parse_dates=True)
        df = pd.read_csv(rfile,header=0,encoding='gbk')
        df = df[df.category=='option']
        df = df[df.id.apply(lambda x: not x.isdigit())]
        df['ticker'] = df.id.apply(lambda x: '-'.join(re.findall('[A-Z]+',x)))
        value_df = df.groupby(['ticker'])[['market value']].sum()
        value_df.loc['call_sum',] = value_df.loc[[x[-1]=='C' for x in value_df.index],'market value'].sum()
        value_df.loc['put_sum',] = value_df.loc[[x[-1]=='P' for x in value_df.index],'market value'].sum()
        value_df.loc['sum',] = value_df.loc['call_sum','market value']+value_df.loc['put_sum','market value']

        #print(df[columns])
        print(value_df)
        if eval(os.environ['OUTPUTFLAG']):
            ofile = '/work/' + uname + '/output/risk/yf_'+ f + '_pos_' + enddt + '.csv'
            value_df.to_csv(ofile,date_format='%Y/%m/%d')
            print('Next step is: cp ' + ofile +  ' /work/jzhu/project/ql/data/')
        else:
            print('no file output')



def cal_risk():
    ofile_list = ['102_dailynav','103_dailynav','wenjian3_dailynav']
    columns = ['NAV',]# date,NAV,return,total return,rolling annualized 20day vol,PLbycatogory-RF,PLbycatogory-bond,PLbycatogory-mutual fund,PLbycatogory-future,PLbycatogory-option,PLbyunderlyingasset-stock,PLbyunderlyingasset-bond,PLbyunderlyingasset-commodity,PLbyunderlyingasset-precious metal,future&option account return,total trade amount,future account asset,occupied margin,available margin,option account asset,option account available fund,option market value,total D/W


    for f in ofile_list:
        rpath = '/work/' + uname + '/input/yf/risk/'
        rfile = rpath + f + '.csv'
        #df = pd.read_csv(rfile,index_col = 0,parse_dates=True)
        df = pd.read_csv(rfile,encoding='gbk')

        df['date']= [ xlrd.xldate_as_datetime(x,0) for x in df['date'] ]
        df = df.set_index('date')
        #res['date'] = pd.to_datetime(res['date'], utc=True)




        ofile = '/work/' + uname + '/output/risk/yf_'+ f + '.csv'
        df_ratio = None
        df_dif = None
        if True: 
            df_ratio = df['NAV']
            print(df_ratio)
            df_dif = pd.DataFrame(df_ratio)
        else:
            continue

        odf = fake_data(df_dif)
        st = date(2000, 1, 8)

        #odf = odf.loc[st:,]
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
        tickers = ['CFSMPA.PO','CFFUPA.PO','CFCUPA.PO', 'CFAUPA.PO','CFMAPA.PO', 'CFRUPA.PO', 'CFIPA.PO','CFAGPA.PO', 'CFSRPA.PO','CFTAPA.PO','CFMPA.PO','CFCPA.PO', 'CFRBPA.PO','CFCFPA.PO','CFJDPA.PO','CFNIPA.PO','CFYPA.PO','CFALPA.PO', 'CFPBPA.PO', 'CFPPPA.PO', 'CFZCPA.PO', 'CFAPA.PO','CFFGPA.PO','CFLPA.PO','CFOIPA.PO','CFPPA.PO','CFJPA.PO','CFBUPA.PO','CFSNPA.PO', 'CFJMPA.PO','CFCSPA.PO','CFHCPA.PO', 'CFRMPA.PO','CFZNPA.PO','CFVPA.PO','CFAPPA.PO','CFSCPA.PO','CFCICA.PO',  'CFFMSA.PO', 'CFPMSA.PO','CYNMSA.PO','CYNHSA.PO', 'CFOPSA.PO', 'CYYLSA.PO', 'CFSCSA.PO','CFCGSA.PO', 'CYOPSA.PO']
    elif os.environ['ASSETTYPE'] == 'ifpa':
        tickers = [ 'IFIFPA.PO','IFICPA.PO','IFIHPA.PO']
    elif os.environ['ASSETTYPE'] == 'spgs' :
        tickers = ['SPGSAG.TR',  'SPGSCL.TR',  'SPGSFC.TR',  'SPGSHU.TR',  'SPGSIL.TR',  'SPGSKW.TR',  'SPGSLV.TR',  'SPGSRE.TR',  'SPGSSO.TR', 'SPGSBR.TR',  'SPGSCN.TR',  'SPGSGC.TR',  'SPGSIA.TR',  'SPGSIN.TR',  'SPGSLC.TR',  'SPGSNG.TR',  'SPGSSB.TR',  'SPGSWH.TR', 'SPGSCC.TR',  'SPGSCT.TR',  'SPGSGO.TR',  'SPGSIC.TR',  'SPGSIZ.TR',  'SPGSLE.TR',  'SPGSPM.TR',  'SPGSSF.TR', 'SPGSCI.TR',  'SPGSEN.TR',  'SPGSHO.TR',  'SPGSIK.TR',  'SPGSKC.TR',  'SPGSLH.TR',  'SPGSPT.TR',  'SPGSSI.TR',]
    elif os.environ['ASSETTYPE'] == 'iv' :
        tickers = ['510050_iv_1m1000.PO','510300_iv_1m1000.PO','sr_iv_1m1000.PO','m_iv_1m1000.PO','c_iv_1m1000.PO','cf_iv_1m1000.PO','cu_iv_1m1000.PO','ma_iv_1m1000.PO','al_iv_1m1000.PO','zc_iv_1m1000.PO','zn_iv_1m1000.PO','ta_iv_1m1000.PO','v_iv_1m1000.PO','pp_iv_1m1000.PO','ru_iv_1m1000.PO','l_iv_1m1000.PO','rm_iv_1m1000.PO','i_iv_1m1000.PO','p_iv_1m1000.PO','pg_iv_1m1000.PO','sc_iv_1m1000.PO', 
                    '510050_iv_6m1000.PO','510300_iv_6m1000.PO','sr_iv_6m1000.PO','m_iv_6m1000.PO','c_iv_6m1000.PO','cf_iv_6m1000.PO','cu_iv_6m1000.PO','ma_iv_6m1000.PO','al_iv_6m1000.PO','zc_iv_6m1000.PO','zn_iv_6m1000.PO','ta_iv_6m1000.PO','v_iv_6m1000.PO','pp_iv_6m1000.PO','ru_iv_6m1000.PO','l_iv_6m1000.PO','rm_iv_6m1000.PO','i_iv_6m1000.PO','p_iv_6m1000.PO','pg_iv_6m1000.PO','sc_iv_6m1000.PO',]
    elif os.environ['ASSETTYPE'] == 'dig' :
        tickers = ['USO.P','UUP.P','TLT.O','VIG.P', 'VBR.P','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EWY.P','VXX.BAT','KWEB.P','ARKK.P', 'XLB.P', 'XLC.P', 'XLI.P', 'XLE.P','XLF.P','XLP.P', 'XLU.P','XLV.P','XLY.P','EFA.P','EEM.P','IYR.P','SPY.P','LIT.P','TAN.P','SNSR.O','BOTZ.O','IWF.P','IWM.P','SKYY.O','HYG.P','GSG.P','EWU.P','EWQ.P','EWG.P','EWJ.P','EWS.P','EWA.P','EWZ.P','FXY.P','FXE.P','FXB.P', 'THD.P','TBT.P', ] # 'VNM.P'
    elif os.environ['ASSETTYPE'] == 'idxetf' :
        #tickers = ['VIX.GI','USO.P','USDCNH.FX','XLK.P','SPGSCL.TR','UUP.P','IBOVESPA.GI','N225.GI','NDX.GI','HSI.HI','TLT.O','VIG.P', 'VBR.P','SOX.GI','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EURUSD.FX','INDA.BAT','AS51.GI','STI.GI','EWY.P']
        tickers = ['VIX.GI','USO.P','USDCNH.FX','SPGSCL.TR','UUP.P','IBOVESPA.GI','N225.GI','NDX.GI','HSI.HI','TLT.O','VIG.P', 'VBR.P','SOX.GI','XT.O','HACK.P','IWN.P','DBA.P','IWD.P','EURUSD.FX','INDA.BAT','AS51.GI','STI.GI','EWY.P','VXX.BAT','KWEB.P','ARKK.P','ARKG.P','GDAXI.GI', 'XLB.P', 'XLC.P', 'XLI.P', 'XLE.P','XLF.P','XLP.P', 'XLU.P','XLV.P','XLY.P','EFA.P','EEM.P','IYR.P','SPY.P','LIT.P','TAN.P','SNSR.O','BOTZ.O','IWF.P','IWM.P','FTSE.GI','SKYY.O','HYG.P','GSG.P','FCHI.GI','VNINDEX.GI','SETI.GI','EWU.P','EWQ.P','EWG.P','EWJ.P','EWS.P','EWA.P','EWZ.P','FXY.P','FXE.P','FXB.P','VNM.P','THD.P','TBT.P', ] 
    elif os.environ['ASSETTYPE'] == 'FX':
        tickers = ['USDX.FX','USDCNH.FX','EURUSD.FX','USDJPY.FX','GBPUSD.FX']
    elif os.environ['ASSETTYPE'] == 'shsz':
        tickers = ['000016.SH','000905.SH','399300.SZ']
    elif os.environ['ASSETTYPE'] == 'tf':
        tickers = ['H00140.SH','H11077.SH',]

    elif os.environ['ASSETTYPE'] == 'nhpa':
        tickers = [ 'NH0001.NHF', 'NH0017.NHF', 'NH0016.NHF', 'NH0015.NHF', 'NH0014.NHF', 'NH0013.NHF', 'NH0012.NHF', 'NH0011.NHF', 'NH0010.NHF', 'NH0009.NHF', 'NH0008.NHF', 'NH0007.NHF', 'NH0006.NHF', 'NH0005.NHF', 'NH0004.NHF', 'NH0003.NHF', 'NH0002.NHF', 'NH0035.NHF', 'NH0034.NHF', 'NH0033.NHF', 'NH0032.NHF', 'NH0031.NHF', 'NH0030.NHF', 'NH0029.NHF', 'NH0028.NHF', 'NH0027.NHF', 'NH0026.NHF', 'NH0025.NHF', 'NH0024.NHF', 'NH0023.NHF', 'NH0022.NHF', 'NH0021.NHF', 'NH0020.NHF', 'NH0019.NHF', 'NH0018.NHF', 'NH0055.NHF', 'NH0054.NHF', 'NH0053.NHF', 'NH0052.NHF', 'NH0051.NHF', 'NH0050.NHF', 'NH0049.NHF', 'NH0048.NHF', 'NH0047.NHF', 'NH0046.NHF', 'NH0045.NHF', 'NH0044.NHF', 'NH0043.NHF', 'NH0042.NHF', 'NH0041.NHF', 'NH0040.NHF', 'NH0039.NHF', 'NH0038.NHF', 'NH0037.NHF', 'NH0036.NHF', 'NHSN.NHF', 'NHSM.NHF', 'NHSF.NHF', 'NHNI.NHF', 'NHLR.NHF', 'NHCS.NHF', ]
    elif os.environ['ASSETTYPE'] == 'nhsa':
        tickers = [ 'NH0800.NHF', 'NH0700.NHF', 'NH0600.NHF', 'NH0500.NHF', 'NH0400.NHF', 'NH0300.NHF', 'NH0200.NHF', 'NH0100.NHF', 'NH0057.NHF', 'NH0056.NHF', 'W00109SPT.NM']
    else:
        tickers = []
        tickers.append(os.environ['ASSETTYPE'] )
    return tickers 



def cal_crv():
    datadict = {}
    ipath = '/work/' + uname + '/data/pol/work/jzhu/input/'
    myassettype =  os.environ['ASSETTYPE'].split('.')[0] #os.environ['ASSETTYPE'][0:2]
    if  myassettype == 'nh' :
        iv_list = ['al','au','c','cf','cu','i','l','m','ma','pp','rm','ru','sr','ta','v','zc','zn','sc','p','pg']
    elif  myassettype  == 'cfsa' :
        iv_list = ['CFPMSA.PO', 'CFFMSA.PO', 'CYNMSA.PO','CYNHSA.PO', 'CFOPSA.PO', 'CYYLSA.PO', 'CFSCSA.PO','CFCGSA.PO']
    elif  myassettype  == 'hz' :
        iv_list = ['000986.SH','000987.SH','000988.SH','000989.SH','000990.SH','000991.SH','000992.SH','000993.SH','000994.SH','000995.SH']
    elif  myassettype  == 'dtta' :
        iv_list = ['NH0100.NHF', 'TFTFPA.PO','000016.SH','399300.SZ','UUP.P']
    elif  myassettype  == 'gtaa' :
        iv_list = ['GSG.P', 'TLT.O','UUP.P','SPY.P','EEM.P','EFA.P','HYG.P','VIX.GI','N225.GI','NDX.GI','BTC.CME','EURUSD.FX', 'IYR.P']
    elif  myassettype  == 'dm' :
        iv_list = ['SPY.P','EFA.P','N225.GI','NDX.GI','GDAXI.GI','FCHI.GI','FTSE.GI']
    elif  myassettype  == 'em' :
        iv_list = ['EEM.P','KS11.GI','HSI.HI','INDA.BAT','AS51.GI', 'IBOVESPA.GI','SETI.GI','VNINDEX.GI']
    elif  myassettype  == 'icln' :
        iv_list = ['LIT.P','TAN.P','SNSR.O','XBI.P','ICLN.O' ] 
    elif  myassettype  == 'xt' :
        iv_list = ['XT.O','HACK.P','ARKK.P', 'GAMR.P','SNSR.O','BOTZ.O','SKYY.O','ESPO.O' ] 

    elif  myassettype  == 'xl' :
        iv_list = [ 'XLB.P', 'XLC.P', 'XLI.P', 'XLE.P','XLF.P','XLP.P', 'XLU.P','XLV.P','XLY.P','XLK.P']
    elif  myassettype  == 'spgs' :
        iv_list = ['SPGSAG.TR',  'SPGSCL.TR',  'SPGSFC.TR',  'SPGSHU.TR',  'SPGSIL.TR',  'SPGSKW.TR',  'SPGSSO.TR', 'SPGSBR.TR',  'SPGSCN.TR',  'SPGSGC.TR',  'SPGSIA.TR',  'SPGSIN.TR',  'SPGSLC.TR',  'SPGSNG.TR',  'SPGSSB.TR',  'SPGSWH.TR', 'SPGSCC.TR',  'SPGSCT.TR',  'SPGSGO.TR',  'SPGSIC.TR',  'SPGSIZ.TR',  'SPGSLE.TR',  'SPGSPM.TR',  'SPGSSF.TR', 'SPGSHO.TR',  'SPGSIK.TR',  'SPGSKC.TR',  'SPGSLH.TR',  'SPGSSI.TR',]#'SPGSCI.TR',  'SPGSEN.TR',  'SPGSPT.TR',  'SPGSRE.TR', 'SPGSLV.TR', 

    for ticker in iv_list :
        if os.environ['ASSETTYPE'].split('.')[1] == 'sect':
            fpath = ipath 
            if re.match(r'.*\.P$',ticker) or  re.match(r'.*\.O$',ticker) or  re.match(r'.*\.GI$',ticker) or  re.match(r'.*\.FX$',ticker)  or  re.match(r'.*\.BAT$',ticker) or  re.match(r'.*\.HI$',ticker) or  re.match(r'.*\.CME$',ticker)  :
                fpath = fpath + 'idxetf/' +  ticker + '.csv'
            elif re.match(r'.*\.SH$',ticker) or re.match(r'.*\.SZ$',ticker):
                fpath = fpath + 'hz/' +  ticker + '.csv'
            elif  re.match(r'.*\.TR$',ticker):
                fpath = fpath + 'global/' +  ticker + '.csv'
            elif re.match(r'.*\.NHF$',ticker) or re.match(r'.*\.NM',ticker):
                fpath = fpath + 'nh/' +  pz_code[ticker] + '.csv'
            else:
                if myassettype == 'nh':
                    fpath = fpath + 'nh/' +  pz_code[ticker] + '.csv'
                else:
                    fpath = "/work/jzhu/data/pol/Index/" +  ticker + '.csv'

            print(fpath)

            data = pd.read_csv(fpath)###bond index
            data.columns=[name.lower() for name in list(data.columns)]
            data.index = data['date'].apply(pd.to_datetime)
            datadict[ticker] = data['close'][-1500:]
            print(ticker,data.iloc[-1,:])

    datadf = pd.DataFrame.from_dict(datadict,orient='columns')

    retdf = np.log(datadf).diff()

    if os.environ['ASSETTYPE'].split('.')[2] == 'cov':
        corsum = np.sqrt(retdf.rolling(21).cov().sum(axis=1))
    elif os.environ['ASSETTYPE'].split('.')[2] == 'corr':
        corrwl = 21
        if myassettype in ('nh','hz','ta','cfsa'):
            corrwl = 63
        corsum = retdf.rolling(corrwl).corr().sum(axis=1)
    else:
        assert(0)

    #testdf = [ corsum.loc[:,iv_list] for i in corsum.index]
    print('realcal')
    for i in corsum.index:
        tmpvalue =  corsum.loc[i,iv_list].sum()
        datadf.loc[i[0],'sum'] = tmpvalue 
    tmpsum  = datadf['sum']

    if True:
        ofile = '/work/' + uname + '/output/ixew/'+ os.environ['ASSETTYPE']  + '.csv'
        if eval(os.environ['OUTPUTFLAG']):
            datadf['ixew']= tmpsum
            odf = pd.DataFrame()
            odf['open']  =  datadf['ixew']
            odf['high']  =  datadf['ixew']
            odf['low']  =  datadf['ixew']
            odf['close']  =  datadf['ixew']
            #odf =  datadf.mean(axis=1)
            odf.to_csv(ofile,date_format='%Y/%m/%d',header=True)
            print('Next step is: cp ' + ofile +  ' /work/jzhu/project/ql/data/')
        else:
            print('no file output')

def output_to_csv(opath,datadf,fld):
    if True:
        ofile = opath
        if eval(os.environ['OUTPUTFLAG']):
            odf = pd.DataFrame()
            odf['open']  = datadf if fld == '' else datadf[fld] 
            odf['high']  = datadf if fld == '' else  datadf[fld] 
            odf['low']   = datadf if fld == '' else datadf[fld] 
            odf['close']  = datadf if fld == '' else datadf[fld] 
            #odf =  datadf.mean(axis=1) 
            odf.to_csv(ofile,date_format='%Y/%m/%d',header=True)
            print('Next step is: cp ' + ofile +  ' /work/jzhu/project/ql/data/')
        else:
            print('no file output')

def cal_csab():
    datacsdict = {}
    rvdict ={}
    fpath = '/work/' + uname + '/data/pol/Index/'
    if  os.environ['ASSETTYPE'][0:2] == 'nh' :
        iv_list =  ['CFSMPA.PO','CFFUPA.PO','CFCUPA.PO', 'CFAUPA.PO','CFMAPA.PO', 'CFRUPA.PO', 'CFIPA.PO','CFAGPA.PO', 'CFSRPA.PO','CFTAPA.PO','CFMPA.PO','CFCPA.PO', 'CFRBPA.PO','CFCFPA.PO','CFJDPA.PO','CFNIPA.PO','CFYPA.PO','CFALPA.PO', 'CFPBPA.PO', 'CFPPPA.PO', 'CFZCPA.PO', 'CFAPA.PO','CFFGPA.PO','CFLPA.PO','CFOIPA.PO','CFPPA.PO','CFJPA.PO','CFBUPA.PO','CFSNPA.PO', 'CFJMPA.PO','CFCSPA.PO','CFHCPA.PO', 'CFRMPA.PO','CFZNPA.PO','CFVPA.PO','CFAPPA.PO','CFSCPA.PO'] #'CFPMSA.PO', 'CFFMSA.PO', 'CYNMSA.PO','CYNHSA.PO', 'CFOPSA.PO', 'CYYLSA.PO', 'CFSCSA.PO','CFCGSA.PO',
    elif  os.environ['ASSETTYPE'][0:2] == 'hz' :
        iv_list =  [ 'IFIFPA.PO','IFICPA.PO','IFIHPA.PO',] 
    elif  os.environ['ASSETTYPE'][0:2] == 'tf' :
        iv_list =  [ 'TFTFPA.PO','TFTSPA.PO','TFTPA.PO'] 
    for i in iv_list: 
        datadict = {}
        if True:
            ipath =  fpath + i + '.csv'
            print(ipath)
            data = pd.read_csv(ipath)
            data.columns=[name.upper() for name in list(data.columns)]
            data.index = data['DATE'].apply(pd.to_datetime)
            datadict[i] = data['CLOSE']

            j = i.replace('A.PO','B.PO')
            ipath =  fpath + j + '.csv'
            print(ipath)
            data = pd.read_csv(ipath)
            data.columns=[name.upper() for name in list(data.columns)]
            data.index = data['DATE'].apply(pd.to_datetime)
            datadict[j] = data['CLOSE']
    
            tmpdf = pd.DataFrame.from_dict(datadict,orient='columns')
            tmpcsdf = tmpdf[j] / tmpdf[i]

            datacsdict[i] = tmpcsdf#datadict[j] - datadict[i] 
            opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.' + i.split('.')[0] + '.csv'
            output_to_csv(opath,tmpcsdf,'')

            rvdict[i] = tmpcsdf.rolling(21).std()
            #rvdict[i] = lgk(tmpcsdf)
            opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.' +  i.split('.')[0] + '.rv.csv'
            output_to_csv(opath,rvdict[i] ,'')
            

    datadf = pd.DataFrame.from_dict(datacsdict,orient='columns')
    datadfmean = datadf.mean(axis=1)
    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.csv'
    output_to_csv(opath,datadfmean,'')

    rvdf = pd.DataFrame.from_dict(rvdict,orient='columns').mean(axis=1)
    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.rv.csv'
    output_to_csv(opath,rvdf,'')

    datadfcorr = datadf.diff().rolling(21).corr().sum(axis=1)
    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.corr.csv'
    output_to_csv(opath,datadfcorr,'')

def cal_hr():
    pathdict = {
                '510300':'input/hz/',
                'VIX.GI':'data/pol/work/jzhu/input/idxetf/',
                'SPY.P':'data/pol/work/jzhu/input/idxetf/',
                'XLB.P':'data/pol/work/jzhu/input/idxetf/',
                'XLC.P':'data/pol/work/jzhu/input/idxetf/',
                'XLE.P':'data/pol/work/jzhu/input/idxetf/',
                'XLF.P':'data/pol/work/jzhu/input/idxetf/',
                'XLI.P':'data/pol/work/jzhu/input/idxetf/',
                'XLK.P':'data/pol/work/jzhu/input/idxetf/',
                'XLY.P':'data/pol/work/jzhu/input/idxetf/',
                'XLV.P':'data/pol/work/jzhu/input/idxetf/',
                'XLP.P':'data/pol/work/jzhu/input/idxetf/',
                'XLU.P':'data/pol/work/jzhu/input/idxetf/',
                'USDX.FX':'data/pol/work/jzhu/input/idxetf/',
                'SPGSGC.TR':'data/pol/work/jzhu/input/global/',
                'hz.iv.1m':'output/ixew/',
                'nh.iv.6m':'output/ixew/',
                'NH0100.NHF':'input/nh/','NH0200.NHF':'input/nh/','NH0300.NHF':'input/nh/','NH0500.NHF':'input/nh/'}

    datadict = {}
    #for i in ['VIX.GI','hz.iv.1m','nh.iv.6m','510300','NH0100.NHF','NH0200.NHF','NH0300.NHF','NH0500.NHF']:
    for i in pathdict.keys():
        if True:
            fpath = '/work/jzhu/' + pathdict[i] + i + '.csv'
            print(fpath)
            data = pd.read_csv(fpath)
            data.columns=[name.upper() for name in list(data.columns)]
            data.index = data['DATE'].apply(pd.to_datetime)
            datadict[i] = data['CLOSE']

    datadf = pd.DataFrame.from_dict(datadict,orient='columns')
    if  os.environ['ASSETTYPE'] == 'iv_sp_hz':
        datadf['ixew']= datadf['VIX.GI'] /  datadf['hz.iv.1m']
    elif  os.environ['ASSETTYPE'] == 'px_cyc_def':
        cycmean = datadf[['XLB.P','XLI.P','XLY.P','XLK.P','XLE.P','XLF.P']].mean(axis=1)
        defmean = datadf[['XLP.P','XLV.P','XLC.P','XLU.P']].mean(axis=1)
        datadf['ixew']= cycmean /  defmean
    elif  os.environ['ASSETTYPE'] == 'px_xlb_sp':
        datadf['ixew']= datadf['XLB.P'] /  datadf['SPY.P']
    elif  os.environ['ASSETTYPE'] == 'px_xlk_sp':
        datadf['ixew']= datadf['XLK.P'] /  datadf['SPY.P']
    elif  os.environ['ASSETTYPE'] == 'px_xle_sp':
        datadf['ixew']= datadf['XLE.P'] /  datadf['SPY.P']
    elif  os.environ['ASSETTYPE'] == 'px_xlf_sp':
        datadf['ixew']= datadf['XLF.P'] /  datadf['SPY.P']
    elif  os.environ['ASSETTYPE'] == 'px_gc_usd':
        datadf['ixew']= datadf['SPGSGC.TR'] /  datadf['USDX.FX']
    elif  os.environ['ASSETTYPE'] == 'iv_nh_hz':
        datadf['ixew']= datadf['nh.iv.6m'] /  datadf['hz.iv.1m']#.fillna(method='ffill')# ,limit=10)
    elif  os.environ['ASSETTYPE'] == 'px_nh_hz':
        datadf['ixew']= datadf['NH0100.NHF'] /  datadf['510300']
    elif  os.environ['ASSETTYPE'] == 'px_ag_nh':
        datadf['ixew']= datadf['NH0300.NHF'] /  datadf['NH0100.NHF']
    elif  os.environ['ASSETTYPE'] == 'px_in_nh':
        datadf['ixew']= datadf['NH0200.NHF'] /  datadf['NH0100.NHF']
    elif  os.environ['ASSETTYPE'] == 'px_ec_nh':
        datadf['ixew']= datadf['NH0500.NHF'] /  datadf['NH0100.NHF']
    elif  os.environ['ASSETTYPE'] == 'px_ag_ec':
        datadf['ixew']= datadf['NH0300.NHF'] /  datadf['NH0500.NHF']
    elif  os.environ['ASSETTYPE'] == 'px_in_ec':
        datadf['ixew']= datadf['NH0200.NHF'] /  datadf['NH0500.NHF']

    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.csv'
    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.csv'
    output_to_csv(opath,datadf,'ixew')


    

def cal_ixew():
    datadict = {}
    ipath = '/work/' + uname + '/data/pol/work/jzhu/input/'
    if  os.environ['ASSETTYPE'][0:2] == 'nh' :
        iv_list = ['al','au','c','cf','cu','i','l','m','ma','pp','rm','ru','sr','ta','v','zc','zn','sc','p','pg']
    elif  os.environ['ASSETTYPE'][0:2] == 'hz' :
        iv_list = [ '510050','510300']#,'000300' ]

    mymonth = os.environ['ASSETTYPE'][-2:]   
    for ticker in iv_list :
        fpath = ipath + 'iv/'
        if os.environ['ASSETTYPE'].split('.')[1] == 'sk':
            tmpdf = cal_skw('/work/jzhu/input/iv/',ticker,mymonth)
            datadict[ticker] = tmpdf
        elif os.environ['ASSETTYPE'].split('.')[1] == 'bfly':
            tmpdf = cal_skw('/work/jzhu/input/iv/',ticker,mymonth,cstype='bfly')
            datadict[ticker] = tmpdf
        elif os.environ['ASSETTYPE'].split('.')[1] == 'cs':
            curvespread, iv1mcls, iv6mcls = cal_cs('/work/jzhu/input/iv/',ticker)
            datadict[ticker] = curvespread 

        elif os.environ['ASSETTYPE'].split('.')[1] == 'corr':
            curvespread, iv1mcls, iv6mcls = cal_cs('/work/jzhu/input/iv/',ticker)
            cordf = iv1mcls.rolling(21).corr(iv6mcls)*100
            datadict[ticker] = cordf 

        elif os.environ['ASSETTYPE'].split('.')[1] == 'viv':
            #curvespread, iv1mcls, iv6mcls = cal_cs('/work/jzhu/input/iv/',ticker)
            curvespread, iv1mcls, iv6mcls = cal_cs('/work/jzhu/data/pol/work/jzhu/input/iv/',ticker,poflag=True)
            if mymonth == '1m':
                cordf = iv1mcls.rolling(21).std()
            elif mymonth == '6m':
                cordf = iv6mcls.rolling(21).std()
            datadict[ticker] = cordf 
    
            opath =  '/work/' + uname + '/output/ixew/viv.' + ticker  + '.' + mymonth +'.csv' 
            print(cordf)
            output_to_csv(opath,cordf,'')

        elif os.environ['ASSETTYPE'].split('.')[1] == 'iv':
            fpath = fpath +  ticker + '_iv_' + mymonth + '1000.PO.csv'
            print(fpath)
            data = pd.read_csv(fpath)###bond index
            data.columns=[name.upper() for name in list(data.columns)]
            data.index = data['DATE'].apply(pd.to_datetime)
            datadict[ticker] = data['CLOSE']
            print(ticker,data.iloc[-1,:])
        else:
            print('wrong ASSETTYPE')
            assert(0)

    print(datadict)
    datadf = pd.DataFrame.from_dict(datadict,orient='columns')
    datadf['ixew']= datadf.mean(axis=1) 
    opath =  '/work/' + uname + '/output/ixew/' + os.environ['ASSETTYPE']  + '.csv' 
    output_to_csv(opath,datadf,'ixew')

def get_path(ticker):
    if True: 
        ipath = '/work/' + uname + '/data/pol/work/jzhu/input/'
        if re.match(r'.*\.TR$',ticker):
            ipath += 'global/'
        elif re.match(r'.*\.NHF$',ticker) or  re.match(r'.*\.NM$',ticker) :
            ipath += 'nh/'
        elif re.match(r'.*\.GI$',ticker) or  re.match(r'.*\.P$',ticker) or  re.match(r'.*\.HI$',ticker) or  re.match(r'.*\.O$',ticker) or  re.match(r'.*\.FX$',ticker) or re.match(r'.*\.BAT$',ticker):
            ipath += 'idxetf/'
        elif re.match(r'.*iv.*\.PO$',ticker):
            ipath += 'iv/'
        elif re.match(r'.*\.SH$',ticker) or  re.match(r'.*\.SZ$',ticker):
            ipath += 'hz/'
        else:
            ipath = '/work/' + uname + '/data/pol/'
            ipath += 'Index/'#'shared/spgs/'
        fpath = ipath + ticker + '.csv'
    return(fpath)



def cal_std():
    tickers = get_tickers() 
    stddict = {}
    for ticker in tickers:
        fpath = get_path(ticker)
        print(fpath)

        data = pd.read_csv(fpath)###bond index

        data.columns=[name.upper() for name in list(data.columns)]
        #data['DATE'] = pd.to_datetime(data['DATE'], utc=True)
        data=data.set_index('DATE')
        if os.environ['ASSETTYPE'] in ['tf']:
            diffdf = (data).diff()
        else:
            diffdf = np.log(data).diff()

             
        stddf = diffdf.rolling(21).std() * np.sqrt(252)
    
        stddict[ticker] = stddf['CLOSE']
        if eval(os.environ['OUTPUTFLAG']):
            opath  = '/work/jzhu/output/cal/std_' + ticker  + '.csv'
            print('OUTPUT to:', opath)
            stddf.to_csv(opath)
    
    if eval(os.environ['OUTPUTFLAG']):
        df = pd.DataFrame(pd.DataFrame.from_dict(stddict,orient='columns').mean(axis=1))
        df.columns = ['OPEN']
        df["HIGH"] = df.iloc[:,0]
        df["LOW"]  = df.iloc[:,0]
        df["CLOSE"] = df.iloc[:,0]
        df["VOLUME"] = np.sign(df.iloc[:,0])*1e9
        df["ADJUSTED"] = df.iloc[:,0]
        df.index.rename('DATE',inplace=True)

        #df =df.set_index(0)
        opath  = '/work/jzhu/output/cal/std_' +  os.environ['ASSETTYPE']  + '_rv.csv'

        print('OUTPUT to:', opath)
        df.to_csv(opath)
    
    
def cal_brd():
    tickers = get_tickers() 

    agg_df = None#pd.DataFrame()

    for ticker in tickers:
        fpath = get_path(ticker)
        print(fpath)

        data = pd.read_csv(fpath)###bond index
        data.columns=[name.upper() for name in list(data.columns)]
        data['DATETIME'] = data['DATE'].apply(pd.to_datetime)
        
        data.index = data['DATE'].apply(pd.to_datetime)
        data['weekday'] = data['DATETIME'].apply(lambda x:x.weekday())+1
        data['day'] = data['DATETIME'].apply(lambda x:x.day)
        data['month'] = data['DATETIME'].apply(lambda x:x.month)
        data['year'] = data['DATETIME'].apply(lambda x:x.year)
        data = data[data['weekday'].isin([1,2,3,4,5])]
        data['chg'] = data['CLOSE']/data['CLOSE'].shift(1)-1.
        data['chg'].iat[0]=0.
        
        dayall = data[['year','month','chg']].groupby(['year','month']).std()
        dayall = dayall.reset_index()
        dayall['std_chg'] = dayall['chg']- dayall['chg'].shift(1)
        dayall['std_chg'].iat[0]=0.
        ##
        monthbegin = data.groupby(['year','month']).head(1)
        monthend = data.groupby(['year','month']).tail(1)
        
        
        monthall = monthend.copy()
        monthall['month_chg']=np.array(monthend['CLOSE'])/np.array(monthend['CLOSE'].shift(1))-1.
        monthall['month_chg'].iloc[0]= monthend['CLOSE'].iloc[0]/monthbegin['CLOSE'].iloc[0]-1.
        #
        monthall_1 = monthbegin.copy()
        monthall_1['month_chg_1']=np.array(monthbegin['CLOSE'].shift(-1))/np.array(monthbegin['CLOSE'])-1.
        monthall_1['month_chg_1'].iloc[-1]=monthend['CLOSE'].iloc[-1]/monthbegin['CLOSE'].iloc[-1]-1.
    
    
        output = np.round(monthall[['month','month_chg']].groupby(['month']).median(),2)
        output.columns=['median']
        output['prob'] = np.round(monthall[['month','month_chg']].groupby(['month']).apply(prob)['month_chg'],2)
        output['std']= np.round(monthall[['month','month_chg']].groupby(['month']).std(),2)
        
        ##std change probability
        output['std_prob'] = np.round(dayall[['month','std_chg']].groupby(['month']).apply(prob)['std_chg'],2)
    
        output['median_begin']=np.round(monthall_1[['month','month_chg_1']].groupby(['month']).median(),2)
        output['prob_begin'] = np.round(monthall_1[['month','month_chg_1']].groupby(['month']).apply(prob)['month_chg_1'],2)
        output['std_begin']= np.round(monthall_1[['month','month_chg_1']].groupby(['month']).std(),2)

        daystd = data['chg'].rolling(21).std()
        if agg_df is None:
            agg_df =  data['chg']/daystd
            magg_df =  monthall['month_chg'] 
        else:
            agg_df = pd.concat([agg_df, data['chg']/daystd ],axis=1)
            magg_df = pd.concat([magg_df,monthall['month_chg']],axis=1)
    agg_df.columns = tickers
    magg_df.columns = tickers
    if True:#eval(os.environ['OUTPUTFLAG']):

        opath  = '/work/jzhu/output/cal/brd_' +  os.environ['ASSETTYPE']  + '_agg.csv'
        print('output to:',opath)
        tmpdf = pd.DataFrame(np.sign(agg_df[np.abs(agg_df)>2]).sum(axis=1))
        odf = fake_data(tmpdf)
        odf.to_csv(opath)

        opath  = '/work/jzhu/output/cal/brd_' +  os.environ['ASSETTYPE']  + '_abs_agg.csv'
        print('output to:',opath)
        tmpdf = pd.DataFrame(np.abs(np.sign(agg_df[np.abs(agg_df)>2])).sum(axis=1))
        odf = fake_data(tmpdf)
        odf.to_csv(opath)


        opath  = '/work/jzhu/output/cal/brd_' +  os.environ['ASSETTYPE']  + '_magg.csv'
        print('output to:',opath)
        tmpdf = pd.DataFrame(np.sign(magg_df).sum(axis=1))
        odf = fake_data(tmpdf)
        odf.to_csv(opath)

import talib
def convert_to_w(df):
    print(df['date'])
    df['date'] = pd.to_datetime(df['date'])

    df['Week_Number'] = df['date'].dt.week
    df['Year'] = df['date'].dt.year

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
            ipath = '/work/jzhu/project/ql/data/'
        else:
            ipath += 'idxetf/'
        ifile = ipath + ticker + '.csv'
        print('input',ifile)
        df = pd.read_csv(ifile)#,index_col = 0,parse_dates=True)
        if os.environ['DERIVED'] in ['iv30','gex','dpi']  :
            df['open'] = df[  os.environ['DERIVED']  ] 
            df['high'] = df[  os.environ['DERIVED']  ] 
            df['low'] = df[  os.environ['DERIVED']  ] 
            df['close'] = df[  os.environ['DERIVED']  ] 


        if wflag:
            dw = convert_to_w(df)
            dw.dropna(inplace=True)
        else:
            dw = df

    return dw 

def cal_oldmg(wflag=True):
    tickers = get_tickers()
    for ticker in tickers:
        dw = get_csv_data(ticker,wflag)
        tp = 20 
        dw['BolU20'], dw['BolM20'], dw['BolL20'] = talib.BBANDS(
            np.double(dw['close'].values),
            timeperiod=tp,
            nbdevup=1,
            nbdevdn=1,
            matype=0)
        tp = 60 
        dw['BolU60'], dw['BolM60'], dw['BolL60'] = talib.BBANDS(
            np.double(dw['close'].values),
            timeperiod=tp,
            nbdevup=1,
            nbdevdn=1,
            matype=0)
        tp = 120 
        dw['BolU120'], dw['BolM120'], dw['BolL120'] = talib.BBANDS(
            np.double(dw['close'].values),
            timeperiod=tp,
            nbdevup=1,
            nbdevdn=1,
            matype=0)
        dw['mg'] =  dw['BolM20'] + dw['BolM60'] + dw['BolM120']
        print(dw['mg'])

        if eval(os.environ['OUTPUTFLAG']):
            opath = '/work/jzhu/output/cal/mg'+ '_' +ticker +'.csv'
            print('output to:', opath)
            dw.to_csv(opath)
        else:
            print(ticker,'\n')

def cal_cs(nhpath ,ticker,poflag=False):
    #for ticker in iv_list:
    if True:
        if poflag:
            np = nhpath +  ticker + '_iv_'+ '1m1000.PO.csv'
        else:
            np = nhpath +  ticker + '_iv_'+ '1m1000.csv'
        print(np)
        iv1m =pd.read_csv(np,encoding='gbk')
        iv1m['date'] = pd.to_datetime(iv1m['date'], utc=True)
        iv1m = iv1m.set_index('date')
        iv1mcls = iv1m['close']

        if poflag:
            np = nhpath + ticker + '_iv_'+ '6m1000.PO.csv'
        else:
            np = nhpath + ticker + '_iv_'+ '6m1000.csv'

        print(np)
        iv6m =pd.read_csv(np,encoding='gbk')
        iv6m['date'] = pd.to_datetime(iv6m['date'], utc=True)
        iv6m = iv6m.set_index('date')
        iv6mcls = iv6m['close']
        curvespread = (iv6mcls - iv1mcls)
        opath = '/work/jzhu/output/cal/cs_'+ticker +'.csv' 
        print('output to:', opath)
        curvespread.to_csv(opath)
        print('cal_cs',iv6mcls)
        #assert(0)
    return(curvespread, iv1mcls, iv6mcls)

def cal_skw(nhpath ,ticker,tenor,cstype='cs'):
    if True:
        if tenor == '1m':
            callstrike = '1100'
            putstrike = '900'
        else:
            callstrike = '1050'
            putstrike = '950'
        np = nhpath +  ticker + '_iv_'+ tenor + callstrike + '.csv'
        print(np)
        iv1m =pd.read_csv(np,encoding='gbk')
        iv1m['date'] = pd.to_datetime(iv1m['date'], utc=True)
        iv1m = iv1m.set_index('date')
        iv1mcls = iv1m['close']

        np = nhpath + ticker + '_iv_'+ tenor + putstrike + '.csv'
        print(np)
        iv6m =pd.read_csv(np,encoding='gbk')
        iv6m['date'] = pd.to_datetime(iv6m['date'], utc=True)
        iv6m = iv6m.set_index('date')
        iv6mcls = iv6m['close']

        np = nhpath + ticker + '_iv_'+ tenor +  '1000.csv'
        print(np)
        ivatm =pd.read_csv(np,encoding='gbk')
        ivatm['date'] = pd.to_datetime(ivatm['date'], utc=True)
        ivatm= ivatm.set_index('date')
        curvespread = (iv6mcls - iv1mcls)
        if  cstype == 'bfly':
            curvespread = (iv6mcls + iv1mcls - ivatm['close'] )
        opath = '/work/jzhu/output/cal/skw_'+ticker +'.csv' 
        print('output to:', opath)
        curvespread.to_csv(opath)
    return(curvespread)

def cal_mas(clspx):
    ma5   = clspx.rolling(5).mean()
    ma10  = clspx.rolling(10).mean()
    ma20  = clspx.rolling(20).mean()
    ma50  = clspx.rolling(40).mean()
    ma60  = clspx.rolling(60).mean()
    ma120 = clspx.rolling(120).mean()
    ma200 = clspx.rolling(200).mean()
    return(ma5,  ma10, ma20,  ma50, ma60, ma120, ma200)


def cal_mg(ma5,  ma10, ma20,  ma40, ma60, ma120, ma200):
    mg1 =  ma5 - ma10
    mg2 =  ma5 - ma20
    mg3 =  ma5 - ma40
    mg4 =  ma5 - ma60
    mg5 =  ma5 - ma120
    mg6 =  ma5 - ma200

    mg7 =  ma10 - ma20
    mg8 =  ma10 - ma40
    mg9 =  ma10 - ma60
    mg10=  ma10 - ma120
    mg11=  ma10 - ma200

    mg12=  ma20 - ma40
    mg13=  ma20 - ma60
    mg14=  ma20 - ma120
    mg15=  ma20 - ma200

    mg16=  ma40 - ma60
    mg17=  ma40 - ma120
    mg18=  ma40 - ma200

    mg19=  ma60 - ma120
    mg20=  ma60 - ma200

    mg21=  ma120 - ma200

    n = 2
    mgsum = 0
    for i in range(1,22):
        tmpstr = 'mg' + str(i)
        #print(tmpstr,eval(tmpstr))
        mgsum += eval(tmpstr)**n
    mgavg= np.sqrt(mgsum)
    return(mgavg)

def cal_vg(NHF,index):
    clspx = np.log(NHF['close']).diff()
    rv20 =  clspx.rolling(20).std()
    rv40 =  clspx.rolling(40).std()
    rv60 =  clspx.rolling(60).std()
    rv120 = clspx.rolling(120).std()
    rv200 = clspx.rolling(200).std()
    rvlgk = lgk(NHF)#clspx.rolling(10).std()#lgk(NHF)
    #print('rvlgk',rvlgk)
    vg1 = rv20 - rv40
    vg2 = rv20 - rv60
    vg3 = rv20 - rv120
    vg4 = rv20 - rv200
    vg5 = rv20 - rvlgk

    vg6 = rv40 - rv60
    vg7 = rv40 - rv120
    vg8 = rv40 - rv200
    vg9 = rv40 - rvlgk

    vg10= rv60 - rv120
    vg11= rv60 - rv200
    vg12= rv60 - rvlgk

    vg13= rv120 - rv200
    vg14= rv120 - rvlgk

    vg15= rv200 - rvlgk

    vgsum = 0
    for i in range(1,16) :
        tmpstr = 'vg' + str(i)
        #print(tmpstr,eval(tmpstr))
        vgsum += eval(tmpstr)**2
    vgavg= np.sqrt(vgsum)
    pcat = pd.concat([rv40, rv60, rv120, rv200, rv20,rvlgk], axis =1 )
    vgmin = pcat.max(axis=1)
    return(vgavg,vgmin)

    
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

    if verbose: print('running function:',func)
    eval(func)

if __name__ == '__main__':
   main() 
