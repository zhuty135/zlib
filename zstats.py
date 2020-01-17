#!/usr/bin/python3

import pandas as pd
import numpy as np
from empyrical import sharpe_ratio, annual_return,max_drawdown, cum_returns, annual_volatility 
def get_max_dd(cumret):
    dd = (np.maximum.accumulate(cumret) - cumret)/np.maximum.accumulate(cumret)
    return np.max(dd)

def get_sharpe(returns, rf=0, days=252):
    volatility = returns.std()
    sharpe_ratio = (returns.mean() - rf) / volatility * np.sqrt(days)
    return sharpe_ratio

def result_stats(perf,verbose=False):
    prets = perf['returns']
    sr = sharpe_ratio(returns = prets)
    ann = annual_return( returns = prets, period = 'daily')
    avol = annual_volatility( returns = prets, period = 'daily')
    maxdd  = max_drawdown(prets)#perf['max_drawdown']       
    num_of_txns = perf['transactions'].size
    if verbose:
        print('sr',sr)
        print('ann',ann)
        print('avol',avol)
        print(maxdd,get_max_dd(perf['portfolio_value']))
        print('num_of_txns',num_of_txns)
    return sr, ann, avol,maxdd, num_of_txns 

def prob(x):
    return np.sum(x>0)/len(x)

def cal_prob():
    #return np.sum(x>0)/len(x)

    tickers = ['CFCUPA.PO','CFAUPA.PO','CFMAPA.PO','CFRUPA.PO','CFIPA.PO','CFAGPA.PO','CFNIPA.PO','CFYPA.PO',
              'CFPPPA.PO','CFPBPA.PO','CFSRPA.PO','CFTAPA.PO','CFMPA.PO','CFCPA.PO','CFRBPA.PO',
              'CFCFPA.PO','CFJDPA.PO','CFALPA.PO','CFZCPA.PO','CFZNPA.PO','CFPPA.PO','CFOIPA.PO',
              'CFLPA.PO','CFAPA.PO','CFVPA.PO','CFJPA.PO','CFJMPA.PO','CFFGPA.PO']

    for ticker in tickers:
        uname = 'jzhu' 
        data = pd.read_csv('/work/' + uname + '/data/pol/Index/'+ticker+'.csv')###bond index
        data.columns=[name.upper() for name in list(data.columns)]
        data['DATETIME'] = data['DATE'].apply(pd.to_datetime)
        
        data.index = data['DATE'].apply(pd.to_datetime)
        #data = data[data.index<=pd.to_datetime('2018/4/1')]
        data['CLOSE'].plot()
        ##calendar dates
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
    
    
        output = monthall[['month','month_chg']].groupby(['month']).median()
        output.columns=['median']
        output['prob'] = monthall[['month','month_chg']].groupby(['month']).apply(prob)['month_chg']
        output['std']= monthall[['month','month_chg']].groupby(['month']).std()
        
        ##std change probability
        output['std_prob'] = dayall[['month','std_chg']].groupby(['month']).apply(prob)['std_chg']
    
        output['median_begin']=monthall_1[['month','month_chg_1']].groupby(['month']).median()
        output['prob_begin'] = monthall_1[['month','month_chg_1']].groupby(['month']).apply(prob)['month_chg_1']
        output['std_begin']= monthall_1[['month','month_chg_1']].groupby(['month']).std()
        output.to_csv('/work/jzhu/output/cal/calendar_'+ticker +'.csv')

def main():
    cal_prob()

if __name__ == '__main__':
   main() 
