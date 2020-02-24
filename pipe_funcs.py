from zipline.pipeline import Pipeline, CustomFactor
from zipline.pipeline.factors import AnnualizedVolatility, Returns, BollingerBands, SimpleMovingAverage, RSI
from math import sqrt
import numpy as np
import configparser
import os
import pwd
uname = pwd.getpwuid(os.getuid()).pw_name

u_sect = os.environ['MODEL_SELECT'] 
m_select = u_sect
MNAME =  u_sect# os.environ['MODEL_SELECT'] #eval(cp.get(sect,'M'))
cp = configparser.ConfigParser()
sect = 'base'
m_path = '/work/' + uname + '/project/gitrepos/slib/config/' + m_select + '.cfg'
print(m_path)
cp.read(m_path)
DAY_HACK_ENABLED = eval(cp.get(sect,'DAY_HACK_ENABLED'))
BB_WIN_LEN = eval(cp.get(sect,'BB_WIN_LEN'))
WIN_LEN = eval(cp.get(sect,'WIN_LEN'))
W_WIN_LEN = eval(cp.get(sect,'W_WIN_LEN'))
SHARPE =  cp.getboolean(sect,'sharpe')
LONG_ONLY =  eval(cp.get(u_sect,'long_only'))
NUM_TOP_POSITIONS = eval(cp.get(sect,'num_top_positions'))
TRIGGER_LEVEL =  cp.getfloat(sect,'trigger_level')
PIPE_ENABLED =  eval(cp.get(sect,'pipe_enabled'))
import os

class ComputeVol1M(CustomFactor):
    inputs = [Returns(window_length=2)]
    window_length = 21

    def compute(self, today, assets, out, returns):
        out[:] = np.nanstd(returns, axis=0) * np.sqrt(250)

class ComputeVol3M(CustomFactor):
    inputs = [Returns(window_length=2)]
    window_length = 63

    def compute(self, today, assets, out, returns):
        out[:] = np.nanstd(returns, axis=0) * np.sqrt(250)

class ComputeVol6M(CustomFactor):
    inputs = [Returns(window_length=2)]
    window_length = 126

    def compute(self, today, assets, out, returns):
        out[:] = np.nanstd(returns, axis=0) * np.sqrt(250)

class SidFactor(CustomFactor):
    inputs = []
    window_length = 1
    def compute(self, today, assets_id, out):
        out[:] = assets_id

class Cud(CustomFactor):
    window_length = 131
    dailyreturns = (Returns(window_length=2) )
    inputs = [dailyreturns]
    def compute (self, today, assets, out, dailyreturns) :
        cumrets = np.cumsum(np.log(dailyreturns[-131:-1,:]+1), axis =0)#
        weeklyrets = np.diff(cumrets,5,axis=0)[::5]
        total_count = weeklyrets.shape[0]
        up_count = np.sum((weeklyrets > 0), axis =0)
        if VERBOSE: print('check type:',weeklyrets[:,20])
        if VERBOSE: print('up/total', up_count[20], total_count)
        out[:] = up_count


def make_pipeline(underly_sid, shortable_sid):#,DAY_HACK_ENABLED=True,BB_WIN_LEN=126,WIN_LEN=20):
    sid = SidFactor() 
    pipeline_underly_sid = sid.eq(underly_sid[0])
    print("jzzz:", type(pipeline_underly_sid))
    print("MNAME:", MNAME)
    for i in underly_sid:
        pipeline_underly_sid = pipeline_underly_sid | sid.eq(i)
        print('jzzz:', pipeline_underly_sid)
    pipeline_shortable_sid = sid.eq(shortable_sid[0])
    for i in shortable_sid:
        pipeline_shortable_sid  = pipeline_shortable_sid  | sid.eq(i)
    #Filter for our choice of assets.
    bbands = BollingerBands(window_length=BB_WIN_LEN, k=sqrt(BB_WIN_LEN/5),mask = pipeline_underly_sid)
    lower, middle, upper = bbands
    #cud = Cud()#mask(pipeline underly_sid)
    returns_1D = Returns(window_length = 2, mask = pipeline_underly_sid)
    returns_6M = Returns(window_length = WIN_LEN, mask = pipeline_underly_sid)
    returns_6M = returns_6M if DAY_HACK_ENABLED else returns_6M - returns_1D
    returns_3M = Returns(window_length = 60, mask = pipeline_underly_sid)
    returns_3M = returns_3M if DAY_HACK_ENABLED else returns_3M - returns_1D
    returns_5M = Returns(window_length = WIN_LEN, mask = pipeline_underly_sid)#for es only
    returns_5M = returns_5M if DAY_HACK_ENABLED else returns_5M - returns_1D
    
    returns_1M = Returns(window_length = 20, mask = pipeline_underly_sid)#for es only
    returns_1M = returns_1M if DAY_HACK_ENABLED else returns_1M - returns_1D
    
    returns_5D = Returns(window_length =W_WIN_LEN,mask = pipeline_underly_sid)
    returns_5D = returns_5D if DAY_HACK_ENABLED else returns_5D - returns_1D
    returns_6D = Returns(window_length =7,mask = pipeline_underly_sid) - returns_1D
    annualizedVolatility_6M = AnnualizedVolatility(window_length = WIN_LEN,mask = pipeline_underly_sid)
    annualizedVolatility_3M = AnnualizedVolatility(window_length = int(WIN_LEN/2), mask = pipeline_underly_sid)
    annualizedVolatility_1M = AnnualizedVolatility(window_length = int(WIN_LEN/6), mask = pipeline_underly_sid)
    #annualizedVolatility_6M = ComputeVol6M(mask = pipeline_underly_sid)
    #annualizedVolatility_3M = ComputeVol3M(mask = pipeline_underly_sid)
    #annualizedVolatility_1M = ComputeVol1M(mask = pipeline_underly_sid)
    annualizedVolatility = max(annualizedVolatility_6M, annualizedVolatility_3M,annualizedVolatility_1M)
    sharpes_6M = returns_6M/annualizedVolatility_6M
    raw_signals = None
    if SHARPE:
        raw_signals = sharpes_6M
    else:
        raw_signals = returns_6M
    signals = raw_signals if LONG_ONLY else (raw_signals*raw_signals)
    #positive_return = (returns_6M > 0)
    #trigger = (returns 5D > -trigger_level) if LONG_ONLY else ((returns_5D < trigger_level)

    sigtop = signals.top(NUM_TOP_POSITIONS)
    raw_sig_top = raw_signals.top(NUM_TOP_POSITIONS)
    raw_sig_bot = raw_signals.bottom(NUM_TOP_POSITIONS)

    alongs = sigtop & raw_sig_top & (raw_signals > 0) & (returns_5D > -TRIGGER_LEVEL) # & (cud >= 13)
    ashorts = sigtop & raw_sig_bot &(raw_signals <= 0 ) & (returns_5D < TRIGGER_LEVEL) & pipeline_shortable_sid
    #alongs = (annualizedVolatility_6M> 0) #shorts# signals.top(NUN TOP POSITIONS)

    if not PIPE_ENABLED:
        alongs = (annualizedVolatility_6M > 0)
        ashorts = (annualizedVolatility_6M < 0)# if LONG ONLY else longs
    long_short_screen = alongs if LONG_ONLY else (alongs | ashorts)

    my_screen = (annualizedVolatility_6M > 0) if MNAME in ['es','bd','dm','ed','md']  else long_short_screen 
    #my_screen = long_short_screen if False else (annualizedVolatility_6M > 0)#hlw
    print('finger1',MNAME)
    rsi = RSI(window_length=10,mask = pipeline_underly_sid)
    print('rsipipe',annualizedVolatility_6M)
    print('fxxxDAY_HACK_ENABLED',DAY_HACK_ENABLED)
    #Only compute volatilities for assets that satisfy the requirements of our rolling strategy.
    return Pipeline(
        columns= {
            #'cud' : cud,
            'volatility' : annualizedVolatility,
            'sharpes_6W' : sharpes_6M,
            'drets': returns_1D,
            'wrets': returns_5D,
            'returns_6D': returns_6D,
            'returns_1M': returns_1M,
            'returns_3M': returns_3M,
            'returns_5M': returns_5M,
            'returns_6M': returns_6M,
            #'signals': (signals)*signals,
           'shorts' : ashorts,
            #'wrets' : returns_5D,
            'lower' : lower,
            'middle' :middle,
            'upper' :upper,
            #'rsi':rsi.top(3),
            'rsi_longs': rsi.top(3),
            'rsi_shorts': rsi.bottom(3),
            #'annualizedVolatility :annualizedVolatility_6M,
        },
        screen = my_screen
    )
