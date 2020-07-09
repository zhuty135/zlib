import numpy as np
from zipline.api import symbol
MB_HACK = 1
def get_chipin(a, w, context, m='mb',nd=18):
    ndz = nd/4
    ndz_flag =  context.bb_day_count_df.loc[a,'ndzero'] > ndz
    flag1  = context.bb_day_count_df.loc[a,'nd'] > nd
    chipinfactor =  min(nd, context.bb_day_count_df.loc[a,'ndzero'])/nd #was '16'
    if ndz_flag : print('mbflag=',a,'chipinfactor=',chipinfactor, context.bb_day_count_df.loc[a,'nd'] ,w)
    adj_w =  MB_HACK*chipinfactor if ndz_flag else (1 if w > 1 and flag1 else w)
    if adj_w < 0:
        assert(0)
    return adj_w


def bench_time(context,data,w,nd):

    if True:
        for a in context.tickers:
            w_new_vs_1w = int(np.sign(context.final_w[a]) * np.sign(w))
            print(w_new_vs_1w,context.final_w[a] ,w)
            if w_new_vs_1w >= MB_HACK*MB_HACK*4:
                print('nd2',context.bb_day_count_df.loc[(a),'nd'] )
                context.bb_day_count_df.loc[(a),'nd'] += 1 #if REBL_DAILY == 0 else 10
                print('nd2after',context.bb_day_count_df.loc[(a),'nd'] )

                context.bb_day_count_df.loc[(a),'ndzero'] =0#-= 1 if REBL_DAILY == 0 else 5

            elif w_new_vs_1w >= MB_HACK*MB_HACK:

                context.bb_day_count_df.loc[(a),'nd'] += 1 #if REBL_DAILY == 0 else 5
                context.bb_day_count_df.loc[(a),'ndzero'] =0# -= 1 if REBL_DAILY == 0 else 5

            else:
                context.bb_day_count_df.loc[(a),'nd'] = 0
                context.bb_day_count_df.loc[(a),'ndzero'] += 1
            adj_w = get_chipin(a,w, context,nd=nd)

    return adj_w

