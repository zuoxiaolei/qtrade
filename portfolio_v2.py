from src.qtrade.mysql_util import *
import numpy as np
import pandas as pd
import math

m_days = 25

codes = ['518880', # 黄金ETF
         '159941', #纳指
         '159915', #创业板
         '512890'  #上证180
         ]

with get_connection() as cursor:
    sql = f'''
    select code, date, close
    from etf.ods_etf_history
    where code in {tuple(codes)} and date>='2019-01-01'
    order by code, date
    '''
    cursor.execute(sql)
    data = cursor.fetchall()

df = pd.DataFrame(data, columns=['code','date','close'])

data_dict = {}
id2date_dict = {}
for ele in codes:
    sigle_df = df[df.code==ele]
    sigle_df.index = range(len(sigle_df))
    data_dict[ele] = sigle_df
def get_history_data(code, index, m_days):
    ele = data_dict[code]
    select_df = ele.iloc[(index+1-m_days):index+1]
    return select_df

def get_current_best_code():
    score_list = []

    for etf in codes:
        df = get_history_data(etf, 1307, 25)
        y = np.log(df.close)
        x = df['num'] = np.arange(y.size)
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
        score = annualized_returns * r_squared
        score_list.append(score)
    return codes[np.argmax(score_list)]


def get_rank(etf_pool):
    score_list = []
    for etf in etf_pool:
        df = get_history_data(etf, g.m_days, '1d', ['close'])
        y = df['log'] = np.log(df.close)
        x = df['num'] = np.arange(df.log.size)
        slope, intercept = np.polyfit(x, y, 1)
        annualized_returns = math.pow(math.exp(slope), 250) - 1
        r_squared = 1 - (sum((y - (slope * x + intercept))**2) / ((len(y) - 1) * np.var(y, ddof=1)))
        score = annualized_returns * r_squared
        score_list.append(score)
    df = pd.DataFrame(index=etf_pool, data={'score':score_list})
    df = df.sort_values(by='score', ascending=False)
    rank_list = list(df.index)
    return rank_list

