import pandas as pd
from tqdm import tqdm

df = pd.read_parquet("etf.parquet")
df["increase_rate"] = df["close"]/df["close_1"]-1
df["increase_rate"] = df["increase_rate"]*100

from src.qtrade.mysql_util import *

with get_connection() as cursor:
    sql = """
    select code
    from etf.dim_etf_scale
    where scale>=10
    limit 100
    """
    cursor.execute(sql)
    codes = [ele[0] for ele in cursor.fetchall()]

all_result = []
for code in tqdm(codes):
    df1 = df[df.code==code]
    df1["increase_rate"].describe()
    score = [1-0.05*ele for ele in range(20, 0, -1)]
    res = df1["increase_rate"].quantile(score)
    df1.index = range(len(df1))
    
    profit_array = []
    ele = 0.9
    threshold = -res[ele]
    result = []
    for ele in range(len(df1)-1):
        increase_rate = df1.loc[ele, 'increase_rate']
        date = df1.loc[ele, 'date']
        if increase_rate<=threshold:
            next_increase_rate = df1.loc[ele+1, 'increase_rate']
            result.append([date, next_increase_rate])
    if result:
        profit = sum([ele[1] for ele in result])
        avg_profit = sum([ele[1] for ele in result])/len(result)
    else:
        profit = avg_profit = 0
    profit_array.append([threshold, avg_profit])
    best_parameter = sorted(profit_array, key=lambda x:x[1], reverse=True)[0]
    all_result.append([code, *best_parameter])

all_result = pd.DataFrame(all_result, columns=['code', 'threshold', 'avg_profit'])
all_result = all_result[all_result.avg_profit>=0.2]

