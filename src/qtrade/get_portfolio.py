import pandas as pd
from mysql_util import get_connection
import riskfolio as rp
from pathlib import Path
import os
import datetime


def get_data():
    sql = '''
    select code, t1.date, close rate
    from etf.ods_etf_history t1
    join (select distinct date from etf.ads_eft_portfolio_rpt) t2
      on t1.date=t2.date
    where code in ('518880', '512890', '159941') 
              and t1.date>= '2019-01-21'
    order by code, date
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    df1 = pd.DataFrame(data, columns=['code', 'date', 'rate'])
    df2 = pd.read_csv("profit_stock.csv", dtype={'时间': object})
    df2["date"] = df2["时间"]
    df2["rate"] = df2["策略收益"] + 100
    df2["code"] = "stock5"
    df2 = df2[['code', 'date', 'rate']]
    df2['date'] = df2['date'].map(lambda x: x[:10])
    dates = list(set(df1['date'].tolist()).intersection(df2['date'].tolist()))

    df2 = df2[df2.date.isin(dates)]
    print(df2.head())
    df = pd.concat([df1, df2], axis=0)
    df = df.pivot(index="date", columns="code", values="rate")
    df.to_csv("temp.csv")
    return df


def get_data2():
    data_path = Path("data/okex")
    all_files = data_path.glob("*.csv")
    dfs = []

    for ele in all_files:
        stock_code = os.path.basename(ele).rstrip(".csv")
        df = pd.read_csv(ele)
        df['code'] = stock_code
        df['rate'] = df['ratio'] + 1
        df['date'] = df['statTime'].map(lambda x: datetime.datetime.fromtimestamp(int(x / 1000)))
        dfs.append(df[['code', 'date', 'rate']])
    df = pd.concat(dfs, axis=0)
    df = df.pivot(index="date", columns="code", values="rate")
    df.to_csv("temp.csv")
    return df

import numpy as np
data = get_data2()
data = data.pct_change()
data = data.replace([np.inf, -np.inf], np.nan)
Y = data.dropna()
Y.to_csv("temp2.csv", index=True)
# Building the portfolio object
port = rp.Portfolio(returns=Y)

# Calculating optimal portfolio

# Select method and estimate input parameters:

method_mu = 'hist'  # Method to estimate expected returns based on historical data.
method_cov = 'hist'  # Method to estimate covariance matrix based on historical data.

port.assets_stats(method_mu=method_mu, method_cov=method_cov, d=0.94)

# Estimate optimal portfolio:

model = 'Classic'  # Could be Classic (historical), BL (Black Litterman) or FM (Factor Model)
rm = 'MV'  # Risk measure used, this time will be variance
obj = 'Sharpe'  # Objective function, could be MinRisk, MaxRet, Utility or Sharpe
hist = True  # Use historical scenarios for risk measures that depend on scenarios
rf = 0  # Risk free rate
l = 0  # Risk aversion factor, only useful when obj is 'Utility'

w = port.optimization(model=model, rm=rm, obj=obj, rf=rf, l=l, hist=hist)

w = w.sort_values(by="weights", ascending=False)
print(w.head(10))

# weights
# 159941  0.156622
# 512890  0.001609
# 518880  0.365059
# stock5  0.476710
