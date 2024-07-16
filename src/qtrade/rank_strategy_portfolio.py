import pandas as pd
from datetime import datetime, timezone, date
import empyrical
import math
import numpy as np
from mysql_util import *
from tqdm import tqdm
import json
import duckdb
from send_message import send_nasdaq_strategy

def get_etf_data():
    
    sql ="""
    select date, rate close 
    from ads_eft_portfolio_rpt t
    order by date
    """
    
    etf_data = []
    with get_connection() as cursor:
        cursor.execute(sql)
        etf_data = cursor.fetchall()
    etf_data = pd.DataFrame(etf_data, columns=['date', 'close'])
    return etf_data

def get_params(df):
    df = df.sort_values(by="date")
    df = df.drop_duplicates(subset=["date"])
    df["c"] = df.close.map(float)
    df["future_increase_rate1"] = (df.c.shift(-1)/df.c-1)*100
    
    df["date"] = df["date"].apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
    df["year"] = df["date"].map(lambda x: x.year)
    mean_rate = df["future_increase_rate1"].mean()
    
    def get_rank(series):
        data = series.values
        return np.argsort(-data)[-1]
    
    for i in range(2, 21):
        df[f"c_rank{i}"] = df.c.rolling(i).apply(get_rank)
    
    # get model parameter
    feature_score = {}
    for i in range(2, 21):
        tmp = df.groupby(f"c_rank{i}", as_index=False)["future_increase_rate1"].mean()
        tmp = tmp[tmp["future_increase_rate1"]>=mean_rate]
        feature_score[f"c_rank{i}"] = tmp[f"c_rank{i}"].tolist()
    model = feature_score
    print(model)
    keys = list(model.keys())
    # print(model)
    def get_rank_strategy(row):
        score = 0
        for ele in keys:
            if row[ele] in model[ele]:
                score += 1
        return score
    df["buy_sell_label"] = df.apply(get_rank_strategy, axis=1)
    return df, model

def back_test(is_buy_array, close_array, date_array):
    profit = 1
    last_status = 0
    profits = []
    
    for i in range(1, len(close_array)):
        is_buy = is_buy_array[i]
        last_price = close_array[i-1]
        current_price = close_array[i]
        profit = profit*(current_price/last_price) if last_status else profit
        profits.append([date_array[i], profit, is_buy])
        last_status = is_buy
        
    df_profits = pd.DataFrame(profits, columns=["date", "profit", "status"])
    df_profits.index = df_profits["date"]
    df_profits["increase_rate"] = df_profits["profit"] / df_profits["profit"].shift() - 1
    sharpe = empyrical.sharpe_ratio(df_profits["increase_rate"])
    max_drawdown = empyrical.max_drawdown(df_profits["increase_rate"])
    annual_return = math.pow(profit, 365/len(df_profits)) - 1
    return profit, sharpe, annual_return, max_drawdown, df_profits

def get_all_rank_params():
    etf_data = get_etf_data()
    result = []
    df = etf_data
    
    df, model = get_params(df)
    df_select = df[df.date==df.date.max()].copy(deep=True)
    df_select["date"] = df_select["date"].map(lambda x: datetime.strftime(x, "%Y-%m-%d"))
    close_array = df.c.values
    date_array = df.date.values
    
    i = 7
    result = []
    is_buy_array = df.apply(lambda x: x["buy_sell_label"]>=i, axis=1) - 0
    is_buy_array = is_buy_array.values
    profit, sharpe, annual_return, max_drawdown, df_profits = back_test(is_buy_array, close_array, date_array)
    df_profits = df_profits.reset_index(drop=True)
    result.append(("portfolio", profit, sharpe, annual_return, max_drawdown, json.dumps(model)))
    sql = """
    replace into etf.ads_etf_rank_stratgegy_params(code, profit, sharpe, annual_return, max_drawdown, model)
    values (%s, %s, %s, %s, %s, %s)
    """
    insert_table_by_batch(sql, result)

if __name__ == "__main__":
    get_all_rank_params()
    # send_nasdaq_strategy()
    