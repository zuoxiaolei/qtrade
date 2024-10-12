import pandas as pd
from datetime import datetime
import empyrical
import math
import numpy as np
from mysql_util import *
from tqdm import tqdm
import json

def get_etf_data():
    sql_history_data = """
    select code,date,close
    from etf.ods_etf_history t
    where code='512890'
    order by code, date
    """
    etf_data = []
    with get_connection() as cursor:
        cursor.execute(sql_history_data)
        etf_data = cursor.fetchall()
    etf_data = pd.DataFrame(etf_data, columns=['code', 'date', 'close'])
    return etf_data, ['512890']

max_period = 20

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
    
    # get model parameter
    feature_score = {}
    for i in range(2, 21):
        df[f"c_rank{i}"] = df.c.rolling(i).apply(get_rank)
        tmp = df.groupby(f"c_rank{i}", as_index=False)["future_increase_rate1"].mean()
        tmp = tmp[tmp["future_increase_rate1"]>=mean_rate]
        feature_score[f"c_rank{i}"] = tmp[f"c_rank{i}"].tolist()
    
    model = feature_score
    keys = list(model.keys())
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
    etf_data, codes = get_etf_data()
    result = []
    all_df = []
    for code in tqdm(codes):
        df = etf_data[etf_data.code==code]
        if len(df)>=1000:
            df, model = get_params(df)
            df_select = df
            df_select["date"] = df_select["date"].map(lambda x: datetime.strftime(x, "%Y-%m-%d"))
            all_df.extend(df_select[["date", "code", "buy_sell_label"]].values.tolist())
            close_array = df.c.values
            date_array = df.date.values
            is_buy_array = df.apply(lambda x: x["buy_sell_label"]>8, axis=1) - 0
            is_buy_array = is_buy_array.values
            profit, sharpe, annual_return, max_drawdown, df_profits = back_test(is_buy_array, close_array, date_array)
            result.append((code, profit, sharpe, annual_return, max_drawdown, json.dumps(model)))
    sql = """
    replace into etf.ads_etf_rank_stratgegy_params(code, profit, sharpe, annual_return, max_drawdown, model)
    values (%s, %s, %s, %s, %s, %s)
    """
    insert_table_by_batch(sql, result)

    sql2 = """
     replace into etf.ads_etf_rank_strategy_detail
     values (%s, %s, %s)
    """
    insert_table_by_batch(sql2, all_df)


if __name__ == "__main__":
    get_all_rank_params()
    