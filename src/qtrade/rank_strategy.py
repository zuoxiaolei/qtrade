import pandas as pd
from datetime import datetime, timezone, date
import empyrical
import math
import numpy as np
from mysql_util import *
from tqdm import tqdm
import json
import duckdb

def get_etf_data():
    
    sql = """
    select distinct t1.code as code
    from etf.dim_etf_scale t1
    join (
    select code, count(1) cnt
    from etf.ods_etf_history
    group by code
    having cnt>=1000
    )t2
    on t1.code=t2.code
    where scale>=10
    """
    with get_connection() as cursor:
        cursor.execute(sql)
        codes = cursor.fetchall()
        codes = [ele[0] for ele in codes]
    
    sql_history_data = f"""
    select code,date,close
    from etf.ods_etf_history t
    where code in {tuple(codes)}
    order by code, date
    """
    etf_data = []
    with get_connection() as cursor:
        cursor.execute(sql_history_data)
        etf_data = cursor.fetchall()
    etf_data = pd.DataFrame(etf_data, columns=['code', 'date', 'close'])
    return etf_data, codes

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
    return profit, sharpe, annual_return, max_drawdown

def get_all_rank_params():
    etf_data, codes = get_etf_data()
    result = []
    all_df = []
    for code in tqdm(codes):
        df = etf_data[etf_data.code==code]
        if len(df)>=1000:
            df, model = get_params(df)
            df_select = df.copy(deep=True)
            df_select["date"] = df_select["date"].map(lambda x: datetime.strftime(x, "%Y-%m-%d"))
            all_df.extend(df_select[["date", "code", "buy_sell_label"]].values.tolist())
            close_array = df.c.values
            date_array = df.date.values
            is_buy_array = df.apply(lambda x: x["buy_sell_label"]>=7, axis=1) - 0
            is_buy_array = is_buy_array.values
            profit, sharpe, annual_return, max_drawdown = back_test(is_buy_array, close_array, date_array)
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


def back_test222(is_buy_array, close_array, date_array, future_increase_rate1):
    profit = 1
    last_status = 0
    profits = []
    
    for i in range(1, len(close_array)-1):
        is_buy = is_buy_array[i]
        profit = profit*(1+future_increase_rate1[i]/100) if is_buy else profit
        profits.append([date_array[i], profit, is_buy])
        
    df_profits = pd.DataFrame(profits, columns=["date", "profit", "status"])
    df_profits.index = df_profits["date"]
    df_profits["increase_rate"] = df_profits["profit"] / df_profits["profit"].shift() - 1
    sharpe = empyrical.sharpe_ratio(df_profits["increase_rate"])
    max_drawdown = empyrical.max_drawdown(df_profits["increase_rate"])
    annual_return = math.pow(profit, 365/len(df_profits)) - 1
    return profit, sharpe, annual_return, max_drawdown


def back_test2():
    sql = '''
    select code
    from etf.ads_etf_rank_stratgegy_params t1
    where sharpe>=1
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        codes = cursor.fetchall()
        codes = [ele[0] for ele in codes]
        
    sql_history_data = f"""
    select code,date,close
    from etf.ods_etf_history t
    where code in {tuple(codes)}
    order by code, date
    """
    etf_data = []
    with get_connection() as cursor:
        cursor.execute(sql_history_data)
        etf_data = cursor.fetchall()
    etf_data = pd.DataFrame(etf_data, columns=['code', 'date', 'close'])
    df_all = []
    for code in tqdm(codes):
        df = etf_data[etf_data.code==code]
        df, model = get_params(df)
        df_all.append(df)

    df = pd.concat(df_all, axis=0)
    sql = """
    select code, date, c, buy_sell_label, future_increase_rate1
    from (
    select code, date, c, buy_sell_label, future_increase_rate1,
           row_number() over(partition by date order by buy_sell_label desc) rn
    from df
    )t
    where rn<=1
    order by date
    """
    df = duckdb.query(sql).to_df()
    
    # df.to_csv("temp.csv", index=False)
    df = df[df.date>="2019-01-01"]
    print(df.tail())
    close_array = df.c.values
    date_array = df.date.values
    is_buy_array = df.apply(lambda x: x["buy_sell_label"]>=7, axis=1) - 0
    is_buy_array = is_buy_array.values
    future_increase_rate1 = df["future_increase_rate1"].values
    profit, sharpe, annual_return, max_drawdown = back_test222(is_buy_array, close_array, date_array, future_increase_rate1)
    print(profit, sharpe, annual_return, max_drawdown)
    
if __name__ == "__main__":
    get_all_rank_params()
