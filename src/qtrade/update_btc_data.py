import pandas as pd
from mysql_util import *
import pytz
import datetime

sql = """
select ts, c
from okex.btc_history_data t 
order by ts
"""

with get_connection() as cursor:
    cursor.execute(sql)
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['ts', 'c'])


def ts_to_local(ts):
    tz = pytz.timezone('Asia/Shanghai')
    t = datetime.datetime.fromtimestamp(int(ts / 1000), tz)
    return t


def get_profit(window=7):
    df["max10"] = df["c"].rolling(window=window).max()
    df["min10"] = df["c"].rolling(window=window).min()
    df["is_buy"] = df.apply(lambda x: x["max10"] == x["c"] or x["min10"] == x["c"], axis=1)
    df["date"] = df["ts"].map(ts_to_local).map(lambda x: x.strftime("%Y-%m-%d"))
    df.index = df.date

    max_len = len(df)
    close_array = df["c"].tolist()
    is_buy_array = df["is_buy"].tolist()
    date_array = df["date"].tolist()
    profit = 1
    all_profit = []
    last_status = 0

    for i in range(max_len):
        all_is_buy = is_buy_array[i]
        last_price = close_array[i - 1]
        current_price = close_array[i]
        profit = profit * (current_price / last_price) if last_status else profit
        last_status = 1 if all_is_buy else 0
        all_profit.append([date_array[i], profit, current_price / last_price - 1])
    insert_sql = """
    replace into okex.ads_btc_strategy_rpt
    values (%s, %s, %s)
    """
    insert_table_by_batch(insert_sql, all_profit)


get_profit()
