import pandas as pd
from tqdm import tqdm
import empyrical
import akshare as ak
from mysql_util import insert_table_by_batch, get_connection
import pytz

tz = pytz.timezone('Asia/Shanghai')
from datetime import timedelta, datetime


def update_board_data():
    now = datetime.now(tz).strftime("%Y%m%d")
    last_day = datetime.now(tz) + timedelta(days=-20)
    last_day = last_day.strftime("%Y%m%d")
    df_board_concept_name = ak.stock_board_concept_name_em()
    board_list = df_board_concept_name["板块名称"].unique().tolist()
    data = []
    for ele in tqdm(board_list):
        df = ak.stock_board_concept_hist_em(symbol=ele, start_date=last_day,
                                            end_date=now, adjust="")
        df['name'] = ele
        data.append(df)
    df = pd.concat(data, axis=0)
    df = df[['日期', 'name', '收盘']]

    sql = '''
    replace into stock.ods_board_history values (%s, %s, %s)
    '''
    insert_table_by_batch(sql, df.values.tolist(), batch_size=1000)


def calc_indicators(df_returns):
    accu_returns = empyrical.cum_returns_final(df_returns)
    annu_returns = empyrical.annual_return(df_returns)
    max_drawdown = empyrical.max_drawdown(df_returns)
    sharpe = empyrical.sharpe_ratio(df_returns)
    return accu_returns, annu_returns, max_drawdown, sharpe


def get_profit():
    sql = '''select * from stock.ods_board_history'''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['日期', 'name', '收盘'])
    df = df[~df['name'].isin(['昨日连板_含一字', '昨日连板', '昨日涨停', '昨日涨停_含一字'])]
    df['increase10'] = df['收盘'] / df.groupby('name')['收盘'].shift(5) - 1
    df = df[~df['increase10'].isna()]
    dates = df['日期'].unique().tolist()
    dates = sorted(dates)
    profits = []

    last_price = None
    last_stock = None
    df.index = range(len(df))

    for ele in tqdm(dates):
        df_today = df[df['日期'] == ele]
        max_index = df_today['increase10'].idxmax()
        name = df_today.loc[max_index, 'name']
        current_price = df_today.loc[max_index, '收盘']
        last_profit = 1 if not profits else profits[-1][-1]
        if name == last_stock:
            profits.append([ele, name, last_profit * (current_price / last_price)])
        else:
            profits.append([ele, name, last_profit])
        last_price = current_price
        last_stock = name

    sql = '''replace into stock.board_profit values (%s, %s, %s)'''
    insert_table_by_batch(sql, profits, batch_size=1000)
    # df2["profit2"] = df2["profit"] / df2["profit"].shift() - 1
    #
    # accu_returns, annu_returns, max_drawdown, sharpe = calc_indicators(df2["profit2"])
    # print(accu_returns, annu_returns, max_drawdown, sharpe)
    # df2["profit"].plot()
    # plt.show()
    # df2.to_csv(str(period) + ".csv", index=False)


if __name__ == '__main__':
    update_board_data()
    get_profit()
