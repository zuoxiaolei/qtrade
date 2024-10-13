from concurrent.futures import ThreadPoolExecutor
import tqdm
from get_etf_scale import get_all_fund_scale
import akshare as ak
import retrying
from mysql_util import get_connection, time_cost, get_max_date, insert_table_by_batch
import time
import pytz
import pandas as pd
import numpy as np

thread_num = 10
tz = pytz.timezone('Asia/Shanghai')


@time_cost
def update_etf_scale():
    scale_df = get_all_fund_scale()
    etf_scale_data = scale_df.values.tolist()
    print("update etf.dim_etf_scale")
    start_time = time.time()
    sql = '''
        replace into etf.dim_etf_scale(code, scale)
        values (%s, %s)
        '''
    insert_table_by_batch(sql, etf_scale_data)
    end_time = time.time()
    print(end_time - start_time)  # 0.12199997901916504


@time_cost
def update_etf_basic_info():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df.sort_values(by=[])
    fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df[['基金代码', '基金简称', '类型', '折价率']]
    print("update etf.dim_etf_basic_info")
    sql = f'''
        replace into etf.dim_etf_basic_info(code, name, type, discount_rate)
        values (%s, %s, %s, %s)
        '''
    insert_table_by_batch(sql, fund_etf_fund_daily_em_df.values.tolist())


@time_cost
def get_etf_codes():
    with get_connection() as cursor:
        sql = '''select distinct code from etf.dim_etf_basic_info'''
        cursor.execute(sql)
        res = cursor.fetchall()
        codes = [ele[0] for ele in res]
    return codes


@time_cost
def update_etf_history_data(full=False):
    # codes = get_etf_codes()
    codes = ['518880', '512890', '159941']
    start_date = get_max_date(n=1)
    start_date = start_date.replace('-', '')
    start_date = "19900101" if full else start_date

    @retrying.retry(stop_max_attempt_number=5)
    def get_exchange_fund_data(code):
        try:
            df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date=start_date,
                                     end_date="21000101", adjust="hfq")
            columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
            df = df[columns]
            df.columns = ['date', 'open', 'close', 'high', 'low', 'volume']
            df['code'] = code
            df = df[['code', 'date', 'open', 'close', 'high', 'low', 'volume']]
            sql = '''
                replace into etf.ods_etf_history(code, date, open, close, high, low, volume)
                values (%s, %s, %s, %s, %s, %s, %s)
                '''
            insert_table_by_batch(sql, df.values.tolist())
            time.sleep(0.5)
            del df
        except Exception as e:
            import traceback
            traceback.print_exc()

    with ThreadPoolExecutor(thread_num) as executor:
        list(tqdm.tqdm(executor.map(get_exchange_fund_data, codes), total=len(codes)))


@time_cost
def update_trade_date():
    with get_connection() as cursor:
        sql = '''
        replace into etf.dim_etf_trade_date
        select date,
            row_number() over (order by date desc) rn
        from (
        select distinct date
        from etf.ods_etf_history
        ) t
        order by date desc
        '''
        cursor.execute(sql)


def get_portfolio_report():
    weight_dict = {'518880': 0.4997765583588797,
                   '512890': 0.29836978509792955,
                   '159941': 0.20185365654319073}
    columns = ['518880', '512890', '159941']
    sql = '''
    select code, date, close, (close-close_lag1)/close_lag1*100 rate
    from (
    select code, date, close, lag(close, 1) over (partition by code order by date) close_lag1
    from (
    select *, count(1) over (partition by date) cnt
    from etf.ods_etf_history
    where code in ('518880', '512890', '159941')
    ) t 
    where cnt=3 and date>='2019-01-18'
    )t
    where close_lag1 is not null
    order by date
    '''
    with get_connection() as cursor:
        cursor.execute(sql)
        data = cursor.fetchall()
    df = pd.DataFrame(data, columns=['code', 'date', 'close', 'rate'])
    df = df.sort_values(by="date")

    df_list = [df.loc[df.code == ele, ["date", "rate"]] for ele in columns]
    pandas_df = df_list[0]
    pandas_df = pandas_df.reset_index(drop=True)
    for num, ele in enumerate(columns):
        pandas_df[ele] = df_list[num]["rate"].values

    pandas_df = pandas_df.sort_values(by='date')
    pandas_df.index = pandas_df['date']
    pandas_df = pandas_df.drop(['date'], axis=1)
    weight_list = [weight_dict[k] for k in columns]
    pandas_df = pandas_df[columns]
    pandas_df['rate'] = pandas_df.apply(lambda x: np.dot(weight_list, x.tolist()), axis=1)
    pandas_df['rate_cum'] = pandas_df['rate'] / 100 + 1
    pandas_df['rate_cum'] = pandas_df['rate_cum'].cumprod()
    pandas_df["year"] = pandas_df.index.map(lambda x: x[:4])
    pandas_df["month"] = pandas_df.index.map(lambda x: x[:7])
    df_year = pandas_df[['year', 'rate']].groupby("year", as_index=False)["rate"].sum()
    df_month = pandas_df[['month', 'rate']].groupby("month", as_index=False)["rate"].sum()

    data = list(zip(pandas_df.index.tolist(), pandas_df["rate_cum"].tolist(),
                    pandas_df["rate"].tolist()))
    sql = '''
    replace into etf.ads_eft_portfolio_rpt
    values (%s, %s, %s)
    '''
    insert_table_by_batch(sql, data)

    sql = '''
    replace into etf.ads_etf_portfolio_profit_summary
    values (%s, 'year', %s)
    '''
    insert_table_by_batch(sql, df_year.values.tolist())

    sql = '''
    replace into etf.ads_etf_portfolio_profit_summary
    values (%s, 'month', %s)
    '''
    insert_table_by_batch(sql, df_month.values.tolist())


def run_every_day():
    update_etf_history_data()
    update_trade_date()
    get_portfolio_report()


if __name__ == "__main__":
    run_every_day()
