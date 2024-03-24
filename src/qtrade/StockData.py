import os
import re
from concurrent.futures import ThreadPoolExecutor

import akshare as ak
import easyquotation
import pandas as pd
import psutil
import requests
import retrying
import tqdm
import sys
import time
from pyspark.sql import SparkSession

data_path = 'data/stock_data/'
cpu_count = psutil.cpu_count()
thread_num = 10
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36"}


def get_spark():
    parallel_num = str(cpu_count * 3)
    spark = SparkSession.builder \
        .appName("chain merge") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", parallel_num) \
        .config("spark.default.parallelism", parallel_num) \
        .config("spark.ui.showConsoleProgress", True) \
        .config("spark.executor.memory", '1g') \
        .config("spark.driver.memory", '2g') \
        .config("spark.driver.maxResultSize", '2g') \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.executor.extraJavaOptions", "-Xss1024M") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


@retrying.retry(stop_max_attempt_number=10, stop_max_delay=10000)
def get_fund_scale(code="159819"):
    url = f"https://fund.eastmoney.com/{code}.html"
    resp = requests.get(url, headers=headers)
    resp.encoding = resp.apparent_encoding
    scale = re.findall("基金规模</a>：(.*?)亿元", resp.text)[0].strip()
    return code, float(scale)


def get_fund_scale2(code="159819"):
    try:
        return get_fund_scale(code)
    except:
        None


def get_all_fund_scale():
    fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
    codes = fund_etf_fund_daily_em_df["基金代码"].tolist()
    with ThreadPoolExecutor(thread_num) as executor:
        fund_scale = list(tqdm.tqdm(executor.map(get_fund_scale2, codes), total=len(codes)))
    fund_scale = [ele for ele in fund_scale if ele]
    fund_scale = pd.DataFrame(fund_scale, columns=['code', 'scale'])
    fund_scale_old = pd.read_csv("data/dim/scale.csv")
    fund_scale_old["code"] = fund_scale_old["code"].map(str)
    fund_scale_merge = pd.concat([fund_scale, fund_scale_old], axis=0)
    fund_scale_merge = fund_scale_merge.drop_duplicates(subset=["code"])
    fund_scale_merge = fund_scale_merge.sort_values(by=["scale", "code"], ascending=False)
    fund_scale_merge.to_csv("data/dim/scale.csv", index=False)


def get_gongmu_fund_basic_info():
    fund_name_em_df = ak.fund_name_em()
    fund_name_em_df.columns = ['code', 'pinyin', 'name', 'type', 'pinyingquancheng']
    fund_name_em_df.to_csv("data/dim/fund_name_em_df.csv", index=False)


def get_stock_basic_info():
    stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
    stock_zh_a_spot_em_df = stock_zh_a_spot_em_df[['代码', '名称']]
    stock_zh_a_spot_em_df.columns = ['code', 'name']
    stock_zh_a_spot_em_df.to_csv('data/dim/stock_zh_a_spot_em_df.csv', index=False)


def get_gongmu_history(thread_num):
    basic_info = pd.read_csv('data/dim/fund_name_em_df.csv', dtype={"code": object})
    codes = basic_info['code'].unique().tolist()

    def get_history_df(code):
        try:
            fund_open_fund_info_em_df = ak.fund_open_fund_info_em(fund=code, indicator='累计净值走势')
            fund_open_fund_info_em_df.columns = ['date', 'close']
            fund_open_fund_info_em_df['code'] = code
            print(code)
            fund_open_fund_info_em_df.to_csv(f"data/ods/fund/{code}.csv", index=False)
            time.sleep(0.5)
            return fund_open_fund_info_em_df
        except:
            return None

    with ThreadPoolExecutor(thread_num) as executor:
        dfs = list(tqdm.tqdm(executor.map(get_history_df, codes), total=len(codes)))


class StockData:
    def __init__(self):
        self.exchang_eft_basic_info_filename = "data/dim/exchang_eft_basic_info.csv"

    def save_exchang_fund_basic_info(self):
        fund_etf_fund_daily_em_df = ak.fund_etf_fund_daily_em()
        fund_etf_fund_daily_em_df = fund_etf_fund_daily_em_df.sort_values(by=["基金代码", "基金简称"])
        if len(fund_etf_fund_daily_em_df) > 700:
            fund_etf_fund_daily_em_df.to_csv(self.exchang_eft_basic_info_filename, index=False)

    def load_exchang_eft_basic_info(self):
        df = pd.read_csv(self.exchang_eft_basic_info_filename, dtype={"基金代码": object})
        return df

    def update_data(self):
        fund_etf_fund_daily_em_df = self.load_exchang_eft_basic_info()
        codes = fund_etf_fund_daily_em_df['基金代码'].unique().tolist()

        @retrying.retry(stop_max_attempt_number=5)
        def get_exchange_fund_data(code):
            try:
                df = ak.fund_etf_hist_em(symbol=code, period="daily", start_date="19900101",
                                         end_date="21000101", adjust="hfq")
                columns = ['日期', '开盘', '收盘', '最高', '最低', '成交量']
                df = df[columns]
                df.columns = ['date', 'open', 'close', 'high', 'low', 'volume']
                df['code'] = code
                time.sleep(0.5)
                return df
            except Exception as e:
                import traceback
                traceback.print_exc()
                return None

        with ThreadPoolExecutor(thread_num) as executor:
            dfs = list(tqdm.tqdm(executor.map(get_exchange_fund_data, codes), total=len(codes)))
            dfs = [ele for ele in dfs if ele is not None]
        dfs = pd.concat(dfs, axis=0)
        dfs = dfs.sort_values(by=['code', 'date'])
        dfs.to_csv("data/ods/fund.csv", index=False)

    def get_stock_data(self, code):
        df = pd.read_csv(os.path.join(data_path, f"{code}.csv"))
        return df

    def save_rt_data(self):
        df = pd.read_csv("data/ods/fund.csv", dtype={'code': object})
        quotation = easyquotation.use('sina')
        codes = df["code"].tolist()
        realtime_data = quotation.stocks(codes)
        realtime_df = []
        for code in realtime_data:
            real_stock = realtime_data[code]
            date = real_stock['date']
            open = real_stock["open"]
            close = real_stock['close']
            high = real_stock['high']
            low = real_stock['low']
            volume = real_stock['volume']
            now = real_stock['now']
            increase_rate = now / close
            realtime_df.append([date, open, close, high, low, volume, code, increase_rate])
        columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'code', 'increase_rate']
        realtime_df = pd.DataFrame(realtime_df, columns=columns)
        print("realtime_df", realtime_df.head())
        realtime_df.to_csv("data/ods/realtime_sina.csv", index=False)

    def get_rt_data(self):
        self.save_rt_data()
        df = pd.read_csv("data/ods/fund.csv", dtype={'code': object})
        realtime_df = pd.read_csv("data/ods/realtime_sina.csv", dtype={'code': object})
        tail_df = df.groupby("code").tail(1).copy(deep=True)
        realtime_date = realtime_df.iloc[0]['date']
        if realtime_date not in set(tail_df['date'].unique().tolist()):
            tail_df["date"] = realtime_date
            realtime_df = realtime_df[['code', 'date', 'increase_rate']]
            tail_df = tail_df.merge(realtime_df, on=['code', 'date'], how='left')
            tail_df["close"] = tail_df["close"] * tail_df["increase_rate"]
            tail_df = tail_df[['date', 'open', 'close', 'high', 'low', 'volume', 'code']]
            df = pd.concat([df, tail_df], axis=0)
        df = df.sort_values(by=['code', 'date'])
        tail_df = df.groupby("code").tail(1).copy(deep=True)
        tail_df.to_csv("data/ads/exchang_fund_rt_latest.csv", index=False)
        df.to_csv("data/ads/exchang_fund_rt.csv", index=False)

    def get_market_data(self, thread_num):
        stocks = ak.stock_zh_a_spot_em()
        stock_codes = stocks["代码"]

        def get_market_df(code):
            stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="hfq")
            if len(stock_zh_a_hist_df) > 0:
                stock_zh_a_hist_df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'turnover', 'increase',
                                              'increase_rate', 'increase_amount', 'exchange_rate']
                stock_zh_a_hist_df['code'] = code
            else:
                stock_zh_a_hist_df = None
            time.sleep(0.5)
            return stock_zh_a_hist_df

        with ThreadPoolExecutor(thread_num) as pool:
            dfs = list(tqdm.tqdm(pool.map(get_market_df, stock_codes), total=len(stock_codes)))
            dfs = [ele for ele in dfs if ele is not None]
        market_df = pd.concat(dfs, axis=0)
        market_df = market_df.sort_values(by=['code', 'date'])
        dfs = list(market_df.groupby("code", as_index=False))
        for key, ele in dfs:
            ele.to_csv(f"data/ods/market_df/{key}.csv", index=False)


def get_market_increase_decrease_cnt_rt():
    stock_cnt = pd.read_csv("data/ads/stock_cnt.csv")
    quotation = easyquotation.use('sina')
    real_stocks = quotation.market_snapshot(prefix=True)
    realtime_date = real_stocks['sz000001']['date']
    if realtime_date not in set(stock_cnt.date.unique().tolist()):
        increase_cnt = decrease_cnt = 0
        for code in real_stocks:
            real_stock = real_stocks[code]
            close = real_stock['close']
            now = real_stock['now']
            rate = 0 if close == 0.0 else now / close
            if rate < 1:
                decrease_cnt += 1
            else:
                increase_cnt += 1
        stock_cnt.loc[len(stock_cnt)] = (realtime_date, increase_cnt, decrease_cnt, increase_cnt / decrease_cnt)
    stock_cnt.to_csv("data/ads/stock_cnt_rt.csv", index=False)


def get_market_increase_decrease_cnt():
    spark = get_spark()
    stock_df = spark.read.csv("data/ods/market_df", header=True, inferSchema=True)
    stock_df.createOrReplaceTempView("stock_df")
    sql = '''
    select date,
           count(if(increase_rate>0, code, null)) increase_cnt,
           count(if(increase_rate<=0, code, null)) decrease_cnt
    from stock_df
    group by date
    order by date
    '''
    stock_cnt = spark.sql(sql)
    stock_cnt = stock_cnt.toPandas()
    stock_cnt['date'] = stock_cnt['date'].map(lambda x: x.strftime("%Y-%m-%d"))
    stock_cnt["increase_rate"] = stock_cnt["increase_cnt"] / stock_cnt["decrease_cnt"]
    stock_cnt.to_csv("data/ads/stock_cnt.csv", index=False)
    spark.stop()


def run_every_day():
    s = StockData()
    s.save_exchang_fund_basic_info()  # cost 0.7 seconds
    s.update_data()  # cost 18 seconds
    get_all_fund_scale()  # cost 11.9 seconds
    # s.get_market_data(2)  # cost 291 seconds
    # get_market_increase_decrease_cnt()  # 20 seconds


def run_every_day2():
    get_gongmu_fund_basic_info()
    get_gongmu_history(2)
    get_stock_basic_info()


def run_every_day3():
    s = StockData()
    s.get_market_data(2)  # cost 291 seconds


def run_every_minute():
    s = StockData()
    s.get_rt_data()  # cost 20 seconds
    get_market_increase_decrease_cnt_rt()
    get_etf_strategy()


def run():
    start_time = time.time()
    run_function = sys.argv[1]
    if run_function == "run_every_day":
        pass
    elif run_function == "run_every_minute":
        run_every_minute()
    elif run_function == "run_every_day2":
        run_every_day2()
    elif run_function == "run_every_day3":
        run_every_day3()
    else:
        raise Exception("function not find!")
    print(f"run qdata cost: {time.time() - start_time} second")


def get_etf_strategy():
    etf_df = pd.read_csv('data/rsrs_etf.csv', dtype={"code": object})
    etf_df['slope_standard_last'] = etf_df.groupby('code')['slope_standard'].shift(1)
    best_params = pd.read_csv('data/best_params.csv', dtype={"code": object})
    best_params = best_params[best_params.high > best_params.low]
    best_params = best_params[(best_params.high - best_params.low) > 0.5]
    scale = pd.read_csv("data/dim/scale.csv", dtype={"code": object})
    etf_df = etf_df.merge(best_params, on=['code'])
    etf_df = etf_df.merge(scale, on=['code'])
    etf_df = etf_df[etf_df.scale >= 20]
    etf_df = etf_df.sort_values('scale', ascending=False)
    etf_df = etf_df[etf_df.date == etf_df.date.max()]

    def get_buy_signal(row):
        low, hight = row['low'], row['high']
        rsrs, rsrs_last = row['slope_standard'], row['slope_standard_last']
        if rsrs >= hight:
            return 'sell'
        elif rsrs_last <= low <= rsrs:
            return 'buy'
        else:
            return 'hold'

    etf_df['signal'] = etf_df.apply(get_buy_signal, axis=1)
    etf_df = etf_df[etf_df['signal'].isin(['buy', 'sell'])]
    etf_df = etf_df.groupby('code').tail(1)
    etf_df.to_csv('data/etf_strategy.csv', index=False)


if __name__ == '__main__':
    run()
    # get_gongmu_fund_basic_info()
    # get_gongmu_history()
    # get_stock_basic_info()
