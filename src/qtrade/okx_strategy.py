import pandas as pd
import requests
import time
from tqdm import tqdm
from datetime import datetime, timedelta
import pytz
from mysql_util import get_connection, insert_table_by_batch
import riskfolio as rp
import numpy as np

tz = pytz.timezone('Asia/Shanghai')

def exception_handler_decorator(function):
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as e:
            import traceback
            traceback.print_exc()
    return wrapper

class OuYiStragegy(object):

    def __init__(self, is_local=True) -> None:
        self.user_page = "https://www.okx.com/cn/copy-trading/account/{user_id}?tab=swap"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
            "Accept-Language": "zh-CN,zh;q=0.9"
        }
        self.proxies = {'http': 'http://localhost:7890', 'https': 'http://localhost:7890'} if is_local else {}

    @exception_handler_decorator
    def get_copy_trading_person(self, start, size=9):
        url = "https://www.okx.com/priapi/v5/ecotrade/public/follow-rank"
        now = datetime.now(tz)
        yestday = now - timedelta(days=1)
        yestday_str = yestday.strftime("%Y%m%d")
        data_version = yestday_str + "153000"
        params = {'size': size,
                  'type': 'followTotalPnl',
                  'start': start,
                  'fullState': 1,
                  'traderPeriod': 90,
                  'dataVersion': int(data_version),
                  'countryId': 'CN',
                  't': int(time.time() * 1000)
                  }
        res = requests.get(url, params=params, headers=self.headers, proxies=self.proxies)
        res = res.json()
        ranks = res['data'][0]['ranks']
        user_info = [(now.strftime("%Y-%m-%d"), ele['nickName'], ele['uniqueName'],
                      float(ele['followPnl']),
                      float(ele['historyFollowerNum']),
                      float(ele['pnl']),
                      float(ele['winRatio']),
                      float(ele['yieldRatio'])) for ele in ranks]
        sql = '''replace into okex.user_info_daily values (%s, %s, %s, %s, %s, %s, %s, %s)'''
        insert_table_by_batch(sql, user_info)

    def get_all_trade_person(self):
        total = 200
        for i in range(1, total):
            self.get_copy_trading_person(i)

    def get_detail(self, user_id):
        url = "https://www.okx.com/priapi/v5/ecotrade/public/position-history"
        params = {'size': 10,
                  'instType': 'SWAP',
                  'uniqueName': user_id,
                  't': int(time.time() * 1000)}
        res = requests.get(url, params=params,
                           headers=self.headers,
                           proxies=self.proxies)
        df = pd.DataFrame(res.json()['data'])
        try:
            df['lever'] = df['lever'].map(lambda x: float(x))
            lever = float(df['lever'].mean())
        except:
            lever = 1000

        sql = '''replace into okex.user_level values (%s, %s)'''
        insert_table_by_batch(sql, [(user_id, lever)])

    def get_all_detail(self):
        sql = '''
        select distinct unique_name
        from okex.user_info_daily t
        where date in (select max(date) from okex.user_info_daily)
        '''
        with get_connection() as cursor:
            cursor.execute(sql)
            unique_names = cursor.fetchall()
            unique_names = [ele[0] for ele in unique_names]

        sql = '''truncate table okex.user_profit_history'''
        with get_connection() as cursor:
            cursor.execute(sql)

        for ele in tqdm(unique_names):
            self.get_detail(ele)
            # time.sleep(1)

    def get_portfolio(self):
        sql = '''
        replace into okex.best_user_portfolio
        select date, t1.unique_name, nick_name,
                     CONCAT('https://www.okx.com/cn/copy-trading/account/', t1.unique_name, '?tab=swap') profile_url,
                     weight
        from (
        select max(date) date,
                    unique_name,
                    sum(if(rate - last_rate>0, 1, -1))/sum(1)*max(final_rate) weight	 
        from (
        select unique_name, date, rate,
                     first_value(rate) over (partition by unique_name order by date desc) final_rate,
                     lag(rate, 1) over (partition by unique_name order by date) last_rate
        from okex.user_profit_history
        )t 
        where last_rate is not null
        group by unique_name
        )t1
        join (
        select distinct unique_name, nick_name
                from okex.user_info_daily t
                where date in (select max(date) from okex.user_info_daily)
        )t2
        on t1.unique_name = t2.unique_name
        order by weight desc
        '''
        with get_connection() as cursor:
            cursor.execute(sql)


if __name__ == '__main__':
    # get_all_trade_person()
    # get_all_detail()
    oy = OuYiStragegy(is_local=False)
    oy.get_all_trade_person()
    oy.get_all_detail()
    # oy.get_portfolio()
